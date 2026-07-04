import "dotenv/config";
import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "../generated/prisma/client.js";

const PAGE_SIZE = 50;
const REQUEST_DELAY_MS = Number(process.env.REQUEST_DELAY_MS ?? 300);
const MAX_RETRIES = Number(process.env.MAX_RETRIES ?? 5);

const API_BASE =
  "https://infra.zynerd.com/api_v2/closing_ranks/allotments_table_data";

const ROUND_FIELDS = [
  "cr2025Round1",
  "cr2025Round2",
  "cr2025Round3",
  "cr2025Round4",
  "cr2025Round5",
  "cr2025Round6",
];

const adapter = new PrismaPg({
  connectionString: process.env.DATABASE_URL,
});

const prisma = new PrismaClient({ adapter });

function getAuthToken() {
  const token = process.env.ZYNERD_AUTH_TOKEN;
  if (!token) {
    throw new Error("ZYNERD_AUTH_TOKEN is missing. Add it to your .env file.");
  }
  return token;
}

function extractRecords(response) {
  if (Array.isArray(response?.data?.records)) return response.data.records;
  if (Array.isArray(response?.records)) return response.records;
  if (Array.isArray(response?.data)) return response.data;
  if (Array.isArray(response)) return response;
  return [];
}

function getParamsFromSourceRecord(sourceRecord) {
  const institute = sourceRecord.institute;
  const course = sourceRecord.course;
  const quota = sourceRecord.quota;

  if (
    !institute ||
    typeof institute !== "object" ||
    !course ||
    typeof course !== "object" ||
    !quota ||
    typeof quota !== "object" ||
    !institute.id ||
    !course.id ||
    !quota.id ||
    !sourceRecord.category
  ) {
    return null;
  }

  return {
    instituteId: institute.id,
    courseId: course.id,
    quotaId: quota.id,
    category: sourceRecord.category,
  };
}

function getRoundsFromRecord(sourceRecord) {
  const rounds = [];

  for (const field of ROUND_FIELDS) {
    const roundData = sourceRecord[field];
    if (!roundData || typeof roundData !== "object") continue;

    const round = roundData.round ?? null;
    const session = roundData.session ?? "2025";
    if (!round) continue;

    rounds.push({
      round: Number(round),
      session: String(session),
    });
  }

  return rounds;
}

function mapRecordToDb(record, neetCounsellingDataId) {
  const counselling = record.counselling ?? {};

  return {
    id: record.id,
    neetCounsellingDataId,
    counsellingId: counselling.id,
    session: record.session ?? null,
    round: record.round != null ? String(record.round) : null,
    rank: record.rank ?? null,
    aiRank: record.ai_rank ?? null,
    counsellingRank: record.counselling_rank ?? null,
    state: record.state ?? null,
    category: record.category ?? null,
    institute: record.institute ?? null,
    course: record.course ?? null,
    quota: record.quota ?? null,
    inserviceCandidate: record.inservice_candidate ?? null,
    candidateFlag: record.candidate_flag ?? null,
    counsellingMeta: {
      id: counselling.id,
      name:
        counselling.name ??
        counselling.short_name ??
        `Counselling ${counselling.id}`,
    },
  };
}

async function ensureCounselling(counselling) {
  if (!counselling?.id) {
    throw new Error("Record is missing counselling.id");
  }

  await prisma.counselling.upsert({
    where: { id: counselling.id },
    update: { name: counselling.name },
    create: {
      id: counselling.id,
      name: counselling.name,
    },
  });
}

async function saveAllotment(record, neetCounsellingDataId) {
  const mapped = mapRecordToDb(record, neetCounsellingDataId);
  await ensureCounselling(mapped.counsellingMeta);

  const { counsellingMeta, ...data } = mapped;

  await prisma.allotmentsTableData.upsert({
    where: { id: data.id },
    update: data,
    create: data,
  });
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchAllotmentsPage(counsellingId, offset, params, authToken) {
  const url = `${API_BASE}?offset=${offset}&counselling_id=${counsellingId}`;

  for (let attempt = 1; attempt <= MAX_RETRIES; attempt++) {
    const response = await fetch(url, {
      method: "POST",
      headers: {
        accept: "application/json, text/plain, */*",
        "app-type": "web",
        authorization: authToken,
        "content-type": "application/json",
        origin: "https://www.zynerd.com",
        referer: "https://www.zynerd.com/",
      },
      body: JSON.stringify({
        institute_id: params.instituteId,
        course_id: params.courseId,
        quota_id: params.quotaId,
        category: params.category,
        round: params.round,
        session: params.session,
      }),
    });

    if (response.ok) {
      if (REQUEST_DELAY_MS > 0) await sleep(REQUEST_DELAY_MS);
      return response.json();
    }

    const body = await response.text();
    const isRetryable = [429, 502, 503].includes(response.status);

    if (isRetryable && attempt < MAX_RETRIES) {
      const waitMs = REQUEST_DELAY_MS * 2 ** attempt * 10;
      console.warn(
        `  rate limited (${response.status}), retry ${attempt}/${MAX_RETRIES} in ${waitMs}ms...`,
      );
      await sleep(waitMs);
      continue;
    }

    throw new Error(`API failed (${response.status}): ${body.slice(0, 300)}`);
  }
}

async function syncAllotmentsForCombo(
  counsellingId,
  sourceRecord,
  params,
  roundInfo,
  authToken,
) {
  const requestParams = {
    ...params,
    round: roundInfo.round,
    session: roundInfo.session,
  };

  let offset = 0;
  let totalSaved = 0;

  while (true) {
    const payload = await fetchAllotmentsPage(
      counsellingId,
      offset,
      requestParams,
      authToken,
    );
    const records = extractRecords(payload);

    if (records.length === 0) break;

    for (const record of records) {
      await saveAllotment(record, sourceRecord.id);
      totalSaved += 1;
    }

    const total = payload?.data?.total;
    if (typeof total === "number" && offset + records.length >= total) break;
    if (records.length < PAGE_SIZE) break;

    offset += PAGE_SIZE;
  }

  return totalSaved;
}

async function markRecordProceed(sourceRecordId, proceededRounds, isProceed) {
  await prisma.neetCounsellingData.update({
    where: { id: sourceRecordId },
    data: { proceededRounds, isProceed },
  });
}

/**
 * Process ALL rounds (up to 6) for one neet_counselling_data record.
 * Marks is_proceed=true only when every round for that record is done.
 * Cron waits 1 minute before picking the next record.
 */
export async function processNextAllotmentTick(counsellingId) {
  const authToken = getAuthToken();

  const sourceRecord = await prisma.neetCounsellingData.findFirst({
    where: { counsellingId, isProceed: false },
    orderBy: { id: "asc" },
  });

  if (!sourceRecord) {
    const pending = await prisma.neetCounsellingData.count({
      where: { counsellingId, isProceed: false },
    });
    return {
      done: true,
      counsellingId,
      pending,
      saved: 0,
      message: `All neet_counselling_data records processed for counselling_id=${counsellingId}.`,
    };
  }

  const params = getParamsFromSourceRecord(sourceRecord);
  const rounds = getRoundsFromRecord(sourceRecord);

  if (!params || rounds.length === 0) {
    await markRecordProceed(sourceRecord.id, 0, true);
    const pending = await prisma.neetCounsellingData.count({
      where: { counsellingId, isProceed: false },
    });
    return {
      done: false,
      skipped: true,
      neetDataId: sourceRecord.id,
      saved: 0,
      pending,
      message: `Skipped neet_data_id=${sourceRecord.id} (missing params or rounds), marked is_proceed=true.`,
    };
  }

  if (sourceRecord.proceededRounds >= rounds.length) {
    await markRecordProceed(sourceRecord.id, rounds.length, true);
    const pending = await prisma.neetCounsellingData.count({
      where: { counsellingId, isProceed: false },
    });
    return {
      done: false,
      neetDataId: sourceRecord.id,
      saved: 0,
      isProceed: true,
      pending,
      message: `neet_data_id=${sourceRecord.id} already complete, marked is_proceed=true.`,
    };
  }

  let totalSaved = 0;
  let proceededRounds = sourceRecord.proceededRounds;

  // console.log(
  //   `Processing neet_data_id=${sourceRecord.id} — ${rounds.length} round(s) total`,
  // );
  // console.log(
  //   `  institute=${params.instituteId} course=${params.courseId} quota=${params.quotaId} category=${params.category}`,
  // );

  for (let i = proceededRounds; i < rounds.length; i++) {
    const roundInfo = rounds[i];
    // console.log(
    //   `  Round ${i + 1}/${rounds.length} (API round=${roundInfo.round}, session=${roundInfo.session})`,
    // );

    const saved = await syncAllotmentsForCombo(
      counsellingId,
      sourceRecord,
      params,
      roundInfo,
      authToken,
    );
    totalSaved += saved;
    proceededRounds = i + 1;

    await markRecordProceed(sourceRecord.id, proceededRounds, false);
    console.log(`    saved ${saved} allotments`);
  }

  await markRecordProceed(sourceRecord.id, rounds.length, true);

  const pending = await prisma.neetCounsellingData.count({
    where: { counsellingId, isProceed: false },
  });

  return {
    done: pending === 0,
    counsellingId,
    neetDataId: sourceRecord.id,
    roundsCompleted: rounds.length,
    saved: totalSaved,
    isProceed: true,
    pending,
    message: `Completed all ${rounds.length} round(s) for neet_data_id=${sourceRecord.id} (counselling_id=${counsellingId}), is_proceed=true. Saved ${totalSaved} allotments.`,
  };
}

/** First counselling (asc id) from Counselling table that still has pending allotments. */
export async function getActiveCounselling() {
  const counsellings = await prisma.counselling.findMany({
    orderBy: { id: "asc" },
  });

  for (const counselling of counsellings) {
    const total = await prisma.neetCounsellingData.count({
      where: { counsellingId: counselling.id },
    });

    if (total === 0) continue;

    const pending = await prisma.neetCounsellingData.count({
      where: { counsellingId: counselling.id, isProceed: false },
    });

    if (pending > 0) {
      return { ...counselling, pending, total };
    }
  }

  return null;
}

/**
 * Process up to `recordsPerTick` neet_counselling_data rows for the active counselling.
 * When a counselling is fully done, the next cron tick moves to the next counselling id (asc).
 */
export async function processAllotmentBatch(
  recordsPerTick = Number(process.env.ALLOTMENT_RECORDS_PER_TICK ?? 4),
) {
  const active = await getActiveCounselling();

  if (!active) {
    return {
      done: true,
      saved: 0,
      recordsProcessed: 0,
      message: "All counsellings fully processed.",
    };
  }

  console.log(
    `Active counselling_id=${active.id} — ${active.name} (${active.pending} pending / ${active.total} total)`,
  );

  let totalSaved = 0;
  let recordsProcessed = 0;
  const results = [];

  for (let i = 0; i < recordsPerTick; i++) {
    const result = await processNextAllotmentTick(active.id);
    results.push(result);
    recordsProcessed += 1;
    totalSaved += result.saved ?? 0;

    if (result.done) {
      const next = await getActiveCounselling();
      return {
        done: !next,
        counsellingId: active.id,
        counsellingName: active.name,
        recordsProcessed,
        saved: totalSaved,
        nextCounsellingId: next?.id ?? null,
        nextCounsellingName: next?.name ?? null,
        pending: next?.pending ?? 0,
        message: next
          ? `Finished counselling_id=${active.id}. Next tick will use counselling_id=${next.id} (${next.name}).`
          : `Finished counselling_id=${active.id}. All counsellings complete.`,
        results,
      };
    }
  }

  const stillPending = await prisma.neetCounsellingData.count({
    where: { counsellingId: active.id, isProceed: false },
  });
  console.log(`stillPending: ${stillPending}`);
  console.log('counsellingId: ${active.id}');

  return {
    done: false,
    counsellingId: active.id,
    counsellingName: active.name,
    recordsProcessed,
    saved: totalSaved,
    pending: stillPending,
    message: `Processed ${recordsProcessed} record(s) for counselling_id=${active.id}. ${stillPending} still pending.`,
    results,
  };
}

export async function syncAllotmentsData(counsellingId) {
  const result = await processNextAllotmentTick(counsellingId);
  return result.saved ?? 0;
}

async function main() {
  const result = await processAllotmentBatch();
  console.log(result.message);
  if (result.saved > 0) console.log(`Saved ${result.saved} allotment records.`);
  if (result.pending != null) console.log(`Pending records: ${result.pending}`);
  if (result.nextCounsellingId) {
    console.log(
      `Next counselling: id=${result.nextCounsellingId} (${result.nextCounsellingName})`,
    );
  }
}

const isDirectRun = process.argv[1]?.includes("sync-allotments-data");

if (isDirectRun) {
  main()
    .catch((error) => {
      console.error("Sync failed:", error);
      process.exit(1);
    })
    .finally(async () => {
      await prisma.$disconnect();
    });
}
