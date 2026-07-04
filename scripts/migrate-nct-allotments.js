import "dotenv/config";
import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaClient } from "../generated/prisma/client.js";

/**
 * Migrates rows from `allotments_table_data` (this server) into the
 * `nct_allotment` table on the api.esaral server.
 *
 * Flow:
 *   1. Read AllotmentsTableData rows with is_proceed = false (cursor paginated by id).
 *   2. POST each batch to the api.esaral migration endpoint, which resolves the
 *      foreign keys (counselling / category / institute / quota) and inserts.
 *   3. Mark rows the API confirmed as inserted with is_proceed = true.
 *      Rows the API skipped (unmatched FKs) stay is_proceed = false so they can
 *      be retried after the master data is fixed.
 */

const BATCH_SIZE = 1000;
const REQUEST_DELAY_MS = 500;

const API_URL = "https://api.esaral.com/admin/v1/webhooks/nct-allotment-migration";

const adapter = new PrismaPg({
  connectionString: process.env.DATABASE_URL,
});

const prisma = new PrismaClient({ adapter });

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

/** Convert a source AllotmentsTableData row into the API payload shape. */
function toApiRecord(row) {
  const institute = row.institute && typeof row.institute === "object" ? row.institute : null;
  const quota = row.quota && typeof row.quota === "object" ? row.quota : null;
  const course = row.course && typeof row.course === "object" ? row.course : null;

  return {
    sourceId: row.id,
    counsellingId: row.counsellingId ?? null,
    category: row.category ?? null,
    instituteZynerdId: institute?.id ?? null,
    quotaZynerdId: quota?.id ?? null,
    courseName: course?.name ?? null,
    state: row.state ?? null,
    rank: row.rank ?? null,
    aiRank: row.aiRank ?? null,
    round: row.round != null ? Number(row.round) : null,
  };
}

async function sendBatch(records) {
  const response = await fetch(API_URL, {
    method: "POST",
    headers: {
      accept: "application/json",
      "content-type": "application/json",
    },
    body: JSON.stringify({ records }),
  });

  const body = await response.json().catch(() => ({}));

  if (!response.ok) {
    throw new Error(
      `Migration API failed (${response.status}): ${JSON.stringify(body).slice(0, 300)}`,
    );
  }

  return body?.data ?? {};
}

export async function migrateNctAllotments() {
  let cursorId = 0;
  let totalInserted = 0;
  let totalSkipped = 0;
  let totalProcessed = 0;

  while (true) {
    const rows = await prisma.allotmentsTableData.findMany({
      where: { isProceed: false, id: { gt: cursorId } },
      orderBy: { id: "asc" },
      take: BATCH_SIZE,
    });

    if (rows.length === 0) break;

    const records = rows.map(toApiRecord);
    const result = await sendBatch(records);

    const succeededSourceIds = Array.isArray(result.succeededSourceIds)
      ? result.succeededSourceIds
      : [];

    if (succeededSourceIds.length > 0) {
      await prisma.allotmentsTableData.updateMany({
        where: { id: { in: succeededSourceIds } },
        data: { isProceed: true },
      });
    }

    totalInserted += result.insertedCount ?? succeededSourceIds.length;
    totalSkipped += result.skippedCount ?? 0;
    totalProcessed += rows.length;
    cursorId = rows[rows.length - 1].id;

    console.log(
      `Batch up to id=${cursorId}: inserted=${result.insertedCount ?? 0}, skipped=${result.skippedCount ?? 0} (running: processed=${totalProcessed}, inserted=${totalInserted}, skipped=${totalSkipped})`,
    );

    if (REQUEST_DELAY_MS > 0) await sleep(REQUEST_DELAY_MS);
  }

  return { totalProcessed, totalInserted, totalSkipped };
}

async function main() {
  console.log(`Migrating allotments to: ${API_URL}`);
  const summary = await migrateNctAllotments();
  console.log(
    `Done. processed=${summary.totalProcessed}, inserted=${summary.totalInserted}, skipped=${summary.totalSkipped}`,
  );
}

main().catch((error) => {
  console.error("Migration failed:", error);
  process.exit(1);
});