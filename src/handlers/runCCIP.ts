import { CCIP_LOOKBACK_DAYS, ccipDateRange, ccipDayStart, fetchEventsForDate } from "../adapters/ccip";
import { sql } from "../utils/db";
import { throwIfAborted } from "../utils/errors";
import { insertTransactionRows } from "../utils/wrappa/postgres/write";
import { ccipRowsToMove, ccipUSDTotals, diffCCIPSnapshot, groupCCIPEvents, StoredCCIPEvent } from "./ccipSnapshot";
import { runCCIPDailyRun } from "./ccipDailyRun";
import { decodeCCIPTransactionHash } from "../utils/ccipTransactionHash";

export async function reconcileCCIPDay(date: string, dryRun = false, signal?: AbortSignal) {
  ccipDateRange(date, date);
  const expected = groupCCIPEvents(await fetchEventsForDate(date, signal));
  const start = new Date(ccipDayStart(date));
  const end = new Date(start.getTime() + 86400000);
  throwIfAborted(signal);
  // Dry runs are enforced by Postgres, including the config lookup below.
  return sql.begin(dryRun ? "read only" : "", async (tx) => {
    const stored = await tx<StoredCCIPEvent[]>`
      SELECT t.* FROM bridges.transactions t JOIN bridges.config c ON c.id = t.bridge_id
      WHERE c.bridge_name = 'ccip' AND t.ts >= ${start} AND t.ts < ${end}
      ORDER BY t.id
    `;
    let diff = diffCCIPSnapshot(expected, stored);
    const [published] = await tx`
      SELECT COALESCE(SUM(d.total_deposited_usd), 0) AS deposit_usd,
             COALESCE(SUM(d.total_withdrawn_usd), 0) AS withdraw_usd
      FROM bridges.daily_volume d JOIN bridges.config c ON c.id = d.bridge_id
      WHERE c.bridge_name = 'ccip' AND d.ts = ${start}
    `;
    let changed = [...diff.added, ...diff.updated];
    const movedFromDates = new Set<string>();
    if (changed.length) {
      const keysToCheck = changed.flatMap((event) => [
        event,
        { ...event, tx_hash: decodeCCIPTransactionHash(event.tx_hash) },
      ]);
      const outside = await tx<StoredCCIPEvent[]>`
        SELECT DISTINCT t.* FROM jsonb_to_recordset(${tx.json(keysToCheck)})
          AS e(chain text, tx_hash text, token text, tx_from text, tx_to text)
        JOIN bridges.config c ON c.bridge_name = 'ccip' AND c.chain = e.chain
        JOIN bridges.transactions t ON t.bridge_id = c.id AND t.chain = e.chain
          AND t.tx_hash = e.tx_hash AND t.token = e.token
          AND t.tx_from = e.tx_from AND t.tx_to = e.tx_to
        WHERE t.ts < ${start} OR t.ts >= ${end}
      `;
      const rowsToMove: StoredCCIPEvent[] = [];
      // A timestamp correction is safe to move only if the provider no longer
      // reports this key on the old day. Genuine cross-day batches still fail.
      for (const oldDate of new Set(outside.map((row) => row.ts.toISOString().slice(0, 10)))) {
        ccipDateRange(oldDate, oldDate);
        const oldEvents = await fetchEventsForDate(oldDate, signal);
        if (!oldEvents.length)
          throw new Error(`Cannot verify CCIP timestamp correction from an empty snapshot for ${oldDate}`);
        const moving = ccipRowsToMove(
          expected,
          outside.filter((row) => row.ts.toISOString().slice(0, 10) === oldDate),
          groupCCIPEvents(oldEvents)
        );
        if (moving.length) {
          rowsToMove.push(...moving);
          movedFromDates.add(oldDate);
        }
      }
      if (rowsToMove.length) {
        diff = diffCCIPSnapshot(expected, [...stored, ...rowsToMove]);
        changed = [...diff.added, ...diff.updated];
      }
    }
    const summary = {
      date,
      dryRun,
      added: diff.added.length,
      updated: diff.updated.length,
      deleted: diff.deleted.length,
      unchanged: diff.unchanged,
      movedFromDates: [...movedFromDates].sort(),
      storedUSD: ccipUSDTotals(stored),
      expectedUSD: ccipUSDTotals(expected),
      publishedUSD: { depositUSD: published.deposit_usd, withdrawUSD: published.withdraw_usd },
    };
    if (dryRun) return summary;

    for (const chain of new Set(expected.map((event) => event.chain))) {
      await tx`INSERT INTO bridges.config (bridge_name, chain) VALUES ('ccip', ${chain})
        ON CONFLICT (bridge_name, chain) DO NOTHING`;
    }
    const configs = await tx`SELECT id, chain FROM bridges.config WHERE bridge_name = 'ccip'`;
    const ids = new Map(configs.map((config) => [config.chain as string, config.id as string]));
    if (diff.deleted.length) {
      // large_transactions has an ON DELETE CASCADE foreign key to these rows.
      await tx`DELETE FROM bridges.transactions WHERE id IN ${tx(diff.deleted.map((row) => row.id))}`;
    }
    for (let offset = 0; offset < changed.length; offset += 200) {
      throwIfAborted(signal);
      const rows = changed.slice(offset, offset + 200).map((event) => ({
        ...event,
        bridge_id: ids.get(event.chain)!,
        tx_block: 0,
        txs_counted_as: null,
        origin_chain: null,
      }));
      await insertTransactionRows(tx, true, rows, "upsert", true);
    }
    throwIfAborted(signal);
    return summary;
  });
}

export async function runCCIPBackfillMode(startDate: string, endDate: string, signal?: AbortSignal) {
  let startTimestamp = ccipDayStart(startDate) / 1000;
  let endTimestamp = ccipDayStart(endDate) / 1000 + 86400;
  for (const date of ccipDateRange(startDate, endDate)) {
    const summary = await reconcileCCIPDay(date, false, signal);
    console.log("[CCIP]", JSON.stringify(summary));
    for (const oldDate of summary.movedFromDates) {
      startTimestamp = Math.min(startTimestamp, ccipDayStart(oldDate) / 1000);
      endTimestamp = Math.max(endTimestamp, ccipDayStart(oldDate) / 1000 + 86400);
    }
  }
  return { startTimestamp, endTimestamp };
}

export async function runCCIPDefaultMode(signal?: AbortSignal) {
  const today = Math.floor(Date.now() / 86400000) * 86400000;
  const dates = ccipDateRange(
    new Date(today - CCIP_LOOKBACK_DAYS * 86400000).toISOString().slice(0, 10),
    new Date(today - 86400000).toISOString().slice(0, 10)
  );
  return runCCIPDailyRun(
    dates,
    async (date, signal) => {
      const summary = await reconcileCCIPDay(date, false, signal);
      console.log("[CCIP]", JSON.stringify(summary));
      return summary;
    },
    signal
  );
}
