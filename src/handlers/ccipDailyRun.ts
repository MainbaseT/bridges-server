import { ccipDayStart } from "../adapters/ccip";
import { formatError, isAbortError, throwIfAborted } from "../utils/errors";

type ReconcileDay = (date: string, signal?: AbortSignal) => Promise<{ movedFromDates: string[] }>;

export async function runCCIPDailyRun(dates: string[], reconcile: ReconcileDay, signal?: AbortSignal) {
  const aggregationDates = new Set<string>();
  const failedDays: Array<{ date: string; error: string }> = [];
  for (const date of [...dates].sort().reverse()) {
    throwIfAborted(signal);
    try {
      const result = await reconcile(date, signal);
      aggregationDates.add(date);
      for (const oldDate of result.movedFromDates) aggregationDates.add(oldDate);
    } catch (error) {
      if (isAbortError(error) || signal?.aborted) throw error;
      const message = formatError(error);
      failedDays.push({ date, error: message });
      console.error(`[CCIP] Failed ${date}; continuing with other days: ${message}`);
    }
  }
  throwIfAborted(signal);
  const rangeDates = [...new Set([...dates, ...aggregationDates])].sort();
  if (!rangeDates.length) throw new Error("CCIP daily run requires at least one date");
  return {
    startTimestamp: ccipDayStart(rangeDates[0]) / 1000,
    endTimestamp: ccipDayStart(rangeDates[rangeDates.length - 1]) / 1000 + 86400,
    aggregationDates: [...aggregationDates].sort().reverse(),
    failedDays,
    degraded: failedDays.length > 0,
    error: failedDays.length ? failedDays.map(({ date, error }) => `${date}: ${error}`).join("; ") : undefined,
  };
}
