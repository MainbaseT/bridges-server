import { CCIP_LOOKBACK_DAYS, ccipDayStart } from "../adapters/ccip";
import { formatError, isAbortError, throwIfAborted } from "../utils/errors";

const DEFAULT_AGGREGATION_LOOKBACK_SECONDS = 36 * 60 * 60;

type BridgeAggregationPipelineOptions = {
  bridgeName: string;
  signal: AbortSignal;
  aggregate: (startTimestamp: number, endTimestamp: number, bridgeName: string, signal: AbortSignal) => Promise<void>;
  getCurrentTimestamp: () => number;
  lookbackSeconds?: number;
  startTimestamp?: number;
  aggregationDates?: string[];
};

export const runBridgeAggregationPipeline = async ({
  bridgeName,
  signal,
  aggregate,
  getCurrentTimestamp,
  lookbackSeconds = DEFAULT_AGGREGATION_LOOKBACK_SECONDS,
  startTimestamp,
  aggregationDates,
}: BridgeAggregationPipelineOptions): Promise<void | { degraded: true; error: string }> => {
  if (bridgeName === "ccip" && aggregationDates !== undefined) {
    const failures: string[] = [];
    for (const date of [...new Set(aggregationDates)].sort().reverse()) {
      throwIfAborted(signal);
      const start = ccipDayStart(date) / 1000;
      try {
        await aggregate(start, start + 86400, bridgeName, signal);
      } catch (error) {
        if (isAbortError(error) || signal.aborted) throw error;
        const message = `${date}: ${formatError(error)}`;
        failures.push(message);
        console.error(`[CCIP] Aggregation failed for ${message}`);
      }
    }
    throwIfAborted(signal);
    if (failures.length) return { degraded: true, error: failures.join("; ") };
    return;
  }
  const now = getCurrentTimestamp();
  const endTimestamp = bridgeName === "ccip" ? Math.floor(now / 86400) * 86400 : now;
  const window = bridgeName === "ccip" ? CCIP_LOOKBACK_DAYS * 86400 : lookbackSeconds;
  await aggregate(startTimestamp ?? endTimestamp - window, endTimestamp, bridgeName, signal);
};

export const publishAggregations = async (
  aggregationRuns: Promise<unknown>[],
  aggregateHourly: () => Promise<void>,
  aggregateDaily: () => Promise<void>
) => {
  await Promise.all(aggregationRuns);
  await aggregateHourly();
  await aggregateDaily();
};
