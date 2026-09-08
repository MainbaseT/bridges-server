import assert from "node:assert/strict";
import test from "node:test";
import { runCCIPDailyRun } from "./ccipDailyRun";
import { publishAggregations, runBridgeAggregationPipeline } from "../server/dedicatedPipeline";
import { jobCompletedSuccessfully } from "../server/cronState";
import { createAbortError } from "../utils/errors";

test("a bad historical CCIP day does not block fresh ingestion, aggregation or publication", async () => {
  const signal = new AbortController().signal;
  const fetched: string[] = [];
  const result = await runCCIPDailyRun(
    ["2026-09-02", "2026-09-03", "2026-09-04", "2026-09-05"],
    async (date) => {
      fetched.push(date);
      if (date === "2026-09-03") throw new Error("Unknown CCIP chain new for 2026-09-03");
      return { movedFromDates: [] };
    },
    signal
  );
  assert.deepEqual(fetched, ["2026-09-05", "2026-09-04", "2026-09-03", "2026-09-02"]);
  assert.equal(result.degraded, true);
  assert.match(result.error!, /2026-09-03.*Unknown CCIP chain new/);
  assert.deepEqual(
    result.failedDays.map((day) => day.date),
    ["2026-09-03"]
  );
  assert.ok(jobCompletedSuccessfully({ status: result.degraded ? "degraded" : "ok", result }));

  const calls: string[] = [];
  const aggregation = runBridgeAggregationPipeline({
    bridgeName: "ccip",
    signal,
    aggregationDates: result.aggregationDates,
    getCurrentTimestamp: () => result.endTimestamp,
    aggregate: async (start, end) => {
      assert.equal(end - start, 86400);
      calls.push(new Date(start * 1000).toISOString().slice(0, 10));
    },
  });
  await publishAggregations(
    [aggregation],
    async () => {
      calls.push("publish hourly");
    },
    async () => {
      calls.push("publish daily");
    }
  );
  assert.deepEqual(calls, ["2026-09-05", "2026-09-04", "2026-09-02", "publish hourly", "publish daily"]);
});

test("CCIP retains old dates affected by successful timestamp corrections", async () => {
  const result = await runCCIPDailyRun(["2026-09-04", "2026-09-05"], async (date) => ({
    movedFromDates: date === "2026-09-05" ? ["2026-08-20", "2026-09-04"] : [],
  }));
  assert.deepEqual(result.aggregationDates, ["2026-09-05", "2026-09-04", "2026-08-20"]);
  assert.equal(result.startTimestamp, Date.parse("2026-08-20") / 1000);
  assert.equal(result.endTimestamp, Date.parse("2026-09-06") / 1000);
  assert.equal(result.degraded, false);
});

test("if all CCIP days fail, no stale ten-day aggregation is started", async () => {
  const result = await runCCIPDailyRun(["2026-09-04", "2026-09-05"], async () => {
    throw new Error("HTTP 503");
  });
  assert.equal(result.failedDays.length, 2);
  assert.equal(result.degraded, true);
  assert.deepEqual(result.aggregationDates, []);
  await runBridgeAggregationPipeline({
    bridgeName: "ccip",
    signal: new AbortController().signal,
    aggregationDates: result.aggregationDates,
    getCurrentTimestamp: () => result.endTimestamp,
    aggregate: async () => {
      assert.fail("Failed days must not be aggregated");
    },
  });
});

test("one CCIP aggregation failure still allows other days to be aggregated and published", async () => {
  const calls: string[] = [];
  const aggregation = runBridgeAggregationPipeline({
    bridgeName: "ccip",
    signal: new AbortController().signal,
    aggregationDates: ["2026-09-04", "2026-09-05"],
    getCurrentTimestamp: () => Date.parse("2026-09-06") / 1000,
    aggregate: async (start) => {
      const date = new Date(start * 1000).toISOString().slice(0, 10);
      calls.push(date);
      if (date === "2026-09-05") throw new Error("Price service unavailable");
    },
  });
  const result = await aggregation;
  assert.equal(result?.degraded, true);
  assert.match(result!.error, /2026-09-05.*Price service unavailable/);
  await publishAggregations(
    [aggregation],
    async () => {
      calls.push("publish hourly");
    },
    async () => {
      calls.push("publish daily");
    }
  );
  assert.deepEqual(calls, ["2026-09-05", "2026-09-04", "publish hourly", "publish daily"]);
});

test("CCIP cancellation stops both ingestion and aggregation instead of being swallowed", async () => {
  const controller = new AbortController();
  const calls: string[] = [];
  await assert.rejects(
    () =>
      runCCIPDailyRun(
        ["2026-09-04", "2026-09-05"],
        async (date) => {
          calls.push(date);
          controller.abort();
          throw createAbortError();
        },
        controller.signal
      ),
    { name: "AbortError" }
  );
  assert.deepEqual(calls, ["2026-09-05"]);
  await assert.rejects(
    () =>
      runBridgeAggregationPipeline({
        bridgeName: "ccip",
        signal: new AbortController().signal,
        aggregationDates: ["2026-09-04", "2026-09-05"],
        getCurrentTimestamp: () => 0,
        aggregate: async () => {
          throw createAbortError();
        },
      }),
    { name: "AbortError" }
  );
});
