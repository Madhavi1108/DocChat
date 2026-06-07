import test from "node:test";
import assert from "node:assert/strict";
import prisma from "../utils/prismaClient.js";
import redis from "../utils/redis.js";
import { Queue } from "bullmq";

// Disconnect from the real Redis server to prevent reconnect attempts in the test
redis.disconnect();

// Mock the methods on the singleton instances to prevent network calls
prisma.$queryRaw = async () => [1];
redis.ping = async () => "PONG";
redis.multi = () => ({
    hincrby: function() { return this; },
    incrbyfloat: function() { return this; },
    incr: function() { return this; },
    exec: async () => []
});
redis.hgetall = async () => ({});
redis.get = async () => "0";

// Mock BullMQ Queue prototype
Queue.prototype.getJobCounts = async () => ({
    waiting: 2,
    active: 1,
    failed: 0,
    completed: 5
});

const { checkHealth, recordIngestionJobDuration, getPrometheusMetrics } = await import("../utils/metrics.js");

test("metrics checkHealth structure and properties", async () => {
    const { isHealthy, status } = await checkHealth();
    assert.equal(isHealthy, true);
    assert.equal(status.status, "OK");
    assert.equal(status.services.database, "UP");
    assert.equal(status.services.redis, "UP");
});

test("metrics getPrometheusMetrics output formatting", async () => {
    await recordIngestionJobDuration(12.5);

    const metricsStr = await getPrometheusMetrics();
    assert.ok(typeof metricsStr === "string");
    assert.match(metricsStr, /http_request_duration_seconds/);
    assert.match(metricsStr, /bullmq_queue_jobs_/);
    assert.match(metricsStr, /ingestion_job_duration_seconds/);
});
