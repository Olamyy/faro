package dev.faro.flink;

import java.util.concurrent.atomic.AtomicLong;

final class IntervalCounters {

    final AtomicLong input = new AtomicLong(0);
    final AtomicLong output = new AtomicLong(0);
    final AtomicLong eventTimeMaxMs = new AtomicLong(Long.MIN_VALUE);
    final AtomicLong eventTimeMinMs = new AtomicLong(Long.MIN_VALUE);
    private long intervalStartMs = System.currentTimeMillis();

    void recordTimestamp(long ts) {
        if (ts != Long.MIN_VALUE) {
            eventTimeMaxMs.getAndUpdate(prev -> prev == Long.MIN_VALUE ? ts : Math.max(prev, ts));
            eventTimeMinMs.getAndUpdate(prev -> prev == Long.MIN_VALUE ? ts : Math.min(prev, ts));
        }
    }

    record Snapshot(long nowMs, long input, long output, long maxEventTimeMs, long minEventTimeMs, long intervalMs) {}

    Snapshot snapshot() {
        long now = System.currentTimeMillis();
        long i = input.getAndSet(0);
        long o = output.getAndSet(0);
        long maxMs = eventTimeMaxMs.getAndSet(Long.MIN_VALUE);
        long minMs = eventTimeMinMs.getAndSet(Long.MIN_VALUE);
        long intervalMs = now - intervalStartMs;
        intervalStartMs = now;
        return new Snapshot(now, i, o, maxMs, minMs, intervalMs);
    }
}
