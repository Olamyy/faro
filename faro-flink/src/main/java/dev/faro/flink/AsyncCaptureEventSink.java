package dev.faro.flink;

import dev.faro.core.CaptureEvent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Bounded capture buffer that drains to a delegate {@link CaptureEventSink} on a background
 * thread. On overflow the oldest queued event is discarded and {@link #droppedSinceLastFlush()}
 * is set, allowing the next emitted capture event to carry {@code capture_drop_since_last = true}.
 *
 * <p>Flink operators are single-threaded per subtask, so the {@code poll() + offer()} overflow
 * sequence is not atomic but is safe in practice — concurrent {@link #emit} calls do not occur.
 */
public final class AsyncCaptureEventSink implements CaptureEventSink {

    private final CaptureEventSink delegate;
    private final ArrayBlockingQueue<CaptureEvent> queue;
    private final AtomicBoolean dropped = new AtomicBoolean(false);
    private final Thread drainThread;
    private volatile boolean closed = false;

    public AsyncCaptureEventSink(CaptureEventSink delegate, int capacity) {
        this.delegate = delegate;
        this.queue = new ArrayBlockingQueue<>(capacity);
        this.drainThread = new Thread(this::drainLoop, "faro-async-drain");
        this.drainThread.setDaemon(true);
        this.drainThread.start();
    }

    @Override
    public void emit(CaptureEvent event) {
        if (closed) return;
        if (!queue.offer(event)) {
            queue.poll();
            dropped.set(true);
            queue.offer(event);
        }
    }

    @Override
    public boolean droppedSinceLastFlush() {
        return dropped.getAndSet(false);
    }

    @Override
    public void close() {
        closed = true;
        drainThread.interrupt();
        try {
            drainThread.join(5_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        delegate.close();
    }

    private void drainLoop() {
        while (!closed || !queue.isEmpty()) {
            try {
                CaptureEvent event = queue.poll(100, TimeUnit.MILLISECONDS);
                if (event != null) {
                    delegate.emit(event);
                }
            } catch (InterruptedException e) {
                CaptureEvent remaining;
                while ((remaining = queue.poll()) != null) {
                    delegate.emit(remaining);
                }
            }
        }
    }


}
