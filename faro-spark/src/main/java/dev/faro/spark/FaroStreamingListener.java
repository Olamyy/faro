package dev.faro.spark;

import org.apache.spark.sql.streaming.StreamingQueryListener;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Makes the current watermark available to {@link FaroSpark#trace} between micro-batches.
 *
 * <p>Register before starting the streaming query:
 * <pre>{@code
 * FaroStreamingListener listener = new FaroStreamingListener();
 * spark.streams().addListener(listener);
 *
 * FaroSpark faro = new FaroSpark("my-pipeline", sinkFactory)
 *     .withStreamingContext("eventTime", null, listener);
 * }</pre>
 */
public final class FaroStreamingListener extends StreamingQueryListener {

    private final AtomicReference<String> lastWatermark = new AtomicReference<>(null);

    @Override
    public void onQueryStarted(QueryStartedEvent event) {}

    @Override
    public void onQueryProgress(QueryProgressEvent event) {
        String wm = event.progress().eventTime().get("watermark");
        if (wm != null && !wm.isEmpty()) {
            lastWatermark.set(wm);
        }
    }

    @Override
    public void onQueryTerminated(QueryTerminatedEvent event) {
        lastWatermark.set(null);
    }

    public String currentWatermark() {
        return lastWatermark.get();
    }
}
