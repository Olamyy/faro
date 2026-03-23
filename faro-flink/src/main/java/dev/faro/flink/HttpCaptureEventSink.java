package dev.faro.flink;

import dev.faro.core.CaptureEvent;
import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.RequestBody;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;

/**
 * {@link CaptureEventSink} that POSTs each event as a JSON body to a configured URL.
 *
 * <p>Requests are fire-and-forget: the call is made synchronously on the calling thread but
 * failures are logged and never propagated. Use {@link AsyncCaptureEventSink} as a wrapper
 * if the operator thread must not block on network I/O.
 *
 * <p>If the client cannot be initialised, the sink enters a disabled state and {@link #emit}
 * becomes a no-op.
 */
public final class HttpCaptureEventSink implements CaptureEventSink {

    private static final Logger LOG = LoggerFactory.getLogger(HttpCaptureEventSink.class);
    private static final MediaType JSON = MediaType.get("application/json");

    private final String url;
    private final OkHttpClient client;

    HttpCaptureEventSink(String url, Duration connectTimeout, Duration readTimeout) {
        this.url = url;
        this.client = new OkHttpClient.Builder()
                .connectTimeout(connectTimeout)
                .readTimeout(readTimeout)
                .build();
    }

    @Override
    public void emit(CaptureEvent event) {
        Request request = new Request.Builder()
                .url(url)
                .post(RequestBody.create(event.toJson(), JSON))
                .build();
        try (okhttp3.Response response = client.newCall(request).execute()) {
            if (!response.isSuccessful()) {
                LOG.warn("Faro: HTTP sink received non-2xx response {} from {}", response.code(), url);
            }
        } catch (Exception e) {
            LOG.warn("Faro: failed to POST capture event to {}: {}", url, e.getMessage());
        }
    }

    @Override
    @SuppressWarnings("resource")
    public void close() {
        client.dispatcher().executorService().shutdown();
        client.connectionPool().evictAll();
    }

    /**
     * Returns a serializable factory for {@code HttpCaptureEventSink}.
     *
     * <p>Default timeouts are 5 s connect and 10 s read. Override via
     * {@link #factory(String, Duration, Duration)}.
     */
    public static CaptureEventSinkFactory factory(String url) {
        return factory(url, Duration.ofSeconds(5), Duration.ofSeconds(10));
    }

    public static CaptureEventSinkFactory factory(String url, Duration connectTimeout, Duration readTimeout) {
        return () -> new HttpCaptureEventSink(url, connectTimeout, readTimeout);
    }
}
