package dev.faro.e2e;

enum CaptureMode {
    AGGREGATE,
    ENTITY;

    static CaptureMode fromEnv() {
        String v = System.getenv().getOrDefault("FARO_CAPTURE_MODE", "AGGREGATE");
        return valueOf(v.toUpperCase());
    }
}
