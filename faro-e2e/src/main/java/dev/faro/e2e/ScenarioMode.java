package dev.faro.e2e;

enum ScenarioMode {
    NORMAL,
    SILENT_DEVICE,
    SLOW_DEVICE,
    GRADUAL_DRIFT,
    STEP_CHANGE,
    NULL_RATE,
    CARDINALITY_DROP,
    CARDINALITY_SPIKE,
    LATE_EVENTS,
    SINK_BACKPRESSURE,
    /**
     * Automatically rotates through all scenarios (excluding CARDINALITY_SPIKE and SINK_BACKPRESSURE)
     * every {@link #PHASE_LENGTH_SLOTS} window slots.
     */
    ROTATING;

    /** Number of window slots each scenario phase lasts in ROTATING mode. */
    static final int PHASE_LENGTH_SLOTS = 5;

    /** Ordered list of scenarios that rotate in ROTATING mode. */
    static final ScenarioMode[] ROTATION = {
        NORMAL,
        SILENT_DEVICE,
        SLOW_DEVICE,
        GRADUAL_DRIFT,
        STEP_CHANGE,
        NULL_RATE,
        CARDINALITY_DROP,
        LATE_EVENTS,
    };

    static ScenarioMode fromEnv() {
        String v = System.getenv().getOrDefault("FARO_SCENARIO", "NORMAL");
        return valueOf(v.toUpperCase());
    }

    /** Returns the effective scenario for a given window slot (used in ROTATING mode). */
    static ScenarioMode forSlot(long windowSlot) {
        int phase = (int) ((windowSlot / PHASE_LENGTH_SLOTS) % ROTATION.length);
        return ROTATION[phase];
    }
}
