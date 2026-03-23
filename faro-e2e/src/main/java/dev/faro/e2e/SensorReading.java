package dev.faro.e2e;

import java.io.Serializable;

public final class SensorReading implements Serializable {

    public String deviceId;
    public double temperature;
    public long eventTime;

    public SensorReading(String deviceId, double temperature, long eventTime) {
        this.deviceId = deviceId;
        this.temperature = temperature;
        this.eventTime = eventTime;
    }

    @Override
    public String toString() {
        return "SensorReading{deviceId='" + deviceId + "', temperature=" + temperature
                + ", eventTime=" + eventTime + '}';
    }
}
