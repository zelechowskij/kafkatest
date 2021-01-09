package KafkaStreams;

import java.util.UUID;

public class CarState {

    UUID vehicle_id;
    String vehicle_type;
    long timestamp;
    float left_front_ps;
    float right_front_ps;
    float left_rear_ps;
    float right_rear_ps;
    float latitude;
    float longitude;
    float fuel_level;
    int current_speed;
    float battery_voltage;
    int mileage;
    float acceleration;
    boolean fuel_alert;
    boolean battery_alert;
    boolean left_front_ps_alert;
    boolean right_front_ps_alert;
    boolean left_rear_ps_alert;
    boolean right_rear_ps_alert;

    public CarState(UUID vehicle_id, String vehicle_type, long timestamp, float left_front_ps, float right_front_ps, float left_rear_ps, float right_rear_ps, float latitude, float longitude, float fuel_level, int current_speed, float battery_voltage, int mileage, float acceleration, boolean fuel_alert, boolean battery_alert, boolean left_front_ps_alert, boolean right_front_ps_alert, boolean left_rear_ps_alert, boolean right_rear_ps_alert) {
        this.vehicle_id = vehicle_id;
        this.vehicle_type = vehicle_type;
        this.timestamp = timestamp;
        this.left_front_ps = left_front_ps;
        this.right_front_ps = right_front_ps;
        this.left_rear_ps = left_rear_ps;
        this.right_rear_ps = right_rear_ps;
        this.latitude = latitude;
        this.longitude = longitude;
        this.fuel_level = fuel_level;
        this.current_speed = current_speed;
        this.battery_voltage = battery_voltage;
        this.mileage = mileage;
        this.acceleration = acceleration;
        this.fuel_alert = fuel_alert;
        this.battery_alert = battery_alert;
        this.left_front_ps_alert = left_front_ps_alert;
        this.right_front_ps_alert = right_front_ps_alert;
        this.left_rear_ps_alert = left_rear_ps_alert;
        this.right_rear_ps_alert = right_rear_ps_alert;
    }

    public UUID getVehicle_id() {
        return vehicle_id;
    }

    public String getVehicle_type() {
    return vehicle_type;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public float getLeft_front_ps() {
        return left_front_ps;
    }

    public float getRight_front_ps() {
        return right_front_ps;
    }

    public float getLeft_rear_ps() {
        return left_rear_ps;
    }

    public float getRight_rear_ps() {
        return right_rear_ps;
    }

    public float getLatitude() {
        return latitude;
    }

    public float getLongitude() {
        return longitude;
    }

    public float getFuel_level() {
        return fuel_level;
    }

    public int getCurrent_speed() {
        return current_speed;
    }

    public float getBattery_voltage() {
        return battery_voltage;
    }

    public int getMileage() {
        return mileage;
    }

    public float getAcceleration() {
        return acceleration;
    }

    public boolean isFuel_alert() {
        return fuel_alert;
    }

    public boolean isBattery_alert() {
        return battery_alert;
    }

    public boolean isLeft_front_ps_alert() {
        return left_front_ps_alert;
    }

    public boolean isRight_front_ps_alert() {
        return right_front_ps_alert;
    }

    public boolean isLeft_rear_ps_alert() {
        return left_rear_ps_alert;
    }

    public boolean isRight_rear_ps_alert() {
        return right_rear_ps_alert;
    }
}
