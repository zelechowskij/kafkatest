
import java.util.Scanner;
import java.io.IOException;


public class Helper {

    public static void main(String[] args) throws IOException {
        Scanner scanner = new Scanner(System.in);
        String avro = "{\"namespace\":\"example.car\",\"type\":\"record\",\"name\":\"car_status\",\"fields\":[{\"name\":\"vehicle_id\",\"type\":\"int\"},{\"name\":\"vehicle_type\",\"type\":\"string\"},{\"name\":\"timestamp\",\"type\":\"long\"},{\"name\":\"left_front_ps\",\"type\":\"float\"},{\"name\":\"right_front_ps\",\"type\":\"float\"},{\"name\":\"left_rear_ps\",\"type\":\"float\"},{\"name\":\"right_rear_ps\",\"type\":\"float\"},{\"name\":\"latitude\",\"type\":\"float\"},{\"name\":\"longitude\",\"type\":\"float\"},{\"name\":\"fuel_level\",\"type\":\"float\"},{\"name\":\"error_code\",\"type\":\"string\"},{\"name\":\"current_speed\",\"type\":\"int\"},{\"name\":\"battery_voltage\",\"type\":\"float\"},{\"name\":\"mileage\",\"type\":\"int\"},{\"name\":\"acceleration\",\"type\":\"float\"}]}\n";

        System.out.println(avro);
        String payload = "{ \"schema\": \"" + avro.replace("\"", "\\\"").replace("\t", "")
                .replace("\n", "") + "\" }";
        System.out.println(payload);
    }

}
