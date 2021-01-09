package KafkaStreams;
import Constants.KafkaConstants;
import com.datastax.driver.core.Session;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

public class StreamProcessing {

//TODO add default values to key/value schema, validation engine
    StreamsBuilder builder = new StreamsBuilder();
    Topology topology = builder.build();
    private KStream<GenericRecord, GenericRecord> basicStream;

    private static Properties createStreamProperties(){
        final Properties props = new Properties();
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaConstants.KAFKA_BROKERS);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "app-id");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        props.put("schema.registry.url", "http://127.0.0.1:8081");
        return props;
    }


    public static void main(String[] args) throws InterruptedException {

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, GenericAvroSerde.class);
        streamsConfiguration.put("schema.registry.url", "http://127.0.0.1:8081");
        final Map<String, String> serdeConfig = Collections.singletonMap("schema.registry.url", "http://127.0.0.1:8081");
        final Serde<GenericRecord> keyGenericAvroSerde = new GenericAvroSerde();
        keyGenericAvroSerde.configure(serdeConfig, true); // `true` for record keys
        final Serde<GenericRecord> valueGenericAvroSerde = new GenericAvroSerde();
        valueGenericAvroSerde.configure(serdeConfig, false); // `false` for record values


        KeyspaceRepository schemaRepository;
        Session session;
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);
        String keyspaceName = "car";

        StreamsBuilder builder = new StreamsBuilder();

        KStream<GenericRecord,GenericRecord> basicStream =
                builder.stream("car-state-test1");

        basicStream.foreach((key,value) -> System.out.println(key + "=>" + value));


        KStream<GenericRecord,GenericRecord> fuelStream = basicStream.peek((key, value) ->
            {
                if ((float) value.get("fuel_level") < 5) {
                value.put("fuel_alert", true);

            }
            if ((float) value.get("battery_voltage") < 12) {
                value.put("battery_alert", true);

            }
            if ((float) value.get("left_front_ps") < 1.9) {
                    value.put("left_front_ps_alert", true);

                }
            if ((float) value.get("right_front_ps") < 1.9) {
                    value.put("right_front_ps_alert", true);

                }
            if ((float) value.get("left_rear_ps") < 1.9) {
                    value.put("left_rear_ps_alert", true);

                }
            if ((float) value.get("right_rear_ps") < 1.9) {
                    value.put("right_rear_ps_alert", true);
                }
});

        KStream stream = fuelStream.peek( (key,value) -> client.insertCarStateByID(new CarState(
                (UUID) key.get("vehicle_id"),
                value.get("vehicle_type").toString(),
                (long) value.get("timestamp"),
                (float) value.get("left_front_ps"),
                (float) value.get("right_front_ps"),
                (float) value.get("left_rear_ps"),
                (float) value.get("right_rear_ps"),
                (float) value.get("latitude"),
                (float) value.get("longitude"),
                (float) value.get("fuel_level"),
                (int) value.get("current_speed"),
                (float) value.get("battery_voltage"),
                (int) value.get("mileage"),
                (float) value.get("acceleration"),
                (boolean) value.get("fuel_alert"),
                (boolean) value.get("battery_alert"),
                (boolean) value.get("left_front_ps_alert"),
                (boolean) value.get("right_front_ps_alert"),
                (boolean) value.get("left_rear_ps_alert"),
                (boolean) value.get("right_rear_ps_alert")

        )));

        fuelStream.to("fuel-alert", Produced.with(keyGenericAvroSerde, valueGenericAvroSerde));


        final KafkaStreams streams = new KafkaStreams(builder.build(), createStreamProperties());
        streams.start();
    }

}
