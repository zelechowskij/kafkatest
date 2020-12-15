import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;


public class CassandraConnector {

    private Cluster cluster;

    private Session session;

    private static final String TABLE_NAME = "car.state";

    public void connect(String node, Integer port) {
        com.datastax.driver.core.Cluster.Builder b = com.datastax.driver.core.Cluster.builder().addContactPoint(node);
        if (port != null) b.withPort(port);

        cluster = b.build();

        session = cluster.connect();
    }

    public Session getSession() {
        return this.session;
    }
    public void close(){
        session.close();
        cluster.close();
    }

    public void createTable() {
        StringBuilder sb = new StringBuilder("CREATE TABLE IF NOT EXISTS ")
                .append(TABLE_NAME).append("(")
                .append("vehicle_id uuid, ")
                .append("timestamp timestamp,")
                .append("vehicle_type text,")
                .append("left_front_ps float, ")
                .append("right_front_ps float, ")
                .append("left_rear_ps float, ")
                .append("right_rear_ps float, ")
                .append("latitude float, ")
                .append("longitude float, ")
                .append("fuel_level float, ")
                .append("current_speed int, ")
                .append("battery_voltage float, ")
                .append("mileage int, ")
                .append("acceleration float, ")
                .append("fuel_alert boolean,")
                .append("battery_alert boolean,")
                .append("left_front_ps_alert boolean, ")
                .append("right_front_ps_alert boolean, ")
                .append("left_rear_ps_alert boolean, ")
                .append("right_rear_ps_alert boolean,")
                .append("PRIMARY KEY (vehicle_id, timestamp)")
                .append(") WITH CLUSTERING ORDER BY (timestamp DESC);");

        String query = sb.toString();
        session.execute(query);
    }

    public void alterTableCarState(String columnName, String columnType) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ")
                .append(TABLE_NAME).append(" ADD ")
                .append(columnName).append(" ")
                .append(columnType).append(";");

        String query = sb.toString();
        session.execute(query);
    }

    public void insertCarStateByID(CarState car) {
        StringBuilder sb = new StringBuilder("INSERT INTO ")
                .append(TABLE_NAME).append("(id, title) ")
                .append("VALUES (").append(car.getVehicle_id())
                .append(", '").append(car.getTimestamp())
                .append(", '").append(car.getVehicle_type())
                .append(", '").append(car.getLeft_front_ps())
                .append(", '").append(car.getRight_front_ps())
                .append(", '").append(car.getLeft_rear_ps())
                .append(", '").append(car.getRight_rear_ps())
                .append(", '").append(car.getLatitude())
                .append(", '").append(car.getLongitude())
                .append(", '").append(car.getFuel_level())
                .append(", '").append(car.getCurrent_speed())
                .append(", '").append(car.getBattery_voltage())
                .append(", '").append(car.getMileage())
                .append(", '").append(car.getAcceleration())
                .append(", '").append(car.isFuel_alert())
                .append(", '").append(car.isBattery_alert())
                .append(", '").append(car.isLeft_front_ps_alert())
                .append(", '").append(car.isRight_front_ps_alert())
                .append(", '").append(car.isLeft_rear_ps_alert())
                .append(", '").append(car.isRight_rear_ps_alert())
                .append("');");

        String query = sb.toString();
        session.execute(query);
    }

    public static void main(String[] args) {
        KeyspaceRepository schemaRepository;
        Session session;
        CassandraConnector client = new CassandraConnector();
        client.connect("127.0.0.1", 9042);
        session = client.getSession();
        schemaRepository = new KeyspaceRepository(session);
        String keyspaceName = "car";
        schemaRepository.createKeyspace(keyspaceName,"SimpleStrategy",1);
        client.createTable();
    }

}
