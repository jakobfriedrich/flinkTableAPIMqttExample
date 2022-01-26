package spendreport;

import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.types.Row;
import org.eclipse.paho.client.mqttv3.*;
import scala.util.parsing.json.JSON;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

public class MQTTSourceFunction<T> implements SourceFunction<T> {

    // ----- Required property keys
    public static final String URL = "mqtt.server.url";
    public static final String CLIENT_ID = "mqtt.client.id";
    public static final String TOPIC = "mqtt.topic";

    // ------ Optional property keys
    public static final String USERNAME = "mqtt.username";
    public static final String PASSWORD = "mqtt.password";


    private final Properties properties;
    private final Class<T> classType;

    // ------ Runtime fields
    private transient MqttClient client;
    private transient volatile boolean running;
    private transient Object waitLock;

    public MQTTSourceFunction(Properties properties, Class<T> type) {
        checkProperty(properties, URL);
        checkProperty(properties, CLIENT_ID);
        checkProperty(properties, TOPIC);

        this.properties = properties;
        this.classType = type;
    }

    private static void checkProperty(Properties p, String key) {
        if (!p.containsKey(key)) {
            throw new IllegalArgumentException("Required property '" + key + "' not set.");
        }
    }



    @Override
    public void run(final SourceContext<T> ctx) throws Exception {
        MqttConnectOptions connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);

        if (properties.containsKey(USERNAME)) {
            connectOptions.setUserName(properties.getProperty(USERNAME));
        }

        if (properties.containsKey(PASSWORD)) {
            connectOptions.setPassword(properties.getProperty(PASSWORD).toCharArray());
        }

        connectOptions.setAutomaticReconnect(true);

        client = new MqttClient(properties.getProperty(URL), properties.getProperty(CLIENT_ID));
        client.connect(connectOptions);

        client.subscribe(properties.getProperty(MQTTSourceFunction.TOPIC), (topic, message) -> {
            String msg = new String(message.getPayload(), StandardCharsets.UTF_8);
            ObjectMapper mapper = new ObjectMapper();
            T t = mapper.readValue(msg, classType);
            ObjectNode actualObj = (ObjectNode) mapper.readTree(msg);
            //Row row = Row.of(actualObj.get("nameid").asInt(),actualObj.get("name").asText(), actualObj.get("timeValue").asText());
            /*final DateTimeFormatter formatter = DateTimeFormatter
                    .ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(ZoneId.systemDefault());
            Instant result = Instant.from(formatter.parse(actualObj.get("timeValue").asText()));*/

            //User user = new User(actualObj.get("nameid").asInt(),actualObj.get("name").asText(),actualObj.get("timeValue").asText());
            ctx.collect(t);
        });

        running = true;
        waitLock = new Object();

        while (running) {
            synchronized (waitLock) {
                waitLock.wait(100L);
            }

        }
    }

    @Override
    public void cancel() {
        close();
    }

    private void close() {
        try {
            if (client != null) {
                client.disconnect();
            }
        } catch (MqttException exception) {

        } finally {
            this.running = false;
        }

        // leave main method
        synchronized (waitLock) {
            waitLock.notify();
        }
    }
}