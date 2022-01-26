package spendreport;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;




public class Example {
    public static final String URL = "tcp://broker.mqttdashboard.com";
    public static final String CLIENT_ID1 = "clientId-7lu4jy22Ar";
    public static final String CLIENT_ID2 = "clientId-7lu4jy22Af";
    public static final String TOPIC = "testtopic/jakob";
    public static final String TOPIC2 = "testtopic/device";


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        Properties prop1 = new Properties();
        prop1.put(MQTTSourceFunction.URL, URL);
        prop1.put(MQTTSourceFunction.CLIENT_ID, CLIENT_ID1);
        prop1.put(MQTTSourceFunction.TOPIC, TOPIC);

        Properties prop2 = new Properties();
        prop2.put(MQTTSourceFunction.URL, URL);
        prop2.put(MQTTSourceFunction.CLIENT_ID, CLIENT_ID2);
        prop2.put(MQTTSourceFunction.TOPIC, TOPIC2);

        DataStream<DeviceAction> deviceActionDataStream = env.addSource(new MQTTSourceFunction(prop1, DeviceAction.class)).returns(DeviceAction.class);
        DataStream<Device> deviceDataStream = env.addSource(new MQTTSourceFunction(prop2, Device.class)).returns(Device.class);


        final Schema deviceActionSchema = Schema.newBuilder()
                .column("actionid", DataTypes.INT())
                .column("userid", DataTypes.INT())
                .column("deviceid", DataTypes.INT())
                .column("event_name", DataTypes.STRING())
                .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
                .watermark("rowtime", "rowtime")
                .build();
        Table deviceActionInputTable = tableEnv.fromDataStream(deviceActionDataStream, deviceActionSchema);
        deviceActionInputTable.printSchema();
        tableEnv.createTemporaryView("DeviceActionInputTable", deviceActionInputTable);

        final Schema deviceSchema = Schema.newBuilder()
                .column("deviceid", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("status", DataTypes.STRING())
                .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
                .watermark("rowtime", "rowtime")
                .build();
        Table deviceInputTable = tableEnv.fromDataStream(deviceDataStream, deviceSchema);
        deviceInputTable.printSchema();
        tableEnv.createTemporaryView("DeviceInputTable", deviceInputTable);

        Table projection = tableEnv.sqlQuery("SELECT event_name From DeviceActionInputTable");
        Table tumblingWindow = tableEnv.sqlQuery("Select window_start, window_end, COUNT(actionid) AS number_of_results "+
                "FROM TABLE(TUMBLE(TABLE DeviceActionInputTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))" +
                "GROUP BY window_start, window_end"
        );
        Table hoppingWindow = tableEnv.sqlQuery("Select window_start, window_end, COUNT(actionid) AS number_of_results "+
                "FROM TABLE(HOP(TABLE DeviceActionInputTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES))" +
                "GROUP BY window_start, window_end"
        );
        Table cumulateWindow = tableEnv.sqlQuery("Select window_start, window_end, COUNT(actionid) AS number_of_results "+
                "FROM TABLE(CUMULATE(TABLE DeviceActionInputTable, DESCRIPTOR(rowtime), INTERVAL '2' MINUTES, INTERVAL '10' MINUTES))" +
                "GROUP BY window_start, window_end"
        );
        Table groupAggregation = tableEnv.sqlQuery("select Count(*) From DeviceActionInputTable group by userid");
        Table windowAggregation = tableEnv.sqlQuery("Select window_start, window_end, COUNT(actionid) AS number_of_results "+
                "FROM TABLE(TUMBLE(TABLE DeviceActionInputTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))" +
                "GROUP BY window_start, window_end"
        );
        Table overAggregation = tableEnv.sqlQuery(" Select actionid, userid, deviceid, event_name, COUNT(actionid) "+
                "OVER (PARTITION BY deviceid ORDER BY rowtime) AS number_of_results " +
                "FROM DeviceActionInputTable"
        );

        Table regularJoin = tableEnv.sqlQuery("SELECT * FROM DeviceActionInputTable " +
               "INNER JOIN DeviceInputTable "+
                "ON DeviceActionInputTable.deviceid = DeviceInputTable.deviceid"
        );

        Table intervalJoin = tableEnv.sqlQuery("SELECT * FROM DeviceActionInputTable, DeviceInputTable "+
                "WHERE DeviceActionInputTable.deviceid = DeviceInputTable.deviceid " +
                "AND DeviceActionInputTable.rowtime >= DeviceInputTable.rowtime"
        );

        Table windowJoin = tableEnv.sqlQuery("SELECT L.event_name, R.name, R.deviceid, L.window_start, L.window_end FROM  ( "+
                "SELECT * FROM TABLE(TUMBLE(TABLE DeviceActionInputTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))) L " +
                "INNER JOIN (SELECT * FROM TABLE(TUMBLE(TABLE DeviceInputTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))) R "+
                "ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.deviceid = R.deviceid"
        );

        Table whereFilter = tableEnv.sqlQuery("SELECT * FROM DeviceActionInputTable WHERE DeviceActionInputTable.userid = 1");

        Table havingFilter = tableEnv.sqlQuery("SELECT deviceid, COUNT(*) as numberOfResultsForDevice FROM DeviceActionInputTable GROUP BY DeviceActionInputTable.deviceid HAVING DeviceActionInputTable.deviceid > 1");

        Table topNFilter = tableEnv.sqlQuery("SELECT * FROM (" +
                " SELECT *, ROW_NUMBER() OVER (ORDER BY rowtime DESC) AS rownum FROM DeviceActionInputTable) "+
                " WHERE rownum <= 2");

        Table patternRecognition = tableEnv.sqlQuery("SELECT T.aid, T.bid FROM DeviceActionInputTable " +
                " MATCH_RECOGNIZE ( " +
                        "PARTITION BY userid " +
                        "ORDER BY rowtime "+
                        "MEASURES "+
                            "A.deviceid AS aid, "+
                            "B.deviceid AS bid "+
                        "ONE ROW PER MATCH "+
                        "PATTERN (A B) "+
                        "DEFINE " +
                            "A AS deviceid = 1, "+
                            "B AS deviceid = 2 "+
                        ") AS T"
);

        patternRecognition.execute().print();
        env.execute("Window WordCount");

    }

}