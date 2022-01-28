package spendreport;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.util.Properties;




public class Example {
    //URL of the broker
    public static final String URL = "tcp://broker.mqttdashboard.com";
    //CLIENT_ID for the first topic
    public static final String CLIENT_ID1 = "clientId-7lu4jy22Ar";
    //CLIENT_ID for the second topic
    public static final String CLIENT_ID2 = "clientId-7lu4jy22Af";
    //first topic to subscribe to
    public static final String TOPIC = "testtopic/jakob";
    //second topic to subscribe to
    public static final String TOPIC2 = "testtopic/device";


    public static void main(String[] args) throws Exception {
        StreamTableEnvironment tableEnv;
        Table executedTable;
        //switch statement to map the first parameter to the correct environment
        switch (args[0]){
            case "testing":
                tableEnv = initializeTestingEnvironment();
                break;
            case "live":
                tableEnv = initializeLiveEnvironment();
                break;
            default:
                throw new IllegalArgumentException("Please provide one of the following arguments for the environment: 'testing' 'live'");
        }

        //selects event_name column from incoming events at DeviceActionInputTable
        Table projection = tableEnv.sqlQuery("SELECT event_name FROM DeviceActionInputTable");
        //calculates number of events at DeviceActionInputTable for each 10 minute size tumbling window
        Table tumblingWindow = tableEnv.sqlQuery("SELECT window_start, window_end, COUNT(*) AS number_of_results "+
                "FROM TABLE(TUMBLE(TABLE DeviceActionInputTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))" +
                "GROUP BY window_start, window_end"
        );
        //calculates number of events at DeviceActionInputTable for each 10 minute size 5 minute slide hopping  window
        Table hoppingWindow = tableEnv.sqlQuery("SELECT window_start, window_end, COUNT(*) AS number_of_results "+
                "FROM TABLE(HOP(TABLE DeviceActionInputTable, DESCRIPTOR(rowtime), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES))" +
                "GROUP BY window_start, window_end"
        );
        //calculates number of events at DeviceActionInputTable for each 10 minute size 2 minute step cumulate window
        Table cumulateWindow = tableEnv.sqlQuery("SELECT window_start, window_end, COUNT(*) AS number_of_results "+
                "FROM TABLE(CUMULATE(TABLE DeviceActionInputTable, DESCRIPTOR(rowtime), INTERVAL '2' MINUTES, INTERVAL '10' MINUTES))" +
                "GROUP BY window_start, window_end"
        );
        //calculates the number of events at DeviceActionInputTable for every user
        Table groupAggregation = tableEnv.sqlQuery("SELECT COUNT(*) as number_of_results_per_user, userid FROM DeviceActionInputTable GROUP BY userid");
        //calculates the number of events at DeviceActionInputTable for every tumbling window
        Table windowAggregation = tableEnv.sqlQuery("SELECT window_start, window_end, COUNT(*) AS number_of_results "+
                "FROM TABLE(TUMBLE(TABLE DeviceActionInputTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))" +
                "GROUP BY window_start, window_end"
        );
        //calculates the number of events at DeviceActionInputTable over every deviceid
        Table overAggregation = tableEnv.sqlQuery("SELECT actionid, userid, deviceid, event_name, COUNT(*) "+
                "OVER (PARTITION BY deviceid ORDER BY rowtime) AS number_of_results_per_device " +
                "FROM DeviceActionInputTable"
        );
        //inner joins events at DeviceActionInputTable and DeviceInputTable over deviceid
        Table regularJoin = tableEnv.sqlQuery("SELECT da.deviceid, da.event_name, da.event_time as action_time, d.name, d.status, d.event_time as device_status_time FROM DeviceActionInputTable as da " +
               "INNER JOIN DeviceInputTable as d "+
                "ON da.deviceid = d.deviceid"
        );
        //interval joins events at DeviceActionInputTable and DeviceInputTable over deviceid for all events with event_time of DeviceActionInputTable >= event_time of DeviceInputTable
        Table intervalJoin = tableEnv.sqlQuery("SELECT da.deviceid, da.event_name, da.rowtime as action_time, d.name, d.status, d.rowtime as device_status_time FROM DeviceActionInputTable as da, DeviceInputTable as d "+
                "WHERE da.deviceid = d.deviceid " +
                "AND da.rowtime >= d.rowtime"
        );
        //inner joins events at DeviceActionInputTable and DeviceInputTable over deviceid that are in the same 10 minutes window
        Table windowJoin = tableEnv.sqlQuery("SELECT L.event_name, R.name, R.deviceid, L.window_start, L.window_end FROM  ( "+
                "SELECT * FROM TABLE(TUMBLE(TABLE DeviceActionInputTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))) L " +
                "INNER JOIN (SELECT * FROM TABLE(TUMBLE(TABLE DeviceInputTable, DESCRIPTOR(rowtime), INTERVAL '10' MINUTES))) R "+
                "ON L.window_start = R.window_start AND L.window_end = R.window_end AND L.deviceid = R.deviceid"
        );
        //filters events at DeviceActionInputTable to events with userid 1
        Table whereFilter = tableEnv.sqlQuery("SELECT * FROM DeviceActionInputTable WHERE DeviceActionInputTable.userid = 1");
        //filters aggregations at DeviceActionInputTable to aggregations with deviceid > 1
        Table havingFilter = tableEnv.sqlQuery("SELECT deviceid, COUNT(*) as numberOfResultsForDevice FROM DeviceActionInputTable GROUP BY DeviceActionInputTable.deviceid HAVING DeviceActionInputTable.deviceid > 1");
        //filters events at DeviceActionInputTable to last two arriving
        Table topNFilter = tableEnv.sqlQuery("SELECT * FROM (" +
                " SELECT *, ROW_NUMBER() OVER (ORDER BY rowtime DESC) AS rownum FROM DeviceActionInputTable) "+
                " WHERE rownum <= 2");
        //recognizes the pattern of two events at DeviceActionInputTable. First one with deviceid 1 and the second one with deviceid 2. The event_time of them has to be ascending.
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

        //switch statement to map the second Parameter to the associated query
        switch (args[1].toLowerCase()){
            case "projection":
                executedTable = projection;
                break;
            case "tumblingwindow":
                executedTable = tumblingWindow;
                break;
            case "hoppingwindow":
                executedTable = hoppingWindow;
                break;
            case "cumulatewindow":
                executedTable = cumulateWindow;
                break;
            case "groupaggregation":
                executedTable = groupAggregation;
                break;
            case "windowaggregation":
                executedTable = windowAggregation;
                break;
            case "overaggregation":
                executedTable = overAggregation;
                break;
            case "regularjoin":
                executedTable = regularJoin;
                break;
            case "intervaljoin":
                executedTable = intervalJoin;
                break;
            case "windowjoin":
                executedTable = windowJoin;
                break;
            case "wherefilter":
                executedTable = whereFilter;
                break;
            case "havingfilter":
                executedTable = havingFilter;
                break;
            case "topnfilter":
                executedTable = topNFilter;
                break;
            case "patternrecognition":
                executedTable = patternRecognition;
                break;
            default:
                throw new IllegalArgumentException("Please provide one of the following arguments for the sql example: 'projection' 'tumblingWindow' 'hoppingWindow' 'cumulateWindow' 'groupAggregation' 'windowAggregation' 'overAggregation' 'regularJoin' 'intervalJoin' 'windowJoin' 'whereFilter' 'havingFilter' 'topNFilter' 'patternRecognition'");

        }
        executedTable.execute().print();


    }

    /**
     * Generates a StreamTableEnvironment reading the data from the txt files given in resources.
     * @return a testing Environment to execute queries on.
    */
    private static StreamTableEnvironment initializeTestingEnvironment(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE DeviceActionInputTable (\n" +
                "  actionid INT, \n" +
                "  event_name STRING,\n" +
                "  userid INT, \n" +
                "  deviceid INT, \n" +
                "  event_time STRING, \n" +
                "  rowtime as CAST(event_time AS TIMESTAMP_LTZ(3)) , \n" +
                "  WATERMARK FOR rowtime AS rowtime \n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'target\\classes\\deviceActionInput.txt',\n" +
                "  'format' = 'json', \n" +
                "  'json.fail-on-missing-field' = 'true'\n" +
                ")");

        tableEnv.executeSql("CREATE TABLE DeviceInputTable (\n" +
                "  status STRING, \n" +
                "  name STRING,\n" +
                "  deviceid INT, \n" +
                "  event_time STRING, \n" +
                "  rowtime as CAST(event_time AS TIMESTAMP_LTZ(3)) , \n" +
                "  WATERMARK FOR rowtime AS rowtime \n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'target\\classes\\deviceInput.txt',\n" +
                "  'format' = 'json', \n" +
                "  'json.fail-on-missing-field' = 'true'\n" +
                ")");

        return tableEnv;
    }

    /**
     * Generates a StreamTableEnvironment reading the data from the subscribed topics. Data at this resource has to be JSON and formatted to map on the classes DeviceAction and Device.
     * @return a live mqtt broker Environment to execute queries on.
     */
    private static StreamTableEnvironment initializeLiveEnvironment(){
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

        DataStream<DeviceAction> deviceActionDataStream = env.addSource(new MQTTSourceFunction<DeviceAction>(prop1, DeviceAction.class)).returns(DeviceAction.class);
        DataStream<Device> deviceDataStream =  env.addSource(new MQTTSourceFunction<Device>(prop2, Device.class)).returns(Device.class);


        final Schema deviceActionSchema = Schema.newBuilder()
                .column("actionid", DataTypes.INT())
                .column("userid", DataTypes.INT())
                .column("deviceid", DataTypes.INT())
                .column("event_name", DataTypes.STRING())
                .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
                .watermark("rowtime", "rowtime")
                .build();
        Table deviceActionInputTable = tableEnv.fromDataStream(deviceActionDataStream, deviceActionSchema);
        tableEnv.createTemporaryView("DeviceActionInputTable", deviceActionInputTable);

        final Schema deviceSchema = Schema.newBuilder()
                .column("deviceid", DataTypes.INT())
                .column("name", DataTypes.STRING())
                .column("status", DataTypes.STRING())
                .columnByExpression("rowtime", "CAST(event_time AS TIMESTAMP_LTZ(3))")
                .watermark("rowtime", "rowtime")
                .build();
        Table deviceInputTable = tableEnv.fromDataStream(deviceDataStream, deviceSchema);
        tableEnv.createTemporaryView("DeviceInputTable", deviceInputTable);
        return tableEnv;
    }

}