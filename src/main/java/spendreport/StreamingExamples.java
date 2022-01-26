package spendreport;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

public class StreamingExamples {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        tableEnv.executeSql("CREATE TABLE MyUserTable (\n" +
                "  nameid INT, \n" +
                "  PRIMARY KEY (nameid) NOT ENFORCED,\n" +
                "  `name` STRING,\n" +
                "  `timeValue` TIMESTAMP(3), \n" +
                "  WATERMARK FOR timeValue AS timeValue , \n" +
                "  `proctime` AS PROCTIME()   \n" +
                ") WITH (\n" +
                "  'connector' = 'filesystem',\n" +
                "  'path' = 'C:\\Users\\jakob\\flink\\flink-1.14.3\\frauddetection\\src\\main\\java\\spendreport\\input.txt',\n" +
                "  'format' = 'json', \n" +
                "  'json.fail-on-missing-field' = 'true'\n" +
                ")");


        Table projection = tableEnv.sqlQuery("SELECT name From MyUserTable");
        Table tumblingWindow = tableEnv.sqlQuery("Select window_start, window_end, COUNT(nameid) AS number_of_results "+
                        "FROM TABLE(TUMBLE(TABLE MyUserTable, DESCRIPTOR(timeValue), INTERVAL '10' MINUTES))" +
                        "GROUP BY window_start, window_end"
                );
        Table hoppingWindow = tableEnv.sqlQuery("Select window_start, window_end, COUNT(nameid) AS number_of_results "+
                "FROM TABLE(HOP(TABLE MyUserTable, DESCRIPTOR(timeValue), INTERVAL '5' MINUTES, INTERVAL '10' MINUTES))" +
                "GROUP BY window_start, window_end"
        );
        Table cumulateWindow = tableEnv.sqlQuery("Select window_start, window_end, COUNT(nameid) AS number_of_results "+
                "FROM TABLE(CUMULATE(TABLE MyUserTable, DESCRIPTOR(timeValue), INTERVAL '2' MINUTES, INTERVAL '10' MINUTES))" +
                "GROUP BY window_start, window_end"
        );
        Table groupAggregation = tableEnv.sqlQuery("select Count(*) From MyUserTable group by nameid");
        hoppingWindow.execute().print();
        cumulateWindow.execute().print();

        /*DataStream<Row> resultStream = tableEnv.toDataStream(groupAggregation);
        resultStream.print();
        env.execute();*/
    }
}
