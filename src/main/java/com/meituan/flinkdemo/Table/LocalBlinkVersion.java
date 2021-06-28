package com.meituan.flinkdemo.Table;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
public class LocalBlinkVersion {
    public static void main(String[] args) {
        // blink 版本的流式查询
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings settings = EnvironmentSettings
//                .newInstance()
//                .useBlinkPlanner()
//                .inStreamingMode()
//                .build();
//        TableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);

        // blink 版本的批式处理
        EnvironmentSettings bsettings = EnvironmentSettings
                .newInstance()
                .useBlinkPlanner()
                .inBatchMode()
                .build();
        TableEnvironment btableEnv = TableEnvironment.create(bsettings);
    }
}
