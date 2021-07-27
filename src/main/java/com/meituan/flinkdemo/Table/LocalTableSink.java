package com.meituan.flinkdemo.Table;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.meituan.flinkdemo.Entity.Rate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSink;
import org.apache.flink.api.java.io.jdbc.JDBCAppendTableSinkBuilder;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import java.util.Properties;

public class LocalTableSink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, environmentSettings);

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group1");
        props.put("auto.offset.reset", "latest");
        Properties prop2 = new Properties();
        prop2.put("bootstrap.servers", "localhost:9092");

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("test", new SimpleStringSchema(), props);
        DataStream<String> stream = env.addSource(consumer);
        DataStream<Tuple3<Long, String, Integer>> tupleStream = stream
                .map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
                    @Override
                    public Tuple3<Long, String, Integer> map(String value) throws Exception {
                        Rate rate = JSONObject.parseObject(value, Rate.class);
                        System.out.println("=====" + JSON.toJSONString(rate));
                        return Tuple3.of(rate.getTimestamp(), rate.getHbdm(), rate.getNum());
                    }
                });

        Table tableOriginal = tableEnvironment
                .fromDataStream(tupleStream)
                .renameColumns("f0 as ts, f1 as hbdm, f2 as num");
//        String[] fieldNames = { "ts", "hbdm","num" };
//        TypeInformation[] fieldTypes = { Types.LONG, Types.STRING ,Types.INT };
//        tableEnvironment
//                .registerTableSink("sink",new CsvTableSink("./sink")
//                .configure(fieldNames,fieldTypes));

        JDBCAppendTableSink sink =  new JDBCAppendTableSinkBuilder()
                .setDBUrl("jdbc:mysql://localhost:3306/mydatabase")
                .setDrivername("com.mysql.jdbc.Driver")
                .setUsername("root")
                .setPassword("123456")
                .setBatchSize(1000)
                .setQuery("REPLACE INTO rate(ts,hbdm,num) values(?,?,?)")
                .setParameterTypes(new TypeInformation[]{Types.LONG,Types.STRING,Types.INT})
                .build();
        tableEnvironment.registerTableSink("sink",
                new String[]{"ts","hbdm","num"},
                new TypeInformation[]{Types.LONG,Types.STRING,Types.INT},
                sink);


        tableOriginal.insertInto("sink");
        env.execute("");
    }
}
