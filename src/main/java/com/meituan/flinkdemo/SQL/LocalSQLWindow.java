package com.meituan.flinkdemo.SQL;

import com.alibaba.fastjson.JSONObject;
import com.meituan.flinkdemo.Entity.Rate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class LocalSQLWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
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
//                        System.out.println("=====" + JSON.toJSONString(rate));
                        return Tuple3.of(rate.getTimestamp(), rate.getHbdm(), rate.getNum());
                    }
                })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.seconds(1)) {
                    @Override
                    public long extractTimestamp(Tuple3<Long, String, Integer> element) {
                        return element.f0;
                    }
                });
        // proctime 处理时间 rawtime 事件时间
        Table table = tableEnvironment
                .fromDataStream(tupleStream,"f0,f1,f2, pt.proctime")
                .renameColumns("f0 as ts,f1 as hbdm, f2 as num");

        tableEnvironment.registerTable("rate",table);

        // 滚动窗口
        // Table windowTable = tableEnvironment.sqlQuery("select hbdm, TUMBLE_START(pt,INTERVAL '1' SECOND) AS rStart , TUMBLE_END(pt,INTERVAL '1' SECOND) AS rEnd , TUMBLE_PROCTIME(pt,INTERVAL '1' SECOND) AS pTime ,COUNT(num) AS cnt_num from rate GROUP BY TUMBLE(pt,INTERVAL '1' SECOND ),hbdm");
        // 滑动窗口
        //Table windowTable = tableEnvironment.sqlQuery("select hbdm, HOP_START(pt,INTERVAL '1' SECOND , INTERVAL '2' SECOND ) AS rStart , HOP_PROCTIME(pt,INTERVAL '1' SECOND, INTERVAL '2' SECOND ) AS pTime ,COUNT(num) AS cnt_num from rate GROUP BY HOP(pt,INTERVAL '1' SECOND , INTERVAL '2' SECOND ),hbdm");
        // 会话窗口
        Table windowTable = tableEnvironment.sqlQuery("select hbdm, SESSION_START(pt,INTERVAL '1' SECOND ) AS sStart , SESSION_END(pt,INTERVAL '1' SECOND ) AS sEnd, COUNT(num) AS cnt_num from rate GROUP BY SESSION(pt,INTERVAL '1' SECOND ),hbdm");


//        Table table1 = tableEnv.sqlQuery("SELECT userId, TUMBLE_START(rowTime, INTERVAL '1' MINUTE) AS rStart, COUNT(1) AS countNum from Users GROUP BY TUMBLE(rowTime, INTERVAL '1' MINUTE),userId");
//        tableEnv.toAppendStream(table1, TypeInformation.of(Row.class)).print("tumble:");
//
//        Table table2 = tableEnv.sqlQuery("SELECT userId,TUMBLE_START(rowTime, INTERVAL '1' MINUTE) AS rStart, TUMBLE_END(rowTime, INTERVAL '1' MINUTE) AS rEnd, TUMBLE_ROWTIME(rowTime,INTERVAL '1' MINUTE) AS pTime,COUNT(1) AS countNum from Users GROUP BY TUMBLE(rowTime, INTERVAL '1' MINUTE),userId");
//        tableEnv.toAppendStream(table2, TypeInformation.of(Row.class)).print("tumble2:");
//
//        Table table3 = tableEnv.sqlQuery("SELECT userId,HOP_START(rowTime, INTERVAL '1' MINUTE,INTERVAL '5' MINUTE) as hStart,COUNT(1) AS countNum from Users GROUP BY HOP(rowTime,INTERVAL '1' MINUTE,INTERVAL '5' MINUTE),userId");
//        tableEnv.toAppendStream(table3, TypeInformation.of(Row.class)).print("hop:");
//
//        Table table4 = tableEnv.sqlQuery("SELECT userId,SESSION_START(rowTime,INTERVAL '1' MINUTE) AS sStart,COUNT(1) AS countNum from Users GROUP BY SESSION(rowTime,INTERVAL '1' MINUTE),userId");
//        tableEnv.toAppendStream(table4, TypeInformation.of(Row.class)).print("session:");



        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnvironment.toRetractStream(windowTable, org.apache.flink.types.Row.class);
        resultStream.addSink(new FlinkKafkaProducer<Tuple2<Boolean, Row>>("sink", new SerializationSchema<Tuple2<Boolean, Row>>() {
            @Override
            public byte[] serialize(Tuple2<Boolean, Row> element) {
                return (element.f0.toString() + "," + element.f1.toString()).getBytes(StandardCharsets.UTF_8);
            }
        },prop2));

        env.execute("");
    }
}
