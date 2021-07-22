package com.meituan.flinkdemo.Table;

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
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class LocalTableWindow {
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

        table.printSchema();

        Table windowTable = table
                .window(Tumble.over("1.seconds").on("pt").as("window"))
                .groupBy("hbdm,window")
                .select("hbdm,sum(num),min(ts),max(ts)");


//        Table table = tableEnvironment
////                .fromDataStream(tupleStream,"f0,f1,f2,pt.proctime")
//                .fromDataStream(tupleStream,"f0.rowtime as ts,f1,f2")
//                .renameColumns("f1 as hbdm, f2 as num");
//
//        table.printSchema();
//
//        Table windowTable = table
//                .window(Tumble.over("1.seconds").on("ts").as("Window"))
//                .groupBy("hbdm,Window")
//                .select("hbdm,sum(num),min(ts),max(ts)");



        DataStream<Tuple2<Boolean,Row>> resultStream = tableEnvironment.toRetractStream(windowTable, org.apache.flink.types.Row.class);
        resultStream.addSink(new FlinkKafkaProducer<Tuple2<Boolean, Row>>("sink", new SerializationSchema<Tuple2<Boolean, Row>>() {
            @Override
            public byte[] serialize(Tuple2<Boolean, Row> element) {
                return (element.f0.toString() + "," + element.f1.toString()).getBytes(StandardCharsets.UTF_8);
            }
        },prop2));

        env.execute("");
    }
}

