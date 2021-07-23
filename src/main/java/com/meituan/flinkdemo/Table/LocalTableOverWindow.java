package com.meituan.flinkdemo.Table;

import com.alibaba.fastjson.JSONObject;
import com.meituan.flinkdemo.Entity.Rate;
import com.meituan.flinkdemo.Utils.SetKafkaConsumer;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.table.api.*;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @author hujian
 */
public class LocalTableOverWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(1000L);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, environmentSettings);

        Properties prop2 = new Properties();
        prop2.put("bootstrap.servers", "localhost:9092");

        FlinkKafkaConsumer consumer = SetKafkaConsumer.getKafkaConsumer();
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
                .fromDataStream(tupleStream, "f0,f1,f2, pt.proctime")
                .renameColumns("f0 as ts,f1 as hbdm, f2 as num");

        table.printSchema();

        Table windowTable = table
                .window(Over
                        .partitionBy("hbdm")
                        .orderBy("pt")
//                        .preceding("UNBOUNDED_RANGE")
//                        .preceding("10.seconds")
                        .preceding("10.rows")
//                        .following("CURRENT_RANGE")
                        .as("w"))
                .select("hbdm, num.sum over w");


        DataStream<Tuple2<Boolean, Row>> resultStream = tableEnvironment.toRetractStream(windowTable, org.apache.flink.types.Row.class);
        resultStream.addSink(new FlinkKafkaProducer<>("sink",
//                new SerializationSchema<Tuple2<Boolean, Row>>() {
//                    @Override
//                    public byte[] serialize(Tuple2<Boolean, Row> element) {
//                        return (element.f0.toString() + "," + element.f1.toString()).getBytes(StandardCharsets.UTF_8);
//                    }
//                },
                new KafkaSerializationSchema<Tuple2<Boolean,Row>>(){
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<Boolean, Row> booleanRowTuple2, @Nullable Long aLong) {
                        return new ProducerRecord<>("sink",(booleanRowTuple2.f0.toString() + booleanRowTuple2.f1.toString()).getBytes(StandardCharsets.UTF_8));
                    }
                },
                prop2,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        ));

        env.execute("");
    }
}
