//
//package com.meituan.flinkdemo.Table;
//
//import com.alibaba.fastjson.JSONObject;
//import com.meituan.flinkdemo.Entity.Rate;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SerializationSchema;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
//import org.apache.flink.table.api.EnvironmentSettings;
//import org.apache.flink.table.api.Table;
//import org.apache.flink.table.api.java.StreamTableEnvironment;
//import org.apache.flink.types.Row;
//
//import java.nio.charset.StandardCharsets;
//import java.util.Properties;
//
//public class LocalUpsertStream {
//    public static void main(String[] args) throws Exception {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
//        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);
//        Properties props = new Properties();
//        props.put("bootstrap.servers", "localhost:9092");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("group.id", "group1");
//        props.put("auto.offset.reset", "latest");
//        Properties prop2 = new Properties();
//        prop2.put("bootstrap.servers", "localhost:9092");
//        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("test",new SimpleStringSchema(),props);
//        FlinkKafkaProducer producer = new FlinkKafkaProducer("sink",new SimpleStringSchema(),prop2);
//
//        DataStream<String> stream = env.addSource(consumer);
//        DataStream<Tuple3<Long, String, Integer>> tupleStream = stream
//                .map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
//                    @Override
//                    public Tuple3<Long, String, Integer> map(String value) throws Exception {
//                        Rate rate = JSONObject.parseObject(value, Rate.class);
//                        System.out.println("=====" + rate.toString());
//                        return Tuple3.of(rate.getTimestamp(), rate.getHbdm(), rate.getNum());
//                    }
//                });
//        DataStream<Tuple2<String, Integer>> tuple2Stream = tupleStream
//                .map(new MapFunction<Tuple3<Long, String, Integer>, Tuple2<String, Integer>>() {
//                    @Override
//                    public Tuple2<String, Integer> map(Tuple3<Long, String, Integer> value) throws Exception {
//                        return Tuple2.of(value.f1,value.f2);
//                    }
//                });
//        Table tableOriginal = tableEnv
//                .fromDataStream(tuple2Stream)
//                .renameColumns("f0 as hbdm,f1 as num")
//                .;
//        tableEnv.registerTable("tableOriginal",tableOriginal);
//        tableOriginal.printSchema();
//
//
//
////        DataStream<Tuple2<Boolean, Row>> rowStream = tableEnv.toRetractStream(retractTable,Row.class);
////        rowStream.print();
//        DataStream<<Boolean,Row>> upsertStream = tableEnv.
//
//
//        env.execute("");
//
//    }
//}
