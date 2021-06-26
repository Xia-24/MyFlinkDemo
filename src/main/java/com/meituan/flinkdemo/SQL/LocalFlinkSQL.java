package com.meituan.flinkdemo.SQL;


import com.alibaba.fastjson.JSONObject;
import com.meituan.flinkdemo.Entity.Rate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.util.Properties;

public class LocalFlinkSQL {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env, environmentSettings);
//        tableEnvironment
//                .connect(
//                        new Kafka()
//                                .version("universal")
//                                .topic("test")
//                                .startFromLatest()
//                                .property("zookeeper.connect", "localhost:2181")
//                                .property("bootstrap.servers", "localhost:9092")
//                )
//                .withFormat(
//                        new Json()
//                                .failOnMissingField(true)
//                                .deriveSchema()
//                )
//                .withSchema(
//                        new Schema()
//                                .field("timestamp", Types.LONG)
//                                .field("hbdm", Types.STRING)
//                                .field("num", Types.INT)
//                )
//                .inAppendMode()
//                .registerTableSource("table_rate");
//        StreamTableEnvironment tableEnvironment2 = StreamTableEnvironment.create(env,environmentSettings);
//        String sql = "select * from table_rate";
//        Table table = tableEnvironment.sqlQuery(sql);
//        table.printSchema();
//        DataStream<Row> dataStream = tableEnvironment.toAppendStream(table, Row.class);
//        dataStream.print();

        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group1");
        props.put("auto.offset.reset", "latest");
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("test", new SimpleStringSchema(), props);
        DataStream<String> stream = env.addSource(consumer);
//        DataStream<Rate> dataStream = stream
//                .map((MapFunction<String, Rate>) value -> {
////            System.out.println(value);
//            return JSONObject.parseObject(value,Rate.class);
//        });
        DataStream<Tuple3<Long, String, Integer>> tupleStream = stream
                .map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
                    @Override
                    public Tuple3<Long, String, Integer> map(String value) throws Exception {
                        Rate rate = JSONObject.parseObject(value, Rate.class);
                        System.out.println("=====" + rate.toString());
                        return Tuple3.of(rate.getTimestamp(), rate.getHbdm(), rate.getNum());
                    }
                });

        Table tableOriginal = tableEnvironment
                .fromDataStream(tupleStream)
                .renameColumns("f0 as ts, f1 as hbdm, f2 as num");

//        tableEnvironment.registerTable("tableOriginal",tableOriginal);

        System.out.println("====schema");
        tableOriginal.printSchema();
        Table selectTable = tableOriginal
//                .select("f1")
                .filter("hbdm === 'CNY'");

        //Table selectTable = tableEnvironment.sqlQuery("SELECT * FROM " + tableOriginal + " WHERE f1 = 'CNY'");
//        Table selectTable = tableEnvironment.sqlQuery("SELECT * FROM  tableOriginal  WHERE hbdm = 'CNY'");

//        TupleTypeInfo<Tuple3<Long,String,Integer>> tupleType = new TupleTypeInfo<>(
//                Types.LONG,
//                Types.STRING,
//                Types.INT);
//        DataStream<Tuple3<Long,String,Integer>> resultStream = tableEnvironment.toAppendStream(selectTable,tupleType);
//        DataStream<Rate> resultStream = tableEnvironment.toAppendStream(table,Rate.class);

        DataStream<Row> resultStream = tableEnvironment.toAppendStream(selectTable, Row.class);

        Properties prop2 = new Properties();
        prop2.put("bootstrap.servers", "localhost:9092");

        DataStream<String> sinkstream = resultStream
                .map((MapFunction<Row, String>) value -> {
                    System.out.println("=====");
                    System.out.println(value);
//                        System.out.println(value.getField(0));
//                        System.out.println(value.getField(1));
//                        System.out.println(value.getField(2));

                    return value.toString();
                });
        sinkstream.addSink(new FlinkKafkaProducer<>("sink", new SimpleStringSchema(), prop2));


        env.execute(" ");
    }
}
