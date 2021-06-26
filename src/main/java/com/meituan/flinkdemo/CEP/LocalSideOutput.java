package com.meituan.flinkdemo.CEP;

import com.alibaba.fastjson.JSON;
import com.meituan.flinkdemo.Entity.Rate;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.io.IOException;
import java.util.Properties;

public class LocalSideOutput {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group1");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        OutputTag<String> sideOutputTag = new OutputTag<String>("side-output-tag") {
        };
        OutputTag<String> outputTag = new OutputTag<String>("outputTag") {
        };

        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("test", new DeserializationSchema() {
            @Override
            public Rate deserialize(byte[] bytes) throws IOException {
                System.out.println(" test sout --------");
                String[] res = new String(bytes).split(",");
                System.out.println(res[0]);
                System.out.println(res[1]);
                System.out.println(res[2]);
                Long timestamp = Long.valueOf(res[0]);
                String dm = res[1];
                Integer value = Integer.valueOf(res[2]);
                Rate rate = new Rate(timestamp, dm, value);
                return rate;
            }

            @Override
            public boolean isEndOfStream(Object o) {
                return false;
            }

            @Override
            public TypeInformation getProducedType() {
                return TypeInformation.of(Rate.class);
            }
        }, props);
        DataStream<Rate> rateDataStream = env.addSource(consumer);
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = rateDataStream
                .process(new ProcessFunction<Rate, String>() {
                    @Override
                    public void processElement(Rate value, Context ctx, Collector<String> out) throws Exception {
                        if (value.getHbdm().equals("CNY")) {
                            String str = JSON.toJSONString(value);
                            ctx.output(sideOutputTag, str);
                        } else {
                            out.collect(JSON.toJSONString(value));
                        }
                    }
                });
        DataStream<String> sideStream = stringSingleOutputStreamOperator.getSideOutput(sideOutputTag);
        Properties prop2 = new Properties();
        prop2.put("bootstrap.servers", "localhost:9092");


        sideStream
                .addSink(new FlinkKafkaProducer<>("sink", new SimpleStringSchema(), prop2))
                .setParallelism(1).name("sink");


        env.execute();
    }
}
