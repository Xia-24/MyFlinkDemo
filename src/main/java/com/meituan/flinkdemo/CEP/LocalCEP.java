package com.meituan.flinkdemo.CEP;

import com.alibaba.fastjson.JSON;
import com.meituan.flinkdemo.Entity.Rate;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.log4j.*;

public class LocalCEP {
    public static void main(String[] args) throws Exception {
        Logger.getLogger("Kafka").setLevel(Level.ERROR);
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group1");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        OutputTag<String> outputTag = new OutputTag<String>("outputTag"){};

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
                Rate rate = new Rate(timestamp,dm,value);
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
        },props);

        DataStream<Rate> ratestream = env.addSource(consumer)
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Rate>(Time.milliseconds(1000)) {
                    @Override
                    public long extractTimestamp(Rate rate) {
                        return rate.getTimestamp();
                    }
                });
        KeyedStream<Rate,String> keyedStream = ratestream
                .keyBy(r->r.getHbdm());
//        Pattern<Rate,Rate> pattern = Pattern
//                .<Rate>begin("CNY")
//                .where(new SimpleCondition<Rate>() {
//                    @Override
//                    public boolean filter(Rate value) throws Exception {
//                        return value.getHbdm().equals("CNY");
//                    }
//                })
//                .next("CNY2")
//                .where(new SimpleCondition<Rate>() {
//                    @Override
//                    public boolean filter(Rate value) throws Exception {
//                        return value.getHbdm().equals("CNY");
//                    }
//                })
//                .within(Time.seconds(10));
        Pattern<Rate,Rate> pattern = Pattern
                .<Rate> begin("CNY")
                .times(5,6)
                .where(new SimpleCondition<Rate>() {
                    @Override
                    public boolean filter(Rate value) throws Exception {
                        return value.getHbdm().equals("CNY");
                    }
                });

        Pattern<Rate,Rate> pattern1 = Pattern
                .<Rate> begin("USD")
                .times(5,6)
                .where(new SimpleCondition<Rate>() {
                    @Override
                    public boolean filter(Rate value) throws Exception {
                        return value.getHbdm().equals("USD");
                    }
                });
        OutputTag<Rate> sideOutputTag = new OutputTag<Rate>("side-output-tag"){};
        SingleOutputStreamOperator<Rate> stringSingleOutputStreamOperator = ratestream
                .process(new ProcessFunction<Rate,Rate>() {
                    @Override
                    public void processElement(Rate value, Context ctx, Collector<Rate> out) throws Exception {
                        if(value.getHbdm().equals("CNY")){
                            String str = JSON.toJSONString(value);
                            ctx.output(sideOutputTag,value);
                        }
                        else{
                            out.collect(value);
                        }
                    }
                });
        DataStream<Rate> sideStream = stringSingleOutputStreamOperator.getSideOutput(sideOutputTag);
        PatternStream<Rate> patternStream = CEP.pattern(sideStream,pattern);


        SingleOutputStreamOperator<String> sinkstream = patternStream
                .flatSelect(outputTag, new PatternFlatTimeoutFunction<Rate, String>() {
                            @Override
                            public void timeout(Map<String, List<Rate>> map, long l, Collector<String> collector) throws Exception {
                                Rate rate = map.get("CNY").iterator().next();
                                collector.collect(rate.getTimestamp() + "   is timeout");
                            }
                        },
                        new PatternFlatSelectFunction<Rate, String>() {
                            @Override
                            public void flatSelect(Map<String, List<Rate>> map, Collector<String> collector) throws Exception {
                                System.out.println("size ================†∂");
                                System.out.println(map.get("CNY").size());
                                Rate rate = map.get("CNY").iterator().next();
                                collector.collect(JSON.toJSONString(rate));
//                                collector.collect(rate.getTimestamp() + "  is selected");
                            }
                        });
        stringSingleOutputStreamOperator.print();
        stringSingleOutputStreamOperator.getSideOutput(outputTag).print(">>>>");

        Properties prop2 = new Properties();
        prop2.put("bootstrap.servers","localhost:9092");


        sinkstream
                .addSink(new FlinkKafkaProducer<>("sink", new SimpleStringSchema(), prop2))
                .setParallelism(1).name("sink");


        env.execute();


    }
}
