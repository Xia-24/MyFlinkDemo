package com.meituan.flinkdemo;

import com.meituan.flink.common.config.JobConf;
import com.meituan.flink.common.config.KafkaTopic;
import com.meituan.flink.common.kafka.MTKafkaConsumer010;
import com.meituan.flink.common.kafka.MTKafkaProducer010;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
//import org.rocksdb.BloomFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class FlinkJoin {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJoin.class);
    private static final String READ_KAFKA_TOPIC1 = "app.flinkrate";
    private static final String READ_KAFKA_TOPIC2 = "app.flinkorder";
    private static final String WRITE_KAFKA_TOPIC = "app.join";


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        MTKafkaConsumer010 mtKafkaConsumer010 = new MTKafkaConsumer010(args);
        DataStream<Tuple3<Long,String,Integer>> ratestream = null;
        Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> consumerEntry = mtKafkaConsumer010
                .build(new DeserializationSchema() {
                    @Override
                    public Tuple3<Long,String,Integer> deserialize(byte[] bytes) throws IOException {
                        String[] res = new String(bytes).split(",");
                        Long timestamp = Long.valueOf(res[0]);
                        String dm = res[1];
                        Integer value = Integer.valueOf(res[2]);
                        return Tuple3.of(timestamp,dm,value);
                    }

                    @Override
                    public boolean isEndOfStream(Object o) {
                        return false;
                    }

                    @Override
                    public TypeInformation getProducedType() {
                        return TypeInformation.of(new TypeHint<Tuple3<Long,String,Integer>>() {
                        });
                    }
                })
                .getConsumerByName(READ_KAFKA_TOPIC1, "xr_inf_namespace");
        ratestream = env.addSource(consumerEntry.getValue())
                .setParallelism(1)
                .uid(READ_KAFKA_TOPIC1)
                .name(READ_KAFKA_TOPIC1);

        DataStream<Tuple5<Long,String,Integer,String,Integer>> orderstream = null;
        Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> consumerEntry2 = mtKafkaConsumer010
                .build(new DeserializationSchema() {
                    @Override
                    public Object deserialize(byte[] bytes) throws IOException {
                        String[] res = new String(bytes).split(",");
                        if(res.length == 5){
                            Long timestamp = Long.valueOf(res[0]);
                            String catlog = res[1];
                            Integer subcat = Integer.valueOf(res[2]);
                            String dm = res[3];
                            Integer value = Integer.valueOf(res[4]);
                            return Tuple5.of(timestamp,catlog,subcat,dm,value);
                        }
                        else {
                            return Tuple5.of(0,0,0,0,0);
                        }


                    }

                    @Override
                    public boolean isEndOfStream(Object o) {
                        return false;
                    }

                    @Override
                    public TypeInformation getProducedType() {
                        return TypeInformation.of(new TypeHint<Tuple5<Long,String,Integer,String,Integer>>() {
                        });
                    }
                })
                .getConsumerByName(READ_KAFKA_TOPIC2, "xr_inf_namespace");
        orderstream = env.addSource(consumerEntry2.getValue())
                .setParallelism(1)
                .uid(READ_KAFKA_TOPIC2)
                .name(READ_KAFKA_TOPIC2);

        KeyedStream<Tuple3<Long,String,Integer>, Tuple> keyedstream = ratestream.keyBy(0,1);

        SingleOutputStreamOperator<Tuple3<Long,String,Integer>> res = keyedstream.process(new KeyedProcessFunction<Tuple, Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>>() {
            //保存分组数据去重后用户ID的布隆过滤器
            private transient ValueState<BloomFilter> bloomState = null;
            //保存去重后总人数的state，加transient禁止参与反序列化
            private transient ValueState<Integer> timeCountState = null;
//            //保存活动的点击数的state
//            private transient ValueState<Integer> clickState = null;

            @Override
            public void processElement(Tuple3<Long, String, Integer> input, Context context, Collector<Tuple3<Long, String, Integer>> collector) throws Exception {
                Long timestamp = input.f0;
                String hbdm = input.f1;
                Integer num = input.f2;
                String str = Long.toString(timestamp/1000) + hbdm;


                BloomFilter bloomFilter = bloomState.value();
                Integer timecount = timeCountState.value();
//                Integer hbdmcount = clickState.value();

                if(bloomFilter == null){
                    bloomFilter = BloomFilter.create(Funnels.unencodedCharsFunnel(),10000000);
                    timecount = 0;
                }
                if(!bloomFilter.mightContain(str)){
                    bloomFilter.put(str);
                    timecount += 1;
                }
                bloomState.update(bloomFilter);
                timeCountState.update(timecount);
                collector.collect(Tuple3.of(timestamp,hbdm,num));


            }

            @Override
            public void open(Configuration parameters) throws Exception{
                ValueStateDescriptor<BloomFilter> bloomDescriptor = new ValueStateDescriptor<>(
                        "bl",
                        TypeInformation.of(new TypeHint<BloomFilter>() {
                        })
                );
                ValueStateDescriptor<Integer> timeCountDescriptor = new ValueStateDescriptor<>(
                        "time-cnt",
                        Integer.class
                );
                bloomState = getRuntimeContext().getState(bloomDescriptor);
                timeCountState = getRuntimeContext().getState(timeCountDescriptor);
            }
        });

        MTKafkaProducer010 mtKafkaProducer010 = new MTKafkaProducer010(args);
        mtKafkaProducer010.build(new SimpleStringSchema());
        Map<KafkaTopic, FlinkKafkaProducer010> topic2producers = mtKafkaProducer010.getTargetTopicsToProducers();

        DataStream<String> newstream = res.map(new MapFunction<Tuple3<Long, String, Integer>, String>() {
            @Override
            public String map(Tuple3<Long, String, Integer> tp3) throws Exception {
                return tp3.toString();
            }
        });

        // 添加一个Kafka Data Sink
        for(Map.Entry<KafkaTopic,FlinkKafkaProducer010> entry:topic2producers.entrySet()){
            newstream.addSink(entry.getValue())
                    .setParallelism(entry.getKey().getParallelism())
                    .uid(WRITE_KAFKA_TOPIC).name(WRITE_KAFKA_TOPIC);
        }

        env.execute((new JobConf(args)).getJobName());




//        Long delay = 1000L;
//        DataStream<Tuple3<Long,String,Integer>> rateTimedString = ratestream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.milliseconds(delay)) {
//            @Override
//            public long extractTimestamp(Tuple3<Long, String, Integer> longStringIntegerTuple3) {
//                return (Long)longStringIntegerTuple3.getField(0);
//            }
//        });
//        DataStream<Tuple5<Long,String,Integer,String,Integer>> orderTimedStream = orderstream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Long, String, Integer, String, Integer>>() {
//            @Override
//            public long extractAscendingTimestamp(Tuple5<Long, String, Integer, String, Integer> longStringIntegerStringIntegerTuple5) {
//                return (Long)longStringIntegerStringIntegerTuple5.getField(0);
//            }
//        });
//        DataStream<Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer>> joinedstream = orderTimedStream.join(rateTimedString).where(new KeySelector<Tuple5<Long, String, Integer, String, Integer>,String>() {
//            @Override
//            public String getKey(Tuple5<Long, String, Integer, String, Integer> longStringIntegerStringIntegerTuple5) throws Exception {
//                return longStringIntegerStringIntegerTuple5.getField(3).toString();
//            }
//        }).equalTo(new KeySelector<Tuple3<Long, String, Integer>, String>() {
//            @Override
//            public String getKey(Tuple3<Long, String, Integer> longStringIntegerTuple3) throws Exception {
//                return longStringIntegerTuple3.getField(1).toString();
//            }
//        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
//                .apply(new JoinFunction<Tuple5<Long, String, Integer, String, Integer>, Tuple3<Long, String, Integer>, Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer>>() {
//                    @Override
//                    public Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer> join(Tuple5<Long, String, Integer, String, Integer> first, Tuple3<Long, String, Integer> second) throws Exception {
//                        Integer res = (Integer) second.getField(2)*(Integer) first.getField(4);
//                        return Tuple9.of(first.f0,first.f1,first.f2,first.f3,first.f4,second.f0,second.f1,second.f2,res);
//                    }
//                });
//        joinedstream.print();
//
//
//
//        MTKafkaProducer010 mtKafkaProducer010 = new MTKafkaProducer010(args);
//        mtKafkaProducer010.build(new SimpleStringSchema());
//        Map<KafkaTopic, FlinkKafkaProducer010> topic2producers = mtKafkaProducer010.getTargetTopicsToProducers();
//
//        DataStream<String> newstream = joinedstream.map(new MapFunction<Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer>, String>() {
//            @Override
//            public String map(Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer> longStringIntegerStringIntegerLongStringIntegerIntegerTuple9) throws Exception {
//                return longStringIntegerStringIntegerLongStringIntegerIntegerTuple9.toString();
//            }
//        });
//
//        // 添加一个Kafka Data Sink
//        for(Map.Entry<KafkaTopic,FlinkKafkaProducer010> entry:topic2producers.entrySet()){
//            newstream.addSink(entry.getValue())
//                    .setParallelism(entry.getKey().getParallelism())
//                    .uid(WRITE_KAFKA_TOPIC).name(WRITE_KAFKA_TOPIC);
//        }
//
//        env.execute((new JobConf(args)).getJobName());



    }
}
