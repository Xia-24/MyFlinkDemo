package com.meituan.flinkdemo;

import com.meituan.flink.common.config.JobConf;
import com.meituan.flink.common.config.KafkaTopic;
import com.meituan.flink.common.kafka.MTKafkaConsumer010;
import com.meituan.flink.common.kafka.MTKafkaProducer010;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple9;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Random;

public class FlinkJoin {
    private static final Logger LOG = LoggerFactory.getLogger(FlinkJoin.class);
    private static final String READ_KAFKA_TOPIC1 = "app.flinkorder";
    private static final String READ_KAFKA_TOPIC2 = "app.flinkrate";
    private static final String WRITE_KAFKA_TOPIC = "app.join";


    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(1);

        MTKafkaConsumer010 mtKafkaConsumer010Rate = new MTKafkaConsumer010(args);

        mtKafkaConsumer010Rate.build(new DeserializationSchema() {
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
        });


        
        MTKafkaConsumer010 mtKafkaConsumer010Order = new MTKafkaConsumer010(args);
        mtKafkaConsumer010Order.build(new DeserializationSchema() {
            @Override
            public Object deserialize(byte[] bytes) throws IOException {
                String[] res = new String(bytes).split(",");
                Long timestamp = Long.valueOf(res[0]);
                String catlog = res[1];
                Integer subcat = Integer.valueOf(res[2]);
                String dm = res[3];
                Integer value = Integer.valueOf(res[4]);
                return Tuple5.of(timestamp,catlog,subcat,dm,value);

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
        });
        DataStream<Tuple3<Long,String,Integer>> ratestream = null;
        Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> consumerEntry = mtKafkaConsumer010Rate.getConsumerByName(READ_KAFKA_TOPIC1, "xr_inf_namespace");
        ratestream = env.addSource(consumerEntry.getValue())
                .setParallelism(1)
                .uid(READ_KAFKA_TOPIC1)
                .name(READ_KAFKA_TOPIC1);

        DataStream<Tuple5<Long,String,Integer,String,Integer>> orderstream = null;
        Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> consumerEntry2 = mtKafkaConsumer010Order.getConsumerByName(READ_KAFKA_TOPIC2, "xr_inf_namespace");
        orderstream = env.addSource(consumerEntry2.getValue())
                .setParallelism(1)
                .uid(READ_KAFKA_TOPIC2)
                .name(READ_KAFKA_TOPIC2);

        Long delay = 1000L;
        DataStream<Tuple3<Long,String,Integer>> rateTimedString = ratestream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.milliseconds(delay)) {
            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> longStringIntegerTuple3) {
                return (Long)longStringIntegerTuple3.getField(0);
            }
        });
        DataStream<Tuple5<Long,String,Integer,String,Integer>> orderTimedStream = orderstream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Long, String, Integer, String, Integer>>() {
            @Override
            public long extractAscendingTimestamp(Tuple5<Long, String, Integer, String, Integer> longStringIntegerStringIntegerTuple5) {
                return (Long)longStringIntegerStringIntegerTuple5.getField(0);
            }
        });
        DataStream<Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer>> joinedstream = orderTimedStream.join(rateTimedString).where(new KeySelector<Tuple5<Long, String, Integer, String, Integer>,String>() {
            @Override
            public String getKey(Tuple5<Long, String, Integer, String, Integer> longStringIntegerStringIntegerTuple5) throws Exception {
                return longStringIntegerStringIntegerTuple5.getField(3).toString();
            }
        }).equalTo(new KeySelector<Tuple3<Long, String, Integer>, String>() {
            @Override
            public String getKey(Tuple3<Long, String, Integer> longStringIntegerTuple3) throws Exception {
                return longStringIntegerTuple3.getField(1).toString();
            }
        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple5<Long, String, Integer, String, Integer>, Tuple3<Long, String, Integer>, Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer>>() {
                    @Override
                    public Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer> join(Tuple5<Long, String, Integer, String, Integer> first, Tuple3<Long, String, Integer> second) throws Exception {
                        Integer res = (Integer) second.getField(2)*(Integer) first.getField(4);
                        return Tuple9.of(first.f0,first.f1,first.f2,first.f3,first.f4,second.f0,second.f1,second.f2,res);
                    }
                });
        joinedstream.print();


        
        MTKafkaProducer010 mtKafkaProducer010 = new MTKafkaProducer010(args);
        mtKafkaProducer010.build(new SimpleStringSchema());
        Map<KafkaTopic, FlinkKafkaProducer010> topic2producers = mtKafkaProducer010.getTargetTopicsToProducers();

        // 添加一个Kafka Data Sink
        for(Map.Entry<KafkaTopic,FlinkKafkaProducer010> entry:topic2producers.entrySet()){
            joinedstream.addSink(entry.getValue())
                    .setParallelism(entry.getKey().getParallelism())
                    .uid(WRITE_KAFKA_TOPIC).name(WRITE_KAFKA_TOPIC);
        }

        env.execute((new JobConf(args)).getJobName());





    }
}
