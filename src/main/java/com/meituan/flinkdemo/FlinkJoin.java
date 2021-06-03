//package com.meituan.flinkdemo;
//
//import com.dianping.squirrel.asyncclient.api.async.SquirrelAsyncCommands;
//import com.dianping.squirrel.asyncclient.core.*;
//import com.meituan.flink.common.config.JobConf;
//import com.meituan.flink.common.config.KafkaTopic;
//import com.meituan.flink.common.kafka.MTKafkaConsumer010;
//import com.meituan.flink.common.kafka.MTKafkaProducer010;
//import org.apache.flink.api.common.functions.FilterFunction;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.DeserializationSchema;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.api.common.typeinfo.TypeHint;
//import org.apache.flink.api.common.typeinfo.TypeInformation;
//import org.apache.flink.api.java.tuple.Tuple;
//import org.apache.flink.api.java.tuple.Tuple3;
//import org.apache.flink.api.java.tuple.Tuple5;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.shaded.guava18.com.google.common.hash.BloomFilter;
//import org.apache.flink.shaded.guava18.com.google.common.hash.Funnels;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.KeyedStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.datastream.WindowedStream;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
//import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
//import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.api.windowing.triggers.Trigger;
//import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
//import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
//import org.apache.flink.table.sources.wmstrategies.WatermarkStrategy;
//import org.apache.flink.util.Collector;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//import redis.clients.jedis.Jedis;
//
//import java.io.IOException;
//import java.nio.charset.StandardCharsets;
//import java.sql.Timestamp;
//import java.util.Map;
//
//public class FlinkJoin {
//    private static final Logger LOG = LoggerFactory.getLogger(FlinkJoin.class);
//    private static final String READ_KAFKA_TOPIC1 = "app.flinkrate";
//    private static final String READ_KAFKA_TOPIC2 = "app.flinkorder";
//    private static final String WRITE_KAFKA_TOPIC = "app.join";
//    private static SquirrelConfig squirrelConfig = new SquirrelConfig();
//
//
//
//    public static void main(String[] args) throws Exception {
//        squirrelConfig.setReadTimeout(1000);
//        squirrelConfig.setRouterType(RouterType.MASTER_SLAVE);
//        squirrelConfig.setIdcSensitive(true);
//        squirrelConfig.setSerializeType("hessian");
//        squirrelConfig.setUseBackupRequest(true);
//        SquirrelClient squirrelClient = SquirrelClient.createCluster("redis-mtuav-udm_dev", squirrelConfig);
//        SquirrelAsyncCommands asyncCommands = squirrelClient.async();
////        StoreKey storeKey = new StoreKey("Category", key);
////        SquirrelFuture squirrelFuture = asyncCommands.get(storeKey);
//// 注册回调
//;
//
//
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setParallelism(10);
//
//        MTKafkaConsumer010 mtKafkaConsumer010 = new MTKafkaConsumer010(args);
//        DataStream<Tuple3<Long, String, Integer>> ratestream = null;
//        Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> consumerEntry = mtKafkaConsumer010
//                .build(new DeserializationSchema() {
//                    @Override
//                    public Tuple3<Long, String, Integer> deserialize(byte[] bytes) throws IOException {
//                        System.out.println(" test sout --------");
//                        String[] res = new String(bytes).split(",");
//                        Long timestamp = Long.valueOf(res[0]);
//                        String dm = res[1];
//                        Integer value = Integer.valueOf(res[2]);
//                        return Tuple3.of(timestamp, dm, value);
//                    }
//
//                    @Override
//                    public boolean isEndOfStream(Object o) {
//                        return false;
//                    }
//
//                    @Override
//                    public TypeInformation getProducedType() {
//                        return TypeInformation.of(new TypeHint<Tuple3<Long, String, Integer>>() {
//                        });
//                    }
//                })
//                .getConsumerByName(READ_KAFKA_TOPIC1, "xr_inf_namespace");
//        ratestream = env.addSource(consumerEntry.getValue())
//                .setParallelism(10)
//                .uid(READ_KAFKA_TOPIC1)
//                .name(READ_KAFKA_TOPIC1);
//
//
////        KeyedStream<Tuple3<Long,String,Integer>, Tuple> keyedstream = ratestream.keyBy(0,1);
//
//
//        Long delay = 1000L;
//        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> res = ratestream
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.milliseconds(delay)) {
//                    @Override
//                    public long extractTimestamp(Tuple3<Long, String, Integer> longStringIntegerTuple3) {
//                        return (Long) longStringIntegerTuple3.getField(0);
//                    }
//                });
//
//        WindowedStream<Tuple3<Long, String, Integer>, Tuple, TimeWindow> windowds = res.keyBy(0, 1)
//                .window(TumblingEventTimeWindows.of(Time.seconds(30)));
//
//        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> processDS = windowds
//                .trigger(new Trigger<Tuple3<Long, String, Integer>, TimeWindow>() {
//                    @Override
//                    public TriggerResult onElement(Tuple3<Long, String, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
//                        return TriggerResult.FIRE_AND_PURGE;
//                    }
//
//                    @Override
//                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//                        return TriggerResult.CONTINUE;
//                    }
//
//                    @Override
//                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
//                        return TriggerResult.CONTINUE;
//                    }
//
//                    @Override
//                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {
//
//                    }
//                }).process(new ProcessWindowFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>, Tuple, TimeWindow>() {
//                    private Jedis jedis;
//                    private MyBloomFilter mybloomFilter;
//
//                    @Override
//                    public void open(Configuration parameters) throws Exception{
//                        jedis = new Jedis("",2222);
//                        long s = System.currentTimeMillis();
//                        MyBloomFilter myBloomFilter = new MyBloomFilter(1 << 30);
//                        long e = System.currentTimeMillis();
//                        System.out.println( "Created myBloomFilter, time cost: " + (e - s));
//                    }
//
//                    @Override
//                    public void process(Tuple tuple, Context context, Iterable<Tuple3<Long, String, Integer>> elements, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
//                        String windowEnd = new Timestamp(context.window().getEnd()).toString();
//                        String bitMapKey = "BitMap_" + windowEnd;
//                        StoreKey storeKey = new StoreKey("Category", bitMapKey);
//                        Tuple3 tp3 = elements.iterator().next();
//                        Long timestamp = (Long) tp3.f0;
//                        String hbdm = tp3.f1.toString();
//                        Integer num = (Integer) tp3.f2;
//                        String str = Long.toString(timestamp/1000) + hbdm;
//                        long offset = mybloomFilter.getOffset(str);
////                        Boolean exists = jedis.getbit(bitMapKey,offset);
//                        SquirrelFuture<Boolean> exists = asyncCommands.getBit(storeKey,offset);
//                        if(exists.get()){
//                            jedis.setbit(bitMapKey,offset,true);
//                            out.collect(Tuple3.of(timestamp,hbdm,num));
////                            jedis.hincrBy(windowEnd,1)
//                        }
//
//                    }
//                });
//
//
//        MTKafkaProducer010 mtKafkaProducer010 = new MTKafkaProducer010(args);
//        mtKafkaProducer010.build(new SimpleStringSchema());
//        Map<KafkaTopic, FlinkKafkaProducer010> topic2producers = mtKafkaProducer010.getTargetTopicsToProducers();
//
//        DataStream<String> newstream = processDS.map(new MapFunction<Tuple3<Long, String, Integer>, String>() {
//            @Override
//            public String map(Tuple3<Long, String, Integer> tp3) throws Exception {
//                return tp3.toString();
//            }
//        });
//
//        // 添加一个Kafka Data Sink
//        for (Map.Entry<KafkaTopic, FlinkKafkaProducer010> entry : topic2producers.entrySet()) {
//            newstream.addSink(entry.getValue())
//                    .setParallelism(entry.getKey().getParallelism())
//                    .uid(WRITE_KAFKA_TOPIC).name(WRITE_KAFKA_TOPIC);
//        }
//
//        env.execute((new JobConf(args)).getJobName());
//    }
//    static class MyBloomFilter {
//        //减少哈希冲突优化1：增加过滤器容量为数据3-10倍
//        //定义布隆过滤器容量，最好传入2的整次幂数据
//        private long cap;
//        public MyBloomFilter(long cap) {
//            this.cap = cap;
//        }
//        //传入一个字符串，获取在BitMap中的位置
//        public long getOffset(String value){
//            long result = 0L;
//            //减少哈希冲突优化2：优化哈希算法
//            //对字符串每个字符的Unicode编码乘以一个质数31再相加
//            for (char c : value.toCharArray()){
//                result += result * 31 + c;
//            }
//            //取模，使用位与运算代替取模效率更高
//            return  result & (cap - 1);
//        }}
//
//
//
//
////        Long delay = 1000L;
////        DataStream<Tuple3<Long,String,Integer>> rateTimedString = ratestream.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.milliseconds(delay)) {
////            @Override
////            public long extractTimestamp(Tuple3<Long, String, Integer> longStringIntegerTuple3) {
////                return (Long)longStringIntegerTuple3.getField(0);
////            }
////        });
////        DataStream<Tuple5<Long,String,Integer,String,Integer>> orderTimedStream = orderstream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple5<Long, String, Integer, String, Integer>>() {
////            @Override
////            public long extractAscendingTimestamp(Tuple5<Long, String, Integer, String, Integer> longStringIntegerStringIntegerTuple5) {
////                return (Long)longStringIntegerStringIntegerTuple5.getField(0);
////            }
////        });
////        DataStream<Tuple9<Long,String,Integer,String,Integer,Long,String,Integer,Integer>> joinedstream = orderTimedStream.join(rateTimedString).where(new KeySelector<Tuple5<Long, String, Integer, String, Integer>,String>() {
////            @Override
////            public String getKey(Tuple5<Long, String, Integer, String, Integer> longStringIntegerStringIntegerTuple5) throws Exception {
////                return longStringIntegerStringIntegerTuple5.getField(3).toString();
////            }
////        }).equalTo(new KeySelector<Tuple3<Long, String, Integer>, String>() {
////            @Override
////            public String getKey(Tuple3<Long, String, Integer> longStringIntegerTuple3) throws Exception {
////                return longStringIntegerTuple3.getField(1).toString();
////            }
////        }).window(TumblingEventTimeWindows.of(Time.seconds(10)))
////                .apply(new JoinFunction<Tuple5<Long, String, Integer, String, Integer>, Tuple3<Long, String, Integer>, Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer>>() {
////                    @Override
////                    public Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer> join(Tuple5<Long, String, Integer, String, Integer> first, Tuple3<Long, String, Integer> second) throws Exception {
////                        Integer res = (Integer) second.getField(2)*(Integer) first.getField(4);
////                        return Tuple9.of(first.f0,first.f1,first.f2,first.f3,first.f4,second.f0,second.f1,second.f2,res);
////                    }
////                });
////        joinedstream.print();
////
////
////
////        MTKafkaProducer010 mtKafkaProducer010 = new MTKafkaProducer010(args);
////        mtKafkaProducer010.build(new SimpleStringSchema());
////        Map<KafkaTopic, FlinkKafkaProducer010> topic2producers = mtKafkaProducer010.getTargetTopicsToProducers();
////
////        DataStream<String> newstream = joinedstream.map(new MapFunction<Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer>, String>() {
////            @Override
////            public String map(Tuple9<Long, String, Integer, String, Integer, Long, String, Integer, Integer> longStringIntegerStringIntegerLongStringIntegerIntegerTuple9) throws Exception {
////                return longStringIntegerStringIntegerLongStringIntegerIntegerTuple9.toString();
////            }
////        });
////
////        // 添加一个Kafka Data Sink
////        for(Map.Entry<KafkaTopic,FlinkKafkaProducer010> entry:topic2producers.entrySet()){
////            newstream.addSink(entry.getValue())
////                    .setParallelism(entry.getKey().getParallelism())
////                    .uid(WRITE_KAFKA_TOPIC).name(WRITE_KAFKA_TOPIC);
////        }
////
////        env.execute((new JobConf(args)).getJobName());
//
//
//
////    }
//}
