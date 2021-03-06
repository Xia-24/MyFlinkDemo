package com.meituan.flinkdemo.MafkaOrKafka;

import com.dianping.squirrel.asyncclient.api.async.SquirrelAsyncCommands;
import com.dianping.squirrel.asyncclient.core.*;
import com.meituan.flinkdemo.Entity.Rate;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.Serializable;
import java.sql.Timestamp;
import java.util.Properties;

public class LocalFlinkReadKafka {


    public static void main(String[] args) throws Exception {


        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group1");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("test", new DeserializationSchema() {
            @Override
            public Tuple3<Long, String, Integer> deserialize(byte[] bytes) throws IOException {
                System.out.println(" test sout --------");
                String[] res = new String(bytes).split(",");
                Long timestamp = Long.valueOf(res[0]);
                String dm = res[1];
                Integer value = Integer.valueOf(res[2]);
                return Tuple3.of(timestamp, dm, value);
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

        DataStream<Rate> ratestream =  null;
        Long delay = 1000L;



        ratestream = env.addSource(consumer);

        // Filter
        SingleOutputStreamOperator<Rate> res = ratestream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Rate>(Time.milliseconds(delay)) {
            @Override
            public long extractTimestamp(Rate rate) {
                return rate.getTimestamp();
            }
        });

        WindowedStream<Rate,Tuple,TimeWindow> windows = res.keyBy("source_id","code")
                .window(TumblingEventTimeWindows.of(Time.seconds(30)));

        SingleOutputStreamOperator<Tuple3<Long,String,Integer>> processDS = windows
                .trigger(new Trigger<Rate, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Rate element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.FIRE_AND_PURGE;
                    }

                    @Override
                    public TriggerResult onProcessingTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public TriggerResult onEventTime(long time, TimeWindow window, TriggerContext ctx) throws Exception {
                        return TriggerResult.CONTINUE;
                    }

                    @Override
                    public void clear(TimeWindow window, TriggerContext ctx) throws Exception {

                    }
                })

                .process(new ProcessWindowFunction<Rate, Tuple3<Long, String, Integer>, Tuple, TimeWindow> ()  {
//                    private Jedis jedis;
                    private MyBloomFilter mybloomFilter;
                    private  SquirrelConfig squirrelConfig = new SquirrelConfig();
                    private SquirrelAsyncCommands asyncCommands;

                    @Override
                    public void open(Configuration parameters) throws Exception{
//                        jedis = new Jedis("",2222);
                        long s = System.currentTimeMillis();
                        MyBloomFilter myBloomFilter = new MyBloomFilter(1 << 30);
                        long e = System.currentTimeMillis();
                        System.out.println( "Created myBloomFilter, time cost: " + (e - s));

                        squirrelConfig.setReadTimeout(1000);
                        squirrelConfig.setRouterType(RouterType.MASTER_SLAVE);
                        squirrelConfig.setIdcSensitive(true);
                        squirrelConfig.setSerializeType("hessian4");
                        squirrelConfig.setUseBackupRequest(true);
                        //        squirrelConfig
                        //        squirrelConfig.
                        SquirrelClient squirrelClient = SquirrelClient.createCluster("redis-mtuav-udm_qa", squirrelConfig);
                        asyncCommands = squirrelClient.async();
                    }

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Rate> elements, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
                        String windowEnd = new Timestamp(context.window().getEnd()).toString();
                        String bitMapKey = "BitMap_" + windowEnd;
                        StoreKey storeKey = new StoreKey("stage_drone", bitMapKey);
//                        Tuple3 tp3 = elements.iterator().next();
                        Rate rate = elements.iterator().next();
//                        Long timestamp = (Long) tp3.f0;
//                        String hbdm = tp3.f1.toString();
//                        Integer num = (Integer) tp3.f2;
                        Long timestamp = rate.getTimestamp();
                        String hbdm = rate.getHbdm();
                        Integer num = rate.getNum();
                        String str = Long.toString(timestamp/1000) + hbdm;
                        long offset = mybloomFilter.getOffset(str);
//                        Boolean exists = jedis.getbit(bitMapKey,offset);

//                        ByteArrayOutputStream bos = new ByteArrayOutputStream();
//                        Hessian2Output output = new Hessian2Output(bos);
//                        output.writeObject(offset);
//                        byte[] se = bos.toByteArray();

                        SquirrelFuture<Boolean> exists = asyncCommands.getBit(storeKey,offset);
                        if(exists.get()){
                            asyncCommands.setBit(storeKey,offset,true);
//                            jedis.setbit(bitMapKey,offset,true);
                            out.collect(Tuple3.of(timestamp,hbdm,num));
//                            asyncCommands.hincrByFloat(windowEnd,1)
//                            jedis.hincrBy(windowEnd,1)
                        }

                    }
                });

        Properties prop2 = new Properties();
        prop2.put("bootstrap.servers","localhost:9092");

        DataStream<String> sinkstream = processDS.map(new MapFunction<Tuple3<Long, String, Integer>, String>() {
            @Override
            public String map(Tuple3<Long, String, Integer> value) throws Exception {
                return value.toString();
            }
        });
        sinkstream.addSink(new FlinkKafkaProducer<>("sink", new SimpleStringSchema(), prop2));
        env.execute();
    }
    static class MyBloomFilter implements Serializable {
        //????????????????????????1?????????????????????????????????3-10???
        //??????????????????????????????????????????2??????????????????
        private long cap;
        public MyBloomFilter(long cap) {
            this.cap = cap;
        }
        //?????????????????????????????????BitMap????????????
        public long getOffset(String value){
            long result = 0L;
            //????????????????????????2?????????????????????
            //???????????????????????????Unicode????????????????????????31?????????
            for (char c : value.toCharArray()){
                result += result * 31 + c;
            }
            //???????????????????????????????????????????????????
            return  result & (cap - 1);
        }}


}

