package com.meituan.flinkdemo;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
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
import java.util.*;
import java.util.concurrent.CopyOnWriteArraySet;

public class LocalFlinkReadKafkaWindowJson {


    public static void main(String[] args) throws Exception {


        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group1");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("test", new DeserializationSchema() {
//            @Override
//            public Rate deserialize(byte[] bytes) throws IOException {
////                System.out.println(" test sout --------");
////                String[] res = new String(bytes).split(",");
////                Long timestamp = Long.valueOf(res[0]);
////                String dm = res[1];
////                Integer value = Integer.valueOf(res[2]);
//                String str = bytes.toString();
//                System.out.println("=================================");
//                for(int i = 0;i <bytes.length;++i){
//                    System.out.print(bytes[i]);
//                }
//                Rate rate = JSONObject.parseObject(str,Rate.class);
//                System.out.println(rate.toString());
//                return rate;
//            }
//
//            @Override
//            public boolean isEndOfStream(Object o) {
//                return false;
//            }
//
//            @Override
//            public TypeInformation getProducedType() {
//                return TypeInformation.of(Rate.class);
//            }
//        },props);
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("test",new SimpleStringSchema(),props);

        DataStream<String> stream = env.addSource(consumer);
        DataStream<Rate> ratestream =  null;
        Long delay = 1000L;

        ratestream = stream.map(new MapFunction<String, Rate>() {
            @Override
            public Rate map(String value) throws Exception {
                return JSONObject.parseObject(value,Rate.class);
            }
        });




//        ratestream.keyBy(1,2)
//                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(30)))
//                .trigger(new DedupTrigger(Time.seconds(30).toMilliseconds()))
//                .reduce(new ReduceFunction<Tuple3<Long, String, Integer>>() {
//                    @Override
//                    public Tuple3<Long, String, Integer> reduce(Tuple3<Long, String, Integer> value1, Tuple3<Long, String, Integer> value2) throws Exception {
//                        return value1;
//                    }
//                });



        // Filter
        SingleOutputStreamOperator<Rate> res = ratestream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Rate>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Rate rate) {
                        return rate.getTimestamp();
                    }
                });

        WindowedStream<Rate, String, TimeWindow> windows = res.keyBy(new KeySelector<Rate, String>() {
            @Override
            public String getKey(Rate value) throws Exception {
                return value.getHbdm();
            }
        })
                .window(TumblingEventTimeWindows.of(Time.seconds(30)));

        SingleOutputStreamOperator<String> processDS = windows
                .trigger(new Trigger<Rate,TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Rate rate, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
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

                .process(new ProcessWindowFunction<Rate, String, String, TimeWindow>()  {


                    private Set<String> set;
                    @Override
                    public void open(Configuration parameters) throws Exception{
                        set = new CopyOnWriteArraySet<>();

                        System.out.println("create set");
                    }

                    @Override
                    public void process(String s, Context context, Iterable<Rate> elements, Collector<String> out) throws Exception {

                        Iterator<Rate> iterator = elements.iterator();
                        while(iterator.hasNext()){
                            Rate rate = iterator.next();
                            Long time = rate.getTimestamp();
                            String hbdm = rate.getHbdm();
                            Integer num = rate.getNum();
                            String str = Long.toString(time/10000) + hbdm;
                            System.out.println(str);

                            System.out.println(set.size());
                            System.out.println(set.contains(str));
                            if(!set.contains(str)){
                                set.add(str);
                                out.collect(JSON.toJSONString(rate));
                            }
                        }

                    }
                });


        Properties prop2 = new Properties();
        prop2.put("bootstrap.servers","localhost:9092");

//        DataStream<String> sinkstream = processDS.map(new MapFunction<Tuple3<Long, String, Integer>, String>() {
//            @Override
//            public String map(Tuple3<Long, String, Integer> value) throws Exception {
//                return value.toString();
//            }
//        });
//        sinkstream.addSink(new FlinkKafkaProducer<>("sink", new SimpleStringSchema(), prop2));
        processDS.addSink(new FlinkKafkaProducer<>("sink", new SimpleStringSchema(), prop2));
        env.execute();
    }
    public static class DedupTrigger extends Trigger<Object,TimeWindow>{
        Long sessiongap;
        public DedupTrigger(long sessiongap){
            this.sessiongap = sessiongap;
        }
        @Override
        public TriggerResult onElement(Object element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
            if(window.getEnd() - window.getStart() == sessiongap){
                return TriggerResult.FIRE;
            }
            return TriggerResult.CONTINUE;
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
        @Override
        public boolean canMerge(){
            return true;
        }
        @Override
        public void onMerge(TimeWindow window, OnMergeContext ctx) throws Exception{

        }

    }

}

