package com.meituan.flinkdemo.Stream;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
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
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
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

public class LocalFlinkReadKafkaWindow {


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
//                System.out.println(" test sout --------");
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
                return TypeInformation.of(new TypeHint<Tuple3<Long, String, Integer>>() {
                });
            }
        },props);

        DataStream<Tuple3<Long, String, Integer>> ratestream =  null;
        Long delay = 1000L;



        ratestream = env.addSource(consumer);

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
        SingleOutputStreamOperator<Tuple3<Long,String,Integer>> res = ratestream
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Tuple3<Long, String, Integer>>(Time.milliseconds(delay)) {
                    @Override
                    public long extractTimestamp(Tuple3<Long, String, Integer> element) {
                        return element.getField(0);
                    }
                });

        WindowedStream<Tuple3<Long,String,Integer>, Tuple,TimeWindow> windows = res.keyBy(1)
                .window(TumblingEventTimeWindows.of(Time.seconds(30)));

        SingleOutputStreamOperator<Tuple3<Long,String,Integer>> processDS = windows
                .trigger(new Trigger<Tuple3<Long, String, Integer>, TimeWindow>() {
                    @Override
                    public TriggerResult onElement(Tuple3<Long, String, Integer> element, long timestamp, TimeWindow window, TriggerContext ctx) throws Exception {
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

                .process(new ProcessWindowFunction<Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>, Tuple, TimeWindow>()  {

                    private Set<String> set;
                    @Override
                    public void open(Configuration parameters) throws Exception{
                        set = new CopyOnWriteArraySet<>();

                        System.out.println("create set");
                    }

                    @Override
                    public void process(Tuple tuple, Context context, Iterable<Tuple3<Long, String, Integer>> elements, Collector<Tuple3<Long, String, Integer>> out) throws Exception {

                        Iterator<Tuple3<Long, String, Integer>> iterator = elements.iterator();
                        while(iterator.hasNext()){
                            Tuple3<Long, String, Integer> tp3 = iterator.next();
                            Long time = tp3.f0;
                            String hbdm = tp3.f1;
                            Integer num = tp3.f2;
                            String str = Long.toString(time/10000) + hbdm;
                            System.out.println(str);

                            System.out.println(set.size());
                            System.out.println(set.contains(str));
                            if(!set.contains(str)){
                                set.add(str);
                                out.collect(tp3);
                            }
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

