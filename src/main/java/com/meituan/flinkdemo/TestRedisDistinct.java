//package com.meituan.flinkdemo;
//
//import org.apache.flink.api.common.functions.FlatMapFunction;
//import org.apache.flink.api.common.functions.RichFilterFunction;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.util.Collector;
//import redis.clients.jedis.*;
//
//public class TestRedisDistinct {
//    public static void main(String[] args) {
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        DataStream<String> lineDS = env.readTextFile("input.txt");
//        SingleOutputStreamOperator<String> wordDS = lineDS.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public void flatMap(String s, Collector<String> collector) throws Exception {
//                String [] s1 = s.split(" ");
//                for (String s2:s1){
//                    collector.collect(s2);
//                }
//            }
//        });
//        SingleOutputStreamOperator<String> redisDS = wordDS.filter(new RichFilterFunction<String>() {
//            Jedis jedis = null;
//            @Override
//            public void open(Configuration parameters) throws Exception{
//                jedis = new Jedis();
//            }
//
//            @Override
//            public boolean filter(String s) throws Exception {
//                return false;
//            }
//        })
//    }
//}
