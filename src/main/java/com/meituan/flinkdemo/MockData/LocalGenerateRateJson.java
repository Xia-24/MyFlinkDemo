package com.meituan.flinkdemo.MockData;

import com.alibaba.fastjson.JSON;
import com.meituan.flinkdemo.Entity.Rate;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class LocalGenerateRateJson {
    public static final String[] HBDM = {"BEF","CNY","DEM","EUR","HKD","USD","ITL"};
//    public static final String[] HBDM = {"BEF"};

    public static final int numOfHBDM = 7;

    private static final String WRITE_KAFKA_TOPIC = "app.flinkrate";
    private static final Logger LOG = LoggerFactory.getLogger(LocalGenerateRateJson.class);


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
//        DataStream<String> mystream = env.addSource(new SourceFunction<String>() {
//            @Override
//            public void run(SourceContext<String> ctx) throws Exception {
//
//            }
//
//            @Override
//            public void cancel() {
//
//            }
//        });


        DataStream<String> msgstream = env.addSource(new SourceFunction<String>() {
            private Random r = new Random();
            private static final long serial = 1L;
            boolean running = true;
            int hbdmindex = 0;
            int num = 0;
            int cnt = 0;
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {

                while(running){
                    Thread.sleep(100);
                    if(cnt < 10){
                        cnt ++;
                        Rate rate = new Rate(System.currentTimeMillis(),HBDM[hbdmindex],num);
                        sourceContext.collect(JSON.toJSONString(rate));
//                        sourceContext.collect(String.format("%d,%s,%d",System.currentTimeMillis(),HBDM[hbdmindex],num));
                    }
                    else{
                        cnt = 0;
                        hbdmindex = (hbdmindex + 1)%numOfHBDM;
                        num = (num + 1) % 10;
                        Rate rate = new Rate(System.currentTimeMillis(),HBDM[hbdmindex],num);
                        sourceContext.collect(JSON.toJSONString(rate));
//                        sourceContext.collect(String.format("%d,%s,%d",System.currentTimeMillis(),HBDM[hbdmindex],num));
                    }
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
//

//




        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","localhost:9092");
//        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key ?????????
//        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value ??????
        FlinkKafkaProducer producer = new FlinkKafkaProducer("test",new SimpleStringSchema(),prop);


        msgstream.addSink(producer).setParallelism(1).name("flink2kafka");
        env.execute();

    }

}
