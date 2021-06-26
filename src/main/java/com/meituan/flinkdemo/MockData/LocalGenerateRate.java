package com.meituan.flinkdemo.MockData;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.Random;

public class LocalGenerateRate {
    public static final String[] HBDM = {"BEF", "CNY", "DEM", "EUR", "HKD", "USD", "ITL"};
    private static final String WRITE_KAFKA_TOPIC = "app.flinkrate";
    private static final Logger LOG = LoggerFactory.getLogger(LocalGenerateRate.class);


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<String> msgstream = env.addSource(new SourceFunction<String>() {
            private Random r = new Random();
            private static final long serial = 1L;
            boolean running = true;
            int hbdmindex = 0;
            int num = 0;
            int cnt = 0;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {

                while (running) {
                    Thread.sleep(100);
                    if (cnt < 10) {
                        cnt++;
                        sourceContext.collect(String.format("%d,%s,%d", System.currentTimeMillis(), HBDM[hbdmindex], num));
                    } else {
                        cnt = 0;
                        hbdmindex = (hbdmindex + 1) % 7;
                        num = (num + 1) % 10;
                        sourceContext.collect(String.format("%d,%s,%d", System.currentTimeMillis(), HBDM[hbdmindex], num));
                    }
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "localhost:9092");
//        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
//        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列
        FlinkKafkaProducer producer = new FlinkKafkaProducer("test", new SimpleStringSchema(), prop);


        msgstream.addSink(producer).setParallelism(1).name("flink2kafka");
        env.execute();

    }
}
