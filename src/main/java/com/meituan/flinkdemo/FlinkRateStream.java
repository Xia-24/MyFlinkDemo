package com.meituan.flinkdemo;

import com.meituan.flink.common.config.JobConf;
import com.meituan.flink.common.config.KafkaTopic;
import com.meituan.flink.common.kafka.MTKafkaProducer010;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class FlinkRateStream {
    public static final String[] HBDM = {"BEF","CNY","DEM","EUR","HKD","USD","ITL"};
    private static final String WRITE_KAFKA_TOPIC = "app.flinkrate";
    private static final Logger LOG = LoggerFactory.getLogger(GenerateData.class);


    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> msgstream = env.addSource(new SourceFunction<String>() {
            private Random r = new Random();
            private static final long serial = 1L;
            boolean running = true;
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while(running){
                    Thread.sleep(r.nextInt(3)*1000);
                    sourceContext.collect(String.format("%d,%s,%d",System.currentTimeMillis(),HBDM[r.nextInt(HBDM.length)],r.nextInt(20)));
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
        MTKafkaProducer010 mtKafkaProducer010 = new MTKafkaProducer010(args);
        Map<KafkaTopic, FlinkKafkaProducer010> topic2producers = mtKafkaProducer010.getTargetTopicsToProducers();

        // 添加一个Kafka Data Sink
        for(Map.Entry<KafkaTopic,FlinkKafkaProducer010> entry:topic2producers.entrySet()){
            msgstream.addSink(entry.getValue())
                    .setParallelism(entry.getKey().getParallelism())
                    .uid(WRITE_KAFKA_TOPIC).name(WRITE_KAFKA_TOPIC);
        }
        env.execute((new JobConf(args)).getJobName());
    }
}
