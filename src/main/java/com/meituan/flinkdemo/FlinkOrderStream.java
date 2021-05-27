package com.meituan.flinkdemo;

import com.meituan.flink.common.config.JobConf;
import com.meituan.flink.common.config.KafkaTopic;
import com.meituan.flink.common.kafka.MTKafkaProducer010;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Random;

public class FlinkOrderStream {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateData.class);
    private static final String WRITE_KAFKA_TOPIC = "app.flinkorder";

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<String> msgstream = env.addSource(new SourceFunction<String>() {
            private Random r = new Random();
            private static final long serialV = 1L;
            boolean running = true;
            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {
                while (running){
                    Thread.sleep(r.nextInt(1500));
                    char catlog = (char) (65 + r.nextInt(5));
                    sourceContext.collect(String.format("%d,%s,%d,%s,%d",System.currentTimeMillis(),String.valueOf(catlog),r.nextInt(5),FlinkRateStream.HBDM[r.nextInt(FlinkRateStream.HBDM.length)],r.nextInt(1000)));
                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
//        DataStreamSink<String>
        MTKafkaProducer010 mtKafkaProducer010 = new MTKafkaProducer010(args);
        mtKafkaProducer010.build(new SimpleStringSchema());
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
