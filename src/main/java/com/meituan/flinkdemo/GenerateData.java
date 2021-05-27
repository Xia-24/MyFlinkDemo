package com.meituan.flinkdemo;

import com.alibaba.fastjson.JSON;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.meituan.data.binlog.BinlogEntry;
import com.meituan.data.binlog.BinlogEntryUtil;
import com.meituan.flink.common.config.JobConf;
import com.meituan.flink.common.config.KafkaTopic;
import com.meituan.flink.common.kafka.MTKafkaConsumer010;
import com.meituan.flink.common.kafka.MTKafkaProducer010;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;
import java.util.Random;


public class GenerateData {
    private static final Logger LOG = LoggerFactory.getLogger(GenerateData.class);
    // stage
    private static final String READ_KAFKA_TOPIC = "org.com.sankuai.mad.w.udm.prod";
    private static final String WRITE_KAFKA_TOPIC = "app.testflink2kafka";

    public static void main(String[] args) throws Exception {
            final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


            // 读取权限Topic，请初始化 MTKafkaConsumer010
            MTKafkaConsumer010 mtKafkaConsumer010 = new MTKafkaConsumer010(args); // 通过平台提交的作业，初始化MTKafkaConsumer时请直接传入命令行参数args，无需自己构造！

            // 实例化所有读取权限Topic的consumer，此处使用SimpleStringSchema实例化全部consumer，如需配置其他可选项可参考mtflink-kafka-utils文档
//            mtKafkaConsumer010.build(new RawSchema());

            /* 获取consumer */
            // 方法一，使用getConsumerByName：按Topic名称和Namespace，获取特定的consumer实例。关于namespace，可参考第5节说明
//            Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> consumerEntry = mtKafkaConsumer010.getConsumerByName(READ_KAFKA_TOPIC, "xr_inf_namespace");
            // 方法二，使用getSubscribedTopicsToConsumers：获取所有在平台登记读取的权限Kafka Topic及对应的consumer实例
//            Map<KafkaTopic, FlinkKafkaConsumerBase> topic2consumers = mtKafkaConsumer010.build(new RawSchema())
        Map<KafkaTopic, FlinkKafkaConsumerBase> topic2consumers = mtKafkaConsumer010.build(new SimpleStringSchema())
                            .getSubscribedTopicsToConsumers();

            // 添加一个Kafka Data Source并设置并发度
            DataStream<String> stream = null;
            for(Map.Entry<KafkaTopic,FlinkKafkaConsumerBase> entry:topic2consumers.entrySet()){
                stream = env.addSource(entry.getValue())
                        .setParallelism(entry.getKey().getParallelism())
                        .uid(READ_KAFKA_TOPIC)
                        .name(READ_KAFKA_TOPIC);
            }

            DataStream<String> newstream = stream.map(new FormatResult());
            // 写权限Topic，请初始化 MTKafkaProducer010
            MTKafkaProducer010 mtKafkaProducer010 = new MTKafkaProducer010(args);

            // 实例化所有写出到权限Topic的producer，此处使用SimpleStringSchema实例化全部producer，如需要配置其他可选项可参考mtflink-kafka-utils文档
            mtKafkaProducer010.build(new SimpleStringSchema());

            /* 获取producer */
            // 方法一，使用getProducerByName：按Topic名称获取特定producer实例。
//            Map.Entry<KafkaTopic, FlinkKafkaProducer010> producerEntry = mtKafkaProducer010.getProducerByName(WRITE_KAFKA_TOPIC);
            // 方法二，使用getTargetTopicsToProducers：获取所有在平台登记写出的权限Kafka Topic以及对应的producer实例
            Map<KafkaTopic, FlinkKafkaProducer010> topic2producers = mtKafkaProducer010.getTargetTopicsToProducers();

            // 添加一个Kafka Data Sink
            for(Map.Entry<KafkaTopic,FlinkKafkaProducer010> entry:topic2producers.entrySet()){
                newstream.addSink(entry.getValue())
                        .setParallelism(entry.getKey().getParallelism())
                        .uid(WRITE_KAFKA_TOPIC).name(WRITE_KAFKA_TOPIC);
            }
            env.execute((new JobConf(args)).getJobName());
        }
        public static class FormatResult implements MapFunction<String, String> {

            @Override
            public String map(String s) throws Exception {
                long executeTime = 0;
                MockUserData data = new MockUserData(executeTime);
                LOG.info("==== generate data :" + JSON.toJSONString(data));
//                collector.collect(JSON.toJSONString(data));

                return JSON.toJSONString(data);
            }
        }
        private static class Generator implements FlatMapFunction<BinlogEntry , String> {

            @Override
            public void flatMap(BinlogEntry binlogEntry, Collector<String> collector) throws Exception {
                long executeTime = 0;
                MockUserData data = new MockUserData(executeTime);
                LOG.info("==== generate data :" + JSON.toJSONString(data));
                collector.collect(JSON.toJSONString(data));
            }
        }
    private static class MockUserData {
        private static Random rand = new Random();
        private int id;
        private String name;
        private long ts;
        private int action;

        public MockUserData(long ts) {
            this.ts = ts;
            this.id = rand.nextInt(10);
            this.action = rand.nextInt(5);
            this.name = RandomStringUtils.randomAlphanumeric(6);
        }

        public int getId() {
            return id;
        }

        public void setId(int id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public long getTs() {
            return ts;
        }

        public void setTs(long ts) {
            this.ts = ts;
        }

        public int getAction() {
            return action;
        }

        public void setAction(int action) {
            this.action = action;
        }
    }
    private static class RawSchema extends AbstractDeserializationSchema<BinlogEntry> {

        @Override
        public BinlogEntry deserialize(byte[] message) throws IOException {
            CanalEntry.Entry entry = BinlogEntryUtil.deserializeFromProtoBuf(message);
            if(entry != null){
                BinlogEntry binlogEntry = BinlogEntryUtil.serializeToBean(entry);
                return binlogEntry;
            }
            else{
                return null;
            }



        }
    }
}
