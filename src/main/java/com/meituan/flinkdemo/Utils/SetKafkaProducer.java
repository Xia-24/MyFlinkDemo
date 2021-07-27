package com.meituan.flinkdemo.Utils;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.types.Row;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class SetKafkaProducer {
    public static FlinkKafkaProducer getKafkaProducer(){
        Properties prop = new Properties();
        prop.put("bootstrap.servers", "localhost:9092");

        return new  FlinkKafkaProducer<>("sink",
//                new SerializationSchema<Tuple2<Boolean, Row>>() {
//                    @Override
//                    public byte[] serialize(Tuple2<Boolean, Row> element) {
//                        return (element.f0.toString() + "," + element.f1.toString()).getBytes(StandardCharsets.UTF_8);
//                    }
//                },
                new KafkaSerializationSchema<Tuple2<Boolean, Row>>(){
                    @Override
                    public ProducerRecord<byte[], byte[]> serialize(Tuple2<Boolean, Row> booleanRowTuple2, @Nullable Long aLong) {
                        return new ProducerRecord<>("sink",(booleanRowTuple2.f0.toString() + booleanRowTuple2.f1.toString()).getBytes(StandardCharsets.UTF_8));
                    }
                },
                prop,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE
        );
    }
}
