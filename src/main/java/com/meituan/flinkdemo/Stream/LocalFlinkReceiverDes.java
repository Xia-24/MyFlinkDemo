package com.meituan.flinkdemo.Stream;

import com.meituan.flinkdemo.Entity.Myclass;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

public class LocalFlinkReceiverDes {
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("group.id", "group1");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("test", new DeserializationSchema<Myclass>() {
            @Override
            public Myclass deserialize(byte[] message) throws IOException {
                ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(message));
                Integer age = ois.readInt();
                String name = ois.readUTF();
                Boolean sex = ois.readBoolean();
                Myclass myclass = new Myclass(age,sex,name);
                System.out.println(age);
//                System.out.println(myclass.toString());
                return myclass;

            }

            @Override
            public boolean isEndOfStream(Myclass nextElement) {
                return false;
            }

            @Override
            public TypeInformation getProducedType() {
                return TypeInformation.of(Myclass.class);
            }
        }, props);
//        FlinkKafkaConsumer consumer = new FlinkKafkaConsumer("test",new SimpleStringSchema(),props);
        DataStream<Myclass> myclassDataStream = env.addSource(consumer);
//        DataStream<String> stream = env.addSource(consumer);
        Properties prop2 = new Properties();
        prop2.put("bootstrap.servers","localhost:9092");



//        DataStream<String> stream = myclassDataStream.map(new MapFunction<Myclass, String>() {
//            @Override
//            public String map(Myclass value) throws Exception {
//                System.out.println("-------");
//                System.out.println(value.getSex()+value.getName());
////                System.out.println(value.toString());
//                return value.toString();
//            }
//        });
//        stream.addSink(new FlinkKafkaProducer<>("sink",new SimpleStringSchema(),prop2));
        myclassDataStream.addSink(new FlinkKafkaProducer<>("sink", new SerializationSchema<Myclass>() {
            @Override
            public byte[] serialize(Myclass element) {
                if(element instanceof Myclass){
                    StringBuilder sb = new StringBuilder();
                    sb.append("myclass");
                    sb.append(((Myclass) element).getAge()).append(((Myclass) element).getName()).append(((Myclass) element).getSex());
                    byte[] res = sb.toString().getBytes(StandardCharsets.UTF_8);
//                    for(int i = 0;i<res.length;++i){
//                        System.out.println(res[i]);
//                    }
                    return res;
//                    return ((Myclass) element).getName().getBytes(StandardCharsets.UTF_8);
                }
                else{
                    return new byte[0];
                }
        }}, prop2));
        env.execute();
    }
}
