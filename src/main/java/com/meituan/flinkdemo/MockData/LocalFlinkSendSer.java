package com.meituan.flinkdemo.MockData;


import com.alibaba.fastjson.JSON;
import com.meituan.flinkdemo.Entity.Myclass;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.*;
import java.util.Properties;
import java.util.Random;

public class LocalFlinkSendSer {
    private static Random random = new Random(10);
    private static Boolean running = true;
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.registerType(Myclass.class);
        DataStream<Myclass> myclassDataStream = env.addSource(new SourceFunction<Myclass>() {
            @Override
            public void run(SourceContext<Myclass> ctx) throws Exception {
                while(running){
                    Thread.sleep(1000);
//                    if(random.nextBoolean()){
                    Myclass myclass = new Myclass(random.nextInt(50),random.nextBoolean(), RandomStringUtils.randomAlphanumeric(6));
                    ctx.collect(myclass);
                    System.out.println(JSON.toJSONString(myclass));
//                    }
//                    else{
//                        MyClass2 myClass2 = new MyClass2(RandomStringUtils.randomAlphanumeric(10),RandomStringUtils.randomAlphanumeric(20));
//                        ctx.
//                    }
                }

            }

            @Override
            public void cancel() {
                running = false;
            }
        });

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","localhost:9092");
//        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
//        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列
        FlinkKafkaProducer producer = new FlinkKafkaProducer("test", new SerializationSchema<Myclass>() {
            @Override
            public byte[] serialize(Myclass element) {
                try {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ObjectOutputStream oos = new ObjectOutputStream(baos);
//                    oos.writeInt(element.getAge());
//                    oos.writeUTF(element.getName());
//                    oos.writeBoolean(element.getSex());
                    oos.writeObject(element);
                    oos.flush();
                    return baos.toByteArray();
//                    baos.write(element.getAge());
//                    baos.write(element.getName().getBytes());
//                    baos.write(element.getSex().toString().getBytes());
//                    baos.flush();
//                    return baos.toByteArray();
                } catch (IOException e) {
                    e.printStackTrace();
                }

                return new byte[0];
            }
        },prop);

        myclassDataStream.addSink(producer).setParallelism(1).name("flink2kafka");
        env.execute();

    }

}
