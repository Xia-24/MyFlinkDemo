package com.meituan.flinkdemo.MafkaOrKafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.Random;


public class LocalKafakProducer {
    private Random r = new Random();
    public static final String[] HBDM = {"BEF", "CNY", "DEM", "EUR", "HKD", "USD", "ITL"};


    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
//        ProducerRecord<String, String> record = new ProducerRecord<>(“Kafka”, “Kafka_Products”, “测试”);//Topic Key Value
        int hbdmindex = 0;
        int num = 0;
        int cnt = 0;
        String msg;
        try {
            while (true) {
                //ProducerRecord有多个构造器，这里使用了三个参数的，topic、key、value。
                Thread.sleep(1000);
                if (cnt < 100) {
                    cnt++;
                    msg = String.format("%d,%s,%d", System.currentTimeMillis(), HBDM[hbdmindex], num);
                } else {
                    cnt = 0;
                    hbdmindex = (hbdmindex + 1) % 7;
                    num = (num + 1) % 10;
                    msg = String.format("%d,%s,%d", System.currentTimeMillis(), HBDM[hbdmindex], num);
                }
                producer.send(new ProducerRecord<String, String>("test", "key", msg));
            }
        } catch (Exception e) {
            producer.close();
        }


    }

}
