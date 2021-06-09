package com.meituan.flinkdemo;

import com.meituan.mafka.client.MafkaClient;
import com.meituan.mafka.client.consumer.ConsumeStatus;
import com.meituan.mafka.client.consumer.ConsumerConstants;
import com.meituan.mafka.client.consumer.IConsumerProcessor;
import com.meituan.mafka.client.consumer.IMessageListener;
import com.meituan.mafka.client.message.MafkaMessage;
import com.meituan.mafka.client.message.MessagetContext;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class LocalGenerateDeviceEvent {
    private static final String READ_KAFKA_TOPIC = "app.mafka.udm.deviceevent";
    private static final String WRITE_KAFKA_TOPIC = "app.udm_udw_device_event_duprm";
    private static IConsumerProcessor consumer;
    public static void main(String[] args) throws Exception {

        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String,String> producer = new KafkaProducer<>(props);

        Properties properties = new Properties();
        // 设置业务所在BG的namespace，此参数必须配置且请按照demo正确配置
        properties.setProperty(ConsumerConstants.MafkaBGNamespace, "waimai");
        // 设置消费者appkey，此参数必须配置且请按照demo正确配置
        properties.setProperty(ConsumerConstants.MafkaClientAppkey, "com.sankuai.mad.w.udm");
        // 设置订阅组group，此参数必须配置且请按照demo正确配置
        properties.setProperty(ConsumerConstants.SubscribeGroup, "udm_timeseries_data");

        // 创建topic对应的consumer对象（注意每次build调用会产生一个新的实例），此处配topic名字，请按照demo正确配置
        consumer = MafkaClient.buildConsumerFactory(properties, "udm.timeseries_data");

        consumer.recvMessageWithParallel(String.class, new IMessageListener() {
            @Override
            public ConsumeStatus recvMessage(MafkaMessage message, MessagetContext context) {
                //TODO:业务侧的消费逻辑代码
                try {
                    producer.send(message);
                    System.out.println("message=[" + message.getBody() + "]  partition=" + message.getParttion());
                } catch (Exception e) {
                    e.printStackTrace();
                }
                //返回状态说明：①返回CONSUME_SUCCESS，表示消费成功准备消费下一条消息。
                //            ②返回RECONSUME_LATER，表示请求再次消费该消息，默认最多三次，然后跳过此条消息的消费，开始消费下一条。(算上初始最多消费4次）
                //            ③返回CONSUMER_FAILURE，表示请求继续消费，直到消费成功。
                //注意：如果不想在消费异常时一直进行重试，造成消息积压，可以返回RECONSUME_LATER，详细设置可以看下右上角HELP文档->高阶特性->消费异常重试次数设置
                return ConsumeStatus.CONSUME_SUCCESS;
            }
        });


//        ProducerRecord<String, String> record = new ProducerRecord<>(“Kafka”, “Kafka_Products”, “测试”);//Topic Key Value
        int hbdmindex = 0;
        int num = 0;
        int cnt = 0;
        String msg;
        try{
            while(true) {
                //ProducerRecord有多个构造器，这里使用了三个参数的，topic、key、value。
                Thread.sleep(1000);
                if(cnt < 100){
                    cnt ++;
                    msg = String.format("%d,%s,%d",System.currentTimeMillis(),HBDM[hbdmindex],num);
                }
                else{
                    cnt = 0;
                    hbdmindex = (hbdmindex + 1)%7;
                    num = (num + 1) % 10;
                    msg = String.format("%d,%s,%d",System.currentTimeMillis(),HBDM[hbdmindex],num);
                }
                producer.send(new ProducerRecord<String, String>("test", "key", msg));
            }
        }
        catch (Exception e){
            producer.close();
        }


    }

}
