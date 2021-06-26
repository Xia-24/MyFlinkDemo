package com.meituan.flinkdemo.MafkaOrKafka;


import com.meituan.mafka.client.consumer.ConsumerConstants;
import com.meituan.mafka.client.consumer.IConsumerProcessor;
import com.meituan.mafka.client.consumer.IMessageListener;
import com.meituan.mafka.client.consumer.ConsumeStatus;
import com.meituan.mafka.client.MafkaClient;
import com.meituan.mafka.client.message.MafkaMessage;
import com.meituan.mafka.client.message.MessagetContext;
import java.util.Properties;

public class DemoConsumer {

    /**
     * 注意：服务端对单ip创建相同主题相同队列的消费者实例数有限制，超过100个拒绝创建.
     * */
    private static IConsumerProcessor consumer;

    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        // 设置业务所在BG的namespace，此参数必须配置且请按照demo正确配置
        properties.setProperty(ConsumerConstants.MafkaBGNamespace, "waimai");
        // 设置消费者appkey，此参数必须配置且请按照demo正确配置
        properties.setProperty(ConsumerConstants.MafkaClientAppkey, "com.sankuai.mad.w.udm");
        // 设置订阅组group，此参数必须配置且请按照demo正确配置
        properties.setProperty(ConsumerConstants.SubscribeGroup, "udm_asset_airport_sync");

        // 创建topic对应的consumer对象（注意每次build调用会产生一个新的实例），此处配topic名字，请按照demo正确配置
        consumer = MafkaClient.buildConsumerFactory(properties, "udm.mtuav_asset_airport_test");

        // 调用recvMessageWithParallel设置listener
        // 注意1：可以修改String.class以支持自定义数据类型
        // 注意2：针对同一个consumer对象，只能调用一次该方法；多次调用的话，后面的调用都会报异常
        consumer.recvMessageWithParallel(String.class, new IMessageListener() {
            @Override
            public ConsumeStatus recvMessage(MafkaMessage message, MessagetContext context) {
                //TODO:业务侧的消费逻辑代码
                try {
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

        //如上简单demo可以达到消费者一直处于消费状态,业务可以主动调用consumer.close()来关闭consumer,如消费100个消息主动关闭
    }
}