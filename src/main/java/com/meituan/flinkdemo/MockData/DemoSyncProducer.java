package com.meituan.flinkdemo.MockData;

import com.meituan.mafka.client.consumer.ConsumerConstants;
import com.meituan.mafka.client.MafkaClient;
import com.meituan.mafka.client.producer.IProducerProcessor;
import com.meituan.mafka.client.producer.ProducerResult;
import java.util.Properties;

public class DemoSyncProducer {
    /**
     * producer实例请在业务初始化的时候创建好.
     * producer资源创建好后，再开放业务流量.
     * 请不要频繁创建producer实例：即不要发送一条消息都创建一次producer。一是性能不好，二是服务端会限制连接次数影响消息发送。发送完毕后，请进行close。
     * 注意：服务端对单ip创建相同主题的生产者实例数有限制，超过100个拒绝创建.
     * */
    private static IProducerProcessor producer;

    // 注意：执行main函数若抛出ERROR级别异常，请务必进行观察处理
    public static void main(String[] args) throws Exception {
        Properties properties = new Properties();
        // 设置业务所在BG的namespace，此参数必须配置且请按照demo正确配置
        properties.setProperty(ConsumerConstants.MafkaBGNamespace, "waimai");
        // 设置生产者appkey，此参数必须配置且请按照demo正确配置
        properties.setProperty(ConsumerConstants.MafkaClientAppkey, "com.sankuai.mad.w.udm");

        // 创建topic对应的producer对象（注意每次build调用会产生一个新的实例），此处配置topic名称，请按照demo正确配置
        // 请注意：若调用MafkaClient.buildProduceFactory()创建实例抛出有异常，请重点关注并排查异常原因，不可频繁调用该方法给服务端带来压力。
        producer = MafkaClient.buildProduceFactory(properties, "udm.mtuav_asset_airport");
        for (int i = 0; i < 10; ++ i) {
            try {
                // TODO:业务侧的发送消息逻辑代码
                // 同步发送，注意：producer只实例化一次，不要每次调用sendMessage方法前都创建producer实例
                ProducerResult result = producer.sendMessage("send sync message " + i);
                System.out.println("send " + i + " status: " + result.getProducerStatus());
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }
}