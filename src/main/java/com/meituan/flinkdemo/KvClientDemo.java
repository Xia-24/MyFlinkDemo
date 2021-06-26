//package com.meituan.flinkdemo;
//
//import com.sankuai.kv.client.KvClient;
//import com.sankuai.kv.client.KvConfig;
//import com.sankuai.kv.client.exception.KvException;
//
//public class KvClientDemo {
//    public static void main(String[] args) throws KvException {
//        // step1. 先创建一个KvConfig，确定一些客户端初始化的参数。其中remoteServerName, localAppKey两个参数必填，其他参数选填
//        KvConfig kvConfig = KvConfig.builder("redis-mtuav-udm_dev", "com.sankuai.mad.w.udm").readStrategy("master-slave").build();
//        // step2. 利用前面创建的KvConfig，初始化KvClient
//        KvClient kvClient = KvClient.create(kvConfig);
//    }
//}
