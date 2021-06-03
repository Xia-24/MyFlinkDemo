package com.meituan.flinkdemo;

import com.dianping.squirrel.asyncclient.api.async.SquirrelAsyncCommands;
import com.dianping.squirrel.asyncclient.core.*;

public class helloSquirrel{
    public static void main(String[] args) {
        SquirrelConfig squirrelConfig = new SquirrelConfig();
        squirrelConfig.setReadTimeout(1000);
        squirrelConfig.setRouterType(RouterType.MASTER_SLAVE);
        squirrelConfig.setIdcSensitive(true);
        squirrelConfig.setSerializeType("hessian");
        squirrelConfig.setUseBackupRequest(true);

        SquirrelClient squirrelClient = SquirrelClient.createCluster("redis-mtuav-udm_qa", squirrelConfig);
        SquirrelAsyncCommands asyncCommands = squirrelClient.async();
        String bitMapKey = "BitMap_test";

        StoreKey storeKey = new StoreKey("stage_drone", bitMapKey);

        asyncCommands.set(storeKey,"BitMap_test_value");

//        asyncCommands.
//        SquirrelFuture<Boolean> exists = asyncCommands.getBit(storeKey,0L);
//        asyncCommands.setBit(storeKey,0L,true);
    }
}