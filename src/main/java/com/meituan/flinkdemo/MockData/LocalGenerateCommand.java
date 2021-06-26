package com.meituan.flinkdemo.MockData;

import com.alibaba.fastjson.JSON;
import com.meituan.flinkdemo.Entity.CommandLogRecord;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.text.SimpleDateFormat;
import java.util.Properties;
import java.util.Random;

public class LocalGenerateCommand {
    private static String dt = "2021-06-21";
    private static String message=" ";
    private static String[] serialnumber={"serial1","serial2","serial3","serial4"};
    private static String taskid = " ";
//    private static String ts;
    private static CommandLogRecord.LogType[] type ={CommandLogRecord.LogType.AIRPORT, CommandLogRecord.LogType.DRONE, CommandLogRecord.LogType.DRONE_RETRY, CommandLogRecord.LogType.AIRPORT_RETRY, CommandLogRecord.LogType.AIRPORT_ACK, CommandLogRecord.LogType.DRONE_ACK};
    private static CommandLogRecord.CommandType commandtype = CommandLogRecord.CommandType.ATT_CARGO_ARRIVED_AIRPORT;
    private static String waybill =" ";
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<String> msgstream = env.addSource(new SourceFunction<String>() {
            private Random r = new Random();
            private static final long serial = 1L;
            private Boolean running = true;

            @Override
            public void run(SourceContext<String> sourceContext) throws Exception {

                while(running){
                    Thread.sleep(100);
                    String ts = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(System.currentTimeMillis());
                    CommandLogRecord commandLogRecord = new CommandLogRecord(ts,type[r.nextInt(6)],serialnumber[r.nextInt(4)],String.valueOf(r.nextInt(50)),waybill,commandtype,message,dt);
                    sourceContext.collect(JSON.toJSONString(commandLogRecord));

                }
            }

            @Override
            public void cancel() {
                running = false;
            }
        });
//

//

        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers","localhost:9092");
//        prop.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //key 序列化
//        prop.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"); //value 序列
        FlinkKafkaProducer producer = new FlinkKafkaProducer("test",new SimpleStringSchema(),prop);


        msgstream.addSink(producer).setParallelism(1).name("flink2kafka");
        env.execute();
    }
}
