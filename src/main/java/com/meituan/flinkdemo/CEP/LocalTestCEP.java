//package com.meituan.flinkdemo;
//
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
//import com.meituan.flink.common.config.JobConf;
//import com.meituan.flink.common.config.KafkaTopic;
//import com.meituan.flink.common.kafka.MTKafkaConsumer010;
//import com.meituan.flink.common.kafka.MTKafkaProducer010;
//import lombok.SneakyThrows;
//import org.apache.flink.api.common.functions.MapFunction;
//import org.apache.flink.api.common.serialization.SimpleStringSchema;
//import org.apache.flink.cep.CEP;
//import org.apache.flink.cep.PatternFlatSelectFunction;
//import org.apache.flink.cep.PatternFlatTimeoutFunction;
//import org.apache.flink.cep.PatternStream;
//import org.apache.flink.cep.pattern.Pattern;
//import org.apache.flink.cep.pattern.conditions.SimpleCondition;
//import org.apache.flink.streaming.api.TimeCharacteristic;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//import org.apache.flink.streaming.api.functions.ProcessFunction;
//import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
//import org.apache.flink.streaming.api.windowing.time.Time;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase;
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
//import org.apache.flink.util.Collector;
//import org.apache.flink.util.OutputTag;
//
//import java.text.SimpleDateFormat;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//public class LocalTestCEP {
//    private static final String READ_KAFKA_TOPIC = "app.udm_udw_command";
//    private static final String WRITE_KAFKA_TOPIC = "app.join";
//    public static void main(String[] args) throws Exception {
//        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
//        env.setParallelism(10);
//        Long delay = 1000L;
//
//        MTKafkaConsumer010 mtKafkaConsumer010 = new MTKafkaConsumer010(args);
//        mtKafkaConsumer010.build(new SimpleStringSchema());
//        Map.Entry<KafkaTopic, FlinkKafkaConsumerBase> commandConsumerEntry = mtKafkaConsumer010
//                .getConsumerByName(READ_KAFKA_TOPIC,"xr_inf_namespace");
//        DataStream<String> commandStringStream = env.addSource(commandConsumerEntry.getValue());
//        DataStream<CommandLogRecord> commandStream = commandStringStream
//                .map((MapFunction<String, CommandLogRecord>) value -> JSONObject.parseObject(value,CommandLogRecord.class))
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<CommandLogRecord>(Time.milliseconds(delay)) {
//                    @SneakyThrows
//                    @Override
//                    public long extractTimestamp(CommandLogRecord element) {
//                        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(element.getTs()).getTime());
//                        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(element.getTs()).getTime();
//                    }
//                })
//                .uid(READ_KAFKA_TOPIC)
//                .name(READ_KAFKA_TOPIC);
//        OutputTag<String> airportOutputTag = new OutputTag<String>("airport_output_tag"){};
//        OutputTag<String> droneOutputTag = new OutputTag<String>("drone_output_tag"){};
//        OutputTag<CommandLogRecord> airportRetrySideOutputTag = new OutputTag<CommandLogRecord>("airport_retry_side-output-tag"){};
//        OutputTag<CommandLogRecord> droneRetrySideOutputTag = new OutputTag<CommandLogRecord>("drone_retry_side-output-tag"){};
//        SingleOutputStreamOperator<CommandLogRecord> sideOutputStream = commandStream
//                .process(new ProcessFunction<CommandLogRecord, CommandLogRecord>() {
//                    @Override
//                    public void processElement(CommandLogRecord value, Context ctx, Collector<CommandLogRecord> out) throws Exception {
//                        if(value.getType().equals("airport_retry")){
//                            ctx.output(airportRetrySideOutputTag,value);
//                        }
//                        else if(value.getType().equals("drone_retry")){
//                            ctx.output(droneRetrySideOutputTag,value);
//                        }
//                        else{
//                            out.collect(value);
//                        }
//                    }
//                });
//        DataStream<CommandLogRecord> airportRetryStream = sideOutputStream.getSideOutput(airportRetrySideOutputTag);
//        DataStream<CommandLogRecord> droneRetryStream = sideOutputStream.getSideOutput(droneRetrySideOutputTag);
//        Pattern<CommandLogRecord,CommandLogRecord> pattern1 = Pattern
//                .<CommandLogRecord> begin("airport_retry")
//                .where(new SimpleCondition<CommandLogRecord>() {
//                    @Override
//                    public boolean filter(CommandLogRecord value) throws Exception {
//                        return value.getType().equals("airport_retry");
//                    }
//                })
//                .within(Time.seconds(30));
//        Pattern<CommandLogRecord,CommandLogRecord> pattern2 = Pattern
//                .<CommandLogRecord> begin("drone_retry")
//                .where(new SimpleCondition<CommandLogRecord>() {
//                    @Override
//                    public boolean filter(CommandLogRecord value) throws Exception {
//                        return value.getType().equals("drone_retry");
//                    }
//                })
//                .within(Time.seconds(30));
//
//        PatternStream<CommandLogRecord> airportPatternStream = CEP.pattern(airportRetryStream,pattern1);
//        PatternStream<CommandLogRecord> dronePatternStream = CEP.pattern(droneRetryStream,pattern2);
//        SingleOutputStreamOperator<String> airportOutputStream = airportPatternStream
//                .flatSelect(airportOutputTag
//                        , new PatternFlatTimeoutFunction<CommandLogRecord, String>() {
//                            @Override
//                            public void timeout(Map<String, List<CommandLogRecord>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
//
//                            }
//                        },
//                        new PatternFlatSelectFunction<CommandLogRecord, String>() {
//                            @Override
//                            public void flatSelect(Map<String, List<CommandLogRecord>> pattern, Collector<String> out) throws Exception {
//                                List<CommandLogRecord> list = pattern.get("airport_retry");
//                                Map<String,Integer> retryCount = new HashMap<>();
//                                for(CommandLogRecord commandLogRecord:list){
//                                    String serialNumber = commandLogRecord.getSerialnumber();
//                                    retryCount.put(serialNumber,retryCount.getOrDefault(serialNumber,0)+1);
//                                }
//                                // CommandLogRecord commandLogRecord = pattern.get("airport_retry").iterator().next();
//                                for(Map.Entry<String,Integer> entry:retryCount.entrySet()){
//                                    Integer cnt = entry.getValue();
//                                    if(cnt>=5){
//                                        for(CommandLogRecord command:list){
//                                            if(command.getSerialnumber().equals(entry.getKey())){
//                                                out.collect(entry.getKey() + "," + command.getTask_id());
//                                                break;
//                                            }
//                                        }
//                                    }
//                                }
//                            }
//                        });
//        SingleOutputStreamOperator<String> droneOutputStream = dronePatternStream
//                .flatSelect(droneOutputTag
//                        , new PatternFlatTimeoutFunction<CommandLogRecord, String>() {
//                            @Override
//                            public void timeout(Map<String, List<CommandLogRecord>> pattern, long timeoutTimestamp, Collector<String> out) throws Exception {
//
//                            }
//                        },
//                        new PatternFlatSelectFunction<CommandLogRecord, String>() {
//                            @Override
//                            public void flatSelect(Map<String, List<CommandLogRecord>> pattern, Collector<String> out) throws Exception {
//                                List<CommandLogRecord> list = pattern.get("drone_retry");
//                                Map<String,Integer> retryCount = new HashMap<>();
//                                for(CommandLogRecord commandLogRecord:list){
//                                    String serialNumber = commandLogRecord.getSerialnumber();
//                                    retryCount.put(serialNumber,retryCount.getOrDefault(serialNumber,0)+1);
//                                }
//                                // CommandLogRecord commandLogRecord = pattern.get("airport_retry").iterator().next();
//                                for(Map.Entry<String,Integer> entry:retryCount.entrySet()){
//                                    Integer cnt = entry.getValue();
//                                    if(cnt>=5){
//                                        for(CommandLogRecord command:list){
//                                            if(command.getSerialnumber().equals(entry.getKey())){
//                                                out.collect(entry.getKey() + "," + command.getTask_id());
//                                                break;
//                                            }
//                                        }
//                                    }
//                                }
//                            }
//                        });
//
//        MTKafkaProducer010 mtKafkaProducer010 = new MTKafkaProducer010(args);
//        mtKafkaProducer010.build(new SimpleStringSchema());
//        Map<KafkaTopic, FlinkKafkaProducer010> topic2Produces = mtKafkaProducer010.getTargetTopicsToProducers();
//
//        for(Map.Entry<KafkaTopic,FlinkKafkaProducer010> entry:topic2Produces.entrySet()){
//            airportOutputStream.addSink(entry.getValue())
//                    .setParallelism(entry.getKey().getParallelism())
//                    .uid("airport_retry")
//                    .name("airport_retry");
//            droneOutputStream.addSink(entry.getValue())
//                    .setParallelism(entry.getKey().getParallelism())
//                    .uid("drone_retry")
//                    .name("drone_retry");
//        }
//        env.execute(new JobConf(args).getJobName());
//
//    }
//}
