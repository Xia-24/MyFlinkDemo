package com.meituan.flinkdemo;

//
//StreamExecutionEnvironment env = StreamContextEnvironment.getExecutionEnvironment();
//
//
//DataStreamSource<AdData> dataStreamSource = env.fromCollection(null);
//
//DataStream<AdData> dedupStream = dataStreamSource.map(item -> item).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<AdData>(Time.minutes(1)) {
//
//    @Override
//    public long extractTimestamp(AdData element) {
//        return element.getTime();
//    }
//})	.keyBy(item -> Tuple2.of(TimeWindow.getWindowStartWithOffset(item.getTime(), 0,
//    Time.hours(1).toMilliseconds()) + Time.hours(1).toMilliseconds(), item.getId()))
//    .process(new RocksDbDeduplicateProcessFunc());
//
//
//    env.execute();
public class xxx{
    public static void main(String[] args) {
        Long l = 1622513218014L;
        Long l1 = l / 1000;
        Long l2 = 1622513218515L;
        Long l3 = l2 / 1000;
        System.out.println(l1);
        System.out.println(l3);
    }
}