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