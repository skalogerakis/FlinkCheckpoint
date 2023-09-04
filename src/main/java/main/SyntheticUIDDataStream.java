package main;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;
import org.apache.flink.util.Collector;

import java.security.Timestamp;
import java.util.UUID;

public class SyntheticUIDDataStream {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();




        /**
         * Parameters from CLI, both required for successful execution
         * - c: checkpointing path for storing state
         * - file: file path for reading the input
         */

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        String checkPointPath = parameterTool.get("c");
        if (checkPointPath == null || checkPointPath.trim().isEmpty()) {
            throw new IllegalArgumentException("checkpoint-path is mandatory for storing state");
        }

        long size = parameterTool.getLong("size", -1);
        if (size == -1) {
            throw new IllegalArgumentException("The size in MBs of the Data that the Synthetic Source will produce is mandatory");
        }


        long dataSizeInBytes = size * 1024L * 1024L; // The desired size in MBs of the Synthetic Source

        /**
         * Flink Configuration Setup -> Enable Checkpoint, Retain On Cancellation, Incremental RocksDB checkpoint
         */
        env.enableCheckpointing(15000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointStorage(checkPointPath);

        DataStream<String> uuid = env.addSource(new SyntheticSourceDataGenerator(dataSizeInBytes))
                                     .name("Synthetic Source")
                                     .uid("Synthetic Source");

//        DataStream<Long> uuid = env.addSource(new SyntheticSourceDataGenerator(dataSizeInBytes))
//                .name("Synthetic Source")
//                .uid("Synthetic Source");

        DataStream<Tuple3<String, Integer, Long>> uuidCount = uuid.keyBy((s) -> s)
                                                                    .process(new StatefulReduceFunc())
                                                                    .name("UUID Counter")
                                                                    .uid("UUID Counter");
//        DataStream<Tuple2<Long, Integer>> uuidCount = uuid.keyBy((s) -> s)
//                .process(new StatefulReduceFunc())
//                .name("UUID Counter")
//                .uid("UUID Counter");


        //uuidCount.print();

        env.execute("Synthetic UID Data Stream");
    }

    private static class StatefulReduceFunc extends KeyedProcessFunction<String, String, Tuple3<String, Integer, Long>> {
        private transient ValueState<Integer> count;
        private transient MapState<String, Long> tsRegistry;

        public void open(Configuration parameters) {
            count = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("count", Integer.class));
            tsRegistry = getRuntimeContext().getMapState(new MapStateDescriptor<String,Long>("tsRegistry", String.class, Long.class));

        }
        public void processElement(String s,
                                   Context context,
                                   Collector<Tuple3<String, Integer, Long>> collector) throws Exception {
            Long current_ts = System.currentTimeMillis();
//            int currentCnt = count.value() == null ? 1 : 1 + count.value();

            if(count.value() == null){
                //Update the counter first then add the ts of the UUID in the registry
                count.update(1);
                tsRegistry.put(s, current_ts);
                collector.collect(new Tuple3<>(s, 1, current_ts));
            }else{
                int currentCnt = count.value();
                count.update(currentCnt + 1);
                if(tsRegistry.get(s) < current_ts)
                    tsRegistry.put(s, current_ts);

                collector.collect(new Tuple3<>(s, currentCnt, current_ts));
            }


        }


    }
//    private static class StatefulReduceFunc extends KeyedProcessFunction<Long, Long, Tuple2<Long, Integer>> {
//    private transient ValueState<Integer> count;
//
//    public void open(Configuration parameters) {
//        ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>("count", Integer.class);
//        count = getRuntimeContext().getState(valueStateDescriptor);
//    }
//    public void processElement(Long s,
//                               Context context,
//                               Collector<Tuple2<Long, Integer>> collector) throws Exception {
//        int currentCnt = count.value() == null ? 1 : 1 + count.value();
//        count.update(currentCnt);
//        collector.collect(new Tuple2<>(s, currentCnt));
//    }
//
//
//}

}
