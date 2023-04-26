package main;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
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

        int size = parameterTool.getInt("size", -1);
        if (size == -1) {
            throw new IllegalArgumentException("The size in MBs of the Data that the Synthetic Source will produce is mandatory");
        }


        long dataSizeInBytes = size * 1024 * 1024; // The desired size in MBs of the Synthetic Source

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


        DataStream<Tuple2<String, Integer>> uuidCount = uuid.keyBy((s) -> s)
                                                            .process(new StatefulReduceFunc())
                                                            .name("UUID Counter")
                                                            .uid("UUID Counter");


//        uuidCount.print();

        env.execute("Synthetic UID Data Stream");
    }

    private static class StatefulReduceFunc extends KeyedProcessFunction<String, String, Tuple2<String, Integer>> {
        private transient ValueState<Integer> count;

        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>("count", Integer.class);
            count = getRuntimeContext().getState(valueStateDescriptor);
        }
        public void processElement(String s,
                                   Context context,
                                   Collector<Tuple2<String, Integer>> collector) throws Exception {
            int currentCnt = count.value() == null ? 1 : 1 + count.value();
            count.update(currentCnt);
            collector.collect(new Tuple2<>(s, currentCnt));
        }


    }
}
