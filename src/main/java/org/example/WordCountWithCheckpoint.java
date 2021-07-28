package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
//import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.metrics.Counter;
//import org.apache.flink.runtime.state.storage.FileSystemCheckpointStorage;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;


public class WordCountWithCheckpoint {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //Get parameters from CLI
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        env.getConfig().setGlobalJobParameters(parameterTool);
        String checkPointPath = parameterTool.get("c");
        if (checkPointPath == null || checkPointPath.trim().isEmpty()) {
            throw new IllegalArgumentException("checkpoint-path is mandatory for storing state");
        }


//        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //Use RocksDB and incremental checkpoint
//        env.setStateBackend(new RocksDBStateBackend("hdfs://83.212.109.198:9000/user/skalogerakis/checkpoint", true));
//        env.setStateBackend(new RocksDBStateBackend(Paths.get("/home/skalogerakis/Projects/FlinkCheckpoint/checkpoint/tst").toUri(), true));

        //This is for hdfs deployment

//        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
//        env.getCheckpointConfig().setCheckpointStorage(Paths.get("/home/skalogerakis/Projects/FlinkCheckpoint/checkpoint/tester").toUri());
//        env.getCheckpointConfig().setCheckpointStorage(new FileSystemCheckpointStorage(checkPointPath));

        env.enableCheckpointing(5000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//        env.setStateBackend(new RocksDBStateBackend(Paths.get("/home/skalogerakis/Projects/FlinkCheckpoint/checkpoint/Tester").toUri(), true));
//        env.setStateBackend(new RocksDBStateBackend("hdfs://83.212.109.198:9000/user/skalogerakis/checkpoint", true));

        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointStorage(checkPointPath);

        env.setParallelism(1);

        //In this case use netcat
        DataStream<String> text = env.socketTextStream("localhost", 9999);
        DataStream<String> words = text.map(new RichMapFunction<String, String>() {
            private transient Counter counter;

            @Override
            public void open(Configuration config) {
                this.counter = getRuntimeContext()
                        .getMetricGroup()
                        .counter("myCounter");
            }

            @Override
            public String map(String value) throws Exception {
                this.counter.inc();
                return value;
            }
        }).flatMap(new FlatMapFunction<String, String>() {
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] split = s.split(" ");
                for (String s1 : split) {
                    collector.collect(s1);
                }
            }
        });

        DataStream<Tuple2<String, Integer>> wordCount = words.keyBy((s) -> s).process(new StatefulReduceFunc());

        wordCount.print();
        env.execute("Wordcount");
    }

    private static class StatefulReduceFunc extends KeyedProcessFunction<String, String, Tuple2<String, Integer>> {

        private transient ValueState<Integer> count;

        public void processElement(String s, Context context, Collector<Tuple2<String, Integer>> collector) throws Exception {
            int currentCnt = count.value() == null ? 1 : 1 + count.value();
            count.update(currentCnt);
            collector.collect(new Tuple2<>(s, currentCnt));
        }

        public void open(Configuration parameters) {
            ValueStateDescriptor<Integer> valueStateDescriptor = new ValueStateDescriptor<Integer>("count", Integer.class);
            count = getRuntimeContext().getState(valueStateDescriptor);
        }
    }
}
