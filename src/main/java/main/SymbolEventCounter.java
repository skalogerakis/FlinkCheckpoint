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


// To get metrics via the rest api, use curl from the terminal
// http://localhost:8081/jobs/455e17c95ffba35504f06224de661b76/metrics?get=restartingTime,downtime

// TODO Isolate only the first column from the rest -> cat debs2022-gc-trading-day-08-11-21.csv | cut -d, -f1 > test.csv
// TODO Important -> head -c 50M input.csv > output.csv  -> Isolate 50Mb from an initial file

public class SymbolEventCounter {

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

        String filePath = parameterTool.get("file");
        if (filePath == null ) {
            throw new IllegalArgumentException("file path is mandatory to find the file for reading the input");
        }

        /**
         * Flink Configuration Setup -> Enable Checkpoint, Retain On Cancellation, Incremental RocksDB checkpoint
         */
        env.enableCheckpointing(15000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setStateBackend(new EmbeddedRocksDBStateBackend(true));
        env.getCheckpointConfig().setCheckpointStorage(checkPointPath);



        TextInputFormat inputFormat = new TextInputFormat(new Path(filePath));
        inputFormat.setCharsetName("UTF-8");

        DataStreamSource<String> input = env.readFile(inputFormat, filePath, FileProcessingMode.PROCESS_CONTINUOUSLY, 60000l);

        DataStream<String> words = input.filter((x) -> !x.startsWith("#") && !x.startsWith("ID,SecType") && !x.isEmpty() )
                                        .flatMap(new FlatMapFunction<String, String>() {
                                            public void flatMap(String s,
                                                                Collector<String> col) throws Exception {

                                                String[] split = s.split(",", -1);
                                                col.collect(split[0]);
                                            }
                                        })
                                        .name("Splitter")
                                        .uid("Splitter");

        DataStream<Tuple2<String, Integer>> wordCount = words.keyBy((s) -> s)
                                                             .process(new StatefulReduceFunc())
                                                             .name("Symbol Counter")
                                                             .uid("Symbol Counter");

        wordCount.print();

        env.execute("SymbolCounter");
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
