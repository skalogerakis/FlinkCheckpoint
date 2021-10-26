package org.example;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.metrics.Counter;
import org.apache.flink.runtime.checkpoint.Checkpoint;
import org.apache.flink.runtime.checkpoint.CompletedCheckpoint;
import org.apache.flink.runtime.checkpoint.OperatorState;
import org.apache.flink.runtime.checkpoint.OperatorSubtaskState;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.checkpoint.metadata.MetadataSerializer;
import org.apache.flink.runtime.state.*;
import org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorageAccess;
import org.apache.flink.runtime.state.filesystem.FsCompletedCheckpointStorageLocation;
import org.apache.flink.state.api.ExistingSavepoint;
import org.apache.flink.state.api.Savepoint;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.io.DataInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.state.api.runtime.SavepointLoader.loadSavepointMetadata;

public class CheckpointProcessorAPI {

    public static void main(String[] args) throws Exception {

//        ExecutionEnvironment bEnv   = ExecutionEnvironment.getExecutionEnvironment();
//        EmbeddedRocksDBStateBackend backend = new EmbeddedRocksDBStateBackend();

        CheckpointMetadata ls = loadSavepointMetadata("/home/hdoop/Documents/LocalCHK/127a88781ba47a5a0d7562f0885354c5/chk-6/_metadata");

        System.out.println(ls.getOperatorStates().size());
        Collection<OperatorState> tst = ls.getOperatorStates();
        for(OperatorState s: ls.getOperatorStates()){
            Map<Integer, OperatorSubtaskState> operatorStates = s.getSubtaskStates();
//            Collection<OperatorSubtaskState> temp = operatorStates.values();
//            temp.isEmpty();
            for(OperatorSubtaskState opState: operatorStates.values()){
                for(KeyedStateHandle keyHandle: opState.getManagedKeyedState()){
                    System.out.println(keyHandle.toString());
                }


            }
        }
//        while(ls.getOperatorStates().iterator().hasNext()){
//            Map<Integer, OperatorSubtaskState> operatorStates = ls.getOperatorStates().iterator().next().getSubtaskStates();
//            Collection<OperatorSubtaskState> temp = operatorStates.values();
////            while(temp.iterator().hasNext()){
////                temp.iterator().next().
////                System.out.println(temp.iterator().next().toString());
////            }
//        }
        //        ls.ge
//        ls.


//        bEnv.execute("CheckpointProcessorAPI");
    }

}
