package main;

import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext;
import java.util.Random;
import java.util.UUID;

public class SyntheticSourceDataGenerator extends RichParallelSourceFunction<String> {
    private volatile boolean running = true;
    private long dataSizeInBytes;

    public SyntheticSourceDataGenerator(long dataSizeInBytes) {
        this.dataSizeInBytes = dataSizeInBytes;
    }

    @Override
    public void run(SourceContext<String> sourceContext) throws Exception {
        Random random = new Random();
        StringBuilder builder = new StringBuilder();
        long generatedBytes = 0;
        while (running) {
            if(generatedBytes < dataSizeInBytes){
                builder.setLength(0);
                builder.append(UUID.randomUUID().toString());
                String uid = builder.toString();
                sourceContext.collect(uid);
                generatedBytes += uid.getBytes().length;
            }

        }
    }

//    @Override
//    public void run(SourceContext<Long> sourceContext) throws Exception {
//        long generatedBytes = 0;
//        long id = 0L;
//        while (running) {
//            if(generatedBytes < dataSizeInBytes){
//                sourceContext.collect(id);
//                id++;
//                generatedBytes += 8; // long is 8 bytes
//            }
//
//        }
//    }

    @Override
    public void cancel() {
        running = false;
    }
}

