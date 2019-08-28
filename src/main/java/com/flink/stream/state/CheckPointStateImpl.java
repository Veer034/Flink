package com.flink.stream.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class CheckPointStateImpl {
    public static void main(String[] args) throws Exception {
        //The StreamExecutionEnvironment is the context in which a program is executed & set time characteristics
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        // configure checkpoint millisecond difference
        environment.enableCheckpointing(1000);
        // configure checkpoint min pause, for setting min pause
        environment.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        // configure the timeout 10000 ms, will get discarded if checkpoint operation took more time.
        environment.getCheckpointConfig().setCheckpointTimeout(10000);
        // configure checkpoint more to set to exactly-once (this is the default)
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);  // AT_LEAST_ONCE
        // set comcurrency in checkpoint to be in progress at the same time
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // enable externalized checkpoints which are retained after job cancellation
        environment.getCheckpointConfig().enableExternalizedCheckpoints(ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);  // DELETE_ON_CANCELLATION
        // configure to start the server in case of failure while processing record
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 100));
        // output path can be made dynamic by passing value in ParameterTool class as done in join & connector sub-package
        String output = "file:///Users/ranvsing/CodeBase/Flink/output/state/checkpointstate";
        // Connect to port for listing the incoming data
        DataStream<String> data = environment.socketTextStream("localhost", 9090);

        //Splitting the incoming data using split
        DataStream<Long> sum = data.map(new MapFunction<String, Tuple2<Long, String>>() {
            public Tuple2<Long, String> map(String s) {
                String[] words = s.split(",");
                return new Tuple2<Long, String>(Long.parseLong(words[0]), words[1]);
            }
        })
                .keyBy(0)
                .flatMap(new StatefulMap());
        //Printing the result, which can be seen in the log
        sum.print();
        sum.writeAsText(output, FileSystem.WriteMode.OVERWRITE);
        // execute program
        environment.execute("Checkpoint State Implementation.");
    }

    /**
     * This method contains logic for summing up all the passed values
     */
    public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long> {
        private transient ValueState<Long> sum;
        private transient ValueState<Long> count;

        public void flatMap(Tuple2<Long, String> input, Collector<Long> out) throws Exception {
            Long currCount = count.value();
            Long currSum = sum.value();

            currCount += 1;
            currSum = currSum + Long.parseLong(input.f1);

            count.update(currCount);
            sum.update(currSum);

            if (currCount >= 10) {
                //Store  the values if reached the threshold value
                out.collect(sum.value());
                // then clean up the state
                count.clear();
                sum.clear();
            }
        }

        /**
         * This method important for setting the datatype of the value and list state using the StateDescriptor
         * getRuntimeContext() method of AbstractRichFunction.class will be used for fetching the object of the
         * individual state.
         */
        public void open(Configuration conf) {
            ValueStateDescriptor<Long> descriptor = new ValueStateDescriptor<Long>("sum", TypeInformation.of(new TypeHint<Long>() {
            }), 0L);
            sum = getRuntimeContext().getState(descriptor);

            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>("count", TypeInformation.of(new TypeHint<Long>() {
            }), 0L);
            count = getRuntimeContext().getState(descriptor2);
        }
    }
}
