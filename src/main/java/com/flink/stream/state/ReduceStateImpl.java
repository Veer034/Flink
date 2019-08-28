package com.flink.stream.state;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ReduceStateImpl {
    public static void main(String[] args) throws Exception {
        //The StreamExecutionEnvironment is the context in which a program is executed & set time characteristics
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // output path can be made dynamic by passing value in ParameterTool class as done in join & connector sub-package
        String output = "file:///Users/ranvsing/CodeBase/Flink/output/state/reducestate";
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
        environment.execute("Reduce State Implementation.");
    }

    public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, String>, Long> {
        private transient ValueState<Long> count;
        private transient ReducingState<Long> sum;

        public void flatMap(Tuple2<Long, String> input, Collector<Long> out) throws Exception {
            Long currValue = Long.parseLong(input.f1);
            Long currCount = count.value();
            currCount += 1;
            count.update(currCount);
            sum.add(currValue);
            if (currCount >= 10) {
                //Store  the values if reached the threshold value
                out.collect(sum.get());
                // then clean up the state
                count.clear();
                sum.clear();
            }
        }

        public void open(Configuration conf) {

            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>("count", Long.class, 0L);
            count = getRuntimeContext().getState(descriptor2);

            ReducingStateDescriptor<Long> sumDesc = new ReducingStateDescriptor<Long>("reducing sum", new SumReduce(), Long.class);
            sum = getRuntimeContext().getReducingState(sumDesc);
        }

        public class SumReduce implements ReduceFunction<Long> {
            public Long reduce(Long commlativesum, Long currentvalue) {
                return commlativesum + currentvalue;
            }
        }
    }
}



