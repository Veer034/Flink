package com.flink.stream.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class ListStateImpl {
    public static void main(String[] args) throws Exception {
        //The StreamExecutionEnvironment is the context in which a program is executed & set time characteristics
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // output path can be made dynamic by passing value in ParameterTool class as done in join & connector sub-package
        String output = "file:///Users/ranvsing/CodeBase/Flink/output/state/liststate";

        // Connect to port for listing the incoming data
        DataStream<String> data = environment.socketTextStream("localhost", 9090);
        //Splitting the incoming data using split
        DataStream<Tuple2<String, Long>> sum = data.map(new MapFunction<String, Tuple2<Long, Long>>() {
            public Tuple2<Long, Long> map(String s) {
                String[] words = s.split(",");
                return new Tuple2<Long, Long>(Long.parseLong(words[0]), Long.parseLong(words[1]));
            }
        })
                .keyBy(0)
                .flatMap(new StatefulMap());
        //Printing the result, which can be seen in the log
        sum.print();
        sum.writeAsText(output, FileSystem.WriteMode.OVERWRITE);
        // execute program
        environment.execute("List State implementation.");
    }

    /**
     * This class consist of method to perfom operation on incoming data, prior to using flatMap method open() is
     * used to initiate the value state variables.
     */
    public static class StatefulMap extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<String, Long>> {
        private transient ValueState<Long> count;
        private transient ListState<Long> numbers;

        /**
         * This method contains logic for summing up all the passed values
         *
         * @param input
         * @param out
         * @throws Exception
         */
        public void flatMap(Tuple2<Long, Long> input, Collector<Tuple2<String, Long>> out) throws Exception {
            Long currCount = count.value();
            currCount += 1;
            count.update(currCount);
            numbers.add(input.f1);
            if (currCount >= 20) {
                Long sum = 0L;
                String numbersStr = "";
                for (Long number : numbers.get()) {
                    numbersStr = numbersStr + "," + number;
                    sum = sum + number;
                }
                //Store  the values if reached the threshold value
                out.collect(new Tuple2<String, Long>(numbersStr, sum));
                // then clean up the state
                count.clear();
                numbers.clear();
            }
        }

        /**
         * This method important for setting the datatype of the value and list state using the StateDescriptor
         * getRuntimeContext() method of AbstractRichFunction.class will be used for fetching the object of the
         * individual state.
         */
        public void open(Configuration conf) {
            ListStateDescriptor<Long> listDesc = new ListStateDescriptor<Long>("numbers", Long.class);
            numbers = getRuntimeContext().getListState(listDesc);

            ValueStateDescriptor<Long> descriptor2 = new ValueStateDescriptor<Long>("count", Long.class, 0L);
            count = getRuntimeContext().getState(descriptor2);
        }
    }
}



