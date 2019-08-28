package com.flink.stream.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;


public class BroadcastImpl {
    public static final MapStateDescriptor<String, String> excludeSalaryDescriptor =
            new MapStateDescriptor<String, String>("exclude_salary", BasicTypeInfo.STRING_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO);

    public static void main(String[] args) throws Exception {
        //The StreamExecutionEnvironment is the context in which a program is executed & set time characteristics
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        // output path can be made dynamic by passing value in ParameterTool class as done in join & connector sub-package
        String output = "file:///Users/ranvsing/CodeBase/Flink/output/state/broadcast";
        // input path can be made dynamic by passing value in ParameterTool class as done in join & connector package
        String input = "file:///Users/ranvsing/CodeBase/Flink/input/salaryrange";

        // Connect to port for listing the incoming data
        DataStream<String> excludeSalary = environment.socketTextStream("localhost", 9090);

        // creating broadcast stream for other nodes
        BroadcastStream<String> excludeSalaryBroadcast = excludeSalary.broadcast(excludeSalaryDescriptor);

        // Create Tuple for incoming data divide based on gender group
        DataStream<Tuple2<String, Integer>> employees = environment.readTextFile(input)
                .map(new MapFunction<String, Tuple2<String, String>>() {
                    public Tuple2<String, String> map(String value) {
                        return new Tuple2<String, String>(value.split(",")[1], value);
                    }
                })
                .keyBy(0)
                .connect(excludeSalaryBroadcast)
                .process(new ExcludeSalary());
        //Printing the result, which can be seen in the log
        employees.print();
        employees.writeAsText(output, FileSystem.WriteMode.OVERWRITE);
        //execute program.
        environment.execute("Broadcast Exmaple");
    }

    /**
     * This class will compare the broadcast stream data with internal data, and neglect all the record broadcasted as fault salary record
     */
    public static class ExcludeSalary extends KeyedBroadcastProcessFunction<String, Tuple2<String, String>, String, Tuple2<String, Integer>> {
        private transient ValueState<Integer> countState;

        public void processElement(Tuple2<String, String> value, ReadOnlyContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            Integer currCount = countState.value();
            final String cId = value.f1.split(",")[0];
            // excluding data available in excluded salary
            for (Map.Entry<String, String> cardEntry : ctx.getBroadcastState(excludeSalaryDescriptor).immutableEntries()) {
                final String excludeId = cardEntry.getKey();
                if (cId.equals(excludeId))
                    return;
            }
            // increase cont for data based on the group
            countState.update(currCount + 1);
            // result creation with salary details, last column represent the count based on gender
            out.collect(new Tuple2<String, Integer>(value.f1, currCount + 1));
        }

        // Setting up the fault salary details for broadcast
        public void processBroadcastElement(String salaryRecord, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
            String id = salaryRecord.split(",")[0];
            ctx.getBroadcastState(excludeSalaryDescriptor).put(id, salaryRecord);
        }

        public void open(Configuration conf) {
            ValueStateDescriptor<Integer> desc = new ValueStateDescriptor<Integer>("", BasicTypeInfo.INT_TYPE_INFO, 0);
            countState = getRuntimeContext().getState(desc);
        }
    }
}

