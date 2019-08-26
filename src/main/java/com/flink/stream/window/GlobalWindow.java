package com.flink.stream.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;

public class GlobalWindow {
    public static void main(String[] args) throws Exception
    {
        //The StreamExecutionEnvironment is the context in which a program is executed & set time characteristics
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        // Using this we can set the parallel stream count
        //environment.setParallelism(1);

        // output path can be made dynamic by passing value in ParameterTool class as done in join & connector sub-package
        String output ="file:///Users/ranvsing/CodeBase/Flink/output/window/global";
        // Connect to port for listing the incoming data
        DataStream<String> data = environment.socketTextStream("localhost", 9090);

        // file contains AgeGroup, Salary, Count
        DataStream<Tuple3< String, Integer, Integer>> mapped = data.map(new Tokenizer());

        /**
         * groupBy 'ageGroup' with Global window
         * .trigger() is compulsory for global window, to perform operation on incoming data when it reaches the count,
         * like here the trigger set to 5
         */
        DataStream<Tuple3< String, Integer, Integer>> reduced = mapped
                .keyBy(0)
                .window(GlobalWindows.create())
                .trigger(CountTrigger.of(5))
                .reduce(new ReduceSalary());


        //Printing the result, which can be seen in the log
        reduced.print();
        // Write data into the output file,file will get created in stream folder of output
        reduced.writeAsText(output, FileSystem.WriteMode.OVERWRITE);
        // execute program
        environment.execute("Global Group Salary");
    }

    /**
     * This class used for adding up the salary of all the passed people data and adding up count of such data
     */
    public static class ReduceSalary implements ReduceFunction<Tuple3< String, Integer, Integer>>
    {
        public Tuple3<  String, Integer, Integer> reduce(Tuple3<  String, Integer, Integer> current,
                                                         Tuple3< String, Integer, Integer> pre_result)
        {
            return new Tuple3< String, Integer, Integer>(current.f0, current.f1+pre_result.f1, current.f2 + pre_result.f2);
        }
    }
    /**
     * This class used for dividing the passed data into age group based on ageGroup
     */
    public static class Tokenizer implements MapFunction<String, Tuple3< String, Integer, Integer>> {
        public Tuple3<String, Integer, Integer> map(String value) {
            String[] words = value.split(",");
            Integer age = Integer.parseInt(words[2]);
            String ageGroup;
            if (age < 18)
                ageGroup = "child";
            else if (age < 30)
                ageGroup = "adult";
            else if (age < 50)
                ageGroup = "middle";
            else
                ageGroup = "old";
            return new Tuple3<String, Integer, Integer>(ageGroup, Integer.parseInt(words[3]), 1);
        }
    }
}

