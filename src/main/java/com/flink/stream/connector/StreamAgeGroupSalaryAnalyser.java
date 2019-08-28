package com.flink.stream.connector;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamAgeGroupSalaryAnalyser {
    public static void main(String[] args) throws Exception {
        //The StreamExecutionEnvironment is the context in which a program is executed
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //Setting passed parameter in args array to ParameterTools for setting in the environment
        final ParameterTool params = ParameterTool.fromArgs(args);
        // Setting passed parameters available in the web interface
        environment.getConfig().setGlobalJobParameters(params);

        //Stream conversion for processing the file
        DataStream<String> data = environment.readTextFile(params.get("input"));

        // file contains AgeGroup, Salary, Count
        DataStream<Tuple3<String, Integer, Integer>> mapped = data.map(new Tokenizer());

        // groupBy 'ageGroup'
        DataStream<Tuple3<String, Integer, Integer>> reduced = mapped.keyBy(0).reduce(new ReduceSalary());

        //create datastream of average income based on age group
        DataStream<Tuple2<String, Double>> profitPerGroup = reduced.map(new MapFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Double>>() {
            public Tuple2<String, Double> map(Tuple3<String, Integer, Integer> input) {
                return new Tuple2<String, Double>(input.f0, new Double((input.f1 * 1.0) / input.f2));
            }
        });
        //Printing the result, which can be seen in the log
        profitPerGroup.print();
        // Write data into the output file,file will get created in stream folder of output
        profitPerGroup.writeAsText(params.get("output") + "/groupsalary", FileSystem.WriteMode.OVERWRITE);
        ;
        // execute program
        environment.execute("Avgerage Group Salary");
    }

    /**
     * This class used for adding up the salary of all the passed people data and adding up count of such data
     */
    public static class ReduceSalary implements ReduceFunction<Tuple3<String, Integer, Integer>> {
        public Tuple3<String, Integer, Integer> reduce(Tuple3<String, Integer, Integer> current,
                                                       Tuple3<String, Integer, Integer> pre_result) {
            return new Tuple3<String, Integer, Integer>(current.f0, current.f1 + pre_result.f1, current.f2 + pre_result.f2);
        }
    }

    /**
     * This class used for dividing the passed data into age group based on ageGroup
     */
    public static class Tokenizer implements MapFunction<String, Tuple3<String, Integer, Integer>> {
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
