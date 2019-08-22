package com.flink.stream;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamAggregation
{
    public static void main(String[] args) throws Exception
    {
        //The StreamExecutionEnvironment is the context in which a program is executed
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //Setting passed parameter in args array to ParameterTools for setting in the environment
        final ParameterTool params = ParameterTool.fromArgs(args);
        // Setting passed parameters available in the web interface
        environment.getConfig().setGlobalJobParameters(params);

        //Stream conversion for processing the file
        DataStream<String> data = environment.readTextFile(params.get("input"));

        // gender, age,salary
        DataStream<Tuple3<String,Integer, Integer>> mapped = data.map(new Tokenizer());
        /** group by gender, then performing sum,min,max
         *  For fetching the sum of salary group by gender, we will get list of element. last element of
         *  the group will have the latest value for the group.
         *  The difference between min and minBy is that min returns the minimum value, whereas minBy returns
         *  the element that has the minimum value in this field (same for max and maxBy).
         * */
        mapped.keyBy(0).sum(2).writeAsText(params.get("output")+"/agg-sum");
        mapped.keyBy(0).min(2).writeAsText(params.get("output")+"/agg-min");
        mapped.keyBy(0).minBy(2).writeAsText(params.get("output")+"/agg-minby");
        mapped.keyBy(0).max(2).writeAsText(params.get("output")+"/agg-max");
        mapped.keyBy(0).maxBy(2).writeAsText(params.get("output")+"/agg-maxBy");
        // execute program
        environment.execute("Aggregation");
    }

    /**
     * This class will create Tokens for the passed String and considering important value only, removing useless attribute.
     * In current scenario user id number is not relevant, so removing 1st value.
     */
    public static class Tokenizer implements MapFunction<String, Tuple3<String,Integer,Integer>>
    {
        public Tuple3<String,Integer, Integer> map(String value)
        {
            String[] words = value.split(",");
            return new Tuple3<String,Integer, Integer>(words[1],Integer.parseInt(words[2]),Integer.parseInt(words[3]));
        }
    }
}