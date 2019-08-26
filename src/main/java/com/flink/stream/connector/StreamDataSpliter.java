package com.flink.stream.connector;


import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.*;

public class StreamDataSpliter {
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

        // file contains Name, RollNo, Final marks
        SplitStream<Integer> markRating = data.map(new MapFunction<String, Integer>()
        {
            public Integer map(String value)
            {
                String[] words = value.split(",");
                return Integer.parseInt(words[2]);
            }})
                .split(new OutputSelector<Integer>()
                {
                    public Iterable<String> select(Integer value)
                    {
                        List<String> out = new ArrayList<String>();
                        if (value>=75)
                            out.add("distinction");
                        else if(value>=33)
                            out.add("pass");
                        else
                            out.add("fail");
                        return out;
                    }
                });

        //Divide the data into different stream
        DataStream<Integer> distinction = markRating.select("distinction");
        DataStream<Integer> pass = markRating.select("pass");
        DataStream<Integer> fail = markRating.select("fail");
        // Write data into the output file, 3 files will get created in stream folder of output
        distinction.writeAsText(params.get("output")+"/split-distinction", FileSystem.WriteMode.OVERWRITE);
        pass.writeAsText(params.get("output")+"/split-pass",FileSystem.WriteMode.OVERWRITE);
        fail.writeAsText(params.get("output")+"/split-fail",FileSystem.WriteMode.OVERWRITE);
        // execute program
        environment.execute("Marks Grading");
    }
}



