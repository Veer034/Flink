package com.flink.stream.connector;


import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.*;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.utils.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketStreamWordCount {
    public static void main(String[] args) throws Exception {
        //The StreamExecutionEnvironment is the context in which a program is executed
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //Setting passed parameter in args array to ParameterTools for setting in the environment
        final ParameterTool params = ParameterTool.fromArgs(args);
        // Setting passed parameters available in the web interface
        environment.getConfig().setGlobalJobParameters(params);

        // Can open port by using "nc -l 8191" command
        //After that typed every word will come to this socket listener
        DataStream<String> ping = environment.socketTextStream("localhost", 8191);

        /**
         * This filter consist of token creator for each typed word after checking startsWith("R"), then use keyBy
         * to group the words if same word typed in the socket. Then sum method is used to add the returned value
         * stored in Tuple2 by Tokenizer class
         */
         DataStream<Tuple2<String, Integer>> counts = ping.filter(new FilterFunction<String>() {
            //Will consider only word starting with "R", rest other will be filtered out
            public boolean filter(String value) {
                return value.startsWith("R");
            }
        }).map(new Tokenizer()).keyBy(0).sum(1);
        //Printing the result, which can be seen in the log
        counts.print();
        // execute program
        environment.execute("Word count for SocketStreaming");
    }

    /**
     * This class will create Tokens for the typed word in socket with constant value 1.
     */
    public static final class Tokenizer implements MapFunction<String, Tuple2<String, Integer>> {
        public Tuple2<String, Integer> map(String value) {
            return new Tuple2(value, Integer.valueOf(1));
        }
    }

}

