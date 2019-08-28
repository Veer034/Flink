package com.flink.stream.window;


import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import java.sql.Timestamp;


public class SlidingEventTimeWindow {
    public static void main(String[] args) throws Exception {
        //The StreamExecutionEnvironment is the context in which a program is executed & set time characteristics
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        // Using this we can set the parallel stream count
        //environment.setParallelism(1);

        // output path can be made dynamic by passing value in ParameterTool class as done in join & connector sub-package
        String output = "file:///Users/ranvsing/CodeBase/Flink/output/window/slideevent";
        // Connect to port for listing the incoming data
        DataStream<String> data = environment.socketTextStream("localhost", 9090);
        // Stream contains timestamp and random number
        DataStream<Tuple2<Long, String>> sum = data.map(new MapFunction<String, Tuple2<Long, String>>() {
            public Tuple2<Long, String> map(String s) {
                String[] words = s.split(",");
                return new Tuple2<Long, String>(Long.parseLong(words[0]), words[1]);
            }
        }).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple2<Long, String>>() {
            public long extractAscendingTimestamp(Tuple2<Long, String> t) {
                return t.f0;
            }
        })
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(4), Time.seconds(2)))
                .reduce(new ReduceFunction<Tuple2<Long, String>>() {
                    public Tuple2<Long, String> reduce(Tuple2<Long, String> t1, Tuple2<Long, String> t2) {
                        int num1 = Integer.parseInt(t1.f1);
                        int num2 = Integer.parseInt(t2.f1);
                        int sum = num1 + num2;
                        Timestamp t = new Timestamp(System.currentTimeMillis());
                        return new Tuple2<Long, String>(t.getTime(), "" + sum);
                    }
                });
        sum.writeAsText(output, FileSystem.WriteMode.OVERWRITE);
        // execute program
        environment.execute("Sliding Event Group Salary");
    }
}

