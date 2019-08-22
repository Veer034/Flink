package com.flink.stream;


import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.util.Properties;

public class KafkaStreamConProd {
    public static void main(String[] args) throws Exception {
        //The StreamExecutionEnvironment is the context in which a program is executed
        final StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        //Setting passed parameter in args array to ParameterTools for setting in the environment
        final ParameterTool params = ParameterTool.fromArgs(args);
        // Setting passed parameters available in the web interface
        environment.getConfig().setGlobalJobParameters(params);

        // Kafka properties setup
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9093");

        /**
         * Setup kafka stream consumer on topic "flink-input", here we are considering input will be of string type
         *SimpleStringSchema class used for serialization
         */
        FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(
                "flink-input",
                new SimpleStringSchema(),
                properties);
        //Stream conversion for processing the data from kafka
        DataStream<String> stream = environment.addSource(myConsumer);

        /**
         *  This filter consist of token creator for each typed word after checking the length of the given word
         *  is more than 5,then use keyBy to group the words if same word typed in the socket.
         */
        DataStream<String> result = stream.filter(new FilterFunction<String>() {
            //For filtering all word less than length of 6.
            public boolean filter(String value) {
                return value.length()>5;
            }});

        //Printing the result, which can be seen in the log
        result.print();

        /**
         * Setup kafka producer from flink to one of node of kafka, on particular topic "flink-result", with
         * deserializer SimpleStringSchema.
         */
        FlinkKafkaProducer<String> flinkKafkaProducer = new FlinkKafkaProducer<>("localhost:9093",
                "flink-result", new SimpleStringSchema());

        // Stream creation using WordsCapitalizer class and adding kafka producer to stream.
        result.map(new WordsCapitalizer()).addSink(flinkKafkaProducer);

        // execute program
        environment.execute("Flink Kafka Consumer, Modifier, Producer ");
    }

    /**
     * This class will be used to capitalize all the word using map method.
     */
    public static class WordsCapitalizer implements MapFunction<String, String> {
        @Override
        public String map(String s) {
            return s.toUpperCase();
        }
    }
}
