#!/usr/bin/env bash
flink_path="/usr/local/Cellar/apache-flink/1.8.1/bin/flink"
input_file="--input file:///Users/ranvsing/CodeBase/Flink/input/marksheet"
input_salary_file="--input file:///Users/ranvsing/CodeBase/Flink/input/salaryrange"
output_location="--output file:///Users/ranvsing/CodeBase/Flink/output/connector"

if [ "$1" == "split" ]; then
  $flink_path run -c com.flink.stream.connector.StreamDataSpliter target/StreamProcessor-1.0.jar $input_file $output_location
elif [ "$1" == "socket" ]; then
  $flink_path run -c com.flink.stream.connector.SockerStreamWordCount target/StreamProcessor-1.0.jar
elif [ "$1" == "kafka" ]; then
     $flink_path run -c com.flink.stream.connector.KafkaStreamConProd target/StreamProcessor-1.0.jar
elif [ "$1" == "salary" ]; then
     $flink_path run -c com.flink.stream.connector.StreamAgeGroupSalaryAnalyser target/StreamProcessor-1.0.jar $input_salary_file $output_location
elif [ "$1" == "agg" ]; then
     $flink_path run -c com.flink.stream.connector.StreamAggregation target/StreamProcessor-1.0.jar $input_salary_file $output_location
fi