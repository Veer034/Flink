flink_path="/usr/local/Cellar/apache-flink/1.8.1/bin/flink"
input_file="--input file:///Users/ranvsing/CodeBase/Flink/input/marksheet"
input_salary_file="--input file:///Users/ranvsing/CodeBase/Flink/input/salaryrange"
output_location="--output file:///Users/ranvsing/CodeBase/Flink/output/stream"

if [ "$1" == "split" ]; then
  $flink_path run -c com.flink.stream.StreamDataSpliter target/StreamProcessor-1.0.jar $input_file $output_location
elif [ "$1" == "socket" ]; then
  $flink_path run -c com.flink.stream.SockerStreamWordCount target/StreamProcessor-1.0.jar
elif [ "$1" == "kafka" ]; then
     $flink_path run -c com.flink.stream.KafkaStreamConProd target/StreamProcessor-1.0.jar
elif [ "$1" == "salary" ]; then
     $flink_path run -c com.flink.stream.StreamAgeGroupSalaryAnalyser target/StreamProcessor-1.0.jar $input_salary_file $output_location
elif [ "$1" == "agg" ]; then
     $flink_path run -c com.flink.stream.StreamAggregation target/StreamProcessor-1.0.jar $input_salary_file $output_location
fi