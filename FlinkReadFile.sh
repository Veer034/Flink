
flink_path="/usr/local/Cellar/apache-flink/1.8.1/bin/flink"
input_file="--input1 file:///Users/ranvsing/CodeBase/Flink/input/timezone --input2 file:///Users/ranvsing/CodeBase/Flink/input/timezonesecretcode"
output_location="--output file:///Users/ranvsing/CodeBase/Flink/output/join"

if [ "$1" == "all" ]; then
  $flink_path run -c com.flink.join.InnerJoin target/StreamProcessor-1.0.jar $input_file  $output_location/innerjoin
  $flink_path run -c com.flink.join.LeftOuterJoin target/StreamProcessor-1.0.jar $input_file $output_location/leftouterjoin
  $flink_path run -c com.flink.join.OuterJoin target/StreamProcessor-1.0.jar $input_file $output_location/outerjoin
  $flink_path run -c com.flink.join.RightOuterJoin target/StreamProcessor-1.0.jar $input_file $output_location/rightouterjoin
elif [ "$1" == "inner" ]; then
  $flink_path run -c com.flink.join.InnerJoin target/StreamProcessor-1.0.jar $input_file  $output_location/innerjoin
elif [ "$1" == "leftouter" ]; then
  $flink_path run -c com.flink.join.LeftOuterJoin target/StreamProcessor-1.0.jar $input_file $output_location/leftouterjoin
elif [ "$1" == "outer" ]; then
  $flink_path run -c com.flink.join.OuterJoin target/StreamProcessor-1.0.jar $input_file $output_location/outerjoin
elif [ "$1" == "rightouter" ]; then
     $flink_path run -c com.flink.join.RightOuterJoin target/StreamProcessor-1.0.jar $input_file $output_location/rightouterjoin
fi