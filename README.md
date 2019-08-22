#Flink Stream Processor

Apache Flink is an open source platform which is a streaming data 
flow engine that provides communication, fault-tolerance, and 
data-distribution for distributed computations over data streams.

This project contains code regarding stream,join of data coming from various 
sources, such as file,socket & kafka.

Installation :<br>
Flink (1.8.1):  https://flink.apache.org/downloads.html#apache-flink-181<br>
Kafka (2.1.1): https://kafka.apache.org/downloads 

Follow these documents for command in flink environment.<br>
https://ci.apache.org/projects/flink/flink-docs-stable/ops/cli.html

Follow these documents for setup kafka environment.<br>
https://www.sohamkamani.com/blog/2017/11/22/how-to-install-and-run-kafka/

####Build Command:
    mvn clean install
 This command will create StreamProcessor-1.0.jar file in target directory. 
 This jar file will have all the dependencies within as we have set in pom.xml file.
 
####Shell Script:
    FlinkClusterStart.sh/FlinkClusterStop.sh : 
        For start and stop flink cluster. After starting cluster all subpackage code will work.

    FlinkReadFile.sh :
        For running join subpackage classes with command. Use command 'all' to execute all the file 
        in join subpackage. Other commands are inner, leftouter, outer, rightouter using FlinkReadFile.sh.
    
        For ex:
            ./FlinkReadFile.sh leftouter

    FlinkStream.sh :<br>
        For running stream subpackage classes with command. Various command available for running stream like 
        split,socket,kafka,salary,agg.
        
        For ex:
            ./FlinkStream.sh kafka
        
        

##Flink Cluster UI:
After starting flink cluster Ui can be seen @http://localhost:8081/.

####Dashboard:<br>
Contains details related to all the running,finished,cancelled and failed application.


##Join :
This subpackage consist of code doing task just like join does in SQL, code will be used to 
join the data available in the file. Here file will be working as a table
consist of huge data. In this we will have InnerJoin, OuterJoin, LeftOuterJoin,
RightOuter join.
####InnerJoin : 
    consist of all common data in two files based on joining column(just like WHERE join clause).
####OuterJoin :
    consist of all data both files are integrated together either they are matched or not.
####LeftOuterJoin :
    consist of all data matching between two files and all the left file data.
####RightOuterJoin :
    consist of all data matching between two files and all the right file data.

Note: We have 2 .txt file, timezone.txt file contains id,timezone and 
timezonesecretcode.txt contains id,secreatcode. Files in join subpackage will work on
all the above explained scenario.

    
##Stream
This subpackage consists of code processing live data coming from file,socket or kafka. 
This will make changes in incoming data and perform operation like filtering, grouping 
and writing the generated report in either file, or in kafka. Their various api available
to store the data in various cloud storage DB. 


####KafkaStreamConProd.java :
    contains logic for reading from kafka topic "flink-input" bby connecting to one of the kafka 
    nodes localhost:9093. After filtering only words longer than 5 and converting to Uppercase
    Pushing the data again to another kafka topic "flink-result" in node localhost:9093, we could 
    have used any nodes for publishing the data into kafka
    
Kafka producer console(flink-input):<br>

Kafka consumer console(flink-result):<br>

#####Command used for creating kafka cluster.
    Start Zookeeper:
    —————————-------
    ./bin/zookeeper-server-start.sh config/zookeeper.properties
    
    Start Broker/Server
    ———————————--------
    ./bin/kafka-server-start.sh config/server.1.properties
    ./bin/kafka-server-start.sh config/server.2.properties
    ./bin/kafka-server-start.sh config/server.3.properties
    
    Create producer/consumer topic 
    ———————————--------------------
    ./bin/kafka-topics.sh --create --topic flink-input --zookeeper localhost:2181 --partitions 3 --replication-factor 2  
    ./bin/kafka-topics.sh --create --topic flink-result --zookeeper localhost:2181 --partitions 3 --replication-factor 2
    
    Create Procedure
    ————————————----
    ./bin/kafka-console-producer.sh --broker-list localhost:9093,localhost:9094,localhost:9095 --topic flink-input
    
    Create Consumer
    —————————————-
    ./bin/kafka-console-consumer.sh --bootstrap-server localhost:9093 --topic flink-result
    
-->Link provided above for detailed description of command used for kafka.
   

####SocketStreamWordCount.java :
    contains logic for connecting flink to socket after opening socket using cmd:"nc -l 8191" 
    then prints the word typed along with count. We are only considering words starting from "R",
    we are filtering out all the other using filter method. 
    
Output in logs can be seen:

####StreamAgeGroupSalaryAnalyser.java :
    contains logic for dividing the salary data into age group, then calculate the average salry of each group.
    map and reduce used for calculating the result and producing output file.  


####StreamAggregation.java : 
    contains logic for calulating the min,max,sum,minBy,maxBy based on the group. keyBy() method of DataStream used for
    grouping the data, and above described method used for performing operation on group basis. In ouput we will see 
    complete list of data, but last value in the group will have the latest calculated data.
    The difference between min and minBy is that min returns the minimum value, whereas minBy returns
    the element that has the minimum value in this field (same for max and maxBy)

####StreamDataSpliter.java : 
    contains logic for dividing the passed data into various other files based on 
    the marks obtained by student. 3 files as output will get created distiction,
    pass & fail.  Command :  ./FlinkStream.sh split 
    


#####Note: 
    ->If Flink installation location different then,make path changes in all shell script files.
    ->All report will be generated in output folder.
    ->For running application use shell script statements provided abbove  
    