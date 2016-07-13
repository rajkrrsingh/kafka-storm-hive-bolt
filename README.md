# kafka-storm-hive-bolt

### prerequisites ###

```
// create hive table
CREATE TABLE sample_07_orc(
  code  string,
    description string,
      total_emp string,
        salary string
        )
PARTITIONED BY (location STRING)
CLUSTERED BY (code) into 5 buckets
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY","transactional"="true");

//sample data for table
1,discription,1000,100000,IND

// create kafka input and output topic
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper `hostname`:2181 --replication-factor 1 --partition 1 --topic sampleinput
/usr/hdp/current/kafka-broker/bin/kafka-topics.sh --create --zookeeper `hostname`:2181 --replication-factor 1 --partition 1 --topic samplepleoutput

//topology.properties
zkhost = sandbox.hortonworks.com:2181
inputTopic =sampleinput
outputTopic=sampleoutput
KafkaBroker =sandbox.hortonworks.com:6667
consumerGroup=id7
metaStoreURI = thrift://sandbox.hortonworks.com:9083
dbName = default
tblName = sample_07_orc
```

### checkout git ####
    git clone https://github.com/rajkrrsingh/kafka-storm-hive-bolt.git

### build using maven ###
    mvn clean package

### run on storm cluster ###
    storm jar /tmp/kafka-storm-*.jar com.storm.topology.KafkaTopology  /tmp/topology.properties stormtopo

### to test it working produce some messages using kafka console producer ###
    /usr/hdp/current/kafka-broker/bin/kafka-console-producer.sh --broker-list `hostname`:6667 --topic sampleinput
    1,discription,1000,100000,IND
    1,discription,1000,100000,IND
    1,discription,1000,100000,IND

### check the output in hive table ###
    hive> select * from sample_07_orc;
    OK
    1   discription 1000    100000  IND
    Time taken: 0.512 seconds, Fetched: 1 row(s)

### message from kafkaSpout will be written to kafka output topic(sampleoutput) ###
    /usr/hdp/current/kafka-broker/bin/kafka-console-consumer.sh --zookeeper `hostname`:2181 --topic sampleoutput --from-beginning
    {metadata.broker.list=sandbox.hortonworks.com:6667, request.timeout.ms=30000, client.id=console-consumer-80470, security.protocol=PLAINTEXT}
    1,discription,1000,100000,IND
    1,discription,1000,100000,IND
    1,discription,1000,100000,IND





