package com.storm.topology;

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.spout.SchemeAsMultiScheme;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.log4j.Logger;
import org.apache.storm.hive.bolt.HiveBolt;
import org.apache.storm.hive.bolt.mapper.DelimitedRecordHiveMapper;
import org.apache.storm.hive.common.HiveOptions;
import org.apache.storm.kafka.*;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.Properties;

public class KafkaTopology {
	private static String zkhost, inputTopic, outputTopic, KafkaBroker, consumerGroup;
	private static String metaStoreURI, dbName, tblName;
	private static final Logger logger = Logger.getLogger(KafkaTopology.class);

	public static void main(String[] args) throws Exception {

		// getting properties from topology.properties
		Properties prop = new Properties();
		InputStream input = null;
		input = new FileInputStream(args[0]);
		prop.load(input);
		zkhost = prop.getProperty("zkhost");
		inputTopic = prop.getProperty("inputTopic");
		outputTopic = prop.getProperty("outputTopic");
		KafkaBroker = prop.getProperty("KafkaBroker");
		consumerGroup = prop.getProperty("consumerGroup");
		metaStoreURI = prop.getProperty("metaStoreURI");
		dbName = prop.getProperty("dbName");
		tblName = prop.getProperty("tblName");


		BrokerHosts hosts = new ZkHosts(zkhost);
		SpoutConfig spoutConfig = new SpoutConfig(hosts, inputTopic, "/" + KafkaBroker, consumerGroup);
		spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		spoutConfig.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
		KafkaSpout kafkaSpout = new KafkaSpout(spoutConfig);
		String[] partNames = {"location"};
		String[] colNames = {"code", "description", "total_emp", "salary"};
		DelimitedRecordHiveMapper mapper = new DelimitedRecordHiveMapper().withColumnFields(new Fields(colNames))
				.withPartitionFields(new Fields(partNames));

		// setting hive options
		HiveOptions hiveOptions;
		hiveOptions = new HiveOptions(metaStoreURI, dbName, tblName, mapper).withTxnsPerBatch(250).withBatchSize(2)
				.withIdleTimeout(10).withCallTimeout(10000000);

		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("KafkaSpout", kafkaSpout, 1);
		builder.setBolt("KafkaOutputBolt",
				new KafkaOutputBolt(zkhost, "kafka.serializer.StringEncoder", KafkaBroker, outputTopic), 1)
				.shuffleGrouping("KafkaSpout");

		//setting hive bolt
		builder.setBolt("HiveOutputBolt", new HiveOutputBolt(), 1).shuffleGrouping("KafkaSpout");
		builder.setBolt("HiveBolt", new HiveBolt(hiveOptions)).shuffleGrouping("HiveOutputBolt");

		Config conf = new Config();
		conf.setNumWorkers(1);
		logger.info("Submiting  topology to storm cluster");
		StormSubmitter.submitTopology(args[1], conf, builder.createTopology());

	}
}
