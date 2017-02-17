package com.storm.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class KafkaOutputBolt extends BaseRichBolt {
	private static final long serialVersionUID = 1L;
	private Producer<String, String> producer;
	private String zkConnect, serializerClass, topic, brokerList;
	private static final Logger logger = Logger.getLogger(KafkaOutputBolt.class);
	private Map<String, String> valueMap = new HashMap<String, String>();
	private String dataToTopic = null;
	OutputCollector _collector;

	public KafkaOutputBolt(String zkConnect, String serializerClass, String brokerList, String topic) {
		this.zkConnect = zkConnect;
		this.serializerClass = serializerClass;
		this.topic = topic;
		this.brokerList = brokerList;
	}

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
		Properties props = new Properties();
		props.put("zookeeper.connect", zkConnect);
		props.put("serializer.class", serializerClass);
		props.put("metadata.broker.list", brokerList);
		ProducerConfig config = new ProducerConfig(props);
		producer = new Producer<String, String>(config);

	}

	public void execute(Tuple input) {
		// TODO Auto-generated method stub
		String msg = (String) input.getValue(0);
		// writing msg as it is after getting it from kafkaSpout
		KeyedMessage<String, String> dataValue = new KeyedMessage<String, String>(topic, msg);
		producer.send(dataValue);
		_collector.ack(input);
	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("null"));
	}
}
