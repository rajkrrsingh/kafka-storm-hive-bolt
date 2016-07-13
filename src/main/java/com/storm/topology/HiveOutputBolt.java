package com.storm.topology;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;

import java.util.Map;


public class HiveOutputBolt extends BaseBasicBolt {


	private static final long serialVersionUID = 1L;
	private static final Logger logger = Logger.getLogger(HiveOutputBolt.class);
	OutputCollector _collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		_collector = collector;
	}

	public void execute(Tuple input, BasicOutputCollector outputCollector) {
		Fields fields = input.getFields();
		try {
			System.out.println("input : "+input.getValueByField(fields.get(0)).toString());
			String stockDataStr = input.getValueByField(fields.get(0)).toString();
			String[] stockData = stockDataStr.split(",");
			Values values = new Values(stockData[0],stockData[1],stockData[2],stockData[3],stockData[4]);
			outputCollector.emit(values);
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}



	public void declareOutputFields(OutputFieldsDeclarer ofDeclarer) {
		ofDeclarer.declare(new Fields("code", "description", "total_emp", "salary", "location"));
	}

}
