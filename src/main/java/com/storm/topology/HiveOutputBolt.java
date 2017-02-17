package com.storm.topology;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
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
