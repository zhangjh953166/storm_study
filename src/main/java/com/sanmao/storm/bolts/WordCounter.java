package com.sanmao.storm.bolts;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

import java.util.HashMap;
import java.util.Map;

public class WordCounter implements IRichBolt {
    Integer id;
    String name;
    Map<String,Integer> counters;
    private OutputCollector collector;


    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.counters = new HashMap<String, Integer>();
        this.collector = outputCollector;
        this.name = topologyContext.getThisComponentId();
        this.id = topologyContext.getThisTaskId();
    }

    public void execute(Tuple input) {
        String str=input.getString(0);
        if(!counters.containsKey(str)){
            counters.put(str,1);
        }else{
            Integer c = counters.get(str) + 1;
            counters.put(str,c);
        }
        collector.ack(input);
    }

    public void cleanup() {
        System.out.println("-- count["+name+"-"+"] --");
        for(Map.Entry<String,Integer> entry : counters.entrySet()){
            System.out.println(entry.getKey()+": "+entry.getValue());
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
