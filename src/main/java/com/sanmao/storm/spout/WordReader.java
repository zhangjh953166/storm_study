package com.sanmao.storm.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.util.Map;

public class WordReader implements IRichSpout{
    private FileReader fileReader;
    private boolean completed = false;
    private TopologyContext context;
    private SpoutOutputCollector collector;

    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        try{
            this.context = topologyContext;
            this.fileReader = new FileReader(map.get("wordsFile").toString());
        }catch (FileNotFoundException e) {
            throw new RuntimeException("Error reading file ["+map.get("wordsFile")+"]");
        }catch (Exception ex) {
            throw new RuntimeException("Error file ["+map.get("wordsFile")+"]",ex);
        }
        this.collector = spoutOutputCollector;
    }

    public void close() {    }

    public void activate() {

    }

    public void deactivate() {

    }

    public void nextTuple() {
        if(completed){
            try {
                Thread.sleep(1000);
            }catch (InterruptedException e) {
                ;
            }
            return;
        }
        String str;
        BufferedReader reader = new BufferedReader(fileReader);
        try{
            while((str = reader.readLine()) != null){
                this.collector.emit(new Values(str),str);
            }
        }catch(Exception e){
            throw new RuntimeException("Error reading tuple",e);
        }finally{
            completed = true;
        }
    }

    public void ack(Object o) {
        System.out.println("OK:"+o);
    }

    public void fail(Object o) {
        System.out.println("fail:"+o);
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("line"));
    }

    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
