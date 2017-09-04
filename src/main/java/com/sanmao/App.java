package com.sanmao;

import com.sanmao.storm.bolts.WordNormalizer;
import com.sanmao.storm.spout.WordReader;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws Exception{//定义拓扑
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("word-reader",new WordReader());
        builder.setBolt("word-normalizer",new WordNormalizer()).shuffleGrouping("word-reader");
//配置
        Config conf = new Config();
        conf.put("wordsFile", args[0]);
        conf.setDebug(false);
        //运行拓扑
        conf.put(Config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
//local run
//  LocalCluster cluster = new LocalCluster();
//        cluster.submitTopology("Getting-started-topologie",conf,builder.createTopology());
//        cluster run
        try {
            StormSubmitter.submitTopology("Count-Word-Topology-With_Refresh-Cache",conf,builder.createTopology());
        } catch (AlreadyAliveException|InvalidTopologyException|AuthorizationException e) {
            e.printStackTrace();
        }
        Thread.sleep(1000);
//        cluster.shutdown();
    }
}
