package com.bdp.storm.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;
import com.bdp.storm.bolts.HelloBolt;
import com.bdp.storm.bolts.RandomLineBolt;
import com.bdp.storm.spouts.HelloSpout;
import com.bdp.storm.spouts.RandomLineSpout;

/**
 * Created by austin on 06/10/15.
 */
public class HelloTopology {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();

        builder.setSpout("1", new HelloSpout(), 1);
        builder.setBolt("2", new HelloBolt(), 1).shuffleGrouping("1");

        Config conf = new Config();
        conf.setDebug(true);

        if (args != null && args.length > 0) {
            conf.setNumWorkers(3);

            StormSubmitter.submitTopologyWithProgressBar(args[0], conf, builder.createTopology());
        }
        else {

            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("test", conf, builder.createTopology());
            Utils.sleep(10000);
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }
}
