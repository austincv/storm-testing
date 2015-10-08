package com.bdp.storm.tests;

import backtype.storm.Config;
import backtype.storm.ILocalCluster;
import backtype.storm.Testing;
import backtype.storm.generated.StormTopology;
import backtype.storm.testing.CompleteTopologyParam;
import backtype.storm.testing.MkClusterParam;
import backtype.storm.testing.MockedSources;
import backtype.storm.testing.TestJob;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.bdp.storm.bolts.HelloBolt;
import com.bdp.storm.spouts.HelloSpout;
import junit.framework.TestCase;

import java.util.Map;

/**
 * Created by austin on 06/10/15.
 */
public class HelloTopologyTest extends TestCase{

    public void testWithLocalCluster(){
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(2);
        mkClusterParam.setPortsPerSupervisor(5);
        Config daemonConf = new Config();
        daemonConf.put(Config.SUPERVISOR_ENABLE, false);
        daemonConf.put(Config.TOPOLOGY_ACKER_EXECUTORS, 0);

        Testing.withLocalCluster(mkClusterParam, new TestJob(){

            @Override
            public void run(ILocalCluster iLocalCluster) throws Exception {
                assertNotNull(iLocalCluster.getState());
            }
        });
    }

    public void testHelloTopology(){
        MkClusterParam mkClusterParam = new MkClusterParam();
        mkClusterParam.setSupervisors(4);
        Config daemonConf = new Config();
        daemonConf.put(Config.STORM_LOCAL_MODE_ZMQ, false);
        mkClusterParam.setDaemonConf(daemonConf);

        Testing.withSimulatedTimeLocalCluster(mkClusterParam, new TestJob() {

            @Override
            public void run(ILocalCluster cluster) throws Exception {
                //build the test topology
                TopologyBuilder builder = new TopologyBuilder();
                builder.setSpout("1", new HelloSpout(), 1);
                builder.setBolt("2", new HelloBolt(), 1).shuffleGrouping("1");

                StormTopology topology = builder.createTopology();
                //complete the topology

                //prepare the mock data
                MockedSources mockedSources = new MockedSources();
                mockedSources.addMockData("1", new Values("hello"));

                //prepare the config
                Config conf = new Config();
                conf.setNumWorkers(1);

                CompleteTopologyParam completeTopologyParam = new CompleteTopologyParam();
                completeTopologyParam.setMockedSources(mockedSources);
                completeTopologyParam.setStormConf(conf);

                Map result = Testing.completeTopology(cluster, topology, completeTopologyParam);

                // check whether the result is right
                assertTrue(Testing.multiseteq(new Values(new Values("hello")), Testing.readTuples(result,"1")));
                assertTrue(Testing.multiseteq(new Values(new Values("Bolt says hello")), Testing.readTuples(result,"2")));

            }
        });
    }

}
