package com.bdp.storm.spouts;

/**
 * Created by austin on 06/10/15.
 */

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Random;

public class RandomLineSpout extends BaseRichSpout {

    SpoutOutputCollector _collector;
    Random _rand;

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;
        _rand = new Random();
    }

    public void nextTuple() {
        List<String> lines = null;
        try {
            lines = Files.readAllLines(Paths.get("src/main/resources/data"), Charset.defaultCharset());
            String line = lines.get(_rand.nextInt(lines.size()));
            _collector.emit(new Values(line));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void ack(Object id) {
    }

    @Override
    public void fail(Object id) {
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

}
