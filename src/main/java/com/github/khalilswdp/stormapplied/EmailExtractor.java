package com.github.khalilswdp.stormapplied;

import lombok.extern.slf4j.Slf4j;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

@Slf4j
public class EmailExtractor extends BaseBasicBolt {

    @Override
    public void execute(Tuple tuple, BasicOutputCollector outputCollector) {
        String commit = tuple.getStringByField("commit");
        log.info(String.format("About to extract the email from the commit %s", commit));
        String[] parts = commit.split(" ");
        outputCollector.emit(new Values(parts[1]));
        log.info(String.format("Email %s extracted and emitted", parts[1]));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("email"));
    }
}
