package com.github.khalilswdp.stormapplied;

import org.apache.commons.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class CommitFeedListener extends BaseRichSpout {

    private SpoutOutputCollector outputCollector;
    private List<String> commits;


    @Override
    public void open(Map<String, Object> configMap,
                     TopologyContext context,
                     SpoutOutputCollector outputCollector) {
        this.outputCollector = outputCollector;

        try {
            commits = IOUtils.readLines(
                    Objects.requireNonNull(ClassLoader.getSystemResourceAsStream("changelog.txt")),
                    Charset.defaultCharset().name()
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        for (String commit: commits) {
            outputCollector.emit(new Values(commit));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("commit"));
    }
}
