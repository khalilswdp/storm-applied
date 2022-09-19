package com.github.khalilswdp.stormapplied;

import lombok.extern.slf4j.Slf4j;
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

@Slf4j
public class CommitFeedListener extends BaseRichSpout {

    private SpoutOutputCollector outputCollector;
    private List<String> commits;


    @Override
    public void open(Map<String, Object> configMap,
                     TopologyContext context,
                     SpoutOutputCollector outputCollector) {
        // If we're writing code for a spout that deals with a live data source, such as a message queue
        // This is where we'd put the code for connecting to that data source
        this.outputCollector = outputCollector;

        try {
            commits = IOUtils.readLines(
                    Objects.requireNonNull(ClassLoader.getSystemResourceAsStream("changelog.txt")),
                    Charset.defaultCharset().name()
            );
            log.info("Commit Feed Listener has finished loading up the entire changelog.txt and is ready to start emitting commit by commit");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void nextTuple() {
        log.info("Commit Feed Listener will start emitting commits");
        commits.forEach(
                        commit -> {
                            outputCollector.emit(new Values(commit));
                            log.info(String.format("Commit Feed Listener has emitted %s", commit));
                        });
        log.info("Commit Feed Listener has emitted all commits");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("commit"));
    }
}
