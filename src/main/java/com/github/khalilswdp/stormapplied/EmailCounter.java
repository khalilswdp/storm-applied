package com.github.khalilswdp.stormapplied;

import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class EmailCounter extends BaseBasicBolt {
    private Map<String, Integer> counts;

    @Override
    // This method gets called as storm prepares the bolt before execution and is the method where we'd perform any setup for our bolt
    // In our case: instantiating the in memory map
    public void prepare(Map<String, Object> topoConf, TopologyContext context) {
        counts = Collections.synchronizedMap(new HashMap<>());
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector collector) {
        String email = tuple.getStringByField("email");
        counts.put(email, countFor(email) + 1);
        printCounts();
    }

    private void printCounts() {
        for (String email: counts.keySet()) {
            System.out.printf("%s has count of %s%n", email, counts.get(email));
        }
    }

    private Integer countFor(String email) {
        Integer count = counts.get(email);
        return count == null ? 0 : count;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        // This bolt does not emit anything
        // No need to declare output fields
    }
}
