package com.redislabs;

import com.google.common.util.concurrent.RateLimiter;
import com.redislabs.redisgraph.RedisGraph;
import com.redislabs.redisgraph.ResultSet;
import com.redislabs.redisgraph.Statistics;
import com.redislabs.redisgraph.impl.api.RedisGraphCommand;
import com.sun.org.apache.xalan.internal.lib.ExsltStrings;
import org.HdrHistogram.*;
import com.google.common.util.concurrent.RateLimiter;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.List;

public class ClientThread extends Thread {
    private final int requests;
    private final JedisPool rg;
    private final String query;
    private final String key;
    private final Histogram histogram;
    private final Histogram graphInternalHistogram;
    private final RateLimiter rateLimiter;

    ClientThread(JedisPool pool, Integer requests, String key, String query, ConcurrentHistogram histogram, ConcurrentHistogram graphInternalHistogram) {
        super("Client thread");
        this.requests = requests;
        this.rg = pool;
        this.query = query;
        this.key = key;
        this.histogram = histogram;
        this.graphInternalHistogram = graphInternalHistogram;
        this.rateLimiter = null;
    }

    ClientThread(JedisPool pool, Integer requests, String key, String query, ConcurrentHistogram histogram, ConcurrentHistogram graphInternalHistogram, RateLimiter perClientRateLimiter) {
        super("Client thread");
        this.requests = requests;
        this.rg = pool;
        this.query = query;
        this.key = key;
        this.histogram = histogram;
        this.graphInternalHistogram = graphInternalHistogram;
        this.rateLimiter = perClientRateLimiter;
    }

    public void run() {
        Jedis jedis = this.rg.getResource();
        for (int i = 0; i < requests; i++) {
            if (rateLimiter!=null){
                // blocks the executing thread until a permit is available.
                rateLimiter.acquire(1);
            }
            long startTime = System.nanoTime();
//            Jedis jedis = rg.getResource();
            List<Object> rawResponse = (List<Object>) jedis.sendCommand(RedisGraphCommand.QUERY,key, query,"--compact");
//            rg.close();
            long durationMicros = (System.nanoTime() - startTime) / 1000;
//            String splitted = resultSet.getStatistics().getStringValue(Statistics.Label.QUERY_INTERNAL_EXECUTION_TIME).split(" ")[0];
//            double internalDuration = Double.parseDouble(splitted) * 1000;
//            graphInternalHistogram.recordValue((long) internalDuration);
            histogram.recordValue(durationMicros);
        }
//        System.out.println("My thread run is over");
    }
}
