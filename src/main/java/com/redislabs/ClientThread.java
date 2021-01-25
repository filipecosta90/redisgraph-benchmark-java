package com.redislabs;

import com.google.common.util.concurrent.RateLimiter;
import com.redislabs.redisgraph.RedisGraphContext;
import com.redislabs.redisgraph.ResultSet;
import com.redislabs.redisgraph.Statistics;
import com.sun.org.apache.xalan.internal.lib.ExsltStrings;
import org.HdrHistogram.*;
import com.google.common.util.concurrent.RateLimiter;
import com.redislabs.redisgraph.impl.api.RedisGraph;

public class ClientThread extends Thread {
    private final int requests;
    private final RedisGraph rg;
    private final String query;
    private final String key;
    private final Histogram histogram;
    private final Histogram graphInternalHistogram;
    private final RateLimiter rateLimiter;

    ClientThread(RedisGraph rg, Integer requests, String key, String query, ConcurrentHistogram histogram, ConcurrentHistogram graphInternalHistogram) {
        super("Client thread");
        this.requests = requests;
        this.rg = rg;
        this.query = query;
        this.key = key;
        this.histogram = histogram;
        this.graphInternalHistogram = graphInternalHistogram;
        this.rateLimiter = null;
    }

    ClientThread(RedisGraph rg, Integer requests, String key, String query, ConcurrentHistogram histogram, ConcurrentHistogram graphInternalHistogram, RateLimiter perClientRateLimiter) {
        super("Client thread");
        this.requests = requests;
        this.rg = rg;
        this.query = query;
        this.key = key;
        this.histogram = histogram;
        this.graphInternalHistogram = graphInternalHistogram;
        this.rateLimiter = perClientRateLimiter;
    }

    public void run() {
        for (int i = 0; i < requests; i++) {
            if (rateLimiter!=null){
                // blocks the executing thread until a permit is available.
                rateLimiter.acquire(1);
            }
            long startTime = System.nanoTime();
            RedisGraphContext ctx = rg.getContext();
            ResultSet resultSet = ctx.query(key, query);
            ctx.close();
            long durationMicros = (System.nanoTime() - startTime) / 1000;
            String splitted = resultSet.getStatistics().getStringValue(Statistics.Label.QUERY_INTERNAL_EXECUTION_TIME).split(" ")[0];
            double internalDuration = Double.parseDouble(splitted) * 1000;
            graphInternalHistogram.recordValue((long) internalDuration);
            histogram.recordValue(durationMicros);
        }
//        System.out.println("My thread run is over");
    }
}
