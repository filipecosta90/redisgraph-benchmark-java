package com.redislabs;

import com.redislabs.redisgraph.RedisGraph;
//import org.HdrHistogram.*;

public class ClientThread extends Thread {
    private final int requests;
    private final RedisGraph rg;
    private final String query;
    private final String key;
//    private final Histogram

    ClientThread( RedisGraph rg, Integer requests, String key, String query ) {
        super("my extending thread");
        this.requests = requests;
        this.rg = rg;
        this.query = query;
        this.key = key;
        System.out.println("Client thread created" + this);
    }

    public void run() {
        for (int i = 0; i < requests; i++) {
            long startTime = System.currentTimeMillis();
            rg.query(key,query);
            long duration = System.currentTimeMillis() - startTime;
            System.out.println(duration);
        }
        System.out.println("My thread run is over");
    }
}
