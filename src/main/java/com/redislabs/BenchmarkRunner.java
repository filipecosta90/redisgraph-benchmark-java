package com.redislabs;

import com.google.common.util.concurrent.RateLimiter;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import com.redislabs.redisgraph.impl.api.RedisGraph;
import redis.clients.jedis.JedisPool;
import org.HdrHistogram.*;

import java.util.ArrayList;

@CommandLine.Command(name = "redisgraph-benchmark-java", mixinStandardHelpOptions = true, version = "RedisGraph benchmark Java 0.0.1")
public class BenchmarkRunner implements Runnable {

    @Option(names = {"-s", "--server"},
            description = "Server hostname.", defaultValue = "localhost")
    private  String hostname;

    @Option(names = {"-q", "--query"},
            description = "RG query.", required = true)
    private  String query;

    @Option(names = {"-a", "--password"},
            description = "Redis password.")
    private  String password = null;

    @Option(names = {"-k", "--graph-key"},
            description = "RG key.")
    private  String key = null;

    @Option(names = {"-c", "--clients"},
            description = "Number of clients.", defaultValue = "50")
    private  Integer clients;


    @Option(names = { "--connections"},
            description = "Number of connections per clients.", defaultValue = "8")
    private  Integer connections;

    @Option(names = {"-p", "--port"},
            description = "Number of clients.", defaultValue = "6379")
    private  Integer port;

    @Option(names = {"--rps"},
            description = "Max rps. If 0 no limit is applied and the DB is stressed up to maximum.", defaultValue = "0")
    private  Integer rps;

    @Option(names = {"-n", "--number-requests"},
            description = "Number of requests.", defaultValue = "1000000")
    private  Integer numberRequests;

    public static void main(String[] args) {
        // By implementing Runnable or Callable, parsing, error handling and handling user
        // requests for usage help or version help can be done with one line of code.
        int exitCode = new CommandLine(new BenchmarkRunner()).execute(args);
        System.exit(exitCode);
    }

    public void run() {
        int requestsPerClient = numberRequests / clients;
        int rpsPerClient = rps / clients;
        GenericObjectPoolConfig poolConfig = new GenericObjectPoolConfig();
        JedisPool pool = new JedisPool(poolConfig, hostname,
                port, 2000, password);
        RedisGraph rg = new RedisGraph(pool);
        ConcurrentHistogram histogram = new ConcurrentHistogram(900000000L, 3);
        ConcurrentHistogram graphInternalTime = new ConcurrentHistogram(900000000L, 3);

        ArrayList<ClientThread> threadsArray = new ArrayList<ClientThread>();
        System.out.println("Starting benchmark with "+ clients +" threads. Requests per thread " + requestsPerClient);
        long startTime = System.currentTimeMillis();
        for (int i = 0; i < clients; i++) {
            ClientThread clientThread;
            if (rps>0){
                RateLimiter rateLimiter = RateLimiter.create(rpsPerClient);
                clientThread = new ClientThread(rg, requestsPerClient,key, query, histogram,graphInternalTime, rateLimiter);
            } else {
                clientThread = new ClientThread(rg, requestsPerClient,key, query, histogram,graphInternalTime);
            }
            clientThread.start();
            threadsArray.add(clientThread);
        }
        for (ClientThread ct: threadsArray
             ) {
            try {
                ct.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        double totalDuration = (System.currentTimeMillis() - startTime) / 1000.f;
        System.out.println("################# RUNTIME STATS #################");
        System.out.println("Total Duration "+ totalDuration +" Seconds");
        System.out.println("Total Commands issued " + histogram.getTotalCount());
        System.out.println("Overall Client Latency summary (msec):");
        System.out.println("p50 (ms):" + histogram.getValueAtPercentile(50.0)/1000.0f);
        System.out.println("p95 (ms):" + histogram.getValueAtPercentile(95.0)/1000.0f);
        System.out.println("p99 (ms):" + histogram.getValueAtPercentile(99.0)/1000.0f);
        System.out.println("Overall Internal execution time (msec):");
        System.out.println("p50 (ms):" + graphInternalTime.getValueAtPercentile(50.0)/1000.0f);
        System.out.println("p95 (ms):" + graphInternalTime.getValueAtPercentile(95.0)/1000.0f);
        System.out.println("p99 (ms):" + graphInternalTime.getValueAtPercentile(99.0)/1000.0f);
    }
}
