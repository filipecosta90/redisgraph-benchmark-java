package com.redislabs;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import picocli.CommandLine;
import picocli.CommandLine.Option;
import com.redislabs.redisgraph.impl.api.RedisGraph;
import redis.clients.jedis.JedisPool;
import org.hdrhistogram.*;

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
        JedisPool pool = new JedisPool(new GenericObjectPoolConfig(), hostname,
                port, 2000, password);
        RedisGraph rg = new RedisGraph(pool);
        Histogram histogram = new Histogram(3600000000000L, 3);
        ArrayList<ClientThread> threadsArray = new ArrayList<ClientThread>();
        for (int i = 0; i < clients; i++) {
            ClientThread clientThread = new ClientThread(rg, requestsPerClient,key, query, Histogram);
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
        System.out.println("Main thread's run is over");

    }
}
