import picocli.CommandLine;
import picocli.CommandLine.Option;

@CommandLine.Command(name = "redisgraph-benchmark-java", mixinStandardHelpOptions = true, version = "RedisGraph benchmark Java 0.0.1")
public class BenchmarkRunner implements Runnable {

    @Option(names = {"-h", "--hostname"},
            description = "Server hostname.", defaultValue = "localhost")
    private final String hostname = "";

    public static void main(String[] args) {
        // By implementing Runnable or Callable, parsing, error handling and handling user
        // requests for usage help or version help can be done with one line of code.
        int exitCode = new CommandLine(new BenchmarkRunner()).execute(args);
        System.exit(exitCode);
    }

    public void run() {
        ClientThread th1 = new ClientThread(hostname);
        try {
            while (th1.isAlive()) {
                System.out.println("Main thread will be alive till the child thread is live");
                Thread.sleep(1500);
            }
        } catch (InterruptedException e) {
            System.out.println("Main thread interrupted");
        }
        System.out.println("Main thread's run is over");

    }
}
