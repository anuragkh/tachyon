package succinct.test;

import succinct.thrift.SuccinctService;
import succinct.thrift.Commons;

import java.io.IOException;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.DataInputStream;
import java.io.FileInputStream;

import java.util.Random;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Performance {

	private static final long SPLIT_SIZE = 2684354560L;    // TODO: Remove
    private static final int NUM_SPLITS = 5;               // TODO: Remove

    static class Tester extends Thread {
        private TTransport serverTransport;
        private SuccinctService.Client queryHandler;
        private String queryType;
        private String[] queries;
        private long[] randoms;
        private static final long SEND_DURATION = 310000000000L;
        private static final int SEND_SLOT = 1000;
        private static final int LOG_SLOT = 10000;
        private static final long NANOSEC = 1000000000L;

        public Tester(String[] queries, String queryType) {
            try {
	            System.out.println("Connecting to handler...");
	            serverTransport = new TSocket("localhost", Commons.HANDLER_BASE_PORT);
	            TProtocol protocol = new TBinaryProtocol(serverTransport);
	            queryHandler = new SuccinctService.Client(protocol);
	            serverTransport.open();
	            System.out.println("Connected!");
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
            this.queryType = queryType;
            this.queries = queries;
            this.randoms = null;
        }

        public Tester(long[] randoms, String queryType) {
        	try {
	            System.out.println("Connecting to handler...");
	            serverTransport = new TSocket("localhost", Commons.HANDLER_BASE_PORT);
	            TProtocol protocol = new TBinaryProtocol(serverTransport);
	            queryHandler = new SuccinctService.Client(protocol);
	            serverTransport.open();
	            System.out.println("Connected!");
	        } catch (Exception e) {
	            e.printStackTrace();
	        }
            this.queryType = queryType;
            this.randoms = randoms;
            this.queries = null;
        }

        public void countTest() throws org.apache.thrift.TException, IOException {
            queryHandler.connectToQueryServers();
            
            System.out.println("Sending out queries...");
            
            // Sending logic
            String query = queries[0];
            int numQueries = queries.length;
            long i = 0;
            int queryNo = 0;
            long startTime = System.nanoTime();
            while(System.nanoTime() - startTime < SEND_DURATION) {
                if(i % SEND_SLOT == 0) {
                    query = queries[queryNo % numQueries];
                    queryNo++;
                }
                if(i % LOG_SLOT == 0) {
                	System.out.println("Sent out " + i + " queries.");
                }
                queryHandler.count(query);
                i++;
            }
            double totTime = (double)(System.nanoTime() - startTime) / (double)(NANOSEC);
            System.out.println("Thread throughput = " + ((double)i) / ((double)(totTime)));
            BufferedWriter bw = new BufferedWriter(new FileWriter("res_count_throughput", true));
            bw.write(((double)i) / ((double)(totTime)) + "\n");
            bw.close();
            queryHandler.disconnectFromQueryServers();
        }

        public void locateTest() throws org.apache.thrift.TException, IOException {
            queryHandler.connectToQueryServers();
            
            System.out.println("Sending out queries...");

            // Sending logic
            String query = queries[0];
            int numQueries = queries.length;
            long i = 0;
            int queryNo = 0;
            long startTime = System.nanoTime();
            while(System.nanoTime() - startTime < SEND_DURATION) {
                if(i % SEND_SLOT == 0) {
                    query = queries[queryNo % numQueries];
                    queryNo++;
                }
                if(i % LOG_SLOT == 0) {
                	System.out.println("Sent out " + i + " queries.");
                }
                queryHandler.locate(query);
                i++;
            }
            double totTime = (double)(System.nanoTime() - startTime) / (double)(NANOSEC);
            System.out.println("Thread throughput = " + ((double)i) / ((double)(totTime)));
            BufferedWriter bw = new BufferedWriter(new FileWriter("res_locate_throughput", true));
            bw.write(((double)i) / ((double)(totTime)) + "\n");
            bw.close();
            queryHandler.disconnectFromQueryServers();
        }

        public void extractTest() throws org.apache.thrift.TException, IOException {
            queryHandler.connectToQueryServers();
            
            System.out.println("Sending out queries...");

            // Sending logic
            long query = randoms[0];
            int numQueries = randoms.length;
            long i = 0;
            int queryNo = 0;
            long startTime = System.nanoTime();
            while(System.nanoTime() - startTime < SEND_DURATION) {
                if(i % SEND_SLOT == 0) {
                    query = randoms[queryNo % numQueries];
                    queryNo++;
                }
                if(i % LOG_SLOT == 0) {
                	System.out.println("Sent out " + i + " queries.");
                }
               	queryHandler.extract(query, 1024);
                i++;
            }
            double totTime = (double)(System.nanoTime() - startTime) / (double)(NANOSEC);
            System.out.println("Thread throughput = " + ((double)i) / ((double)(totTime)));
            BufferedWriter bw = new BufferedWriter(new FileWriter("res_extract_throughput", true));
            bw.write(((double)i) / ((double)(totTime)) + "\n");
            bw.close();
            queryHandler.disconnectFromQueryServers();
        }

        @Override
        public void run() {
            try {
                if(queryType.equals("count")) {
                    countTest();
                } else if(queryType.equals("locate")) {
                    locateTest();
                } else if(queryType.equals("extract")) {
                    extractTest();
                } else {
                    System.out.println("Not supported");
                }
                serverTransport.close();
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    private static String[] readQueries(String queriesPath, int numQueries) {
        System.out.println("Loading queries " + numQueries + " from path: " + queriesPath);
        File queryFile = new File(queriesPath);
        DataInputStream in = null;
        String queries[] = new String[numQueries];
        try {
            in = new DataInputStream(new FileInputStream(queryFile));
            int len = 8;    // Query Length
            for(int i = 0; i < numQueries; i++) {
                char c;
                while((c = (char)in.read()) != '\t');
                boolean test = (in.read() == '\u0002');
                int l = 0;
                String q = "";
                while(l != len) {
                    c = (char)in.read();
                    q += c;
                    l++;
                }
                test = (in.read() == '\u0003');
                queries[i] = q;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queries;
    }

    private static long[] generateRandoms(int numQueries) {
        long[] randoms = new long[numQueries];
        Random rand = new Random();
        for(int i = 0; i < numQueries; i++) {
            randoms[i] = Math.abs(rand.nextLong() % (SPLIT_SIZE * NUM_SPLITS));
        }

        return randoms;
    }

	public static void performTest(SuccinctService.Client client, String[] args) throws org.apache.thrift.TException, InterruptedException {
		if(args[0].equals("countl")) {
			System.out.println("Performing count latency test...");
			String queriesPath = args[1];
			int numQueries = Integer.parseInt(args[2]);
			int repeat = Integer.parseInt(args[3]);
			client.testCountLatency(queriesPath, numQueries, repeat);
			System.out.println("Done!");
		} else if(args[0].equals("locatel")) {
			System.out.println("Performing locate latency test...");
			String queriesPath = args[1];
			int numQueries = Integer.parseInt(args[2]);
			int repeat = Integer.parseInt(args[3]);
			client.testLocateLatency(queriesPath, numQueries, repeat);
			System.out.println("Done!");
		} else if(args[0].equals("extractl")) {
			System.out.println("Performing extract latency test...");
			int numQueries = Integer.parseInt(args[1]);
			int repeat = Integer.parseInt(args[2]);
			client.testExtractLatency(numQueries, repeat);
			System.out.println("Done!");
		} else if(args[0].equals("countt")) {
			System.out.println("Performing count throughput test...");
			String queriesPath = args[1];
			int numQueries = Integer.parseInt(args[2]);
			int numThreads = Integer.parseInt(args[3]);
			Tester[] testers = new Tester[numThreads];
			String[] queries = readQueries(queriesPath, numQueries);
			for(int i = 0; i < testers.length; i++) {
				testers[i] = new Tester(queries, "count");
				testers[i].start();
			}
			for(int i = 0; i < testers.length; i++) {
				testers[i].join();
			}
			System.out.println("Done!");
		} else if(args[0].equals("locatet")) {
			System.out.println("Performing locate throughput test...");
			String queriesPath = args[1];
			int numQueries = Integer.parseInt(args[2]);
			int numThreads = Integer.parseInt(args[3]);
			Tester[] testers = new Tester[numThreads];
			String[] queries = readQueries(queriesPath, numQueries);
			for(int i = 0; i < testers.length; i++) {
				testers[i] = new Tester(queries, "locate");
				testers[i].start();
			}
			for(int i = 0; i < testers.length; i++) {
				testers[i].join();
			}
			System.out.println("Done!");
		} else if(args[0].equals("extractt")) {
			System.out.println("Performing extract throughput test...");
			int numQueries = Integer.parseInt(args[1]);
			int numThreads = Integer.parseInt(args[2]);
			Tester[] testers = new Tester[numThreads];
			long[] randoms = generateRandoms(numQueries);
			for(int i = 0; i < testers.length; i++) {
				testers[i] = new Tester(randoms, "extract");
				testers[i].start();
			}
			for(int i = 0; i < testers.length; i++) {
				testers[i].join();
			}
			System.out.println("Done!");
		} else {
			System.out.println("Invalid mode.");
		}
	}

	public static void main(String[] args) throws org.apache.thrift.TException, InterruptedException {
		if(args[0].equals("countt") || args[0].equals("locatet") || args[0].equals("extractt")) {
			performTest(null, args);
			return;
		}
        try {
            System.out.println("Connecting to handler...");
            TTransport transport = new TSocket("localhost", Commons.HANDLER_BASE_PORT);
            TProtocol protocol = new TBinaryProtocol(transport);
            SuccinctService.Client client = new SuccinctService.Client(protocol);
            transport.open();
            System.out.println("Connected!");
            performTest(client, args);
            transport.close();
            System.out.print("Connected!");
        } catch (Exception e) {
            e.printStackTrace();
        }
	}
}