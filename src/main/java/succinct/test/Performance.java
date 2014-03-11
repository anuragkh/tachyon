package succinct.test;

import succinct.thrift.SuccinctService;
import succinct.thrift.Commons;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class Performance {

	public static void performTest(SuccinctService.Client client, String[] args) throws	org.apache.thrift.TException {
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
			client.testCountThroughput(queriesPath, numQueries, numThreads);
			System.out.println("Done!");
		} else if(args[0].equals("locatet")) {
			System.out.println("Performing locate throughput test...");
			String queriesPath = args[1];
			int numQueries = Integer.parseInt(args[2]);
			int numThreads = Integer.parseInt(args[3]);
			client.testLocateThroughput(queriesPath, numQueries, numThreads);
			System.out.println("Done!");
		} else if(args[0].equals("extractt")) {
			System.out.println("Performing extract throughput test...");
			int numQueries = Integer.parseInt(args[1]);
			int numThreads = Integer.parseInt(args[2]);
			client.testExtractThroughput(numQueries, numThreads);
			System.out.println("Done!");
		} else {
			System.out.println("Invalid mode.");
		}
	}

	public static void main(String[] args) throws org.apache.thrift.TException {
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