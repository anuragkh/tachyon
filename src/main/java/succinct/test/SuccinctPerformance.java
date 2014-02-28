package succinct.test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;

import succinct.thrift.SuccinctMasterService;
import succinct.thrift.Ports;
import succinct.util.LocationIterator;

import java.util.List;
import java.util.Set;
import java.util.Map;
import java.util.Iterator;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class SuccinctPerformance {

	private final int NUM_QUERIES = 10000;
	private final long DATA_SIZE = 107101885556L;
	private final int EXTRACT_LEN = 1024;
	private String[] queries;
	private long[] locations;
	private SuccinctMasterService.Client mClient;

	public SuccinctPerformance(String masterAddress, String queriesPath) {
		// Set up connection to SuccinctMaster
		try {
			System.out.println("Attempting to connect to SuccinctMaster @ " + masterAddress);
            TTransport transport = new TSocket(masterAddress, Ports.MASTER_BASE_PORT);
            TProtocol protocol = new TBinaryProtocol(transport);
            SuccinctMasterService.Client client = new SuccinctMasterService.Client(protocol);
            System.out.println("Connected to SuccinctMaster!");
            transport.open();
        } catch (Exception e) {
        	e.printStackTrace();
        }

        // Populate queries array
		queries = new String[100000];
		try {
			System.out.println("Populating queries array...");
			BufferedReader queryReader = new BufferedReader(new FileReader(queriesPath));
			for(int i = 0; i < queries.length; i++) {
				queries[i] = queryReader.readLine();
			}
			System.out.println("Read " + queries.length + " queries!");
			queryReader.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private double testCountThroughput() throws org.apache.thrift.TException {
		return 0.0;
	}

	private double testLocateThroughtput() throws org.apache.thrift.TException {
		return 0.0;
	}

	private double testExtractThroughput() throws org.apache.thrift.TException {
		return 0.0;
	}

	private double testCountLatency() throws org.apache.thrift.TException {

		long start;
		double totalTime = 0.0;
		
		for(int i = 0; i < NUM_QUERIES; i++) {
			start = System.nanoTime();
			long count = mClient.count(queries[i % NUM_QUERIES]);
			totalTime += (double)(System.nanoTime() - start) / 1000.0;
			System.out.println(i + "\t" + count);
		}

		System.out.println("Count time = " + totalTime + " us");
		return totalTime;

	}

	private double testLocateLatency() throws org.apache.thrift.TException {

		long start;
		double totalTime = 0.0;
		
		for(int i = 0; i < NUM_QUERIES; i++) {
			start = System.nanoTime();
			mClient.locate(queries[i % queries.length]);
			totalTime += (double)(System.nanoTime() - start) / 1000.0;
		}

		System.out.println("Locate time = " + totalTime + " us");
		return totalTime;

	}

	private double testExtractLatency() throws org.apache.thrift.TException {
		long start;
		double totalTime = 0.0;
		
		for(int i = 0; i < NUM_QUERIES; i++) {
			start = System.nanoTime();
			mClient.extract(locations[i % locations.length], EXTRACT_LEN);
			totalTime += (double)(System.nanoTime() - start) / 1000.0;
		}

		System.out.println("Extract time = " + totalTime + " us");
		return totalTime;
	}

	public static void main(String[] args) throws org.apache.thrift.TException {
		SuccinctPerformance perfTest = new SuccinctPerformance(args[0], args[1]);

		double countTime = perfTest.testCountLatency();
		double locateTime = perfTest.testLocateLatency();
		double extractTime = perfTest.testExtractLatency();

	}
}