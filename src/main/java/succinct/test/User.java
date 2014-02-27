package succinct.test;

import java.io.BufferedReader;
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

public class User {
	public static void main(String[] args) throws IOException {
		BufferedReader in = null;
                int partScheme = 0;
		System.out.println("Connecting to master...");
		try {
			in = new BufferedReader(new InputStreamReader(System.in));
		} catch (Exception e) {
			System.out.println("Error: " + e.toString());
			System.exit(1);
		}
		try {
                	TTransport transport = new TSocket(args[0], Ports.MASTER_BASE_PORT);
                	TProtocol protocol = new TBinaryProtocol(transport);
                	SuccinctMasterService.Client client = new SuccinctMasterService.Client(protocol);
                	System.out.println("Connected!");
                	transport.open();
                	
                	String key;
                        System.out.println("Testing count, locate: ");
                        System.out.print("Enter query: ");
                        String query;

                        while(!(query = in.readLine()).equals("exit")) {
                                long queryCount = client.count(query);
                                System.out.println("Count = " + queryCount);
                                List<Long> queryLocations = client.locate(query);
                                if(queryCount != queryLocations.size()) {
                                        System.out.println("Locate mismatch: [" + queryCount + ", " + queryLocations.size() + "]");

                                }
                                System.out.println("Got " + queryLocations.size() + " locations: ");
                                for(int i = 0; i < queryLocations.size(); i++) {
                                        System.out.print(queryLocations.get(i) + " ");
                                }
                                System.out.println();

                                System.out.println("Testing iterator: ");
                                Iterator<Long> locationIterator = new LocationIterator(client, query);
                                while(locationIterator.hasNext()) {
                                        System.out.print("Iterator still has more values, read more? (y/n): ");
                                        String ans = in.readLine();
                                        if(ans.equals("y") || ans.equals("Y")) {
                                                System.out.println("Location: " + locationIterator.next());
                                        } else {
                                                break;
                                        }
                                }
                                if(locationIterator.hasNext()) {
                                        System.out.println("Exited with values remaining in iterator...");
                                } else {
                                        System.out.println("Iterator exhausted!");
                                }
                                System.out.print("Enter query: ");
                        }
                        System.out.println("Testing extract: ");
                        System.out.print("Enter location to start extracting: ");
                        long location, bytes;
                        while((location = Long.parseLong(in.readLine())) != -1) {
                                System.out.print("Enter number of bytes to extract: ");
                                bytes = Long.parseLong(in.readLine());
                                System.out.println("Extracted text: [" + client.extract(location, bytes) + "]");
                                System.out.print("Enter location to start extracting: ");
                        }
                	
                	transport.close();
                	System.out.println("Connection closed!");
        	} catch (Exception e) {
        	      	e.printStackTrace();
        	      	System.exit(1);
        	}
	}
}