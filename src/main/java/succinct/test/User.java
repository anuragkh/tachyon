package succinct.test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import succinct.thrift.MasterService;
import succinct.thrift.SuccinctService;
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
	// public static final int MASTER_BASE_PORT = 10000;
	// public static final int CLIENT_BASE_PORT = 11000;
	// public static void main(String[] args) throws IOException {
	// 	String clientAddress = null;
	// 	BufferedReader in = null;
 //                int partScheme = 0;
	// 	System.out.println("Connecting to master...");
	// 	try {
	// 		in = new BufferedReader(new InputStreamReader(System.in));
	// 	} catch (Exception e) {
	// 		System.out.println("Error: " + e.toString());
	// 		System.exit(1);
	// 	}
	// 	try {
 //                	TTransport transport = new TSocket(args[0], MASTER_BASE_PORT);
 //                	TProtocol protocol = new TBinaryProtocol(transport);
 //                	MasterService.Client mClient = new MasterService.Client(protocol);
 //                	System.out.println("Connected!");
 //                	transport.open();
                	
 //                	System.out.print("Enter path: ");
 //                	String filePath = in.readLine();
 //                	System.out.print("Enter partition scheme: ");
 //                	partScheme = Integer.parseInt(in.readLine());
 //                        System.out.println("Submitting file for processing...");
 //                	clientAddress = mClient.createSuccinctFile(filePath, partScheme, (byte)'\t', (byte)'\n');
                	
 //                	transport.close();
 //                	System.out.println("Connection closed!");
 //        	} catch (Exception e) {
 //        	      	System.out.println("Error: " + e.toString());
 //        	      	System.exit(1);
 //        	}

 //                System.out.println("Connecting to client at " + clientAddress + "...");
 //                try {
 //                	TTransport transport = new TSocket(clientAddress, CLIENT_BASE_PORT);
 //                	TProtocol protocol = new TBinaryProtocol(transport);
 //                	SuccinctService.Client client = new SuccinctService.Client(protocol);
 //                	System.out.println("Connected!");
 //                	transport.open();
 //                	// System.out.println("Testing getKeyToValuePointer: ");
 //                	// System.out.print("Enter key: ");
 //                	// String key;
 //                	// while(!(key = in.readLine()).equals("exit")) {
 //                	// 	long valuePointer = client.getKeyToValuePointer(key);
 //                	// 	System.out.println("Value pointer = " + valuePointer);
 //                	// 	System.out.print("Enter key: ");
 //                	// }
 //                        String key;
 //                        if(partScheme == 1) {
 //                        	System.out.println("Testing count, locate: ");
 //                        	System.out.print("Enter query: ");
 //                        	String query;

 //                        	while(!(query = in.readLine()).equals("exit")) {
 //                        		long queryCount = client.count(query);
 //                        		System.out.println("Count = " + queryCount);
 //                        		List<Long> queryLocations = client.locate(query);
 //                        		if(queryCount != queryLocations.size()) {
 //                        			System.out.println("Locate mismatch: [" + queryCount + ", " + queryLocations.size() + "]");

 //                        		}
 //                                        System.out.println("Got " + queryLocations.size() + " locations: ");
 //                                        for(int i = 0; i < queryLocations.size(); i++) {
 //                                                System.out.print(queryLocations.get(i) + " ");
 //                                        }
 //                                        System.out.println();

 //                                        System.out.println("Testing iterator: ");
 //                                        Iterator<Long> locationIterator = new LocationIterator(client, query);
 //                                        while(locationIterator.hasNext()) {
 //                                                System.out.print("Iterator still has more values, read more? (y/n): ");
 //                                                String ans = in.readLine();
 //                                                if(ans.equals("y") || ans.equals("Y")) {
 //                                                        System.out.println("Location: " + locationIterator.next());
 //                                                } else {
 //                                                        break;
 //                                                }
 //                                        }
 //                                        if(locationIterator.hasNext()) {
 //                                                System.out.println("Exited with values remaining in iterator...");
 //                                        } else {
 //                                                System.out.println("Iterator exhausted!");
 //                                        }
 //                        		System.out.print("Enter query: ");
 //                        	}
 //                        	System.out.println("Testing extract: ");
 //                        	System.out.print("Enter location to start extracting: ");
 //                        	long location, bytes;
 //                        	while((location = Long.parseLong(in.readLine())) != -1) {
 //                        		System.out.print("Enter number of bytes to extract: ");
 //                        		bytes = Long.parseLong(in.readLine());
 //                        		System.out.println("Extracted text: [" + client.extract(location, bytes) + "]");
 //                        		System.out.print("Enter location to start extracting: ");
 //                        	}
 //                        } else if(partScheme == 0) {
 //                                System.out.println("Testing getVal, getKeys, getRecords, deleteRecord: ");
 //                                System.out.print("Enter key: ");
 //                                while(!(key = in.readLine()).equals("exit")) {
 //                                        String value = client.getValue(key);
 //                                        if(!value.equals(String.valueOf((char)10))) {
 //                                                System.out.println("Value = " + value);
 //                                        } else {
 //                                                System.out.println("Value not found!");
 //                                        }

 //                                        System.out.print("Enter key for delete: ");
 //                                        key = in.readLine();
 //                                        int ret;
 //                                        if((ret = client.deleteRecord(key)) == 0) {
 //                                                System.out.println("Delete successful!");
 //                                        } else {
 //                                                System.out.println("Delete failed: " + ret);
 //                                        }

 //                                        System.out.print("Enter substring: ");
 //                                        String substring = in.readLine();

 //                                        Set<String> keys = client.getKeys(substring);
 //                                        Map<String, String> records = client.getRecords(substring);

 //                                        if(keys.size() != records.size()) {
 //                                                System.out.println("Size mismatch! [" + keys.size() + "," + records.size() + "]");
 //                                        } 
 //                                        System.out.println("Got " + keys.size() + " keys: ");
 //                                        for(String k: keys) {
 //                                                System.out.println(k + " ");
 //                                        }
 //                                        System.out.println("Got " + records.size() + " records: ");
 //                                        for(String k: records.keySet()) {
 //                                                System.out.println("<" + k + ", " + records.get(k) + "> ");
 //                                        }
                                        
 //                                        System.out.print("Enter key: ");
 //                                }
 //                        } else {
 //                                System.out.println("Invalid partitioning scheme.");
 //                        }
 //                	transport.close();
 //                	System.out.println("Connection closed!");
	//         } catch (Exception e) {
	//       	        System.out.println("Error: " + e.toString());
	//       	        System.exit(1);
	//         }
	// }
}