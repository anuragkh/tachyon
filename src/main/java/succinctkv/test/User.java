package succinctkv.test;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import succinctkv.thrift.SuccinctKVMasterService;
import succinctkv.thrift.Ports;

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
                	SuccinctKVMasterService.Client client = new SuccinctKVMasterService.Client(protocol);
                	System.out.println("Connected!");
                	transport.open();
                	
                	String key;
                        System.out.println("Testing getVal, getKeys, getRecords, deleteRecord: ");
                        System.out.print("Enter key: ");
                        while(!(key = in.readLine()).equals("exit")) {
                                String value = client.getValue(key);
                                if(!value.equals(String.valueOf((char)10))) {
                                        System.out.println("Value = " + value);
                                } else {
                                        System.out.println("Value not found!");
                                }

                                System.out.print("Enter key for delete: ");
                                key = in.readLine();
                                int ret;
                                if((ret = client.deleteRecord(key)) == 0) {
                                        System.out.println("Delete successful!");
                                } else {
                                        System.out.println("Delete failed: " + ret);
                                }

                                System.out.print("Enter substring: ");
                                String substring = in.readLine();

                                Set<String> keys = client.getKeys(substring);
                                Map<String, String> records = client.getRecords(substring);

                                if(keys.size() != records.size()) {
                                        System.out.println("Size mismatch! [" + keys.size() + "," + records.size() + "]");
                                } 
                                System.out.println("Got " + keys.size() + " keys: ");
                                for(String k: keys) {
                                        System.out.println(k + " ");
                                }
                                System.out.println("Got " + records.size() + " records: ");
                                for(String k: records.keySet()) {
                                        System.out.println("<" + k + ", " + records.get(k) + "> ");
                                }
                                
                                System.out.print("Enter key: ");
                        }
                	
                	transport.close();
                	System.out.println("Connection closed!");
        	} catch (Exception e) {
        	      	e.printStackTrace();
        	      	System.exit(1);
        	}
	}
}