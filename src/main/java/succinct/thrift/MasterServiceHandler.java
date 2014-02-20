package succinct.thrift;

import java.text.SimpleDateFormat;

import java.util.Calendar;
import java.util.Random;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class MasterServiceHandler implements MasterService.Iface {

	public static final int CLIENT_BASE_PORT = 11000;
	String[] hostNames;

	public MasterServiceHandler(String[] hostNames) {
		this.hostNames = hostNames;

		// Start clients on all machines
		try {
	      	for(int i = 0; i < hostNames.length; i++) {
	        	System.out.println("Connecting to " + hostNames[i] + "...");
	        	TTransport transport = new TSocket(hostNames[i], CLIENT_BASE_PORT);
	        	TProtocol protocol = new TBinaryProtocol(transport);
	        	SuccinctService.Client client = new SuccinctService.Client(protocol);
	        	System.out.println("Connected!");
	        	transport.open();
	        	client.connectToClients();
	        	transport.close();
	        	System.out.println("Connection closed!");
	        	
	      	}
	      	System.out.println("Setup all connections!");
	    } catch (Exception e) {
	      	System.out.println("Error: " + e.toString());
	    }
	}

	@Override
	public String createSuccinctFile(String filePath, int partScheme, byte delim1, byte delim2) throws org.apache.thrift.TException {
		String clientAddress = openSuccinctFile();
		System.out.println("Submitting file to client at " + clientAddress);
		try {
        	TTransport transport = new TSocket(clientAddress, CLIENT_BASE_PORT);
        	TProtocol protocol = new TBinaryProtocol(transport);
        	SuccinctService.Client client = new SuccinctService.Client(protocol);
        	System.out.println("Connected!");
        	transport.open();
        	client.submitFile(filePath, partScheme, delim1, delim2);
        	transport.close();
        	System.out.println("Connection closed!");
	    } catch (Exception e) {
	      	System.out.println("Error: " + e.toString());
	      	e.printStackTrace();
	    }
		return clientAddress;
	}

	@Override
    public String openSuccinctFile() throws org.apache.thrift.TException {
    	return this.hostNames[(new Random()).nextInt(this.hostNames.length)];
    }

}