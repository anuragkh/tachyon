package succinct.thrift;

import java.util.Calendar;
import java.util.Random;
import java.util.ArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class MasterServiceHandler implements MasterService.Iface {

	String[] hostNames;

	public MasterServiceHandler(String[] hostNames) {
		this.hostNames = hostNames;
		ArrayList<TTransport> transports = new ArrayList<>();
		ArrayList<SuccinctService.Client> clients = new ArrayList<>();
		// Start clients on all machines
		try {
	      	for(int i = 0; i < hostNames.length; i++) {
	        	System.out.println("Connecting to " + hostNames[i] + "...");
	        	TTransport transport = new TSocket(hostNames[i], Commons.HANDLER_BASE_PORT);
	        	TProtocol protocol = new TBinaryProtocol(transport);
	        	SuccinctService.Client client = new SuccinctService.Client(protocol);
	        	transport.open();
	        	System.out.println("Connected!");
	        	client.send_initialize(0);
	        	System.out.println("Started initialization at " + hostNames[i]);
	        	transports.add(transport);
	        	clients.add(client);
	      	}
	      	System.out.println("Setup all connections!");
	    } catch (Exception e) {
	      	System.out.println("Error: " + e.toString());
	    }

	    // Clean up connections
	    try {
	    	for(int i = 0; i < hostNames.length; i++) {
	    		clients.get(i).recv_initialize();
	    		System.out.println("Initialization complete at " + hostNames[i]);
	    		transports.get(i).close();
	        	System.out.println("Connection closed!");
	    	}
	    } catch(Exception e) {
	    	e.printStackTrace();
	    }
	}

	@Override
    public String openSuccinctFile() throws org.apache.thrift.TException {
    	return this.hostNames[(new Random()).nextInt(this.hostNames.length)];
    }

}