package succinct.thrift;

import java.util.Calendar;
import java.util.Random;

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

		// Start clients on all machines
		try {
	      	for(int i = 0; i < hostNames.length; i++) {
	        	System.out.println("Connecting to " + hostNames[i] + "...");
	        	TTransport transport = new TSocket(hostNames[i], Commons.HANDLER_BASE_PORT);
	        	TProtocol protocol = new TBinaryProtocol(transport);
	        	SuccinctService.Client client = new SuccinctService.Client(protocol);
	        	System.out.println("Connected!");
	        	transport.open();
	        	client.initialize(0);
	        	transport.close();
	        	System.out.println("Connection closed!");
	      	}
	      	System.out.println("Setup all connections!");
	    } catch (Exception e) {
	      	System.out.println("Error: " + e.toString());
	    }
	}

	@Override
    public String openSuccinctFile() throws org.apache.thrift.TException {
    	return this.hostNames[(new Random()).nextInt(this.hostNames.length)];
    }

}