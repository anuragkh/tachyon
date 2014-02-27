package succinct.thrift;

import java.util.Set;
import java.util.TreeSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.concurrent.*;

import java.io.IOException;
import java.io.File;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class SuccinctWorkerServiceHandler implements SuccinctWorkerService.Iface {

	private int numServers = 1;
    private TreeMap<Long, Integer> localServerOffsetMap;
  	private ArrayList<TTransport> localTransports;
  	private ArrayList<SuccinctService.Client> localServers;
  	private String tachyonMasterAddress;
	
	public SuccinctWorkerServiceHandler(String tachyonMasterAddress) {
		System.out.println("Initializing Succinct Worker Service...");
		this.tachyonMasterAddress = tachyonMasterAddress;
		this.localTransports = new ArrayList<>();
		this.localServers = new ArrayList<>();
        this.localServerOffsetMap = new TreeMap<>();
		System.out.println("Initialization complete!");
	}	

    @Override
    public int startSuccinctServers(int numServers) throws org.apache.thrift.TException {
    	ExecutorService serverExecutor = Executors.newCachedThreadPool();

        File[] dataFiles = new File("succinct/data").listFiles();
    	for(int i = 0; i < numServers; i++) {
    		try {
    			serverExecutor.submit(new SuccinctServiceHandler(this.tachyonMasterAddress, dataFiles[i].getAbsolutePath(), 0, Ports.SERVER_BASE_PORT + i));
    		} catch(IOException e) {
    			System.out.println("Error: SuccinctServiceHandler.java:startServers(): " + e.toString());
    			e.printStackTrace();
    		}
    	}

    	try {
    		Thread.sleep(5000);
    	} catch(InterruptedException ex) {
    		ex.printStackTrace();
		    Thread.currentThread().interrupt();
		}

    	for(int i = 0; i < numServers; i++) {
    		System.out.println("Connecting to server " + i + "...");
        	TTransport transport = new TSocket("localhost", Ports.SERVER_BASE_PORT + i);
        	TProtocol protocol = new TBinaryProtocol(transport);
        	SuccinctService.Client client = new SuccinctService.Client(protocol);
        	transport.open();

        	localServers.add(client);
        	localTransports.add(transport);
        	System.out.print("Connected!");
    	}
    	return 0;
    }

    @Override
    public int initialize(int mode) throws org.apache.thrift.TException {
    	
    	int ret = 0;
    	for(int i = 0; i < localServers.size(); i++) {
    		System.out.println("Initializing local server " + i);
    		ret += localServers.get(i).initialize(mode);
            localServerOffsetMap.put(localServers.get(i).getServerOffset(), i);
    	}

    	return ret;
    }

    @Override
    public long getWorkerOffset() {
        return localServerOffsetMap.firstKey();
    }

    @Override
    public List<Long> locateLocal(String query) throws org.apache.thrift.TException {
    	List<Long> locations = new ArrayList<>();
    	for(int i = 0; i < localServers.size(); i++) {
            localServers.get(i).send_locate(query);
    	}

        for(int i = 0; i < localServers.size(); i++) {
            locations.addAll(localServers.get(i).recv_locate());
        }
    	return locations;
    }

    @Override
    public long countLocal(String query) throws org.apache.thrift.TException {
    	int ret = 0;
    	for(int i = 0; i < localServers.size(); i++) {
    		localServers.get(i).send_count(query);
    	}
        for(int i = 0; i < localServers.size(); i++) {
            ret += localServers.get(i).recv_count();
        }
    	return ret;
    }

    @Override
    public String extractLocal(long loc, long bytes) throws org.apache.thrift.TException {
    	// System.out.println("Received extractLocal query for <" + loc + "," + bytes + ">");
    	// int serverId = findSplit(localServerOffsets, loc);
        Integer lookup = this.localServerOffsetMap.get(loc);
        int serverId = (lookup == null) ? (Integer)(this.localServerOffsetMap.lowerEntry(loc).getValue()) : lookup;
    	// System.out.println("Offset " + loc + " found at serverId = " + serverId);
    	return localServers.get(serverId).extract(loc, bytes);
    }

    @Override
    public Map<Integer,Range> getRangesLocal(String query) throws org.apache.thrift.TException {
        Map<Integer, Range> rangeMap = new HashMap<>();
        for(int i = 0; i < localServers.size(); i++) {
            localServers.get(i).send_getRange(query);
        }
        for(int i = 0; i < localServers.size(); i++) {
            rangeMap.put(i, localServers.get(i).recv_getRange());
        }
        return rangeMap;
    }

    @Override
    public long getLocationLocal(int serverId, long index) throws org.apache.thrift.TException {
        return localServers.get(serverId).getLocation(index);
    }
}