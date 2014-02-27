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

public class SuccinctMasterServiceHandler implements SuccinctMasterService.Iface {

	private int partitionScheme;
	private String[] hostNames;
	private TreeMap<Long, Integer> clientOffsetMap;
	private ArrayList<TTransport> clientTransports;
  	private ArrayList<SuccinctWorkerService.Client> clients;

	public SuccinctMasterServiceHandler(String[] hostNames) {
		System.out.println("Initializing Succinct Master Service...");
		this.hostNames = hostNames;
		this.clientTransports = new ArrayList<>();
		this.clients = new ArrayList<>();
		this.clientOffsetMap = new TreeMap<>();

        try {
            for(int i = 0; i < hostNames.length; i++) {
                System.out.println("Connecting to " + hostNames[i] + "...");
                TTransport transport = new TSocket(hostNames[i], Ports.WORKER_BASE_PORT);
                TProtocol protocol = new TBinaryProtocol(transport);
                SuccinctWorkerService.Client client = new SuccinctWorkerService.Client(protocol);
                transport.open();
                clients.add(client);
                clientTransports.add(transport);
                System.out.print("Connected!");
            }
            System.out.println("Setup all connections!");
        } catch (Exception e) {
            System.out.println("Error: " + e.toString());
        }

        try {
            initialize(0);
        } catch (Exception e) {
            e.printStackTrace();
        }

		System.out.println("Initialization complete!");
	}

    @Override
    public int createSuccinctFile(String filePath) throws org.apache.thrift.TException {
        // TODO: Implement -- will call spark code
        return 0;
    }

    @Override
    public int openSuccinctFile(String fileName) throws org.apache.thrift.TException {
        // TODO: Implement --  will lookup metadata
        return 0;
    }

    @Override
    public int initialize(int mode) throws org.apache.thrift.TException {

    	int ret = 0;
    	for(int i = 0; i < clients.size(); i++) {
    		System.out.println("Asking worker at " + hostNames[i] + " to start servers...");
            clients.get(i).send_startSuccinctServers(1);    		
    	}

        for(int i = 0; i < clients.size(); i++) {            
            ret += clients.get(i).recv_startSuccinctServers();  
            System.out.println("Worker at " + hostNames[i] + " finished starting servers.");
        }

        if(ret != 0) return ret;

        for(int i = 0; i < clients.size(); i++) {
            System.out.println("Initializing Worker at " + hostNames[i]);
            ret += clients.get(i).initialize(mode);
            clientOffsetMap.put(clients.get(i).getWorkerOffset(), i);
        }

    	return ret;
    }

    @Override
    public List<Long> locate(String query) throws org.apache.thrift.TException {
    	List<Long> locations = new ArrayList<>();
    	for(int i = 0; i < clients.size(); i++) {
    		clients.get(i).send_locateLocal(query);
    	}
        for(int i = 0; i < clients.size(); i++) {
            locations.addAll(clients.get(i).recv_locateLocal());
        }
    	return locations;
    }

    @Override
    public long count(String query) throws org.apache.thrift.TException {
    	int ret = 0;
        for(int i = 0; i < clients.size(); i++) {
            clients.get(i).send_countLocal(query);
        }
    	for(int i = 0; i < clients.size(); i++) {
    		ret += clients.get(i).recv_countLocal();
    	}
    	return ret;
    }

    @Override
    public String extract(long loc, long bytes) throws org.apache.thrift.TException {
    	System.out.println("Received extract query for <" + loc + "," + bytes + ">");
    	// int clientId = findSplit(clientOffsets, loc);
        Integer lookup = this.clientOffsetMap.get(loc);
        int clientId = (lookup == null) ? (Integer)(this.clientOffsetMap.lowerEntry(loc).getValue()) : lookup;
    	System.out.println("Offset " + loc + " found at cliendId = " + clientId + " = " + hostNames[clientId]);
    	return clients.get(clientId).extractLocal(loc, bytes);
    }

    @Override
    public Map<Integer,Map<Integer,Range>> getRanges(String query) throws org.apache.thrift.TException {
        Map<Integer, Map<Integer, Range>> rangeMap = new HashMap<>();
        for(int i = 0; i < clients.size(); i++) {
            clients.get(i).send_getRangesLocal(query);
        }
        for(int i = 0; i < clients.size(); i++) {
            rangeMap.put(i, clients.get(i).recv_getRangesLocal());
        }
        return rangeMap;
    }

    @Override
    public long getLocation(int clientId, int serverId, long index) throws org.apache.thrift.TException {
        return clients.get(clientId).getLocationLocal(serverId, index);
    }

}