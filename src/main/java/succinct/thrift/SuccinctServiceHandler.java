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

import java.nio.file.Files;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class SuccinctServiceHandler implements SuccinctService.Iface {

	private int numServers = 1;
	private final byte delim = 10;
	private String[] hostNames;
	private TreeMap<Long, Integer> clientOffsetMap;
    private ArrayList<TTransport> serverTransports;
  	private ArrayList<QueryService.Client> queryServers;
	
	public SuccinctServiceHandler(String[] hostNames) {
		System.out.println("Initializing Succinct Service...");
		this.hostNames = hostNames;
		this.serverTransports = new ArrayList<>();
		this.queryServers = new ArrayList<>();
        this.clientOffsetMap = new TreeMap<>();
		System.out.println("Initialization complete!");
	}

	@Override
    public int connectToQueryServers() throws org.apache.thrift.TException {
    	try {
	      	for(int i = 0; i < hostNames.length; i++) {
	        	System.out.println("Connecting to " + hostNames[i] + "...");
	        	TTransport transport = new TSocket(hostNames[i], Commons.SERVER_BASE_PORT);
	        	TProtocol protocol = new TBinaryProtocol(transport);
	        	QueryService.Client client = new QueryService.Client(protocol);
	        	transport.open();

	        	queryServers.add(client);
	        	serverTransports.add(transport);
	        	System.out.print("Connected!");
	      	}
	      	System.out.println("Setup all connections!");
	    } catch (Exception e) {
	      	System.out.println("Error: " + e.toString());
	      	return -1;
	    }
    	return 0;
    }

    @Override
    public int disconnectFromQueryServers() {
        try {
            for(int i = 0; i < hostNames.length; i++) {
                System.out.println("Disconnecting from " + hostNames[i] + "...");
                serverTransports.get(i).close();
                System.out.println("Done!");
            }
            queryServers.clear();
            serverTransports.clear();
            System.out.println("Destroyed all connections!");
        } catch (Exception e) {
            System.out.println("Error : " + e.toString());
            return -1;
        }
        return 0;
    }

    @Override
    public int initialize(int mode) throws org.apache.thrift.TException {
    	
        int ret = 0;
    	System.out.println("Initializing local server...");
        try {
            System.out.println("Connecting to local server...");
            TTransport transport = new TSocket("localhost", Commons.SERVER_BASE_PORT);
            TProtocol protocol = new TBinaryProtocol(transport);
            QueryService.Client client = new QueryService.Client(protocol);
            transport.open();
            ret = client.initialize(mode);
            transport.close();
            System.out.print("Connected!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    	

    	return ret;
    }

    @Override
    public List<Long> locate(String query) throws org.apache.thrift.TException {
    	List<Long> locations = new ArrayList<>();
    	for(int i = 0; i < queryServers.size(); i++) {
    		queryServers.get(i).send_locate(query);
    	}
        for(int i = 0; i < queryServers.size(); i++) {
            locations.addAll(queryServers.get(i).recv_locate());
        }
    	return locations;
    }
    
    @Override
    public long count(String query) throws org.apache.thrift.TException {
    	int ret = 0;
        for(int i = 0; i < queryServers.size(); i++) {
            queryServers.get(i).send_count(query);
        }
    	for(int i = 0; i < queryServers.size(); i++) {
    		ret += queryServers.get(i).recv_count();
    	}
    	return ret;
    }

    @Override
    public String extract(long loc, long bytes) throws org.apache.thrift.TException {
    	Integer lookup = this.clientOffsetMap.get(loc);
        int clientId = (lookup == null) ? (Integer)(this.clientOffsetMap.lowerEntry(loc).getValue()) : lookup;
    	return queryServers.get(clientId).extract(loc, bytes);
    }

    @Override
    public long getKeyToValuePointer(String key) throws org.apache.thrift.TException {
    	return queryServers.get(key.hashCode() % queryServers.size()).getKeyToValuePointer(key);
    }

    @Override
    public String getValue(String key) throws org.apache.thrift.TException {
    	return queryServers.get(key.hashCode() % queryServers.size()).getValue(key);	
    }

    @Override
    public Set<String> getKeys(String substring) throws org.apache.thrift.TException {
    	Set<String> keys = new TreeSet<>();
        for(int i = 0; i < queryServers.size(); i++) {
            queryServers.get(i).send_getKeys(substring);
        }

    	for(int i = 0; i < queryServers.size(); i++) {
    		keys.addAll(queryServers.get(i).recv_getKeys());
    	}
    	return keys;
    }

    @Override
    public Map<String,String> getRecords(String substring) throws org.apache.thrift.TException {
    	Map<String, String> records = new TreeMap<>();
    	for(int i = 0; i < queryServers.size(); i++) {
    		queryServers.get(i).send_getRecords(substring);
    	}
        for(int i = 0; i < queryServers.size(); i++) {
            records.putAll(queryServers.get(i).recv_getRecords());
        }
    	return records;
    }

    @Override
    public int deleteRecord(String key) throws org.apache.thrift.TException {
    	return queryServers.get(key.hashCode() % queryServers.size()).deleteRecord(key);
    }

    @Override
    public Map<Integer,Range> getRanges(String query) throws org.apache.thrift.TException {
        Map<Integer, Range> rangeMap = new HashMap<>();
        for(int i = 0; i < queryServers.size(); i++) {
            queryServers.get(i).send_getRange(query);
        }
        for(int i = 0; i < queryServers.size(); i++) {
            rangeMap.put(i, queryServers.get(i).recv_getRange());
        }
        return rangeMap;
    }

    @Override
    public long getLocation(int serverId, long index) throws org.apache.thrift.TException {
        return queryServers.get(serverId).getLocation(index);
    }

}