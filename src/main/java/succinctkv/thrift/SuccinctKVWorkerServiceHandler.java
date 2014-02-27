package succinctkv.thrift;

import java.util.Set;
import java.util.TreeSet;
import java.util.Map;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList; 
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

public class SuccinctKVWorkerServiceHandler implements SuccinctKVWorkerService.Iface {

	private int numServers = 1;
	private byte delim = 10;
	private ArrayList<TTransport> localTransports;
  	private ArrayList<SuccinctKVService.Client> localServers;
  	private String tachyonMasterAddress;

	public SuccinctKVWorkerServiceHandler(String tachyonMasterAddress) {
		System.out.println("Initializing Succinct Service...");
		this.tachyonMasterAddress = tachyonMasterAddress;
		this.localTransports = new ArrayList<>();
		this.localServers = new ArrayList<>();
		System.out.println("Initialization complete!");
	}

    @Override
    public int startSuccinctKVServers(int numServers) throws org.apache.thrift.TException {
    	ExecutorService serverExecutor = Executors.newCachedThreadPool();

        File dataPath = new File("succinctkv/data");
        for(int i = 0; i < numServers; i++) {
            try {
                serverExecutor.submit(new SuccinctKVServiceHandler(this.tachyonMasterAddress, dataPath.getAbsolutePath(), delim, 0, Ports.SERVER_BASE_PORT + i));
            } catch(IOException e) {
                System.out.println("Error: SuccinctKVWorkerServiceHandler.java:startServers(): " + e.toString());
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
            SuccinctKVService.Client client = new SuccinctKVService.Client(protocol);
            transport.open();

            localServers.add(client);
            localTransports.add(transport);
            System.out.print("Connected!");
        }
    	return 0;
    }

    @Override
    public int initializeKV(int mode) throws org.apache.thrift.TException {
    	int ret = 0;
    	for(int i = 0; i < localServers.size(); i++) {
    		System.out.println("Initializing local server " + i);
    		ret += localServers.get(i).initializeKV(mode);
    	}

    	return ret;
    }

    @Override
    public long getKeyToValuePointerLocal(String key) throws org.apache.thrift.TException {
    	System.out.println("Received getKeyToValuePointerLocal query for [" + key + "]");
    	return localServers.get(key.hashCode() % localServers.size()).getKeyToValuePointer(key);
    }

    @Override
    public String getValueLocal(String key) throws org.apache.thrift.TException {
    	System.out.println("Received getValueLocal query for [" + key + "]");
    	return localServers.get(key.hashCode() % localServers.size()).getValue(key);
    }

    @Override
    public Set<String> getKeysLocal(String substring) throws org.apache.thrift.TException {
    	System.out.println("Received getKeysLocal query for [" + substring + "]");
    	Set<String> keys = new TreeSet<>();
    	for(int i = 0; i < localServers.size(); i++) {
    		localServers.get(i).send_getKeys(substring);
    	}
        for(int i = 0; i < localServers.size(); i++) {
            keys.addAll(localServers.get(i).recv_getKeys());
        }
    	return keys;
    }

    @Override
    public Map<String,String> getRecordsLocal(String substring) throws org.apache.thrift.TException {
    	System.out.println("Received getRecordsLocal query for [" + substring + "]");
    	Map<String, String> records = new TreeMap<>();
    	for(int i = 0; i < localServers.size(); i++) {
    		localServers.get(i).send_getRecords(substring);
    	}
        for(int i = 0; i < localServers.size(); i++) {
            records.putAll(localServers.get(i).recv_getRecords());
        }
    	return records;
    }

    @Override
    public int deleteRecordLocal(String key) throws org.apache.thrift.TException {
    	System.out.println("Received deleteRecordLocal request for [" + key + "]");
    	return localServers.get(key.hashCode() % localServers.size()).deleteRecord(key);
    }

}