package succinctkv.thrift;

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

public class SuccinctKVMasterServiceHandler implements SuccinctKVMasterService.Iface {

	private String[] hostNames;
    private ArrayList<TTransport> clientTransports;
  	private ArrayList<SuccinctKVWorkerService.Client> clients;
  	
	public SuccinctKVMasterServiceHandler(String[] hostNames) {
		System.out.println("Initializing Succinct Service...");
        this.hostNames = hostNames;
		this.clientTransports = new ArrayList<>();
		this.clients = new ArrayList<>();

         try {
            for(int i = 0; i < hostNames.length; i++) {
                System.out.println("Connecting to " + hostNames[i] + "...");
                TTransport transport = new TSocket(hostNames[i], Ports.WORKER_BASE_PORT);
                TProtocol protocol = new TBinaryProtocol(transport);
                SuccinctKVWorkerService.Client client = new SuccinctKVWorkerService.Client(protocol);
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
            initializeKV(0);
        } catch (Exception e) {
            e.printStackTrace();
        }

		System.out.println("Initialization complete!");
	}

     @Override
    public int initializeKV(int mode) throws org.apache.thrift.TException {

        int ret = 0;
        for(int i = 0; i < clients.size(); i++) {
            System.out.println("Asking worker at " + hostNames[i] + " to start servers...");
            clients.get(i).send_startSuccinctKVServers(1);            
        }

        for(int i = 0; i < clients.size(); i++) {            
            ret += clients.get(i).recv_startSuccinctKVServers();  
            System.out.println("Worker at " + hostNames[i] + " finished starting servers.");
        }

        if(ret != 0) {
            System.out.println("Error in starting SuccinctKV Servers!");
            return ret;  
        } 

        for(int i = 0; i < clients.size(); i++) {
            System.out.println("Initializing Worker at " + hostNames[i]);
            ret += clients.get(i).initializeKV(mode);
        }

        return ret;
    }

    @Override
    public long getKeyToValuePointer(String key) throws org.apache.thrift.TException {
    	System.out.println("Received getKeyToValuePointer query for [" + key + "]");
    	return clients.get(key.hashCode() % clients.size()).getKeyToValuePointerLocal(key);
    }

    @Override
    public String getValue(String key) throws org.apache.thrift.TException {
    	System.out.println("Received getValue query for [" + key + "]");
    	return clients.get(key.hashCode() % clients.size()).getValueLocal(key);	
    }

    @Override
    public Set<String> getKeys(String substring) throws org.apache.thrift.TException {
    	System.out.println("Received getKeys query for [" + substring + "]");
    	Set<String> keys = new TreeSet<>();
        for(int i = 0; i < clients.size(); i++) {
            clients.get(i).send_getKeysLocal(substring);
        }

    	for(int i = 0; i < clients.size(); i++) {
    		keys.addAll(clients.get(i).recv_getKeysLocal());
    	}
    	return keys;
    }

    @Override
    public Map<String,String> getRecords(String substring) throws org.apache.thrift.TException {
    	System.out.println("Received getRecords query for [" + substring + "]");
    	Map<String, String> records = new TreeMap<>();
    	for(int i = 0; i < clients.size(); i++) {
    		clients.get(i).send_getRecordsLocal(substring);
    	}
        for(int i = 0; i < clients.size(); i++) {
            records.putAll(clients.get(i).recv_getRecordsLocal());
        }
    	return records;
    }

    @Override
    public int deleteRecord(String key) throws org.apache.thrift.TException {
    	System.out.println("Received deleteRecord request for [" + key + "]");
    	return clients.get(key.hashCode() % clients.size()).deleteRecordLocal(key);
    }

}