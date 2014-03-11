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
import java.util.Random;

import java.io.IOException;
import java.io.File;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.BufferedWriter;

import java.nio.file.Files;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class SuccinctServiceHandler implements SuccinctService.Iface {

	private final long SPLIT_SIZE = 2684354560L;    // TODO: Remove
    private final int NUM_SPLITS = 5;               // TODO: Remove
	private final byte delim = 10;
	private String[] hostNames;
    private ArrayList<TTransport> serverTransports;
  	private ArrayList<QueryService.Client> queryServers;
	
	public SuccinctServiceHandler(String[] hostNames) {
		System.out.println("Initializing Succinct Service...");
		this.hostNames = hostNames;
		this.serverTransports = new ArrayList<>();
		this.queryServers = new ArrayList<>();
		System.out.println("Initialization complete!");
	}

    public String[] readQueries(String queriesPath, int numQueries) {
        System.out.println("Loading queries " + numQueries + " from path: " + queriesPath);
        File queryFile = new File(queriesPath);
        DataInputStream in = null;
        String queries[] = new String[numQueries];
        try {
            in = new DataInputStream(new FileInputStream(queryFile));
            int len = 8;    // Query Length
            for(int i = 0; i < numQueries; i++) {
                char c;
                while((c = (char)in.read()) != '\t');
                assert (in.read() == '\u0002');
                int l = 0;
                String q = "";
                while((c = (char)in.read()) != '\u0003') {
                    q += c;
                }
                queries[i] = q;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queries;
    }

    public long[] generateRandoms(int numQueries) {
        long[] randoms = new long[numQueries];
        Random rand = new Random();
        for(int i = 0; i < numQueries; i++) {
            randoms[i] = rand.nextLong() % (SPLIT_SIZE * NUM_SPLITS);
        }

        return randoms;
    }

    @Override
    public long testCountLatency(String queriesPath, int numQueries, int repeat) {
        String[] queries = readQueries(queriesPath, numQueries);
        long startTime;
        double totTime;
        try {
            BufferedWriter res = new BufferedWriter(new FileWriter(new File("res_count_latency_" + numQueries + "_" + repeat)));
            this.connectToQueryServers();
            for(int i = 0; i < numQueries; i++) {
                totTime = 0;
                long c = 0;
                for(int j = 0; j < repeat; j++) {
                    startTime = System.nanoTime();
                    long count = this.count(queries[i]);
                    totTime += (double)(System.nanoTime() - startTime) / 1000.0;
                    c += count;
                }
                totTime /= repeat;
                c /= repeat;
                res.write(c + "\t" + totTime + "\n");
            }
            this.disconnectFromQueryServers();
            res.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public long testLocateLatency(String queriesPath, int numQueries, int repeat) {
        String[] queries = readQueries(queriesPath, numQueries);
        long startTime;
        double totTime;
        try {
            BufferedWriter res = new BufferedWriter(new FileWriter(new File("res_locate_latency_" + numQueries + "_" + repeat)));
            this.connectToQueryServers();
            for(int i = 0; i < numQueries; i++) {
                totTime = 0;
                long c = 0;
                for(int j = 0; j < repeat; j++) {
                    startTime = System.nanoTime();
                    long count = this.locate(queries[i]).size();
                    totTime += (double)(System.nanoTime() - startTime) / 1000.0;
                    c += count;
                }
                totTime /= repeat;
                c /= repeat;
                res.write(c + "\t" + totTime + "\n");
            }
            this.disconnectFromQueryServers();
            res.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public long testExtractLatency(int numQueries, int repeat) {
        long[] randoms = generateRandoms(numQueries);
        long startTime;
        double totTime;
        try {
            BufferedWriter res = new BufferedWriter(new FileWriter(new File("res_extract_latency_" + numQueries + "_" + repeat)));
            this.connectToQueryServers();
            for(int i = 0; i < numQueries; i++) {
                totTime = 0;
                long c = 0;
                for(int j = 0; j < repeat; j++) {
                    startTime = System.nanoTime();
                    long bytes = this.extract(randoms[i], 1024).length();
                    totTime += (double)(System.nanoTime() - startTime) / 1000.0;
                    c += bytes;
                }
                totTime /= repeat;
                c /= repeat;
                res.write(randoms[i] + "\t" + c + "\t" + totTime + "\n");
            }
            this.disconnectFromQueryServers();
            res.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
    public long testCountThroughput(String queriesPath, int numQueries, int numThreads) {
        return 0;
    }

    @Override
    public long testLocateThroughput(String queriesPath, int numQueries, int numThreads) {
        return 0;
    }

    @Override
    public long testExtractThroughput(int numQueries, int numThreads) {
        return 0;
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
        int clientId = (int)(loc / SPLIT_SIZE);
        long clientLoc = loc % SPLIT_SIZE;
    	return queryServers.get(clientId).extract(clientLoc, bytes);
    }

    @Override
    public String accessRecord(String recordId, long offset, long bytes) throws org.apache.thrift.TException {
        return queryServers.get(recordId.hashCode() % queryServers.size()).accessRecord(recordId, offset, bytes);
    }

    @Override
    public long getRecordPointer(String recordId) throws org.apache.thrift.TException {
    	return queryServers.get(recordId.hashCode() % queryServers.size()).getRecordPointer(recordId);
    }

    @Override
    public String getRecord(String recordId) throws org.apache.thrift.TException {
    	return queryServers.get(recordId.hashCode() % queryServers.size()).getRecord(recordId);	
    }

    @Override
    public Set<String> getRecordIds(String substring) throws org.apache.thrift.TException {
    	Set<String> recordIds = new TreeSet<>();
        for(int i = 0; i < queryServers.size(); i++) {
            queryServers.get(i).send_getRecordIds(substring);
        }

    	for(int i = 0; i < queryServers.size(); i++) {
    		recordIds.addAll(queryServers.get(i).recv_getRecordIds());
    	}
    	return recordIds;
    }

    @Override
    public long countRecords(String substring) throws org.apache.thrift.TException {
        long count = 0;
        for(int i = 0; i < queryServers.size(); i++) {
            queryServers.get(i).send_countRecords(substring);
        }

        for(int i = 0; i < queryServers.size(); i++) {
            count += queryServers.get(i).recv_countRecords();
        }
        return count;
    }

    @Override
    public Map<String,Long> freqCountRecords(String substring) throws org.apache.thrift.TException {
        Map<String, Long> countMap = new TreeMap<>();
        for(int i = 0; i < queryServers.size(); i++) {
            queryServers.get(i).send_freqCountRecords(substring);
        }

        for(int i = 0; i < queryServers.size(); i++) {
            countMap.putAll(queryServers.get(i).recv_freqCountRecords());
        }
        return countMap;
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
    public int deleteRecord(String recordId) throws org.apache.thrift.TException {
    	return queryServers.get(recordId.hashCode() % queryServers.size()).deleteRecord(recordId);
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