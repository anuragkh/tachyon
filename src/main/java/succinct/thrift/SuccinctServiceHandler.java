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

    class Receiver extends Thread {
        private ArrayList<TTransport> serverTransports;
        private ArrayList<QueryService.Client> queryServers;
        private String queryType;
        private static final long RECEIVE_DURATION = 300000000000L;
        public Receiver(ArrayList<TTransport> serverTransports, ArrayList<QueryService.Client> queryServers, String queryType) {
            this.serverTransports = serverTransports;
            this.queryServers = queryServers;
            this.queryType = queryType;
        }

        private List<Long> receiveLocate() throws org.apache.thrift.TException {
            List<Long> locations = new ArrayList<>();
            for(int i = 0; i < queryServers.size(); i++) {
                locations.addAll(queryServers.get(i).recv_locate());
            }
            return locations;
        } 

        private long receiveCount() throws org.apache.thrift.TException {
            long ret = 0;
            for(int i = 0; i < queryServers.size(); i++) {
                ret += queryServers.get(i).recv_count();
            }
            return ret;
        }

        private String receiveExtract(int clientId) throws org.apache.thrift.TException {
            return queryServers.get(clientId).recv_extract();
        }

        private void countTest() throws org.apache.thrift.TException, IOException {
            long countSum = 0;
            long numResponses = 0;
            long startTime = System.nanoTime();
            while(System.nanoTime() - startTime < RECEIVE_DURATION) {
                countSum += receiveCount();
                numResponses++;
            }
            long totTime = System.nanoTime() - startTime;
            System.out.println("Thread throughput = " + ((double)numResponses) / ((double)(totTime)));
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File("res_count_throughput")));
            bw.write(((double)numResponses) / ((double)(totTime)) + "\n");
            bw.close();
        }

        private void locateTest() throws org.apache.thrift.TException, IOException {
            long locateSum = 0;
            long numResponses = 0;
            long startTime = System.nanoTime();
            while(System.nanoTime() - startTime < RECEIVE_DURATION) {
                locateSum += receiveLocate().size();
                numResponses++;
            }
            long totTime = System.nanoTime() - startTime;
            System.out.println("Thread throughput = " + ((double)numResponses) / ((double)(totTime)));
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File("res_locate_throughput")));
            bw.write(((double)numResponses) / ((double)(totTime)) + "\n");
            bw.close();
        }

        private void extractTest() throws org.apache.thrift.TException, IOException {
            long extractSum = 0;
            long numResponses = 0;
            long startTime = System.nanoTime();
            while(System.nanoTime() - startTime < RECEIVE_DURATION) {
                for(int i = 0; i < queryServers.size(); i++) {
                    extractSum += receiveExtract(i).length();
                    numResponses++;
                }
            }
            long totTime = System.nanoTime() - startTime;
            System.out.println("Thread throughput = " + ((double)numResponses) / ((double)(totTime)));
            BufferedWriter bw = new BufferedWriter(new FileWriter(new File("res_extract_throughput")));
            bw.write(((double)numResponses) / ((double)(totTime)) + "\n");
            bw.close();
        }

        @Override
        public void run() {
            try {
                if(queryType.equals("count")) {
                    countTest();
                } else if(queryType.equals("locate")) {
                    locateTest();
                } else if(queryType.equals("extract")) {
                    extractTest();
                } else {
                    System.out.println("Not supported");
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }

    class Tester extends Thread {
        private ArrayList<TTransport> serverTransports;
        private ArrayList<QueryService.Client> queryServers;
        private String[] hostNames;
        private String queryType;
        private String[] queries;
        private long[] randoms;
        private static final long SEND_DURATION = 310000000000L;
        private static final int SEND_SLOT = 1000;

        public Tester(String [] hostNames, String[] queries, String queryType) {
            this.serverTransports = new ArrayList<>();
            this.queryServers = new ArrayList<>();
            this.hostNames = hostNames;
            this.queryType = queryType;
            this.queries = queries;
            this.randoms = null;
        }

        public Tester(String [] hostNames, long[] randoms, String queryType) {
            this.serverTransports = new ArrayList<>();
            this.queryServers = new ArrayList<>();
            this.hostNames = hostNames;
            this.queryType = queryType;
            this.randoms = randoms;
            this.queries = null;
        }

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
                    System.out.println("Connected!");
                }
                System.out.println("Setup all connections! Currently have " + queryServers.size() + " connections.");
            } catch (Exception e) {
                System.out.println("Error: " + e.toString());
                return -1;
            }
            return 0;
        }

        public int disconnectFromQueryServers() throws org.apache.thrift.TException {
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

        public void sendLocate(String query) throws org.apache.thrift.TException {
            for(int i = 0; i < queryServers.size(); i++) {
                queryServers.get(i).send_locate(query);
            }
        }
        
        public void sendCount(String query) throws org.apache.thrift.TException {
            for(int i = 0; i < queryServers.size(); i++) {
                queryServers.get(i).send_count(query);
            }
        }

        public void sendExtract(long loc, long bytes) throws org.apache.thrift.TException {
            int clientId = (int)(loc / SPLIT_SIZE);
            long clientLoc = loc % SPLIT_SIZE;
            queryServers.get(clientId).send_extract(clientLoc, bytes);
        }

        public void countTest() throws org.apache.thrift.TException, InterruptedException {
            connectToQueryServers();
            Receiver receiveThread = new Receiver(serverTransports, queryServers, queryType);
            System.out.println("Starting receiver...");
            receiveThread.start();
            System.out.println("Started");

            // Sending logic
            String query = queries[0];
            int numQueries = queries.length;
            int i = 0;
            long startTime = System.nanoTime();
            while(System.nanoTime() - startTime < SEND_DURATION) {
                if(i % SEND_SLOT == 0) {
                    query = queries[i % numQueries];
                }
                sendCount(query);
                i++;
            }
            long totTime = startTime - System.nanoTime();
            System.out.println("Send rate: " + ((double)i)/((double)totTime));
            receiveThread.join();
            disconnectFromQueryServers();
        }

        public void locateTest() throws org.apache.thrift.TException, InterruptedException {
            connectToQueryServers();
            Receiver receiveThread = new Receiver(serverTransports, queryServers, queryType);
            receiveThread.start();

            // Sending logic
            String query = queries[0];
            int numQueries = queries.length;
            int i = 0;
            long startTime = System.nanoTime();
            while(System.nanoTime() - startTime < SEND_DURATION) {
                if(i % SEND_SLOT == 0) {
                    query = queries[i % numQueries];
                }
                sendLocate(query);
                i++;
            }
            long totTime = startTime - System.nanoTime();
            System.out.println("Send rate: " + ((double)i)/((double)totTime));
            receiveThread.join();
            disconnectFromQueryServers();
        }

        public void extractTest() throws org.apache.thrift.TException, InterruptedException {
            connectToQueryServers();
            Receiver receiveThread = new Receiver(serverTransports, queryServers, queryType);
            receiveThread.start();

            // Sending logic
            long query = randoms[0];
            int numQueries = randoms.length;
            int i = 0;
            long startTime = System.nanoTime();
            while(System.nanoTime() - startTime < SEND_DURATION) {
                if(i % SEND_SLOT == 0) {
                    query = randoms[i % numQueries];
                }
                sendExtract(query, 1024);
                i++;
            }
            long totTime = startTime - System.nanoTime();
            System.out.println("Send rate: " + ((double)i)/((double)totTime));
            receiveThread.join();
            disconnectFromQueryServers();
        }

        @Override
        public void run() {
            try {
                if(queryType.equals("count")) {
                    countTest();
                } else if(queryType.equals("locate")) {
                    locateTest();
                } else if(queryType.equals("extract")) {
                    extractTest();
                } else {
                    System.out.println("Not supported");
                }
            } catch(Exception e) {
                e.printStackTrace();
            }
        }
    }
	
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
                boolean test = (in.read() == '\u0002');
                int l = 0;
                String q = "";
                while(l != len) {
                    c = (char)in.read();
                    System.out.println("[" + c + "]");
                    q += c;
                    l++;
                }
                test = (in.read() == '\u0003');
                System.out.println("[" + q + "] : " + q.length());
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
            randoms[i] = Math.abs(rand.nextLong() % (SPLIT_SIZE * NUM_SPLITS));
            System.out.println("[" + randoms[i] + "]");
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
                System.out.println(c + "\t" + totTime);
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
                System.out.println(c + "\t" + totTime);
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
                System.out.println(randoms[i] + "\t" + c + "\t" + totTime);
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
        String[] queries = readQueries(queriesPath, numQueries);
        Tester[] testers = new Tester[numThreads];

        System.out.println("Starting " + numThreads + " threads...");
        for(int i = 0; i < numThreads; i++) {
            testers[i] = new Tester(hostNames, queries, "count");
            testers[i].start();
        }
        System.out.println("Started " + numThreads + " threads!");

        for(int i = 0; i < numThreads; i++) {
            try {
                testers[i].join();
            } catch (InterruptedException e) {
                System.out.println("Error waiting for thread " + i + "...");
                e.printStackTrace();
            }
        }
        return 0;
    }

    @Override
    public long testLocateThroughput(String queriesPath, int numQueries, int numThreads) {
        String[] queries = readQueries(queriesPath, numQueries);
        Tester[] testers = new Tester[numThreads];
        for(int i = 0; i < numThreads; i++) {
            testers[i] = new Tester(hostNames, queries, "locate");
            testers[i].start();
        }

        for(int i = 0; i < numThreads; i++) {
            try {
                testers[i].join();
            } catch (InterruptedException e) {
                System.out.println("Error waiting for thread " + i + "...");
                e.printStackTrace();
            }
        }
        return 0;
    }

    @Override
    public long testExtractThroughput(int numQueries, int numThreads) {
        long[] randoms = generateRandoms(numQueries);
        Tester[] testers = new Tester[numThreads];
        for(int i = 0; i < numThreads; i++) {
            testers[i] = new Tester(hostNames, randoms, "extract");
            testers[i].start();
        }

        for(int i = 0; i < numThreads; i++) {
            try {
                testers[i].join();
            } catch (InterruptedException e) {
                System.out.println("Error waiting for thread " + i + "...");
                e.printStackTrace();
            }
        }
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
	        	System.out.println("Connected!");
	      	}
	      	System.out.println("Setup all connections! Currently have " + queryServers.size() + " connections.");
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
        try {
            for(int i = 0; i < queryServers.size(); i++) {
                queryServers.get(i).send_count(query);
            }
        	for(int i = 0; i < queryServers.size(); i++) {
        		ret += queryServers.get(i).recv_count();
        	}
        } catch (Exception e) {
            e.printStackTrace();
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