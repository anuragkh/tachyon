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

	public final int PARTITION_HASH = 0;
	public final int PARTITION_RETAIN = 1;
	private int numServers = 1;
	private byte delim = 10;
	private int localHostId;
	private int partitionScheme;
	private String[] hostNames;
	private long[] clientOffsets;
	private long[] localServerOffsets;
    private TreeMap<Long, Integer> clientOffsetMap;
    private TreeMap<Long, Integer> localServerOffsetMap;
	private ArrayList<TTransport> clientTransports;
  	private ArrayList<SuccinctService.Client> clients;
  	private ArrayList<TTransport> localTransports;
  	private ArrayList<QueryService.Client> localServers;
  	private String tachyonMasterAddress;

  	// Move to better location
  	public static final int CLIENT_BASE_PORT = 11000;
  	public static final int SERVER_BASE_PORT = 12000;
	
	public SuccinctServiceHandler(int localHostId, String[] hostNames, String tachyonMasterAddress) {
		System.out.println("Initializing Succinct Service...");
		this.hostNames = hostNames;
		this.localHostId = localHostId;
		this.tachyonMasterAddress = tachyonMasterAddress;
		this.clientOffsets = new long[hostNames.length];
		this.localServerOffsets = new long[numServers];
		this.clientTransports = new ArrayList<>();
		this.clients = new ArrayList<>();
		this.localTransports = new ArrayList<>();
		this.localServers = new ArrayList<>();
        this.clientOffsetMap = new TreeMap<>();
        this.localServerOffsetMap = new TreeMap<>();
		System.out.println("Initialization complete!");
	}

	// Binary search in offsets to find the split which has the position
	private int findSplit(long[] offsets, long position) {
	    int sp = 0, ep = offsets.length;
	    while (sp < ep) {
	        int m = (sp + ep) / 2;
	        if (offsets[m] == position)
	            return m;
	        else if(position < offsets[m])
	            ep = m - 1;
	        else
	            sp = m + 1;
	    }

	    return sp - 1;
	}

	@Override
	public int notifyClient() throws org.apache.thrift.TException {
		// Not implemented yet
		return 0;
	}

	@Override
    public int connectToClients() throws org.apache.thrift.TException {
    	try {
	      	for(int i = 0; i < hostNames.length; i++) {
	        	System.out.println("Connecting to " + hostNames[i] + "...");
	        	TTransport transport = new TSocket(hostNames[i], CLIENT_BASE_PORT);
	        	TProtocol protocol = new TBinaryProtocol(transport);
	        	SuccinctService.Client client = new SuccinctService.Client(protocol);
	        	transport.open();

	        	clients.add(client);
	        	clientTransports.add(transport);
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
    public long write(String key, String value, int serverId) throws org.apache.thrift.TException {
    	return localServers.get(serverId).write(key, value);
    }

    @Override
    public int startServers(int numServers, int partScheme) throws org.apache.thrift.TException {
    	this.partitionScheme = partScheme;
    	ExecutorService serverExecutor = Executors.newCachedThreadPool();

    	String basePath = "succinct/data/split_" + hostNames[localHostId] + "_";
    	for(int i = 0; i < numServers; i++) {
    		try {
    			serverExecutor.submit(new QueryServiceHandler(this.tachyonMasterAddress, basePath + i, this.delim, 0, SERVER_BASE_PORT + i));
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
        	TTransport transport = new TSocket("localhost", SERVER_BASE_PORT + i);
        	TProtocol protocol = new TBinaryProtocol(transport);
        	QueryService.Client client = new QueryService.Client(protocol);
        	transport.open();

        	localServers.add(client);
        	localTransports.add(transport);
        	System.out.print("Connected!");
    	}
    	return 0;
    }

    @Override
    public int initialize(int mode) throws org.apache.thrift.TException {
    	System.out.println("Client offsets: ");
    	for(int i = 0; i < clientOffsets.length; i++)
    		System.out.println(i + "=>" + clientOffsets[i]);

    	System.out.println("Local server offsets: ");
    	for(int i = 0; i < localServerOffsets.length; i++)
    		System.out.println(i + "=>" + localServerOffsets[i]);

    	int ret = 0;
    	for(int i = 0; i < localServers.size(); i++) {
    		System.out.println("Initializing local server " + i);
    		ret += localServers.get(i).initialize(mode);
    	}

    	return ret;
    }

    @Override
    public int notifyServerSplitOffset(int serverId, long splitOffset) throws org.apache.thrift.TException {
    	System.out.println("Received server split offset notification: [" + serverId + "," + splitOffset + "]");
    	if(serverId == 0) {
			for(int i = 0; i < clients.size(); i++) {
				System.out.println("Notifying client [" + i + "] " + hostNames[i] + " about offset " + splitOffset);
				if(i == localHostId) {
					this.notifyClientSplitOffset(localHostId, splitOffset);
				} else {
					clients.get(i).notifyClientSplitOffset(localHostId, splitOffset);
				}
				System.out.println("Done!");
			}
		}

		System.out.println("Notifying local server [" + serverId + "] about offset " + splitOffset);
		localServerOffsets[serverId] = splitOffset;
        this.localServerOffsetMap.put(splitOffset, serverId);
		localServers.get(serverId).notifySplitOffset(splitOffset);
		System.out.println("Done!");
    	return 0;
    }

    @Override
    public int notifyClientSplitOffset(int clientId, long splitOffset) throws org.apache.thrift.TException {
    	System.out.println("Received clients split offset notification: [" + clientId + ", " + splitOffset + "]");
        clientOffsetMap.put(splitOffset, clientId);
		clientOffsets[clientId] = splitOffset;
    	return 0;
    }

    @Override
    public int submitFile(String filePath, int partScheme, byte delim1, byte delim2) throws org.apache.thrift.TException {

    	this.partitionScheme = partScheme;
		int numSplits = hostNames.length;			// TODO: change

		// Each client is responsible for (num_splits / hostcount) number of servers
		int numServers = (numSplits / hostNames.length);

		// Upgrade delim2 to String
		String delim1String = "" + (char)delim1;
		String delim2String = "" + (char)delim2;

		// Start servers on all clients
		for(int i = 0; i < hostNames.length; i++) {
			System.out.println("Asking client " + hostNames[i] + " to start " + numServers + " servers...");
			clients.get(i).send_startServers(numServers, this.partitionScheme);
		}

        System.out.println("Waiting for all the clients to finish...");

        // Wait for servers to start on all clients
        for(int i = 0; i < hostNames.length; i++) {
            clients.get(i).recv_startServers();
            System.out.println("Client " + hostNames[i] + " finished!");
        }

		if(partitionScheme == PARTITION_HASH) {
			// Distribute key value pairs based on hash-partitioning
			System.out.println("Hash partitioning data...");
			
			try {
				Scanner inputFile = new Scanner(new File(filePath)).useDelimiter(delim2String);
				String line;
				
				while (inputFile.hasNext()) {
					line = inputFile.next();
					String key, value;

					// Extract key and value
					int kvSplitIndex = line.indexOf((char)delim1);
					key = line.substring(0, kvSplitIndex);
					value = line.substring(kvSplitIndex + 1);
					System.out.println("<" + key + ", " + value + ">");

		 			int selectedHost = key.hashCode() % hostNames.length;
		 			int selectedServer = key.hashCode() % numServers;
		 			System.out.println("Selected host = " + hostNames[selectedHost]);
					clients.get(selectedHost).write(key, value, selectedServer);
				}
				inputFile.close();
			} catch (IOException e) {
				System.out.println("Error: " + e.toString());
			}
		} else if(partitionScheme == PARTITION_RETAIN) {
			// In this mode, we ignore delim1, and only use delim2 to get line splits
			System.out.println("Retention partitioning data...");
			
			try {
				File f = new File(filePath);
				long splitSize = (long)(f.length() / numSplits) + 1;
				Scanner inputFile = new Scanner(f).useDelimiter(delim2String);
				String line;
				long lineNo = 0;
				int selectedHost = 0, selectedServer = 0;
				long offset = 0, written = 0;

				System.out.println("Writing to partition " + selectedServer + " on [" + selectedHost + "] " + hostNames[selectedHost]);
				System.out.println("Offset is at " + offset);

				System.out.println("Notifying client [" + selectedHost + "] " + hostNames[selectedHost] + " about offset " + offset);
				clients.get(selectedHost).notifyServerSplitOffset(selectedServer, offset);

				while (inputFile.hasNext()) {
					line = inputFile.next();
					String key, value;
					
					// Extract key and value
					key = String.valueOf(lineNo);
					value = line;
					System.out.println("<" + key + ", " + value + ">");
					
					if((written = clients.get(selectedHost).write(key, value, selectedServer)) >= splitSize) {
						System.out.println("Partition " + selectedServer + " on " + hostNames[selectedHost] + " is full.");
						selectedServer++;
						if(selectedServer == numServers) {
							selectedHost++;
							selectedServer = 0;
						}

						System.out.println("Writing to partition " + selectedServer + " on [" + selectedHost + "]" + hostNames[selectedHost]);
						offset += written;
						System.out.println("Updated offset to " + offset);
						System.out.println("Notifying client [" + selectedHost + "] " + hostNames[selectedHost] + " about offset " + offset);
						clients.get(selectedHost).notifyServerSplitOffset(selectedServer, offset);
					}
					lineNo++;
				}
				inputFile.close();
			} catch (IOException e) {
				System.out.println("Error: " + e.toString());
			}
		} else {
			System.out.println("Partition scheme not supported: " + partScheme);
			return 1;
		}

		// Start initialization at all clients
		for(int i = 0; i < hostNames.length; i++) {
			System.out.println("Starting initialization at client " + i);
			clients.get(i).send_initialize(0);
		}

        System.out.println("Waiting for clients to finish...");
        
        // Wait for initialization to finish
        for(int i = 0; i < hostNames.length; i++) {
            clients.get(i).recv_initialize();
            System.out.println("Client " + hostNames[i] + " finished!");
        }
		return 0;
    }

    @Override
    public List<Long> locate(String query) throws org.apache.thrift.TException {
    	List<Long> locations = new ArrayList<>();
    	for(int i = 0; i < clients.size(); i++) {
    		locations.addAll(clients.get(i).locateLocal(query));
    	}
    	return locations;
    }

    @Override
    public List<Long> locateLocal(String query) throws org.apache.thrift.TException {
    	List<Long> locations = new ArrayList<>();
    	for(int i = 0; i < localServers.size(); i++) {
    		locations.addAll(localServers.get(i).locate(query));
    	}
    	return locations;
    }

    @Override
    public long count(String query) throws org.apache.thrift.TException {
    	int ret = 0;
    	for(int i = 0; i < clients.size(); i++) {
    		ret += clients.get(i).countLocal(query);
    	}
    	return ret;
    }

    @Override
    public long countLocal(String query) throws org.apache.thrift.TException {
    	int ret = 0;
    	for(int i = 0; i < localServers.size(); i++) {
    		ret += localServers.get(i).count(query);
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
    public String extractLocal(long loc, long bytes) throws org.apache.thrift.TException {
    	System.out.println("Received extractLocal query for <" + loc + "," + bytes + ">");
    	// int serverId = findSplit(localServerOffsets, loc);
        Integer lookup = this.localServerOffsetMap.get(loc);
        int serverId = (lookup == null) ? (Integer)(this.localServerOffsetMap.lowerEntry(loc).getValue()) : lookup;
    	System.out.println("Offset " + loc + " found at serverId = " + serverId);
    	return localServers.get(serverId).extract(loc, bytes);
    }

    @Override
    public long getKeyToValuePointer(String key) throws org.apache.thrift.TException {
    	System.out.println("Received getKeyToValuePointer query for [" + key + "]");
    	return clients.get(key.hashCode() % clients.size()).getKeyToValuePointerLocal(key);
    }

    @Override
    public long getKeyToValuePointerLocal(String key) throws org.apache.thrift.TException {
    	System.out.println("Received getKeyToValuePointerLocal query for [" + key + "]");
    	return localServers.get(key.hashCode() % localServers.size()).getKeyToValuePointer(key);
    }

    @Override
    public String getValue(String key) throws org.apache.thrift.TException {
    	System.out.println("Received getValue query for [" + key + "]");
    	return clients.get(key.hashCode() % clients.size()).getValueLocal(key);	
    }

    @Override
    public String getValueLocal(String key) throws org.apache.thrift.TException {
    	System.out.println("Received getValueLocal query for [" + key + "]");
    	return localServers.get(key.hashCode() % localServers.size()).getValue(key);
    }

    @Override
    public Set<String> getKeys(String substring) throws org.apache.thrift.TException {
    	System.out.println("Received getKeys query for [" + substring + "]");
    	Set<String> keys = new TreeSet<>();
    	for(int i = 0; i < clients.size(); i++) {
    		keys.addAll(clients.get(i).getKeysLocal(substring));
    	}
    	return keys;
    }

    @Override
    public Set<String> getKeysLocal(String substring) throws org.apache.thrift.TException {
    	System.out.println("Received getKeysLocal query for [" + substring + "]");
    	Set<String> keys = new TreeSet<>();
    	for(int i = 0; i < localServers.size(); i++) {
    		keys.addAll(localServers.get(i).getKeys(substring));
    	}
    	return keys;
    }

    @Override
    public Map<String,String> getRecords(String substring) throws org.apache.thrift.TException {
    	System.out.println("Received getRecords query for [" + substring + "]");
    	Map<String, String> records = new TreeMap<>();
    	for(int i = 0; i < clients.size(); i++) {
    		records.putAll(clients.get(i).getRecordsLocal(substring));
    	}
    	return records;
    }

    @Override
    public Map<String,String> getRecordsLocal(String substring) throws org.apache.thrift.TException {
    	System.out.println("Received getRecordsLocal query for [" + substring + "]");
    	Map<String, String> records = new TreeMap<>();
    	for(int i = 0; i < localServers.size(); i++) {
    		records.putAll(localServers.get(i).getRecords(substring));
    	}
    	return records;
    }

    @Override
    public int deleteRecord(String key) throws org.apache.thrift.TException {
    	System.out.println("Received deleteRecord request for [" + key + "]");
    	return clients.get(key.hashCode() % clients.size()).deleteRecordLocal(key);
    }

    @Override
    public int deleteRecordLocal(String key) throws org.apache.thrift.TException {
    	System.out.println("Received deleteRecordLocal request for [" + key + "]");
    	return localServers.get(key.hashCode() % localServers.size()).deleteRecord(key);
    }

    @Override
    public Map<Integer,Map<Integer,Range>> getRanges(String query) throws org.apache.thrift.TException {
        Map<Integer, Map<Integer, Range>> rangeMap = new HashMap<>();
        for(int i = 0; i < clients.size(); i++) {
            rangeMap.put(i, clients.get(i).getRangesLocal(query));
        }
        return rangeMap;
    }

    @Override
    public Map<Integer,Range> getRangesLocal(String query) throws org.apache.thrift.TException {
        Map<Integer, Range> rangeMap = new HashMap<>();
        for(int i = 0; i < localServers.size(); i++) {
            rangeMap.put(i, localServers.get(i).getRange(query));
        }
        return rangeMap;
    }

    @Override
    public long getLocation(int clientId, int serverId, long index) throws org.apache.thrift.TException {
        return clients.get(clientId).getLocationLocal(serverId, index);
    }

    @Override
    public long getLocationLocal(int serverId, long index) throws org.apache.thrift.TException {
        return localServers.get(serverId).getLocation(index);
    }


}