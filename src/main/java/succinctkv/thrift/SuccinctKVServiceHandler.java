package succinctkv.thrift;

import org.apache.thrift.TException;

import java.util.Set;
import java.util.TreeSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Iterator;

import java.io.File;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.IntBuffer;

import tachyon.client.InStream;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.ReadType;
import tachyon.command.TFsShell;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

public class SuccinctKVServiceHandler extends succinct.thrift.SuccinctServiceHandler implements SuccinctKVService.Iface {

	// Book keeping data structures
    private byte delim;
    private HashMap<String, Long> keyToValueOffsetMap;
    private TreeMap<Long, String> valueOffsetToKeyMap;

    public SuccinctKVServiceHandler(String tachyonMasterAddress, String dataPath, byte delim, int option, int localPort) throws IOException {
        super(tachyonMasterAddress, dataPath, option, localPort);
        this.keyToValueOffsetMap = new HashMap<>();
        this.valueOffsetToKeyMap = new TreeMap<>();
        this.delim = delim;
    }

    @Override
    public int initializeKV(int mode) throws org.apache.thrift.TException {
        
        System.out.println("Data path is: " + this.dataPath);
        // Read k2vPointers
        BufferedReader k2vReader = null;
        String firstKey = null;
        try {
            File k2vPath = new File(this.dataPath + "/k2vptr");
            System.out.println("k2vPath: " + k2vPath.getAbsolutePath());
            File[] k2vFiles = k2vPath.listFiles();
            for(File f: k2vFiles) {
                if(f.isFile() && !(f.getName().startsWith("_") || f.getName().startsWith("."))) {
                    System.out.println("Reading <key, valuePointer>s from " + f.getAbsolutePath());
                    k2vReader = new BufferedReader(new FileReader(f.getAbsolutePath()));
                    String line = null;
                    while((line = k2vReader.readLine()) != null) {
                        System.out.println("Line: " + line);
                        String[] k2v = line.split("\t");
                        if(firstKey == null) firstKey = k2v[0];
                        keyToValueOffsetMap.put(k2v[0], Long.parseLong(k2v[1]));
                        valueOffsetToKeyMap.put(Long.parseLong(k2v[1]), k2v[0]);
                    }
                    System.out.println("Finished reading " + keyToValueOffsetMap.size() + " <key, valuePointer>s from " + f.getAbsolutePath());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        System.out.println("Copying data structures...");
        String srcPath = this.dataPath + "/values";
        String dstPath = "tachyon://" + tachyonMasterAddress + ":" + 19998 + "/split_" + firstKey.hashCode();
        System.out.println("Source Path: " + srcPath);
        System.out.println("Destination Path: " + dstPath);
        copyDataStructures(srcPath, dstPath);
        System.out.println("Finished constructing data structures...");

        System.out.println("Reading data structures...");
        try {
            File sourceFile = new File(dstPath);
            String tachyonPath = "/" + sourceFile.getName();
            readDataStructures(tachyonPath);
            System.out.println("Finished reading data structures...");
        } catch (IOException e) {
            System.out.println("Error: SuccinctKVServiceHandler.java:initialize(mode): " + e.toString());
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    @Override
    public long getKeyToValuePointer(String key) throws org.apache.thrift.TException {
        System.out.println("Received getKeyToValuePointer request key = " + key);
        System.out.println("Value = " + keyToValueOffsetMap.get(key));
        Long ret = keyToValueOffsetMap.get(key);
        return (ret == null || ret < 0) ? -1 : ret;
    }

    @Override
    public String getValue(String key) throws org.apache.thrift.TException {
        long valuePointer = getKeyToValuePointer(key);
        if(valuePointer >= 0) {
            return extractUntilDelim(valuePointer, (char)delim);
        }
        return String.valueOf((char)delim);
    }

    @Override
    public Set<String> getKeys(String substring) throws org.apache.thrift.TException {
        Pair<Long, Long> range = getRangeBck(substring.toCharArray());
        long sp = range.first, ep = range.second;
        
        Set<String> keys = new TreeSet<>();
        for (long i = 0; i < ep - sp + 1; i++) {
            Map.Entry votkMapEntry = valueOffsetToKeyMap.floorEntry(lookupSA(sp + i));
            if(votkMapEntry != null && keyToValueOffsetMap.get((String)votkMapEntry.getValue()) >= 0)
                keys.add((String)votkMapEntry.getValue());
        }
        return keys;
    }

    @Override
    public Map<String, String> getRecords(String substring) throws org.apache.thrift.TException {
        Pair<Long, Long> range = getRangeBck(substring.toCharArray());
        long sp = range.first, ep = range.second;
        
        Map<String, String> records = new TreeMap<>();
        for (long i = 0; i < ep - sp + 1; i++) {
            Map.Entry votkMapEntry = valueOffsetToKeyMap.floorEntry(lookupSA(sp + i));
            if(votkMapEntry != null && keyToValueOffsetMap.get((String)votkMapEntry.getValue()) >= 0)
                records.put((String)votkMapEntry.getValue(), extractUntilDelim((Long)votkMapEntry.getKey(), (char)delim));
        }
        return records;
    }

    @Override
    public int deleteRecord(String key) throws org.apache.thrift.TException {
        long valuePointer = getKeyToValuePointer(key);
        if(valuePointer < 0) return -1;
        keyToValueOffsetMap.put(key, keyToValueOffsetMap.get(key) | (1L << 63));
        return 0;
    }

    @Override
    public void run() {
        try {
            SuccinctKVService.Processor<SuccinctKVServiceHandler> processor = new SuccinctKVService.Processor<SuccinctKVServiceHandler>(this);
            TServerTransport serverTransport = new TServerSocket(localPort);
            TServer server = new TThreadPoolServer(new
                        TThreadPoolServer.Args(serverTransport).processor(processor));

            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}