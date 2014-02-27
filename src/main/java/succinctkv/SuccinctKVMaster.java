package succinctkv;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

import succinctkv.thrift.SuccinctKVMasterService;
import succinctkv.thrift.SuccinctKVMasterServiceHandler;
import succinctkv.thrift.Ports;

import java.io.IOException;

public class SuccinctKVMaster {

  public static void main(String[] args) throws IOException {
    String[] hostNames = args;
    System.out.println("Creating Succinct KV Master Service...");
    SuccinctKVMasterServiceHandler succinctKVMasterService = 
        new SuccinctKVMasterServiceHandler(hostNames);
    System.out.println("Created!");
    try {
      SuccinctKVMasterService.Processor<SuccinctKVMasterServiceHandler> processor = 
          new SuccinctKVMasterService.Processor<SuccinctKVMasterServiceHandler>(succinctKVMasterService);
      TServerTransport serverTransport = new TServerSocket(Ports.MASTER_BASE_PORT);
      TServer server = new TThreadPoolServer(new
          TThreadPoolServer.Args(serverTransport).processor(processor));
      System.out.println("Starting Succinct KV Master...");
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}