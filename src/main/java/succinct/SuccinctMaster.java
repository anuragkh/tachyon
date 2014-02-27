package succinct;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

import succinct.thrift.SuccinctMasterService;
import succinct.thrift.SuccinctMasterServiceHandler;
import succinct.thrift.Ports;

import java.io.IOException;

public class SuccinctMaster {

  public static void main(String[] args) throws IOException {
    String[] hostNames = args;
    System.out.println("Creating Succinct Master Service...");
    SuccinctMasterServiceHandler masterService = 
        new SuccinctMasterServiceHandler(hostNames);
    System.out.println("Created!");
    try {
      SuccinctMasterService.Processor<SuccinctMasterServiceHandler> processor = 
          new SuccinctMasterService.Processor<SuccinctMasterServiceHandler>(masterService);
      TServerTransport serverTransport = new TServerSocket(Ports.MASTER_BASE_PORT);
      TServer server = new TThreadPoolServer(new
          TThreadPoolServer.Args(serverTransport).processor(processor));
      System.out.println("Starting Succinct Master...");
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}