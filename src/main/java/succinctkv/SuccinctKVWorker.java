package succinctkv;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

import succinctkv.thrift.SuccinctKVWorkerService;
import succinctkv.thrift.SuccinctKVWorkerServiceHandler;
import succinctkv.thrift.Ports;

import java.io.IOException;

public class SuccinctKVWorker {

  public static void main(String[] args) throws IOException {
    String tachyonMasterAddress = args[0];
    System.out.println("Creating Succinct KV Worker Service...");
    SuccinctKVWorkerServiceHandler succinctKVWorkerService = 
        new SuccinctKVWorkerServiceHandler(tachyonMasterAddress);
    System.out.println("Created!");
    try {
      SuccinctKVWorkerService.Processor<SuccinctKVWorkerServiceHandler> processor = 
          new SuccinctKVWorkerService.Processor<SuccinctKVWorkerServiceHandler>(succinctKVWorkerService);
      TServerTransport serverTransport = new TServerSocket(Ports.WORKER_BASE_PORT);
      TServer server = new TThreadPoolServer(new
          TThreadPoolServer.Args(serverTransport).processor(processor));
      System.out.println("Starting Succinct KV Worker...");
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}