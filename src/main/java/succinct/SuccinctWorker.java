package succinct;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

import succinct.thrift.SuccinctWorkerService;
import succinct.thrift.SuccinctWorkerServiceHandler;
import succinct.thrift.Ports;

import java.io.IOException;

public class SuccinctWorker {

  public static void main(String[] args) throws IOException {
    String tachyonMasterAddress = args[0];
    System.out.println("Creating Succinct Worker Service...");
    SuccinctWorkerServiceHandler succinctService = 
        new SuccinctWorkerServiceHandler(tachyonMasterAddress);
    System.out.println("Created!");
    try {
      SuccinctWorkerService.Processor<SuccinctWorkerServiceHandler> processor = 
          new SuccinctWorkerService.Processor<SuccinctWorkerServiceHandler>(succinctService);
      TServerTransport serverTransport = new TServerSocket(Ports.WORKER_BASE_PORT);
      TServer server = new TThreadPoolServer(new
          TThreadPoolServer.Args(serverTransport).processor(processor));
      System.out.println("Starting Succinct Worker...");
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}