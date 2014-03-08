package succinct;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

import succinct.thrift.SuccinctServiceHandler;
import succinct.thrift.SuccinctService;
import succinct.thrift.Commons;
import succinct.thrift.HandlerProcessorFactory;

import java.io.IOException;

public class QueryHandler {

  public static void main(String[] args) throws IOException {
    String[] hostNames = args;
    try {
      SuccinctServiceHandler initHandler = new SuccinctServiceHandler(hostNames);
      initHandler.initialize(0);
      HandlerProcessorFactory handlerFactory = new HandlerProcessorFactory(hostNames);
      TServerTransport serverTransport = new TServerSocket(Commons.HANDLER_BASE_PORT);
      TServer server = new TThreadPoolServer(new
                  TThreadPoolServer.Args(serverTransport).processorFactory(handlerFactory));
      System.out.println("Starting client...");
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}