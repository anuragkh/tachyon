package succinct;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

import succinct.thrift.SuccinctServiceHandler;
import succinct.thrift.SuccinctService;

import java.io.IOException;

public class SuccinctClient {

  public static final int CLIENT_BASE_PORT = 11000;

  public static void main(String[] args) throws IOException {
    String[] hostNames = new String[args.length - 2];
    int localHostId = Integer.parseInt(args[0]);
    String tachyonMasterAddress = args[1];
    System.arraycopy(args, 2, hostNames, 0, args.length - 2);
    System.out.println("Creating succinct service...");
    SuccinctServiceHandler succinctService = new SuccinctServiceHandler(localHostId, hostNames, tachyonMasterAddress);
    System.out.println("Created!");
    try {
      SuccinctService.Processor<SuccinctServiceHandler> processor = new SuccinctService.Processor<SuccinctServiceHandler>(succinctService);
      TServerTransport serverTransport = new TServerSocket(CLIENT_BASE_PORT);
      TServer server = new TThreadPoolServer(new
                  TThreadPoolServer.Args(serverTransport).processor(processor));
      System.out.println("Starting client...");
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}