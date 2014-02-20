package succinct;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

import succinct.thrift.MasterServiceHandler;
import succinct.thrift.MasterService;

import java.io.IOException;

public class SuccinctMaster {

  public static final int MASTER_BASE_PORT = 10000;

  public static void main(String[] args) throws IOException {
    String[] hostNames = new String[args.length - 1];
    String tachyonMasterAddress = args[0];
    System.arraycopy(args, 1, hostNames, 0, args.length - 1);
    MasterServiceHandler masterService = new MasterServiceHandler(hostNames);
    try {
      MasterService.Processor<MasterServiceHandler> processor = new MasterService.Processor<MasterServiceHandler>(masterService);
      TServerTransport serverTransport = new TServerSocket(MASTER_BASE_PORT);
      TServer server = new TThreadPoolServer(new
                  TThreadPoolServer.Args(serverTransport).processor(processor));
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

}