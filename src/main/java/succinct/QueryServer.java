package succinct;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

import succinct.thrift.QueryServiceHandler;
import succinct.thrift.QueryService;
import succinct.thrift.Commons;

import java.io.IOException;

public class QueryServer {

  public static void main(String[] args) throws IOException {
    String tachyonMasterAddress = args[0];
    String dataPath = args[1];
    QueryServiceHandler queryService = new QueryServiceHandler(tachyonMasterAddress, dataPath, Commons.delim, 0);
    try {
      QueryService.Processor<QueryServiceHandler> processor = new QueryService.Processor<QueryServiceHandler>(queryService);
      TServerTransport serverTransport = new TServerSocket(Commons.SERVER_BASE_PORT);
      TServer server = new TThreadPoolServer(new
                  TThreadPoolServer.Args(serverTransport).processor(processor));
      server.serve();
    } catch (Exception e) {
      e.printStackTrace();
    }
  }
}