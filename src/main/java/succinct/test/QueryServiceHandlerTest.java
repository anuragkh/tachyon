package succinct.test;

import succinct.thrift.QueryServiceHandler;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.List;

public class QueryServiceHandlerTest {

	public static void main(String[] args) {
		try {
			String tachyonMasterAddress = args[0];
			QueryServiceHandler handler = new QueryServiceHandler(tachyonMasterAddress, args[1], (byte)2, 1, 10000);
			handler.initialize(0);
			BufferedReader in = new BufferedReader(new InputStreamReader(System.in));
			while(true) {
				System.out.println("Enter query: ");
				String query = in.readLine();
				long c = handler.count(query);
				System.out.println("Count = " + c);
				List<Long> locations = handler.locate(query);
				System.out.println("Locate count = " + locations.size());
			}
		} catch(Exception e) {
			System.out.println("Error: " + e.toString());
			e.printStackTrace();
		}
	}
}