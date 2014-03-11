package succinct.test;

import succinct.thrift.SuccinctServiceHandler;

import java.io.BufferedReader;
import java.io.InputStreamReader;

import java.util.List;

public class SuccinctServiceHandlerTest {

	public static void main(String[] args) {
		try {
			SuccinctServiceHandler handler = new SuccinctServiceHandler(null);
			handler.readQueries(args[0], Integer.parseInt(args[1]));
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}