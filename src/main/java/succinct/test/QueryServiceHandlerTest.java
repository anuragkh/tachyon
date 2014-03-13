package succinct.test;

import succinct.thrift.QueryServiceHandler;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.util.Random;
import java.util.List;

import java.util.List;

public class QueryServiceHandlerTest {

	private static final long SPLIT_SIZE = 2684354560L;    // TODO: Remove
    private static final int NUM_SPLITS = 5;               // TODO: Remove

	public static String[] readQueries(String queriesPath, int numQueries) {
        System.out.println("Loading queries " + numQueries + " from path: " + queriesPath);
        File queryFile = new File(queriesPath);
        DataInputStream in = null;
        String queries[] = new String[numQueries];
        try {
            in = new DataInputStream(new FileInputStream(queryFile));
            int len = 8;    // Query Length
            for(int i = 0; i < numQueries; i++) {
                char c;
                while((c = (char)in.read()) != '\t');
                boolean test = (in.read() == '\u0002');
                int l = 0;
                String q = "";
                while(l != len) {
                    c = (char)in.read();
                    System.out.println("[" + c + "]");
                    q += c;
                    l++;
                }
                test = (in.read() == '\u0003');
                System.out.println("[" + q + "] : " + q.length());
                queries[i] = q;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return queries;
    }

    public long[] generateRandoms(int numQueries) {
        long[] randoms = new long[numQueries];
        Random rand = new Random();
        for(int i = 0; i < numQueries; i++) {
            randoms[i] = Math.abs(rand.nextLong() % (SPLIT_SIZE * NUM_SPLITS));
            System.out.println("[" + randoms[i] + "]");
        }

        return randoms;
    }

	public static void main(String[] args) {
		try {
			String tachyonMasterAddress = args[0];
			QueryServiceHandler handler = new QueryServiceHandler(tachyonMasterAddress, args[1], (byte)10, 1);
			handler.initialize(0);
			// int repeat = 10;
	        // int numQueries = 100;
	        // String queriesPath = args[2];
			// String[] queries = readQueries(queriesPath, numQueries);
	        // long startTime;
	        // double totTime;

	        // long location = 1744638495L;
	        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
	        String query = br.readLine();
	        // List<Long> locs = handler.locate(query);
	        String str = handler.extract(Long.parseLong(query), 1024);
	        System.out.println("Count: " + str);


	        // System.out.println("Testing access psi...");
	        // Test all access psi's
	        // long psi = 0;
	     	// try {
		    //     for(long i = 0; i < 2684354561L; i++) {
		    //     	psi = handler.accessPsi(i);
		    //     	if(i % 100000 == 0L)
		    //     		System.out.println(i + " " + psi);
		    //     }
		    // } catch	(Exception e) {
		    // 	e.printStackTrace();
		    // } 

	        // System.out.println("Test passed!");

	        // try {
	        //     BufferedWriter res = new BufferedWriter(new FileWriter(new File("qs_res_count_latency_" + numQueries + "_" + repeat)));
	        //     for(int i = 0; i < numQueries; i++) {
	        //         totTime = 0;
	        //         long c = 0;
	        //         for(int j = 0; j < repeat; j++) {
	        //             startTime = System.nanoTime();
	        //             long count = handler.count(queries[i]);
	        //             totTime += (double)(System.nanoTime() - startTime) / 1000.0;
	        //             c += count;
	        //         }
	        //         totTime /= repeat;
	        //         c /= repeat;
	        //         res.write(c + "\t" + totTime + "\n");
	        //     }
	        //     res.close();
	        // } catch (Exception e) {
	        //     e.printStackTrace();
	        // }

	        // try {
	        //     BufferedWriter res = new BufferedWriter(new FileWriter(new File("qs_res_locate_latency_" + numQueries + "_" + repeat)));
	        //     for(int i = 0; i < numQueries; i++) {
	        //         totTime = 0;
	        //         long c = 0;
	        //         for(int j = 0; j < repeat; j++) {
	        //             startTime = System.nanoTime();
	        //             long count = handler.locate(queries[i]).size();
	        //             totTime += (double)(System.nanoTime() - startTime) / 1000.0;
	        //             c += count;
	        //         }
	        //         totTime /= repeat;
	        //         c /= repeat;
	        //         res.write(c + "\t" + totTime + "\n");
	        //     }
	        //     res.close();
	        // } catch (Exception e) {
	        //     e.printStackTrace();
	        // }


		} catch(Exception e) {
			System.out.println("Error: " + e.toString());
			e.printStackTrace();
		}
	}
}