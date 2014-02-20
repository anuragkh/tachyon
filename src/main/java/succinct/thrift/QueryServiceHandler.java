package succinct.thrift;

import org.apache.thrift.TException;

import java.util.Set;
import java.util.TreeSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Iterator;

import java.io.File;
import java.io.OutputStream;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.IOException;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.LongBuffer;
import java.nio.IntBuffer;

import tachyon.client.InStream;
import tachyon.client.TachyonByteBuffer;
import tachyon.client.TachyonFS;
import tachyon.client.TachyonFile;
import tachyon.client.ReadType;
import tachyon.command.TFsShell;

import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TServer.Args;
import org.apache.thrift.server.TSimpleServer;
import org.apache.thrift.server.TThreadPoolServer;

public class QueryServiceHandler implements QueryService.Iface, Runnable {

	// Macros
	private static final long two32 = (1L << 32);

	// Local data structures
	TachyonFS tachyonClient;

	ByteBuffer cmap;
	LongBuffer context;
	ByteBuffer slist;
	ByteBuffer dbpos;			// TESTED
	LongBuffer sa;				// TESTED
	LongBuffer sainv;			// TESTED
	LongBuffer neccol;			// TESTED
	LongBuffer necrow;			// TESTED
	LongBuffer rowoffsets;		// TESTED
	LongBuffer coloffsets;		// TESTED
	LongBuffer celloffsets;		// TESTED
	IntBuffer rowsizes;			// TESTED
	IntBuffer colsizes;			// TESTED
	IntBuffer roff;				// TESTED
	IntBuffer coff;				// TESTED
	ByteBuffer[] wavelettree;

	// Metadata
	long sa_n;
	long csa_n;
	int alpha_size;
	int sigma_size;
	int bits;
	int csa_bits;
	int l;
	int two_l;
	int context_size;

	// Table data structures
	int[][] decode_table;
    ArrayList<HashMap<Integer, Integer>> encode_table;
    short[] C16 = new short[17];
    char[] offbits = new char[17];
    char[][] smallrank = new char[65536][16];

    // Book keeping data structures
    int localPort;
    int option;
    long splitOffset;
    FileOutputStream dataFile;
    String dataPath;
    byte delim;
    HashMap<String, Long> keyToValueOffsetMap;
    TreeMap<Long, String> valueOffsetToKeyMap;
    boolean isInitialized;
    String tachyonMasterAddress;

    // TODO: FIX LATER!
    public static class Pair<T1, T2> {

        public T1 first;
        public T2 second;

        public Pair(T1 first, T2 second) {
            this.first = first;
            this.second = second;
        }
    }

    HashMap<Character, Pair<Long, Integer>> C;
    HashMap<Long, Long> contexts;

    private static int intLog2(long n) {
        int l = (n != 0) && ((n & (n - 1)) == 0) ? 0 : 1;
        while ((n >>= 1) > 0) ++l;
        return l;
    }
    
    private static long modulo(long a, long n) {
        while (a < 0)
            a += n;
        return a % n;
    }
    
    private static int popcount(long x) {
        return Long.bitCount(x);
    }

	private long GETRANKL2(long n) {
        return (n >>> 32);
    }
    
    private long GETRANKL1(long n, int i) {
        return (((n & 0xffffffff) >>> (32 - i * 10)) & 0x3ff);
    }
    
    private long GETPOSL2(long n) {
        return (n >>> 31);
    }

    private long GETPOSL1(long n, int i) {
        return (((n & 0x7fffffff) >>> (31 - i * 10)) & 0x3ff);
    }

    public long getVal(LongBuffer B, int i) {
        assert (i >= 0);

        long val;
        long s = (long)(i) * csa_bits;
        long e = s + (csa_bits - 1);

        if ((s / 64) == (e / 64)) {
            val = B.get((int)(s / 64L)) << (s % 64);
            val = val >>> (63 - e % 64 + s % 64);
        } else {
            long val1 = B.get((int)(s / 64L)) << (s % 64);
            long val2 = B.get((int)(e / 64L)) >>> (63 - e % 64);
            val1 = val1 >>> (s % 64 - (e % 64 + 1));
            val = val1 | val2;
        }

        return val;
    }

    private long getValPos(long bitmap[], int pos, int bits) {
        assert (pos >= 0);

        long val;
        long s = (long)pos;
        long e = s + (bits - 1);

        if ((s / 64) == (e / 64)) {
            val = bitmap[(int)(s / 64L)] << (s % 64);
            val = val >>> (63 - e % 64 + s % 64);
        } else {
            val = bitmap[(int)(s / 64L)] << (s % 64);
            val = val >>> (s % 64 - (e % 64 + 1));
            val = val | (bitmap[(int)(e / 64L)] >>> (63 - e % 64));
        }
        assert(val >= 0);
        return val;
    }

    private long getBit(long bitmap[], int i) {
        return ((bitmap[i / 64] >>> (63L - i)) & 1L);
    }

	private long getSelect0(ByteBuffer B, int startPos, int i) {
        
        assert(i >= 0);

        B.position(startPos);
        LongBuffer D = B.asLongBuffer();

        long size = D.get();
    
        long val = i + 1;
        int sp = 0;
        int ep = (int) (size / two32);
        int m;
        long r;
        int pos = 0;
        int block_class, block_offset;
        long sel = 0;
        int lastblock;

        // TODO, remove these and read directly from buffer
        long[] rank_l3 = new long[(int)((size / two32) + 1)];
        long[] pos_l3 = new long[(int)((size / two32) + 1)];
        long[] rank_l12 = new long[(int)((size / 2048) + 1)];
        long[] pos_l12 = new long[(int)((size / 2048) + 1)];

        D.get(rank_l3);
        D.get(pos_l3);
        D.get(rank_l12);
        D.get(pos_l12);
        
        while (sp <= ep) {
            m = (sp + ep) / 2;
            r = (m * two32 - rank_l3[m]);
            if (val > r) {
                sp = m + 1;
            } else {
                ep = m - 1;
            }
        }

        ep = Math.max(ep, 0);
        sel += ep * two32;
        val -= (ep * two32 - rank_l3[ep]);
        pos += pos_l3[ep];
        sp = (int) (ep * two32 / 2048);
        ep = (int) (Math.min(((ep + 1) * two32 / 2048), Math.ceil((double) size / 2048.0)) - 1);

        while (sp <= ep) {
            m = (sp + ep) / 2;
            r = m * 2048 - GETRANKL2(rank_l12[m]);
            if (val > r) {
                sp = m + 1;
            } else {
                ep = m - 1;
            }
        }

        ep = Math.max(ep, 0);
        sel += ep * 2048;
        val -= (ep * 2048 - GETRANKL2(rank_l12[ep]));
        pos += GETPOSL2(pos_l12[ep]);

        assert (val <= 2048);
        r = (512 - GETRANKL1(rank_l12[ep], 1));
        if (sel + 512 < size && val > r) {
            pos += GETPOSL1(pos_l12[ep], 1);
            val -= r;
            sel += 512;
            r = (512 - GETRANKL1(rank_l12[ep], 2));
            if (sel + 512 < size && val > r) {
                pos += GETPOSL1(pos_l12[ep], 2);
                val -= r;
                sel += 512;
                r = (512 - GETRANKL1(rank_l12[ep], 3));
                if (sel + 512 < size && val > r) {
                    pos += GETPOSL1(pos_l12[ep], 3);
                    val -= r;
                    sel += 512;
                }
            }
        }

        assert (val <= 512);
        long bitmap_size = (D.get() / 64) + 1;
        long[] bitmap = new long[(int)bitmap_size];
        D.get(bitmap);

        long countt = 0;
        while (true) {
            block_class = (int) getValPos(bitmap, pos, 4);
            short tempint = (short) offbits[block_class];
            pos += 4;
            block_offset = (int) ((block_class == 0) ? getBit(bitmap, pos) * 16 : 0);
            pos += tempint;

            if (val <= (16 - (block_class + block_offset))) {
                pos -= (4 + tempint);
                break;
            }

            val -= (16 - (block_class + block_offset));
            sel += 16;
            countt++;
        }

        assert (countt <= 32);

        block_class = (int) getValPos(bitmap, pos, 4);
        pos += 4;
        block_offset = (int) getValPos(bitmap, pos, offbits[block_class]);
        lastblock = decode_table[block_class][block_offset];

        long count = 0;
        for (i = 0; i < 16; i++) {
            if (((lastblock >> (15 - i)) & 1) == 0) {
                count++;
            }
            if (count == val) {
                return sel + i;
            }
        }

        return sel;
    }
    
    private long getSelect1(ByteBuffer B, int startPos, int i) {
        assert(i >= 0);

        B.position(startPos);
        LongBuffer D = B.asLongBuffer();

        long size = D.get();
    
        long val = i + 1;
        int sp = 0;
        int ep = (int) (size / two32);
        int m;
        long r;
        int pos = 0;
        int block_class, block_offset;
        long sel = 0;
        int lastblock;

        // TODO, remove these and read directly from buffer
        long[] rank_l3 = new long[(int)((size / two32) + 1)];
        long[] pos_l3 = new long[(int)((size / two32) + 1)];
        long[] rank_l12 = new long[(int)((size / 2048) + 1)];
        long[] pos_l12 = new long[(int)((size / 2048) + 1)];

        D.get(rank_l3);
        D.get(pos_l3);
        D.get(rank_l12);
        D.get(pos_l12);
        
        while (sp <= ep) {
            m = (sp + ep) / 2;
            r = (rank_l3[m]);
            if (val > r) {
                sp = m + 1;
            } else {
                ep = m - 1;
            }
        }

        ep = Math.max(ep, 0);
        sel += ep * two32;
        val -= (rank_l3[ep]);
        pos += pos_l3[ep];
        sp = (int) (ep * two32 / 2048);
        ep = (int) (Math.min(((ep + 1) * two32 / 2048), Math.ceil((double) size / 2048.0)) - 1);

        while (sp <= ep) {
            m = (sp + ep) / 2;
            r = GETRANKL2(rank_l12[m]);
            if (val > r) {
                sp = m + 1;
            } else {
                ep = m - 1;
            }
        }

        ep = Math.max(ep, 0);
        sel += ep * 2048;
        val -= (GETRANKL2(rank_l12[ep]));
        pos += GETPOSL2(pos_l12[ep]);

        assert (val <= 2048);
        r = (GETRANKL1(rank_l12[ep], 1));
        if (sel + 512 < size && val > r) {
            pos += GETPOSL1(pos_l12[ep], 1);
            val -= r;
            sel += 512;
            r = (GETRANKL1(rank_l12[ep], 2));
            if (sel + 512 < size && val > r) {
                pos += GETPOSL1(pos_l12[ep], 2);
                val -= r;
                sel += 512;
                r = (GETRANKL1(rank_l12[ep], 3));
                if (sel + 512 < size && val > r) {
                    pos += GETPOSL1(pos_l12[ep], 3);
                    val -= r;
                    sel += 512;
                }
            }
        }

        assert (val <= 512);

        long bitmap_size = (D.get() / 64) + 1;
        long[] bitmap = new long[(int)bitmap_size];
        D.get(bitmap);

        long countt = 0;
        while (true) {
            block_class = (int) getValPos(bitmap, pos, 4);
            short tempint = (short) offbits[block_class];
            pos += 4;
            block_offset = (int) ((block_class == 0) ? getBit(bitmap, pos) * 16 : 0);
            pos += tempint;

            if (val <= ((block_class + block_offset))) {
                pos -= (4 + tempint);
                break;
            }

            val -= ((block_class + block_offset));
            sel += 16;
            countt++;
        }

        assert (countt <= 32);

        block_class = (int) getValPos(bitmap, pos, 4);
        pos += 4;
        block_offset = (int) getValPos(bitmap, pos, offbits[block_class]);
        lastblock = decode_table[block_class][block_offset];

        long count = 0;
        for (i = 0; i < 16; i++) {
            if (((lastblock >>> (15 - i)) & 1) == 1) {
                count++;
            }
            if (count == val) {
                return sel + i;
            }
        }

        return sel;
    }

    // TESTED
    private long getRank1(ByteBuffer B, int startPos, int query) {
        if(query < 0) return 0;
    
        int l3_idx = (int) (query / two32);
        int l2_idx = query / 2048;
        int l1_idx = (query % 512);
        int rem = ((query % 2048) / 512);
        int block_class, block_offset;

        B.position(startPos);
        LongBuffer D = B.asLongBuffer();
        long size = D.get();

        // TODO: Remove these, and read directly from buffer
        long[] rank_l3 = new long[(int)(size / two32) + 1];
        long[] rank_l12 = new long[(int)(size / 2048) + 1];
        long[] pos_l3 = new long[(int)(size / two32) + 1];
        long[] pos_l12 = new long[(int)(size / 2048) + 1];

        D.get(rank_l3);
        D.get(pos_l3);
        D.get(rank_l12);
        D.get(pos_l12);

        long res = rank_l3[l3_idx] + GETRANKL2(rank_l12[l2_idx]);
        long pos = pos_l3[l3_idx] + GETPOSL2(pos_l12[l2_idx]);

        switch (rem) {
            case 1:
                res += GETRANKL1(rank_l12[l2_idx], 1);
                pos += GETPOSL1(pos_l12[l2_idx], 1);
                break;

            case 2:
                res += GETRANKL1(rank_l12[l2_idx], 1) + GETRANKL1(rank_l12[l2_idx], 2);
                pos += GETPOSL1(pos_l12[l2_idx], 1) + GETPOSL1(pos_l12[l2_idx], 2);
                break;

            case 3:
                res += GETRANKL1(rank_l12[l2_idx], 1) + GETRANKL1(rank_l12[l2_idx], 2) + GETRANKL1(rank_l12[l2_idx], 3);
                pos += GETPOSL1(pos_l12[l2_idx], 1) + GETPOSL1(pos_l12[l2_idx], 2) + GETPOSL1(pos_l12[l2_idx], 3);
                break;

            default:
                break;
        }

        // TODO: remove this and read directly from buffer
        long bitmap_size = (D.get() / 64) + 1;

        long[] bitmap = new long[(int)bitmap_size];
        D.get(bitmap);

        // Popcount
        while (l1_idx >= 16) {
            block_class = (int) getValPos(bitmap, (int)pos, 4);
            pos += 4;
            block_offset = (int) ((block_class == 0) ? getBit(bitmap, (int)pos) * 16 : 0);
            pos += offbits[block_class];
            res += block_class + block_offset;
            l1_idx -= 16;
        }

        block_class = (int) getValPos(bitmap, (int)pos, 4);
        pos += 4;
        block_offset = (int) getValPos(bitmap, (int)pos, offbits[block_class]);   
        res += smallrank[decode_table[block_class][block_offset]][l1_idx];

        return res;
    }

    // TESTED
    private long getRank0(ByteBuffer B, int startPos, int i) {
        return i - getRank1(B, startPos, i) + 1;
    }

    // TESTED
    private static int getRank1(LongBuffer B, int startPos, int size, long i) {
        int sp = 0, ep = size - 1;
        int m;
        
        while (sp <= ep) {
            m = (sp + ep) / 2;
            if (B.get(startPos + m) == i) return m + 1;
            else if(i < B.get(startPos + m)) ep = m - 1;
            else sp = m + 1;
        }

        return ep + 1;
    }

    // TESTED
    private long getValueWtree(ByteBuffer wtree, int contextPos, int cellPos, int s, int e) {

        char m = (char)wtree.get();
        int left = (int)wtree.getLong();
        int right = (int)wtree.getLong();
        int dictPos = wtree.position();
        long p, v;

        if (contextPos > m && contextPos <= e) {
            if(right == 0) return getSelect1(wtree, dictPos, cellPos);
            p = getValueWtree((ByteBuffer)wtree.position(right), contextPos, cellPos, m + 1, e);
            v = getSelect1(wtree, dictPos, (int)p);
        } else {
            if(left == 0) return getSelect0(wtree, dictPos, cellPos);	
            p = getValueWtree((ByteBuffer)wtree.position(left), contextPos, cellPos, s, m);
            v = getSelect0(wtree, dictPos, (int)p);
        }
        
        return v;
    }
    
    private long accessPsi(long i) {
        
        int c, r, r1, p, c_size, c_pos, startPos;
        long c_num, r_off;

        // Search columnoffset
        c = getRank1(coloffsets, 0, sigma_size, i) - 1;

        // Get columnoffset
        c_num = coloffsets.get(c);

        // Search celloffsets
        r1 = getRank1(celloffsets, coff.get(c), colsizes.get(c), i - c_num) - 1;

        // Get position within sublist
        p = (int)(i - c_num - celloffsets.get(coff.get(c) + r1));

        // Search rowoffsets 
        r = (int)neccol.get(coff.get(c) + r1);
        
        // Get rowoffset
        r_off = rowoffsets.get(r);

        // Get context size
        c_size = rowsizes.get(r);

        // Get context pos
        c_pos = getRank1(necrow, roff.get(r), rowsizes.get(r), c) - 1;

        long sl_val = (wavelettree[r] == null) ? p : getValueWtree((ByteBuffer)wavelettree[r].position(0), c_pos, p, 0, c_size - 1);
        long psi_val = r_off + sl_val;

        return psi_val;
    }

    private long lookupSA(long i) {

        long v = 0, r, a;
        while (getRank1(dbpos, 0, (int)i) - getRank1(dbpos, 0, (int)(i - 1)) == 0) {
            i = accessPsi(i);
            v++;
        }
        
        r = modulo(getRank1(dbpos, 0, (int)i) - 1, sa_n);
        a = getVal(sa, (int)r);

        return modulo((two_l * a) - v, sa_n);
    }

    private long lookupSAinv(long i) {

        long acc, pos;
        long v = i % two_l;
        acc = getVal(sainv, (int)(i / two_l));
        pos = getSelect1(dbpos, 0, (int)acc);
        while (v != 0) {
            pos = accessPsi(pos);
            v--;
        }

        return pos;
    }

    private long computeContextVal(char[] p, int sigma_size, int i, int k) {
        long val = 0;
        long max = i + k;
        for (int t = i; t < max; t++) {
            // System.out.println("val = " + val + " char = " + p[t]);
            if(C.containsKey(p[t])) {
                val = val * sigma_size + C.get(p[t]).second;
            } else {
                return -1;
            }
        }

        return val;
    }

    /* Extract portion of text between indices (i, j) */
    private char[] extract_text(long i, long j) {

        char[] txt = new char[(int)(j - i + 2)];
        long s;

        s = lookupSAinv(i);
        int k;
        for (k = 0;  k < j - i + 1; k++) {
            txt[k] = (char)slist.get(getRank1(coloffsets, 0, sigma_size, s) - 1);
            s = accessPsi(s);
        }
        
        txt[k] = '\0';
        
        return txt;
    }

    private String extractUntilDelim(long i) {
        String txt = "";
        StringBuilder extractedText = new StringBuilder();
        long s = lookupSAinv(i);
        char c;

        while((c = (char)slist.get(getRank1(coloffsets, 0, sigma_size, s) - 1)) != this.delim) {
            extractedText.append(c);
            s = accessPsi(s);
        }

        return extractedText.toString();
    }

    /* Binary search */
    private long binSearchPsi(long val, long s, long e, boolean flag) {

        long sp = s;
        long ep = e;
        long m;
 
        while (sp <= ep) {
            m = (sp + ep) / 2;

            long psi_val;
            psi_val = accessPsi(m);

            if (psi_val == val) return m;
            else if(val < psi_val) ep = m - 1;
            else sp = m + 1;
        }

        return flag ? ep : sp;
    }

    /* Get range of SA positions using Slow Backward search */
    private Pair<Long, Long> getRangeBckSlow(char[] p) {
        Pair<Long, Long> range = new Pair<>(0L, -1L);
        int m = p.length;
        long sp, ep, c1, c2;

        if (C.containsKey(p[m - 1])) {
            sp = C.get(p[m - 1]).first;
            ep = C.get((char)(slist.get(C.get(p[m - 1]).second + 1))).first - 1;
        } else return range;

        if(sp > ep) return range;

        for (int i = m - 2; i >= 0; i--) {
            if (C.containsKey(p[i])) {
                c1 = C.get(p[i]).first;
                c2 = C.get((char)(slist.get(C.get(p[i]).second + 1))).first - 1;
            } else return range;
            sp = binSearchPsi(sp, c1, c2, false);
            ep = binSearchPsi(ep, c1, c2, true);
            if (sp > ep) return range;
        }

        range.first = sp;
        range.second = ep;

        return range;
    }

    /* Get range of SA positions using Backward search */
    private Pair<Long, Long> getRangeBck(char[] p) {

        int m = p.length;
        if (m <= 2) {
            return getRangeBckSlow(p);
        }
        Pair<Long, Long> range = new Pair<>(0L, -1L);
        int sigma_id;
        long sp, ep, c1, c2;
        int start_off;
        long context_val, context_id;

        if(C.containsKey(p[m - 3])) {
            sigma_id = C.get(p[m - 3]).second;
            context_val = computeContextVal(p, sigma_size, m - 2, 2);
            
            if(context_val == -1) return range;
            if(!contexts.containsKey(context_val)) return range;
            
            context_id = contexts.get(context_val);
            start_off = getRank1(neccol, coff.get(sigma_id), colsizes.get(sigma_id), context_id) - 1;
            sp = coloffsets.get(sigma_id) + celloffsets.get(coff.get(sigma_id) + start_off);
            if(start_off + 1 < colsizes.get(sigma_id)) {
            	ep = coloffsets.get(sigma_id) + celloffsets.get(coff.get(sigma_id) + start_off + 1) - 1;
        	} else if(sigma_id + 1 < sigma_size) {
            	ep = coloffsets.get(sigma_id + 1) - 1;
        	} else {
                ep = sa_n - 1;
            }
        } else return range;

        if(sp > ep) return range;

        for (int i = m - 4; i >= 0; i--) {
            if (C.containsKey(p[i])) {
                sigma_id = C.get(p[i]).second;
                context_val = computeContextVal(p, sigma_size, i + 1, 2);
                
                if(context_val == -1) return range;
                if(!contexts.containsKey(context_val)) return range;

                context_id = contexts.get(context_val);
                start_off = getRank1(neccol, coff.get(sigma_id), colsizes.get(sigma_id), context_id) - 1;
                c1 = coloffsets.get(sigma_id) + celloffsets.get(coff.get(sigma_id) + start_off);

                if(start_off + 1 < colsizes.get(sigma_id)) {
	                c2 = coloffsets.get(sigma_id) + celloffsets.get(coff.get(sigma_id) + start_off + 1) - 1;
	            } else if(sigma_id + 1 < sigma_size) {
	                c2 = coloffsets.get(sigma_id + 1) - 1;
	            } else {
                    c2 = sa_n - 1;
                }
            } else return range;
            sp = binSearchPsi(sp, c1, c2, false);
            ep = binSearchPsi(ep, c1, c2, true);
            if (sp > ep) return range;
        }
        range.first = sp;
        range.second = ep;

        return range;
    }

    /* Get count of pattern occurrances */
    private long getCountBck(char[] p) {
        Pair<Long, Long> range;
        range = getRangeBck(p);
        return range.second - range.first + 1;
    }

    /* Function for backward search */
    private List<Long> bckSearch(char[] p) {

        Pair<Long, Long> range;
        range = getRangeBck(p);
        
        long sp = range.first, ep = range.second;
        if (ep - sp + 1 <= 0) {
            return new ArrayList<>();
        }
        // long[] positions = new long[ep - sp + 1];
        List<Long> positions = new ArrayList<>();
        for (long i = 0; i < ep - sp + 1; i++) {
            positions.add(lookupSA(sp + i));
        }
        
        return positions;
    }

    public QueryServiceHandler(String tachyonMasterAddress, String dataPath, byte delim, int option, int localPort) throws IOException {

        this.tachyonMasterAddress = tachyonMasterAddress;
        this.localPort = localPort;
        this.option = option;
        this.delim = delim;
        this.dataPath = dataPath;
        this.keyToValueOffsetMap = new HashMap<>();
        this.valueOffsetToKeyMap = new TreeMap<>();
        this.splitOffset = 0;
        if(option == 0) {
            try {
                this.dataFile = new FileOutputStream(new File(this.dataPath));
            } catch(IOException e) {
                System.err.println("Error: QueryServiceHandler.java:QueryServiceHandler(): " + e.toString());
            }
        } else {
            this.dataFile = null;
        }

        // Test rank/select
        // System.out.println("Rank1");
        // long rank1_max, rank0_max;
        // for(int i = 0; i < sa_n; i++) {
        //  System.out.println("i = " + i + " rank1 = " + getRank1(dbpos, 0, i));
        // }

        // rank1_max = getRank1(dbpos, 0, (int)(sa_n - 1));
        // rank0_max = getRank0(dbpos, 0, (int)(sa_n - 1));

        // System.out.println("Select1");
        // for(int i = 0; i < rank1_max; i++) {
        //  System.out.println("i = " + i + " select1 = " + getSelect1(dbpos, 0, i));
        // }

        // System.out.println("Select0");
        // for(int i = 0; i < rank0_max; i++) {
        //  System.out.println("i = " + i + " select0 = " + getSelect0(dbpos, 0, i));
        // }

        // Test accessPsi
        // for(long i = 0; i < sa_n; i++) {
        //  System.out.println("i = " + i + " psi = " + accessPsi(i));
        // }

        // Test SA and SAinv lookups
        // System.out.println("sa");
        // for(int i = 0; i < sa_n; i++) {
        //  System.out.println("i = " + i + " sa = " + lookupSA(i));
        // }

        // System.out.println("sainv");
        // for(int i = 0; i < sa_n; i++) {
        //  System.out.println("i = " + i + " sainv = " + lookupSAinv(i));
        // }

        // Test extract
        // System.out.println(extract_text(0, sa_n - 1));

        // Test count
        // System.out.println("count = " + getCountBck("int".toCharArray()));

    }

    private void constructDataStructures() {
        String execCommand = "succinct/bin/csa " + this.dataPath;
        try {
            Process constrProc = Runtime.getRuntime().exec(execCommand);
            BufferedReader read = new BufferedReader(new InputStreamReader(constrProc.getInputStream()));
            while(read.ready()) {
                System.out.println(read.readLine());
            }
        } catch (IOException e) {
            System.out.println("Error: QueryServiceHandler.java:constructDataStructures(): " + e.toString());
            e.printStackTrace();
        }
    }

    private void copyDataStructures() {
        File sourceDir = new File(this.dataPath + "_index");
        File sourceFile = new File(this.dataPath);
        String destDir = "tachyon://" + tachyonMasterAddress + ":" + 19998 + "/" + sourceFile.getName();
        System.out.println("Source directory: [" + sourceDir.getName() + "]");
        System.out.println("Destination directory: [" + destDir);

        // Create instance of TFsShell
        TFsShell shell = new TFsShell();

        // Clean destination directory
        String[] rmCmd = new String[2];
        rmCmd[0] = "rm";
        rmCmd[1] = destDir;
        try {
            if(shell.run(rmCmd) == -1) {
                System.out.println("Delete successful!");
            } else {
                System.out.println("Delete failed!");
            }
        } catch(Exception e) {
            System.out.println("Error: QueryServiceHandler.java:copyDataStructures(): " + e.toString());
            e.printStackTrace();
        }

        // Copy files to destiantion
        File[] sourceFiles = sourceDir.listFiles();
        String[] copyCmd = new String[3];
        copyCmd[0] = "copyFromLocal";
        for(int i = 0; i < sourceFiles.length; i++) {
            String sourceFilePath = sourceFiles[i].getAbsolutePath();
            String destFilePath = destDir + "/" + sourceFiles[i].getName();
            System.out.println("Copying file " + sourceFilePath + " to " + destFilePath + "...");
            copyCmd[1] = sourceFilePath;
            copyCmd[2] = destFilePath;
            try {
                if(shell.run(copyCmd) == -1) {
                    System.out.println("Copy failed!");
                } else {
                    System.out.println("Copy successful!");
                }
            } catch(Exception e) {
                System.out.println("Error: QueryServiceHandler.java:copyDataStructures(): " + e.toString());
                e.printStackTrace();
            }
        }
    }

    private void readDataStructures(String path) throws IOException {

        // Setup tables
        this.decode_table = new int[17][];
        this.encode_table = new ArrayList<>();
        int[] q = new int[17];
        
        this.C16[0] = 2;
        this.offbits[0] = 1;
        this.C16[1] = 16;
        this.offbits[1] = 4;
        this.C16[2] = 120;
        this.offbits[2] = 7;
        this.C16[3] = 560;
        this.offbits[3] = 10;
        this.C16[4] = 1820;
        this.offbits[4] = 11;
        this.C16[5] = 4368;
        this.offbits[5] = 13;
        this.C16[6] = 8008;
        this.offbits[6] = 13;
        this.C16[7] = 11440;
        this.offbits[7] = 14;
        this.C16[8] = 12870;
        this.offbits[8] = 14;

        for (int i = 0; i <= 16; i++) {
            if (i > 8) {
                this.C16[i] = this.C16[16 - i];
                this.offbits[i] = this.offbits[16 - i];
            }
            decode_table[i] = new int[this.C16[i]];
            HashMap<Integer, Integer> encode_row = new HashMap<>();
            this.encode_table.add(encode_row);
            q[i] = 0;
        }
        q[16] = 1;
        for (int i = 0; i <= 65535; i++) {
            int p = popcount(i);
            decode_table[p % 16][q[p]] = i;
            encode_table.get(p % 16).put(i, q[p]);
            q[p]++;
            for (int j = 0; j < 16; j++) {
                smallrank[i][j] = (char) popcount(i >> (15 - j));
            }
        }

        // Setup Tachyon buffers
        tachyonClient = TachyonFS.get("tachyon://" + tachyonMasterAddress + ":19998/");

        tachyonClient.getFile(path + "/metadata").recache();
        tachyonClient.getFile(path + "/cmap").recache();
        tachyonClient.getFile(path + "/contxt").recache();
        tachyonClient.getFile(path + "/slist").recache();
        tachyonClient.getFile(path + "/dbpos").recache();
        tachyonClient.getFile(path + "/sa").recache();
        tachyonClient.getFile(path + "/sainv").recache();
        tachyonClient.getFile(path + "/neccol").recache();
        tachyonClient.getFile(path + "/necrow").recache();
        tachyonClient.getFile(path + "/rowoffsets").recache();
        tachyonClient.getFile(path + "/coloffsets").recache();
        tachyonClient.getFile(path + "/celloffsets").recache();
        tachyonClient.getFile(path + "/rowsizes").recache();
        tachyonClient.getFile(path + "/colsizes").recache();
        tachyonClient.getFile(path + "/roff").recache();
        tachyonClient.getFile(path + "/coff").recache();

        // Read metadata
        ByteBuffer metadata = tachyonClient.getFile(path + "/metadata").readByteBuffer().DATA;
        metadata.order(ByteOrder.nativeOrder());
        sa_n = metadata.getLong();
        System.out.println("sa_n = " + sa_n);
        csa_n = metadata.getLong();
        System.out.println("csa_n = " + csa_n);
        alpha_size = metadata.getInt();
        System.out.println("alpha_size = " + alpha_size);
        sigma_size = metadata.getInt();
        System.out.println("sigma_size = " + sigma_size);
        bits = metadata.getInt();
        System.out.println("bits = " + bits);
        csa_bits = metadata.getInt();
        System.out.println("csa_bits = " + csa_bits);
        l = metadata.getInt();
        System.out.println("l = " + l);
        two_l = metadata.getInt();
        System.out.println("two_l = " + two_l);
        context_size = metadata.getInt();
        System.out.println("context_size = " + context_size);

        wavelettree = new ByteBuffer[context_size];
        for(int i = 0; i < context_size; i++) {
            try {
                tachyonClient.getFile(path + "/wavelettree_" + i).recache();
                wavelettree[i] = tachyonClient.getFile(path + "/wavelettree_" + i).readByteBuffer().DATA.order(ByteOrder.nativeOrder());
            } catch(Exception e) { // TODO: Do something better than this!
                wavelettree[i] = null;
            }
        }

        // Read byte buffers
        cmap = tachyonClient.getFile(path + "/cmap").readByteBuffer().DATA.order(ByteOrder.nativeOrder());
        context = tachyonClient.getFile(path + "/contxt").readByteBuffer().DATA.order(ByteOrder.nativeOrder()).asLongBuffer();
        slist = tachyonClient.getFile(path + "/slist").readByteBuffer().DATA.order(ByteOrder.nativeOrder());
        dbpos = tachyonClient.getFile(path + "/dbpos").readByteBuffer().DATA.order(ByteOrder.nativeOrder());
        sa = tachyonClient.getFile(path + "/sa").readByteBuffer().DATA.order(ByteOrder.nativeOrder()).asLongBuffer();
        sainv = tachyonClient.getFile(path + "/sainv").readByteBuffer().DATA.order(ByteOrder.nativeOrder()).asLongBuffer();
        neccol = tachyonClient.getFile(path + "/neccol").readByteBuffer().DATA.order(ByteOrder.nativeOrder()).asLongBuffer();
        necrow = tachyonClient.getFile(path + "/necrow").readByteBuffer().DATA.order(ByteOrder.nativeOrder()).asLongBuffer();
        rowoffsets = tachyonClient.getFile(path + "/rowoffsets").readByteBuffer().DATA.order(ByteOrder.nativeOrder()).asLongBuffer();
        coloffsets = tachyonClient.getFile(path + "/coloffsets").readByteBuffer().DATA.order(ByteOrder.nativeOrder()).asLongBuffer();
        celloffsets = tachyonClient.getFile(path + "/celloffsets").readByteBuffer().DATA.order(ByteOrder.nativeOrder()).asLongBuffer();
        rowsizes = tachyonClient.getFile(path + "/rowsizes").readByteBuffer().DATA.order(ByteOrder.nativeOrder()).asIntBuffer();
        colsizes = tachyonClient.getFile(path + "/colsizes").readByteBuffer().DATA.order(ByteOrder.nativeOrder()).asIntBuffer();
        roff = tachyonClient.getFile(path + "/roff").readByteBuffer().DATA.order(ByteOrder.nativeOrder()).asIntBuffer();
        coff = tachyonClient.getFile(path + "/coff").readByteBuffer().DATA.order(ByteOrder.nativeOrder()).asIntBuffer();
        
        // TODO: FIX LATER!!!
        C = new HashMap<>();
        contexts = new HashMap<>();
        for(int i = 0; i < alpha_size; i++) {
            char c = (char)cmap.get();
            long v1 = cmap.getLong();
            int v2 = cmap.getInt();
            C.put(c, new Pair<>(v1, v2));
            System.out.println(c + "=>" + v1 + "," + v2);
        }

        for(int i = 0; i < context_size; i++) {
            contexts.put(context.get(), context.get());
        }
    }

    @Override
    public long write(String key, String value) throws org.apache.thrift.TException {
        try {
            keyToValueOffsetMap.put(key, dataFile.getChannel().position());
            valueOffsetToKeyMap.put(dataFile.getChannel().position(), key);
            dataFile.write(value.getBytes(), 0, value.length());
            dataFile.write(delim);
            return dataFile.getChannel().position();
        } catch (IOException e) {
            System.out.println("Error: QueryServiceHandler.java:write(key, value): " + e.toString());
        }
        return -1;
        
    }

    @Override
    public int notifySplitOffset(long splitOffset) throws org.apache.thrift.TException {
        System.out.println("Received Split Offset Notification: " + splitOffset);
        this.splitOffset = splitOffset;
        return 0;
    }

    public static void printMap(Map mp) {
        Iterator it = mp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
            System.out.println(pairs.getKey() + " = " + pairs.getValue());
        }
    }

    @Override
    public int initialize(int mode) throws org.apache.thrift.TException {
        
        if(dataFile != null) {
            try {   
                dataFile.close();
            } catch (IOException e) {
                System.err.println("Error: QueryServiceHandler.java:initialize(mode): " + e.toString());
                e.printStackTrace();
                return -1;
            }
        }
        // System.out.println("Printing keyToValueOffsetMap: ");
        // printMap(keyToValueOffsetMap);

        // System.out.println("Printing valueOffsetToKeyMap: ");
        // printMap(valueOffsetToKeyMap);

        System.out.println("Constructing data structures...");
        constructDataStructures();
        System.out.println("Finished constructing data structures...");

        System.out.println("Copying data structures...");
        copyDataStructures();
        System.out.println("Finished constructing data structures...");

        System.out.println("Reading data structures...");
        try {
            File sourceFile = new File(this.dataPath);
            String tachyonPath = "/" + sourceFile.getName();
            readDataStructures(tachyonPath);
            System.out.println("Finished reading data structures...");
        } catch (IOException e) {
            System.out.println("Error: QueryServiceHandler.java:initialize(mode): " + e.toString());
            e.printStackTrace();
            return -1;
        }
        return 0;
    }

    @Override
 	public List<Long> locate(String query) throws org.apache.thrift.TException {
 		System.out.println("Received locate query [" + query + "]");
 		return bckSearch(query.toCharArray());
 	}

 	@Override
    public long count(String query) throws org.apache.thrift.TException {
    	System.out.println("Received count query [" + query + "]");
    	return getCountBck(query.toCharArray());
    }

    @Override
    public String extract(long loc, long bytes) throws org.apache.thrift.TException {
    	System.out.println("Received extract query [" + loc + ", " + bytes + "]");
    	return new String(extract_text(loc - splitOffset, (loc - splitOffset) + (bytes - 1)));
    }

    @Override
    public long getKeyToValuePointer(String key) throws org.apache.thrift.TException {
        System.out.println("Received getKeyToValuePointer request key = " + key);
        System.out.println("Value = " + keyToValueOffsetMap.get(key));
        Long ret = keyToValueOffsetMap.get(key);
        return (ret == null || ret < 0) ? -1 : ret;
    }

    @Override
    public String getValue(String key) throws org.apache.thrift.TException {
        long valuePointer = getKeyToValuePointer(key);
        if(valuePointer >= 0) {
            return extractUntilDelim(valuePointer);
        }
        return String.valueOf((char)delim);
    }

    @Override
    public Set<String> getKeys(String substring) throws org.apache.thrift.TException {
        Pair<Long, Long> range = getRangeBck(substring.toCharArray());
        long sp = range.first, ep = range.second;
        
        Set<String> keys = new TreeSet<>();
        for (long i = 0; i < ep - sp + 1; i++) {
            Map.Entry votkMapEntry = valueOffsetToKeyMap.floorEntry(lookupSA(sp + i));
            if(votkMapEntry != null && keyToValueOffsetMap.get((String)votkMapEntry.getValue()) >= 0)
                keys.add((String)votkMapEntry.getValue());
        }
        return keys;
    }

    @Override
    public Map<String, String> getRecords(String substring) throws org.apache.thrift.TException {
        Pair<Long, Long> range = getRangeBck(substring.toCharArray());
        long sp = range.first, ep = range.second;
        
        Map<String, String> records = new TreeMap<>();
        for (long i = 0; i < ep - sp + 1; i++) {
            Map.Entry votkMapEntry = valueOffsetToKeyMap.floorEntry(lookupSA(sp + i));
            if(votkMapEntry != null && keyToValueOffsetMap.get((String)votkMapEntry.getValue()) >= 0)
                records.put((String)votkMapEntry.getValue(), extractUntilDelim((Long)votkMapEntry.getKey()));
        }
        return records;
    }

    @Override
    public int deleteRecord(String key) throws org.apache.thrift.TException {
        long valuePointer = getKeyToValuePointer(key);
        if(valuePointer < 0) return -1;
        keyToValueOffsetMap.put(key, keyToValueOffsetMap.get(key) | (1L << 63));
        return 0;
    }

    @Override
    public Range getRange(String query) throws org.apache.thrift.TException {
        Pair<Long, Long> range = getRangeBck(query.toCharArray());
        return new Range(range.first, range.second);
    }

    @Override
    public long getLocation(long index) throws org.apache.thrift.TException {
        return lookupSA(index);
    }

    @Override
    public void run() {
        try {
            QueryService.Processor<QueryServiceHandler> processor = new QueryService.Processor<QueryServiceHandler>(this);
            TServerTransport serverTransport = new TServerSocket(localPort);
            TServer server = new TThreadPoolServer(new
                        TThreadPoolServer.Args(serverTransport).processor(processor));

            server.serve();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}