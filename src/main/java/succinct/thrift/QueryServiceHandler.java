package succinct.thrift;

import java.util.Set;
import java.util.TreeSet;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Arrays;

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

import tachyon.client.TachyonFS;
import tachyon.command.TFsShell;
import tachyon.client.TachyonByteBuffer;

public class QueryServiceHandler implements QueryService.Iface {

	// Macros
	private static final long two32 = (1L << 32);

	// Local data structures
	TachyonFS tachyonClient;

    // Data -- Global buffer
    private ByteBuffer succinctData;

    // Thread local Buffers
    public static class ByteBufferLocal extends ThreadLocal<ByteBuffer> {
        private ByteBuffer _src;

        public ByteBufferLocal(ByteBuffer src) {
            _src = src;
        }

        @Override
        protected synchronized ByteBuffer initialValue() {
            return _src.duplicate().order(ByteOrder.nativeOrder());
        }
    }

    public static class LongBufferLocal extends ThreadLocal<LongBuffer> {
        private LongBuffer _src;

        public LongBufferLocal(LongBuffer src) {
            _src = src;
        }

        @Override
        protected synchronized LongBuffer initialValue() {
            return _src.duplicate();
        }
    }

    public static class IntBufferLocal extends ThreadLocal<IntBuffer> {
        private IntBuffer _src;

        public IntBufferLocal(IntBuffer src) {
            _src = src;
        }

        @Override
        protected synchronized IntBuffer initialValue() {
            return _src.duplicate();
        }
    }

    // Views
	private ByteBufferLocal cmap;
	private LongBufferLocal context;
	private ByteBufferLocal slist;
	private ByteBufferLocal dbpos;			
	private LongBufferLocal sa;
	private LongBufferLocal sainv;
	private LongBufferLocal neccol;
	private LongBufferLocal necrow;
	private LongBufferLocal rowoffsets;
	private LongBufferLocal coloffsets;
	private LongBufferLocal celloffsets;
	private IntBufferLocal rowsizes;
	private IntBufferLocal colsizes;
	private IntBufferLocal roff;
	private IntBufferLocal coff;
	private ByteBufferLocal[] wavelettree;

	// Metadata
	private long saN;
	private long csaN;
	private int alphaSize;
	private int sigmaSize;
	private int bits;
	private int csaBits;
	private int l;
	private int twoL;
	private int contextSize;

    private int contextLen;

	// Table data structures
	private int[][] decodeTable;
    private ArrayList<HashMap<Integer, Integer>> encodeTable;
    private short[] C16 = new short[17];
    private char[] offbits = new char[17];
    private char[][] smallrank = new char[65536][16];

    // Book keeping data structures
    private int option;
    private long splitOffset;
    private String dataPath;
    private byte delim;
    private HashMap<String, Long> recordOffsetMap;
    private TreeMap<Long, String> recordMap;
    private String tachyonMasterAddress;

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
        long s = (long)(i) * csaBits;
        long e = s + (csaBits - 1);

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

    private long getValPos(LongBuffer bitmap, int pos, int bits) {
        assert (pos >= 0);

        int basePos = bitmap.position();
        long val;
        long s = (long)pos;
        long e = s + (bits - 1);

        if ((s / 64) == (e / 64)) {
            val = bitmap.get(basePos + (int)(s / 64L)) << (s % 64);
            val = val >>> (63 - e % 64 + s % 64);
        } else {
            val = bitmap.get(basePos + (int)(s / 64L)) << (s % 64);
            val = val >>> (s % 64 - (e % 64 + 1));
            val = val | (bitmap.get(basePos + (int)(e / 64L)) >>> (63 - e % 64));
        }
        assert(val >= 0);
        return val;
    }

    private long getBit(long bitmap[], int i) {
        return ((bitmap[i / 64] >>> (63L - i)) & 1L);
    }

    private long getBit(LongBuffer bitmap, int i) {
        return ((bitmap.get(bitmap.position() + (i / 64)) >>> (63L - i)) & 1L);
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
        long rank_l12, pos_l12;

        int l3_size = (int)((size / two32) + 1);
        int l12_size = (int)((size / 2048) + 1);

        int basePos = D.position();
        
        while (sp <= ep) {
            m = (sp + ep) / 2;
            r = (m * two32 - D.get(basePos + m));
            if (val > r) {
                sp = m + 1;
            } else {
                ep = m - 1;
            }
        }

        ep = Math.max(ep, 0);
        val -= (ep * two32 - D.get(basePos + ep));
        pos += D.get(basePos + l3_size + ep);
        sp = (int) (ep * two32 / 2048);
        ep = (int) (Math.min(((ep + 1) * two32 / 2048), Math.ceil((double) size / 2048.0)) - 1);

        D.position(basePos + 2 * l3_size);
        basePos = D.position();

        while (sp <= ep) {
            m = (sp + ep) / 2;
            r = m * 2048 - GETRANKL2(D.get(basePos + m));
            if (val > r) {
                sp = m + 1;
            } else {
                ep = m - 1;
            }
        }

        ep = Math.max(ep, 0);
        sel = (long)(ep) * 2048L;
        rank_l12 = D.get(basePos + ep);
        pos_l12 = D.get(basePos + l12_size + ep);
        val -= (ep * 2048 - GETRANKL2(rank_l12));
        pos += GETPOSL2(pos_l12);

        assert (val <= 2048);
        r = (512 - GETRANKL1(rank_l12, 1));
        if (sel + 512 < size && val > r) {
            pos += GETPOSL1(pos_l12, 1);
            val -= r;
            sel += 512;
            r = (512 - GETRANKL1(rank_l12, 2));
            if (sel + 512 < size && val > r) {
                pos += GETPOSL1(pos_l12, 2);
                val -= r;
                sel += 512;
                r = (512 - GETRANKL1(rank_l12, 3));
                if (sel + 512 < size && val > r) {
                    pos += GETPOSL1(pos_l12, 3);
                    val -= r;
                    sel += 512;
                }
            }
        }

        D.position(basePos + 2 * l12_size);

        assert (val <= 512);
        long bitmap_size = (D.get() / 64) + 1;
        
        long countt = 0;
        while (true) {
            block_class = (int) getValPos(D, pos, 4);
            short tempint = (short) offbits[block_class];
            pos += 4;
            block_offset = (int) ((block_class == 0) ? getBit(D, pos) * 16 : 0);
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

        block_class = (int) getValPos(D, pos, 4);
        pos += 4;
        block_offset = (int) getValPos(D, pos, offbits[block_class]);
        lastblock = decodeTable[block_class][block_offset];

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
        long rank_l12, pos_l12;

        int l3_size = (int)((size / two32) + 1);
        int l12_size = (int)((size / 2048) + 1);
        int basePos = D.position();
        
        while (sp <= ep) {
            m = (sp + ep) / 2;
            r = D.get(basePos + m);
            if (val > r) {
                sp = m + 1;
            } else {
                ep = m - 1;
            }
        }

        ep = Math.max(ep, 0);
        val -= D.get(basePos + ep);
        pos += D.get(basePos + l3_size + ep);
        sp = (int) (ep * two32 / 2048);
        ep = (int) (Math.min(((ep + 1) * two32 / 2048), Math.ceil((double) size / 2048.0)) - 1);

        D.position(basePos + 2 * l3_size);
        basePos = D.position();

        while (sp <= ep) {
            m = (sp + ep) / 2;
            r = GETRANKL2(D.get(basePos + m));
            if (val > r) {
                sp = m + 1;
            } else {
                ep = m - 1;
            }
        }

        ep = Math.max(ep, 0);
        sel = (long)(ep) * 2048L;
        rank_l12 = D.get(basePos + ep);
        pos_l12 = D.get(basePos + l12_size + ep);
        val -= GETRANKL2(rank_l12);
        pos += GETPOSL2(pos_l12);

        assert (val <= 2048);
        r = GETRANKL1(rank_l12, 1);
        if (sel + 512 < size && val > r) {
            pos += GETPOSL1(pos_l12, 1);
            val -= r;
            sel += 512;
            r = GETRANKL1(rank_l12, 2);
            if (sel + 512 < size && val > r) {
                pos += GETPOSL1(pos_l12, 2);
                val -= r;
                sel += 512;
                r = GETRANKL1(rank_l12, 3);
                if (sel + 512 < size && val > r) {
                    pos += GETPOSL1(pos_l12, 3);
                    val -= r;
                    sel += 512;
                }
            }
        }

        D.position(basePos + 2 * l12_size);

        assert (val <= 512);
        long bitmap_size = (D.get() / 64) + 1;

        long countt = 0;
        while (true) {
            block_class = (int) getValPos(D, pos, 4);
            short tempint = (short) offbits[block_class];
            pos += 4;
            block_offset = (int) ((block_class == 0) ? getBit(D, pos) * 16 : 0);
            pos += tempint;

            if (val <= (block_class + block_offset)) {
                pos -= (4 + tempint);
                break;
            }

            val -= (block_class + block_offset);
            sel += 16;
            countt++;
        }

        assert (countt <= 32);

        block_class = (int) getValPos(D, pos, 4);
        pos += 4;
        block_offset = (int) getValPos(D, pos, offbits[block_class]);
        lastblock = decodeTable[block_class][block_offset];

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

        int l3_size = (int)(size / two32) + 1;
        int l12_size = (int)(size / 2048) + 1;
        
        int basePos = D.position();

        long rank_l3 = D.get(basePos + l3_idx);
        long pos_l3 = D.get(basePos + l3_size + l3_idx);
        long rank_l12 = D.get(basePos + l3_size + l3_size + l2_idx);
        long pos_l12 = D.get(basePos + l3_size + l3_size + l12_size + l2_idx);
        D.position(basePos + l3_size + l3_size + l12_size + l12_size);

        long res = rank_l3 + GETRANKL2(rank_l12);
        long pos = pos_l3 + GETPOSL2(pos_l12);

        switch (rem) {
            case 1:
                res += GETRANKL1(rank_l12, 1);
                pos += GETPOSL1(pos_l12, 1);
                break;

            case 2:
                res += GETRANKL1(rank_l12, 1) + GETRANKL1(rank_l12, 2);
                pos += GETPOSL1(pos_l12, 1) + GETPOSL1(pos_l12, 2);
                break;

            case 3:
                res += GETRANKL1(rank_l12, 1) + GETRANKL1(rank_l12, 2) + GETRANKL1(rank_l12, 3);
                pos += GETPOSL1(pos_l12, 1) + GETPOSL1(pos_l12, 2) + GETPOSL1(pos_l12, 3);
                break;

            default:
                break;
        }

        // TODO: remove this and read directly from buffer
        long bitmap_size = (D.get() / 64) + 1;

        // Popcount
        while (l1_idx >= 16) {
            block_class = (int) getValPos(D, (int)pos, 4);
            pos += 4;
            block_offset = (int) ((block_class == 0) ? getBit(D, (int)pos) * 16 : 0);
            pos += offbits[block_class];
            res += block_class + block_offset;
            l1_idx -= 16;
        }

        block_class = (int) getValPos(D, (int)pos, 4);
        pos += 4;
        block_offset = (int) getValPos(D, (int)pos, offbits[block_class]);   
        res += smallrank[decodeTable[block_class][block_offset]][l1_idx];

        return res;
    }

    private long getRank0(ByteBuffer B, int startPos, int i) {
        return i - getRank1(B, startPos, i) + 1;
    }

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
    
    public long accessPsi(long i) {
        
        int c, r, r1, p, c_size, c_pos, startPos;
        long c_num, r_off;

        LongBuffer _neccol = neccol.get();
        LongBuffer _necrow = necrow.get();
        LongBuffer _coloffsets = coloffsets.get();
        LongBuffer _rowoffsets = rowoffsets.get();
        LongBuffer _celloffsets = celloffsets.get();
        IntBuffer _coff = coff.get();
        IntBuffer _roff = roff.get();
        IntBuffer _colsizes = colsizes.get();
        IntBuffer _rowsizes = rowsizes.get();

        // Search columnoffset
        c = getRank1(_coloffsets, 0, sigmaSize, i) - 1;

        // Get columnoffset
        c_num = _coloffsets.get(c);

        // Search celloffsets
        r1 = getRank1(_celloffsets, _coff.get(c), _colsizes.get(c), i - c_num) - 1;

        // Get position within sublist
        p = (int)(i - c_num - _celloffsets.get(_coff.get(c) + r1));

        // Search rowoffsets 
        r = (int)_neccol.get(_coff.get(c) + r1);
        
        // Get rowoffset
        r_off = _rowoffsets.get(r);

        // Get context size
        c_size = _rowsizes.get(r);

        // Get context pos
        c_pos = getRank1(_necrow, _roff.get(r), _rowsizes.get(r), c) - 1;

        long sl_val = (wavelettree[r] == null) ? p : getValueWtree((ByteBuffer)wavelettree[r].get().position(0), c_pos, p, 0, c_size - 1);
        long psi_val = r_off + sl_val;

        return psi_val;
    }

    private long lookupSA(long i) {

        long v = 0, r, a;
        ByteBuffer _dbpos = dbpos.get();
        while (getRank1(_dbpos, 0, (int)i) - getRank1(_dbpos, 0, (int)(i - 1)) == 0) {
            i = accessPsi(i);
            v++;
        }        
        r = modulo(getRank1(_dbpos, 0, (int)i) - 1, csaN);
        a = getVal(sa.get(), (int)r);

        return modulo((twoL * a) - v, saN);
    }

    private long lookupSAinv(long i) {

        long acc, pos;
        long v = i % twoL;
        acc = getVal(sainv.get(), (int) (i / twoL));
        pos = getSelect1(dbpos.get(), 0, (int) acc);
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
        ByteBuffer _slist = slist.get();
        LongBuffer _coloffsets = coloffsets.get();

        for (k = 0;  k < j - i + 1; k++) {
            txt[k] = (char)_slist.get(getRank1(_coloffsets, 0, sigmaSize, s) - 1);
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
        ByteBuffer _slist = slist.get();
        LongBuffer _coloffsets = coloffsets.get();

        while((c = (char)_slist.get(getRank1(_coloffsets, 0, sigmaSize, s) - 1)) != this.delim) {
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

        ByteBuffer _slist = slist.get();

        if (C.containsKey(p[m - 1])) {
            sp = C.get(p[m - 1]).first;
            ep = C.get((char)(_slist.get(C.get(p[m - 1]).second + 1))).first - 1;
        } else return range;

        if(sp > ep) return range;

        for (int i = m - 2; i >= 0; i--) {
            if (C.containsKey(p[i])) {
                c1 = C.get(p[i]).first;
                c2 = C.get((char)(_slist.get(C.get(p[i]).second + 1))).first - 1;
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
        if(m == 0) return new Pair<Long, Long>(0L, -1L);
        if (m <= contextLen) {
            return getRangeBckSlow(p);
        }
        Pair<Long, Long> range = new Pair<>(0L, -1L);
        int sigma_id;
        long sp, ep, c1, c2;
        int start_off;
        long context_val, context_id;

        LongBuffer _neccol = neccol.get();
        LongBuffer _celloffsets = celloffsets.get();
        LongBuffer _coloffsets = coloffsets.get();
        IntBuffer _coff = coff.get();
        IntBuffer _colsizes = colsizes.get();

        if (C.containsKey(p[m - contextLen - 1])) {
            sigma_id = C.get(p[m - contextLen - 1]).second;
            // System.out.println("sigma_id = " + sigma_id);
            context_val = computeContextVal(p, sigmaSize, m - contextLen, contextLen);
            // System.out.println("context_val = " + context_val);

            if (context_val == -1) {
                return range;
            }
            if (!contexts.containsKey(context_val)) {
                return range;
            }

            context_id = contexts.get(context_val);
            // System.out.println("context_id = " + context_id);
            start_off = getRank1(_neccol, _coff.get(sigma_id), _colsizes.get(sigma_id), context_id) - 1;
            // System.out.println("start_off = " + start_off);
            sp = _coloffsets.get(sigma_id) + _celloffsets.get(_coff.get(sigma_id) + start_off);
            // System.out.println("sp = " + sp);
            if (start_off + 1 < _colsizes.get(sigma_id)) {
                ep = _coloffsets.get(sigma_id) + _celloffsets.get(_coff.get(sigma_id) + start_off + 1) - 1;
            } else if (sigma_id + 1 < sigmaSize) {
                ep = _coloffsets.get(sigma_id + 1) - 1;
            } else {
                ep = saN - 1;
            }
            // System.out.println("ep = " + ep);
        } else {
            return range;
        }

        if (sp > ep) {
            return range;
        }

        for (int i = m - contextLen - 2; i >= 0; i--) {
            if (C.containsKey(p[i])) {
                sigma_id = C.get(p[i]).second;
                // System.out.println("sigma_id = " + sigma_id);
                context_val = computeContextVal(p, sigmaSize, i + 1, contextLen);
                // System.out.println("context_val = " + context_val);

                if (context_val == -1) {
                    return range;
                }
                if (!contexts.containsKey(context_val)) {
                    return range;
                }

                context_id = contexts.get(context_val);
                // System.out.println("context_id = " + context_id);
                start_off = getRank1(_neccol, _coff.get(sigma_id), _colsizes.get(sigma_id), context_id) - 1;
                // System.out.println("start_off = " + start_off);
                c1 = _coloffsets.get(sigma_id) + _celloffsets.get(_coff.get(sigma_id) + start_off);
                // System.out.println("c1 = " + c1);

                if (start_off + 1 < _colsizes.get(sigma_id)) {
                    c2 = _coloffsets.get(sigma_id) + _celloffsets.get(_coff.get(sigma_id) + start_off + 1) - 1;
                } else if (sigma_id + 1 < sigmaSize) {
                    c2 = _coloffsets.get(sigma_id + 1) - 1;
                } else {
                    c2 = saN - 1;
                }
                // System.out.println("c2 = " + c2);
            } else {
                return range;
            }
            sp = binSearchPsi(sp, c1, c2, false);
            // System.out.println("sp = " + sp);
            ep = binSearchPsi(ep, c1, c2, true);
            // System.out.println("ep = " + ep);
            if (sp > ep) {
                return range;
            }
        }
        range.first = sp;
        range.second = ep;

        // System.out.println("sp = " + range.first);
        // System.out.println("ep = " + range.second);

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

    public QueryServiceHandler(String tachyonMasterAddress, String dataPath, byte delim, int option) throws IOException {

        this.tachyonMasterAddress = tachyonMasterAddress;
        this.option = option;
        this.delim = delim;
        this.dataPath = dataPath;
        this.recordOffsetMap = new HashMap<>();
        this.recordMap = new TreeMap<>();
        this.contextLen = 3;
        this.splitOffset = 0;

    }

    private void copyDataStructures() {
        File sourceDir = new File(this.dataPath);
        File[] sourceFiles = sourceDir.listFiles();
        Arrays.sort(sourceFiles);
        File sourceFile = sourceFiles[0];
        String destDir = "tachyon://" + tachyonMasterAddress + ":" + 19998 + "/" + sourceDir.getName();
        String sourceFilePath = sourceFile.getAbsolutePath();
        String destFilePath = destDir + "/" + sourceFile.getName();

        // Create instance of TFsShell
        TFsShell shell = new TFsShell();

        // Clean destination directory
        String[] rmCmd = new String[2];
        rmCmd[0] = "rm";
        rmCmd[1] = destFilePath;
        try {
            if(shell.run(rmCmd) == -1) {
                System.out.println("Delete failed!");
            } else {
                System.out.println("Delete successful!");
            }
        } catch(Exception e) {
            System.out.println("Error: QueryServiceHandler.java:copyDataStructures(): " + e.toString());
            e.printStackTrace();
        }

        // Copy files to destiantion
        String[] copyCmd = new String[3];
        copyCmd[0] = "copyFromLocal";
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

    @Override
    public int readDataStructures() throws org.apache.thrift.TException {
        
        // Set up ByteBuffer views
        ByteBuffer data = succinctData.duplicate().order(ByteOrder.nativeOrder());

        // Read metadata
        saN = data.getLong();
        System.out.println("saN = " + saN);
        csaN = data.getLong();
        System.out.println("csaN = " + csaN);
        alphaSize = data.getInt();
        System.out.println("alphaSize = " + alphaSize);
        sigmaSize = data.getInt();
        System.out.println("sigmaSize = " + sigmaSize);
        bits = data.getInt();
        System.out.println("bits = " + bits);
        csaBits = data.getInt();
        System.out.println("csaBits = " + csaBits);
        l = data.getInt();
        System.out.println("l = " + l);
        twoL = data.getInt();
        System.out.println("twoL = " + twoL);
        contextSize = data.getInt();
        System.out.println("contextSize = " + contextSize);

        // Read byte buffers

        // Read cmap
        int cmapSize = (int) data.getLong();
        System.out.println("cmapSize = " + cmapSize);
        cmap = new ByteBufferLocal(data.slice().order(ByteOrder.nativeOrder()));
        // cmap.limit(cmapSize);
        data.position(data.position() + cmapSize);

        // Read contexts
        int contextsSize = (int) data.getLong();
        System.out.println("contextsSize = " + contextsSize);
        context = new LongBufferLocal(data.slice().order(ByteOrder.nativeOrder()).asLongBuffer());
        // context.limit(contextsSize);
        data.position(data.position() + contextsSize);

        // Read slist
        int slistSize = (int) data.getLong();
        System.out.println("slistSize = " + slistSize);
        slist = new ByteBufferLocal(data.slice().order(ByteOrder.nativeOrder()));
        // slist.limit(slistSize);
        data.position(data.position() + slistSize);

        // Read dbpos
        int dbposSize = (int) data.getLong();
        System.out.println("dbposSize = " + dbposSize);
        dbpos = new ByteBufferLocal(data.slice().order(ByteOrder.nativeOrder()));
        // dbpos.limit(dbposSize);
        data.position(data.position() + dbposSize);

        // Read sa
        int saSize = (int) data.getLong();
        System.out.println("saSize = " + saSize);
        sa = new LongBufferLocal(data.slice().order(ByteOrder.nativeOrder()).asLongBuffer());
        // sa.limit(saSize);
        data.position(data.position() + saSize);

        // Read sainv
        int sainvSize = (int) data.getLong();
        System.out.println("sainvSize = " + sainvSize);
        sainv = new LongBufferLocal(data.slice().order(ByteOrder.nativeOrder()).asLongBuffer());
        // sainv.limit(sainvSize);
        data.position(data.position() + sainvSize);

        // Read neccol
        int neccolSize = (int) data.getLong();
        System.out.println("neccolSize = " + neccolSize);
        neccol = new LongBufferLocal(data.slice().order(ByteOrder.nativeOrder()).asLongBuffer());
        // neccol.limit(neccolSize);
        data.position(data.position() + neccolSize);

        // Read necrow
        int necrowSize = (int) data.getLong();
        System.out.println("necrowSize = " + necrowSize);
        necrow = new LongBufferLocal(data.slice().order(ByteOrder.nativeOrder()).asLongBuffer());
        // necrow.limit(necrowSize);
        data.position(data.position() + necrowSize);

        // Read rowoffsets
        int rowoffsetsSize = (int) data.getLong();
        System.out.println("rowoffsetsSize = " + rowoffsetsSize);
        rowoffsets = new LongBufferLocal(data.slice().order(ByteOrder.nativeOrder()).asLongBuffer());
        // rowoffsets.limit(rowoffsetsSize);
        data.position(data.position() + rowoffsetsSize);

        // Read coloffsets
        int coloffsetsSize = (int) data.getLong();
        System.out.println("coloffsetsSize = " + coloffsetsSize);
        coloffsets = new LongBufferLocal(data.slice().order(ByteOrder.nativeOrder()).asLongBuffer());
        // coloffsets.limit(coloffsetsSize);
        data.position(data.position() + coloffsetsSize);

        // Read celloffsets
        int celloffsetsSize = (int) data.getLong();
        System.out.println("celloffsetsSize = " + celloffsetsSize);
        celloffsets = new LongBufferLocal(data.slice().order(ByteOrder.nativeOrder()).asLongBuffer());
        // celloffsets.limit(coloffsetsSize);
        data.position(data.position() + celloffsetsSize);

        // Read rowsizes
        int rowsizesSize = (int) data.getLong();
        System.out.println("rowsizesSize = " + rowsizesSize);
        rowsizes = new IntBufferLocal(data.slice().order(ByteOrder.nativeOrder()).asIntBuffer());
        // rowsizes.limit(rowsizesSize);
        data.position(data.position() + rowsizesSize);

        int colsizesSize = (int) data.getLong();
        System.out.println("colsizesSize = " + colsizesSize);
        colsizes = new IntBufferLocal(data.slice().order(ByteOrder.nativeOrder()).asIntBuffer());
        // colsizes.limit(colsizesSize);
        data.position(data.position() + colsizesSize);

        int roffSize = (int) data.getLong();
        System.out.println("roffSize = " + roffSize);
        roff = new IntBufferLocal(data.slice().order(ByteOrder.nativeOrder()).asIntBuffer());
        // roff.limit(roffSize);
        data.position(data.position() + roffSize);

        int coffSize = (int) data.getLong();
        System.out.println("coffSize = " + coffSize);
        coff = new IntBufferLocal(data.slice().order(ByteOrder.nativeOrder()).asIntBuffer());
        // coff.limit(roffSize);
        data.position(data.position() + coffSize);

        wavelettree = new ByteBufferLocal[this.contextSize];
        for(int i = 0; i < this.contextSize; i++) {
            int wavelettreeSize = (int) data.getLong();
            if(wavelettreeSize == 0) {
                wavelettree[i] = null;
            } else {
                wavelettree[i] = new ByteBufferLocal(data.slice().order(ByteOrder.nativeOrder()));
            }
            data.position(data.position() + wavelettreeSize);
        }

        System.out.println("Mapped Wavelet trees!");
        
        // TODO: FIX LATER!!!
        C = new HashMap<>();
        ByteBuffer _cmap = cmap.get();
        for(int i = 0; i < alphaSize; i++) {
            char c = (char)_cmap.get();
            long v1 = _cmap.getLong();
            int v2 = _cmap.getInt();
            C.put(c, new Pair<>(v1, v2));
            // System.out.println(c + "=>" + v1 + "," + v2);
        }

        System.out.println("Read cmap!");

        contexts = new HashMap<>();
        LongBuffer _context = context.get();
        System.out.println("contextSize = " + this.contextSize);
        for(int i = 0; i < this.contextSize; i++) {
            contexts.put(_context.get(), _context.get());
        }

        System.out.println("Read contexts!");
        
        // // Test rank/select
        // System.out.println("Rank1");
        // long rank1_max, rank0_max;
        // for(int i = 0; i < saN; i++) {
        //     System.out.println("i = " + i + " rank1 = " + getRank1(dbpos, 0, i) + " rank11 = " + getRank11(dbpos, 0, i));
        // }

        // System.out.println("Rank0");
        // for(int i = 0; i < saN; i++) {
        //     System.out.println("i = " + i + " rank0 = " + getRank0(dbpos, 0, i) + " rank00 = " + getRank00(dbpos, 0, i));
        // }

        // rank1_max = getRank1(dbpos, 0, (int)(saN - 1));
        // rank0_max = getRank0(dbpos, 0, (int)(saN - 1));

        // System.out.println("Select1");
        // for(int i = 0; i < rank1_max; i++) {
        //     System.out.println("i = " + i + " select1 = " + getSelect1(dbpos, 0, i) + " select11 = " + getSelect11(dbpos, 0, i));
        // }

        // System.out.println("Select0");
        // for(int i = 0; i < rank0_max; i++) {
        //     System.out.println("i = " + i + " select0 = " + getSelect0(dbpos, 0, i) + " select00 = " + getSelect00(dbpos, 0, i));
        // }

        System.out.println("Loaded data structures from tachyon.");
        return 0;
    }

    @Override
    public long getSplitOffset() throws org.apache.thrift.TException {
        return this.splitOffset;
    }

    private static void printMap(Map mp) {
        Iterator it = mp.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pairs = (Map.Entry)it.next();
            System.out.println(pairs.getKey() + " = " + pairs.getValue());
        }
    }

    @Override
    public int initialize(int mode) throws org.apache.thrift.TException {

        // Setup tables
        this.decodeTable = new int[17][];
        this.encodeTable = new ArrayList<>();
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
            decodeTable[i] = new int[this.C16[i]];
            HashMap<Integer, Integer> encode_row = new HashMap<>();
            this.encodeTable.add(encode_row);
            q[i] = 0;
        }
        q[16] = 1;
        for (int i = 0; i <= 65535; i++) {
            int p = popcount(i);
            decodeTable[p % 16][q[p]] = i;
            encodeTable.get(p % 16).put(i, q[p]);
            q[p]++;
            for (int j = 0; j < 16; j++) {
                smallrank[i][j] = (char) popcount(i >> (15 - j));
            }
        }

        System.out.println("Copying data structures...");
        copyDataStructures();
        System.out.println("Finished copying data structures...");

        // Setup File Data
        File sourceDir = new File(this.dataPath);
        File[] sourceFiles = sourceDir.listFiles();
        Arrays.sort(sourceFiles);
        File sourceFile = sourceFiles[0];
        String tachyonDir = "/" + sourceDir.getName();
        String tachyonPath = tachyonDir + "/" + sourceFile.getName();

        TachyonByteBuffer tachyonBuf;
        try {
            tachyonClient = TachyonFS.get("tachyon://" + tachyonMasterAddress + ":19998/");
            tachyonBuf = tachyonClient.getFile(tachyonPath).readByteBuffer();
            if(tachyonBuf == null) {
                tachyonClient.getFile(tachyonPath).recache();
                tachyonBuf = tachyonClient.getFile(tachyonPath).readByteBuffer();
            }
        } catch (IOException e) {
            e.printStackTrace();
            return -1;
        }
        
        // Read index file
        succinctData = tachyonBuf.DATA.order(ByteOrder.nativeOrder());

        try {
            readDataStructures();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return 0;
    }

    @Override
 	public List<Long> locate(String query) throws org.apache.thrift.TException {
        List<Long> locs;
        try {
            locs = bckSearch(query.toCharArray());
        } catch (Exception e) {
            e.printStackTrace();
            locs = new ArrayList<Long>();
        }
 		return locs;
 	}

 	@Override
    public long count(String query) throws org.apache.thrift.TException {
        long ret = 0;
        try {
            ret = getCountBck(query.toCharArray());
            // System.out.println("ret = " + ret);
        } catch(Exception e) {
            e.printStackTrace();
        }
    	return ret;
    }

    @Override
    public String extract(long loc, long bytes) throws org.apache.thrift.TException {
        String str = "_";
        try {
            str = new String(extract_text(loc - splitOffset, (loc - splitOffset) + (bytes - 1)));
        } catch(Exception e) {
            e.printStackTrace();
        }

    	return str;
    }

    public String accessRecord(String recordId, long offset, long bytes) throws org.apache.thrift.TException {
        long recordPointer = recordOffsetMap.get(recordId);
        long recordOffset = recordPointer + offset;

        String txt = "";
        long s;

        s = lookupSAinv(recordOffset);
        int k;
        ByteBuffer _slist = slist.get();
        LongBuffer _coloffsets = coloffsets.get();
        for (k = 0;  k < bytes; k++) {
            char c = (char)_slist.get(getRank1(_coloffsets, 0, sigmaSize, s) - 1);
            if(c == (char)delim) {
                break;
            }
            txt += c;
            s = accessPsi(s);
        }
        return txt;
    }

    @Override
    public long getRecordPointer(String recordId) throws org.apache.thrift.TException {
        Long ret = recordOffsetMap.get(recordId);
        return (ret == null || ret < 0) ? -1 : ret;
    }

    @Override
    public String getRecord(String recordId) throws org.apache.thrift.TException {
        long recordPointer = getRecordPointer(recordId);
        if(recordPointer >= 0) {
            return extractUntilDelim(recordPointer);
        }
        return String.valueOf((char)delim);
    }

    @Override
    public Set<String> getRecordIds(String substring) throws org.apache.thrift.TException {
        Pair<Long, Long> range = getRangeBck(substring.toCharArray());
        long sp = range.first, ep = range.second;

        Set<String> recordIds = new TreeSet<>();
        for (long i = 0; i < ep - sp + 1; i++) {
            Map.Entry recordIdMapEntry = recordMap.floorEntry(lookupSA(sp + i));
            if(recordIdMapEntry != null && recordOffsetMap.get((String)recordIdMapEntry.getValue()) >= 0)
                recordIds.add((String)recordIdMapEntry.getValue());
        }
        return recordIds;
    }

    @Override
    public Map<String, String> getRecords(String substring) throws org.apache.thrift.TException {
        Pair<Long, Long> range = getRangeBck(substring.toCharArray());
        long sp = range.first, ep = range.second;
        
        Map<String, String> records = new TreeMap<>();
        for (long i = 0; i < ep - sp + 1; i++) {
            Map.Entry recordIdMapEntry = recordMap.floorEntry(lookupSA(sp + i));
            if(recordIdMapEntry != null && recordOffsetMap.get((String)recordIdMapEntry.getValue()) >= 0)
                records.put((String)recordIdMapEntry.getValue(), extractUntilDelim((Long)recordIdMapEntry.getKey()));
        }
        return records;
    }

    @Override
    public long countRecords(String substring) throws org.apache.thrift.TException {
        Pair<Long, Long> range = getRangeBck(substring.toCharArray());
        long sp = range.first, ep = range.second;
        
        long count = 0;
        for (long i = 0; i < ep - sp + 1; i++) {
            Map.Entry recordIdMapEntry = recordMap.floorEntry(lookupSA(sp + i));
            if(recordIdMapEntry != null && recordOffsetMap.get((String)recordIdMapEntry.getValue()) >= 0)
                count++;
        }
        return count;
    }

    @Override
    public Map<String,Long> freqCountRecords(String substring) throws org.apache.thrift.TException {
        Pair<Long, Long> range = getRangeBck(substring.toCharArray());
        long sp = range.first, ep = range.second;
        
        Map<String, Long> recordCounts = new TreeMap<>();
        for (long i = 0; i < ep - sp + 1; i++) {
            Map.Entry recordIdMapEntry = recordMap.floorEntry(lookupSA(sp + i)); 
            if(recordIdMapEntry != null && recordOffsetMap.get((String)recordIdMapEntry.getValue()) >= 0) {
                if(!recordCounts.containsKey((String)recordIdMapEntry.getValue()))
                    recordCounts.put((String)recordIdMapEntry.getValue(), 1L);
                else
                    recordCounts.put((String)recordIdMapEntry.getValue(), recordCounts.get((String)recordIdMapEntry.getValue()) + 1);
            }
        }
        return recordCounts;
    }

    @Override
    public int deleteRecord(String recordId) throws org.apache.thrift.TException {
        long recordPointer = getRecordPointer(recordId);
        if(recordPointer < 0) return -1;
        recordOffsetMap.put(recordId, recordOffsetMap.get(recordId) | (1L << 63));
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
}