package succinct.util;

import java.util.Iterator;
import java.util.Map;

import succinct.thrift.SuccinctService;
import succinct.thrift.Range;

public class LocationIterator implements Iterator<Long> {
    
    private SuccinctService.Client rClient;

    private Map<Integer, Range> rangeMap;
    private int currentServerId;
    private long currentIndex;
    private long currentIndexLimit;
    private boolean isFinished;
    
    public LocationIterator(SuccinctService.Client rClient, String query) {
        this.rClient = rClient;
        try {
             this.rangeMap = rClient.getRanges(query);
        } catch(Exception e) {
            System.out.println("Error: " + e.toString());
            e.printStackTrace();
        }
        this.currentServerId = 0;
        int numNonEmptyRanges = 0;
        for(int i = 0; i < rangeMap.size(); i++) {
            Range p = rangeMap.get(i);
            if(numNonEmptyRanges == 0) this.currentServerId = i;
            if(p.getStartIndex() <= p.getEndIndex()) numNonEmptyRanges++;
        }

        Range currentRange = this.rangeMap.get(currentServerId);
        this.currentIndex = currentRange.getStartIndex();
        this.currentIndexLimit = currentRange.getEndIndex();

        this.isFinished = (numNonEmptyRanges == 0);
    }

    @Override
    public boolean hasNext() {
        return !(isFinished);
    }

    @Override
    public Long next() {
        if(isFinished) return null;
        long ret;
        try {
            ret = rClient.getLocation(currentServerId, currentIndex);
        } catch(Exception e) {
            System.out.println("Error: " + e.toString());
            return null;
        }
        currentIndex++;
        if(currentIndex > currentIndexLimit) {
            currentServerId++;
            if(currentServerId == rangeMap.size()) {
                isFinished = true;
            }
            if(!isFinished) {
                Range currentRange = this.rangeMap.get(currentServerId);
                currentIndex = currentRange.getStartIndex();
                currentIndexLimit = currentRange.getEndIndex();
            }
        }
        return ret;
    }

    @Override
    public void remove() {
        // Do nothing
    }
}