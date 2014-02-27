package succinct.util;

import java.util.Iterator;
import java.util.Map;

import succinct.thrift.SuccinctMasterService;
import succinct.thrift.Range;

public class LocationIterator implements Iterator<Long> {
    
    private SuccinctMasterService.Client rClient;

    private Map<Integer, Map<Integer, Range>> rangeMap;
    private int currentClientId;
    private int currentServerId;
    private long currentIndex;
    private long currentIndexLimit;
    private boolean isFinished;
    
    public LocationIterator(SuccinctMasterService.Client rClient, String query) {
        this.rClient = rClient;
        try {
            this.rangeMap = rClient.getRanges(query);
        } catch(Exception e) {
            System.out.println("Error: " + e.toString());
            e.printStackTrace();
        }
        this.currentClientId = 0;
        this.currentServerId = 0;

        int numNonEmptyRanges = 0;
        for(int i = 0; i < rangeMap.size(); i++) {
            for(int j = 0; j < rangeMap.get(i).size(); j++) {
                Range p = rangeMap.get(i).get(j);
                if(p.getStartIndex() <= p.getEndIndex()) numNonEmptyRanges++;
            }
        }
        this.isFinished = (numNonEmptyRanges == 0);

        if(!isFinished) {
            Range currentRange = this.rangeMap.get(currentClientId).get(currentServerId);
            this.currentIndex = currentRange.getStartIndex();
            this.currentIndexLimit = currentRange.getEndIndex();
        }
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
            ret = rClient.getLocation(currentClientId, currentServerId, currentIndex);
        } catch(Exception e) {
            System.out.println("Error: " + e.toString());
            return null;
        }
        currentIndex++;
        if(currentIndex > currentIndexLimit) {
            currentServerId++;
            if(currentServerId == rangeMap.get(currentClientId).size()) {
                currentServerId = 0;
                currentClientId++;
                if(currentClientId == rangeMap.size())
                    isFinished = true;
            }
            if(!isFinished) {
                Range currentRange = this.rangeMap.get(currentClientId).get(currentServerId);
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