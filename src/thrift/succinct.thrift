namespace java succinct.thrift

struct Range {
    1: required i64 startIndex;
    2: required i64 endIndex;
}

service SuccinctService {
    i32 connectToQueryServers(),								         // Sets up connections to Query Servers
    i32 disconnectFromQueryServers(),                                    // Disconnects from Query Servers
    i32 initialize(1:i32 mode),								             // Partially implemented
    list<i64> locate(1:string query),							         // Get the list of locations
    i64 count(1:string query),								             // Get the count of the substring
    string extract(1:i64 loc, 2:i64 bytes),						         // Extract bytes starting at location
    string accessRecord(1:string recordId, 2:i64 offset, 3:i64 bytes),   // Extract bytes from specified record
    i64 getRecordPointer(1:string recordId),						     // Get the pointer to the record corresponding to recordId
    string getRecord(1:string recordId),                                 // Get the record corresponding to given recordId
    set<string> getRecordIds(1:string substring),                        // Get all recordIds corresponding to given substring
    map<string, string> getRecords(1:string substring),                  // Get all records corresponding to given substring
    i64 countRecords(1:string substring),                                // Get count of all the records in which substring occurs
    map<string, i64> freqCountRecords(1:string substring),               // Get frequency of occurrences of the substring in each record
    i32 deleteRecord(1:string recordId),                                 // Deletes record correspoding to recordId
    map<i32, Range> getRanges(1:string query),				             // Get all SA ranges corresponding to a given query string
    i64 getLocation(1:i32 serverId, 2:i64 index),		                 // Get SA value at specified serverId, index
    
    /* Test Functions */
    i64 testCountLatency(1:string queriesPath, 2:i32 numQueries, 3:i32 repeat),
    i64 testLocateLatency(1:string queriesPath, 2:i32 numQueries, 3:i32 repeat),
    i64 testExtractLatency(1:i32 numQueries, 2:i32 repeat),
    i64 testCountThroughput(1:string queriesPath, 2:i32 numQueries, 3:i32 numThreads),
    i64 testLocateThroughput(1:string queriesPath, 2:i32 numQueries, 3:i32 numThreads),
    i64 testExtractThroughput(1:i32 numQueries, 2:i32 numThreads),

}

service QueryService {
    i32 initialize(1:i32 mode),								             // Initializes QueryServer
    i64 getSplitOffset(),                                                // Get the offset for this split
    list<i64> locate(1:string query),							         // Get the list of locations
    i64 count(1:string query),								             // Get the count of the substring
    string extract(1:i64 loc, 2:i64 bytes),						         // Extract the bytes starting at location
    string accessRecord(1:string recordId, 2:i64 offset, 3:i64 bytes),   // Extract bytes from specified record
    i64 getRecordPointer(1:string recordId),                             // Get the pointer to the record corresponding to recordId
    string getRecord(1:string recordId),                                 // Get the record corresponding to given recordId
    set<string> getRecordIds(1:string substring),                        // Get all recordIds corresponding to given substring
    map<string, string> getRecords(1:string substring),                  // Get all records corresponding to given substring
    i64 countRecords(1:string substring),                                // Get count of all the records in which substring occurs
    map<string, i64> freqCountRecords(1:string substring),               // Get frequency of occurrences of the substring in each record
    i32 deleteRecord(1:string recordId),                                 // Deletes record correspoding to recordId
    Range getRange(1:string query),							             // Get SA range corresponding to given query string
    i64 getLocation(1:i64 index),							             // Get SA value at index
}

service MasterService {
	string openSuccinctFile(),
}
