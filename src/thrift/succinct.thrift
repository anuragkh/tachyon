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
    i64 getKeyToValuePointer(1:string key),						         // Get the pointer to the value corresponding to given key
    string getValue(1:string key),                                       // Get the value corresponding to given key
    set<string> getKeys(1:string substring),                             // Get all keys corresponding to given substring
    map<string, string> getRecords(1:string substring),                  // Get all records corresponding to given substring
    i32 deleteRecord(1:string key),                                      // Deletes record correspoding to key
    map<i32, Range> getRanges(1:string query),				             // Get all SA ranges corresponding to a given query string
    i64 getLocation(1:i32 serverId, 3:i64 index),		                 // Get SA value at specified serverId, index
}

service QueryService {
    i32 initialize(1:i32 mode),								             // Initializes QueryServer
    i32 getSplitOffset(),                                                // Get the offset for this split
    list<i64> locate(1:string query),							         // Get the list of locations
    i64 count(1:string query),								             // Get the count of the substring
    string extract(1:i64 loc, 2:i64 bytes),						         // Extract the bytes starting at location
    i64 getKeyToValuePointer(1:string key),						         // Get the pointer to the value corresponding to given key
    string getValue(1:string key),                                       // Get the value corresponding to given key
    set<string> getKeys(1:string substring),                             // Get all keys corresponding to given substring
    map<string, string> getRecords(1:string substring),                  // Get all records corresponding to given substring
    i32 deleteRecord(1:string key),                                      // Deletes record correspoding to key
    Range getRange(1:string query),							             // Get SA range corresponding to given query string
    i64 getLocation(1:i64 index),							             // Get SA value at index
}

service MasterService {
	string openSuccinctFile(),
}
