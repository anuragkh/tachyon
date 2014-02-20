namespace java succinct.thrift

struct FileHandle {
    1: required string responsibleClient;
    2: required string fileName;
}

struct Range {
    1: required i64 startIndex;
    2: required i64 endIndex;
}

service SuccinctService {
    i32 notifyClient(),									// Notify the client that the server is up
    i32 connectToClients(),								// Sets up connections to all clients
    i64 write(1:string key, 2:string value, 3:i32 serverId),				// Writes the key value pair to designated split
    i32 startServers(1:i32 numServers, 2:i32 partScheme),				// Starts the servers on local machine
    i32 initialize(1:i32 mode),								// Partially implemented
    i32 notifyServerSplitOffset(1:i32 serverId, 2:i64 splitOffset),			// Not implemented right now
    i32 notifyClientSplitOffset(1:i32 clientId, 2:i64 splitOffset),			// Not implemented right now
    i32 submitFile(1:string filePath, 2:i32 partScheme, 3:byte delim1, 4:byte delim2),	// Not implemented right now
    list<i64> locate(1:string query),							// Get the list of locations (global)
    list<i64> locateLocal(1:string query),						// Get the list of locations (local)
    i64 count(1:string query),								// Get the count of the substring (global)
    i64 countLocal(1:string query),							// Get the count of the substring (local)
    string extract(1:i64 loc, 2:i64 bytes),						// Extract bytes starting at location (global)
    string extractLocal(1:i64 loc, 2:i64 bytes),					// Extract bytes starting at location (local)
    i64 getKeyToValuePointer(1:string key),						// Get the pointer to the value corresponding to given key (global)
    i64 getKeyToValuePointerLocal(1:string key),					// Get the pointer to the value corresponding to given key (local)
    string getValue(1:string key),                                                      // Get the value corresponding to given key (global)
    string getValueLocal(1:string key),                                                 // Get the value corresponding to given key (local)
    set<string> getKeys(1:string substring),                                            // Get all keys corresponding to given substring (global)
    set<string> getKeysLocal(1:string substring),                                       // Get all keys corresponding to given substring (local)
    map<string, string> getRecords(1:string substring),                                 // Get all records corresponding to given substring (global)
    map<string, string> getRecordsLocal(1:string substring),                            // Get all records corresponding to given substring (local)
    i32 deleteRecord(1:string key),                                                     // Deletes record correspoding to key (global)
    i32 deleteRecordLocal(1:string key),                                                // Deletes record correspoding to key (local)
    map<i32, map<i32, Range>> getRanges(1:string query),				// Get all SA ranges corresponding to a given query string (global)
    map<i32, Range> getRangesLocal(1:string query),					// Get all SA ranges corresponding to a given query string (local)
    i64 getLocation(1:i32 clientId, 2:i32 serverId, 3:i64 index),			// Get SA value at specified clientId, serverId, index (global)
    i64 getLocationLocal(1:i32 serverId, 2:i64 index), 					// Get SA value at specified serverId, index (local)
}

service QueryService {
    i64 write(1:string key, 2:string value),						// Writes the key value pair to the designated split
    i32 initialize(1:i32 mode),								// Partially implemented
    i32 notifySplitOffset(1:i64 split_offset),						// Not implemented right now
    list<i64> locate(1:string query),							// Get the list of locations
    i64 count(1:string query),								// Get the count of the substring
    string extract(1:i64 loc, 2:i64 bytes),						// Extract the bytes starting at location
    i64 getKeyToValuePointer(1:string key),						// Get the pointer to the value corresponding to given key
    string getValue(1:string key),                                                      // Get the value corresponding to given key
    set<string> getKeys(1:string substring),                                            // Get all keys corresponding to given substring
    map<string, string> getRecords(1:string substring),                                 // Get all records corresponding to given substring
    i32 deleteRecord(1:string key),                                                     // Deletes record correspoding to key
    Range getRange(1:string query),							// Get SA range corresponding to given query string
    i64 getLocation(1:i64 index),							// Get SA value at index
}

service MasterService {
	string createSuccinctFile(1:string filePath, 2:i32 partScheme, 3:byte delim1, 4:byte delim2),
	string openSuccinctFile(),
}
