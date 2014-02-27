namespace java succinct.thrift

struct Range {
    1: required i64 startIndex;
    2: required i64 endIndex;
}

service SuccinctMasterService {
    i32 initialize(1:i32 mode),								// Initialize all clients
    i32 createSuccinctFile(1:string filePath),						// Calls spark code
    i32 openSuccinctFile(1:string fileName),						// Returns handle to file
    list<i64> locate(1:string query),							// Get the list of locations (global)
    i64 count(1:string query),								// Get the count of the substring (global)
    string extract(1:i64 loc, 2:i64 bytes),						// Extract bytes starting at location (global)
    map<i32, map<i32, Range>> getRanges(1:string query),				// Get all SA ranges corresponding to a given query string (global)
    i64 getLocation(1:i32 clientId, 2:i32 serverId, 3:i64 index),			// Get SA value at specified clientId, serverId, index (global)
}

service SuccinctWorkerService {
    i32 startSuccinctServers(1:i32 numServers),						// Starts the servers on local machine
    i32 initialize(1:i32 mode),								// Initialize client
    i64 getWorkerOffset(),								// Returns offset at Worker
    list<i64> locateLocal(1:string query),						// Get the list of locations (local)
    i64 countLocal(1:string query),							// Get the count of the substring (local)
    string extractLocal(1:i64 loc, 2:i64 bytes),					// Extract bytes starting at location (local)
    map<i32, Range> getRangesLocal(1:string query),					// Get all SA ranges corresponding to a given query string (local)
    i64 getLocationLocal(1:i32 serverId, 2:i64 index), 					// Get SA value at specified serverId, index (local)
}

service SuccinctService {
    i32 initialize(1:i32 mode),								// Initialize server
    i64 getServerOffset(),								// Returns the offset at this server
    list<i64> locate(1:string query),							// Get the list of locations
    i64 count(1:string query),								// Get the count of the substring
    string extract(1:i64 loc, 2:i64 bytes),						// Extract the bytes starting at location
    Range getRange(1:string query),							// Get SA range corresponding to given query string
    i64 getLocation(1:i64 index),							// Get SA value at index
}
