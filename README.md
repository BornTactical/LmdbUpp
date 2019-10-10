# LmdbUpp
LMDB wrapper for use with U++ 

# Dependencies
* LMDB is included with the project. https://github.com/LMDB/lmdb
* Ultimate++ https://www.ultimatepp.org

# Usage (subject to change)

```C++
#include <Core/Core.h>
#include <LmdbUpp/LmdbUpp.h>
#include <Ficl/Ficl.h>

using namespace Upp;

struct Test {
    String      testString;
    Vector<int> testVector;
    uint64      testInt;
    
    void Serialize(Stream& s) {
        s % testString % testVector % testInt;
    }
    
    String ToString() const {
        String out;
        out <<
            "{\ntestString: " << testString <<
            "\ntestVector: "  << testVector <<
            "\ntestInt: "     << testInt <<
            "\n}\n";
        return out;
    }
};

KeyValueStore<uint64, Test> kvs("cache", "Test/v0.1");

void GenerateDatabase() {
    Test t;
    t.testString = "Record";
    t.testVector = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };
    t.testInt    = 1234;
    
    for(uint64 i = 0; i < 10000; i++) {
        kvs.Add(i, t, false);
    }
    
    kvs.Commit();
}

void SearchKeys() {
    auto cur = kvs.FindFirst(5000);
    Cout() << "key: " << cur.Key() << "\n " << cur.Value();
}

CONSOLE_APP_MAIN {
    StdLogSetup(LOG_COUT);
    GenerateDatabase();
    SearchKeys();
}
```

# TODO

* Improved cursor support
* Support for some kind of string_view to prevent unnecessary copying
* Consider this a very early WIP that has not been thoroughly tested. 
* DO NOT USE THIS ON CRITICAL SYSTEMS

