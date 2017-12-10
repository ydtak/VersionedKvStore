#include "VersionedKvStore.h"

#include <iostream>
#include <string>

using namespace std;


void testGetSetBasic() {
    VersionedKvStore<string, string> kvstore; 
    kvstore.set("hello", "world");
    cout << kvstore.get("hello") << endl;
}
    
void testEraseBasic() {
    VersionedKvStore<string, string> kvstore; 
    kvstore.set("hello", "world");
    cout << kvstore.get("hello") << endl;
    kvstore.erase("hello");
    cout << kvstore.get("hello") << endl;
}

void testSaveBasic() {
    VersionedKvStore<string, string> kvstore;
    kvstore.set("hello", "world");
    cout << kvstore.get("hello") << endl;
    unsigned version1 = kvstore.save();
    kvstore.set("hello", "foo");
    cout << kvstore.get("hello") << ' ' << kvstore.get("hello", version1) << endl;
}

void testSizeBasic() {
    VersionedKvStore<string, string> kvstore;
    kvstore.set("hello", "world");
    cout << kvstore.size() << endl;
    kvstore.set("foo", "bar");
    cout << kvstore.size() << endl;

    unsigned version1 = kvstore.save();
    kvstore.erase("foo");
    cout << kvstore.size() << ' ' << kvstore.size(version1) << endl;

    unsigned version2 = kvstore.save();
    kvstore.erase("hello");
    cout << kvstore.size() << ' ' << kvstore.size(version2) << endl;
}

void testSaveErased() {
    VersionedKvStore<string, string> kvstore;
    kvstore.set("hello", "world");
    unsigned version1 = kvstore.save();
    kvstore.erase("hello");
    unsigned version2 = kvstore.save();
    kvstore.set("hello", "world");
    unsigned version3 = kvstore.save();
    for (unsigned version = version1; version <= version3; ++version) {
        cout << kvstore.get("hello", version) << ':' << kvstore.size(version) << ' ';
    }
    cout << endl;
}

void testValuePersistsBasic() {
    VersionedKvStore<string, string> kvstore;
    kvstore.set("hello", "world");
    kvstore.set("foo", "bar");
    unsigned v1 = kvstore.save();

    kvstore.erase("foo");
    unsigned v2 = kvstore.save();

    kvstore.set("foo", "bar");
    unsigned v3 = kvstore.save();

    kvstore.set("hello", "there");
    for (unsigned v = v1; v <= v3 + 1; ++v) {
        cout << kvstore.get("hello", v) << '-' << kvstore.get("foo", v) << endl;
    }
}

int main() {
    // testSetBasic();
    // testEraseBasic();
    // testSaveBasic();
    // testSaveErased();
    // testSizeBasic();
    testValuePersistsBasic();
}