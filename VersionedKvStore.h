//
// VersionedKvStore.h
//
// A data structure to hold key value pairs supporting
// snapshots of the different versions of the structure 
// at user-defined points in time.
//
// by Daniel Takayama 2017
//
//

#ifndef __VERSIONED_KV_STORE__
#define __VERSIONED_KV_STORE__

#include <unordered_map>
#include <vector>
using std::unordered_map;
using std::vector;

/** Key value store data structure that supports snapshots. */
template <typename K, typename V>
class VersionedKvStore {
private:
    /** Structure to hold diff for snapshot. */
    struct Diff {
        bool deleted;
        Diff* prev_diff;
        unsigned version;
        V value; 
    };

    /** Instantiates new diff structure for current key value store version. */
    Diff* newDiff() {
        Diff* diff = new Diff();
        diff->deleted = false;
        diff->prev_diff = nullptr;
        diff->version = maxVersion();
        return diff;
    }

    /** 
     * Checks for redundancy between diff and its previous diff for given key. 
     * Deletes redundant diff if it exists.
     */
    void checkAndDeleteRedundantDiff(K key) {
        if (key_value_store[key]->prev_diff 
                && diffsEqual(key_value_store[key], key_value_store[key]->prev_diff)) {
            Diff* duplicate = key_value_store[key];
            key_value_store[key] = key_value_store[key]->prev_diff;
            delete duplicate;
        }
    }

    /** Returns true if d1 and d2 hold equivalent state information about the key value store. */
    bool diffsEqual(Diff* d1, Diff* d2) {
        return d1->value == d2->value && d1->deleted == d2->deleted;
    }

    /** 
     * Returns latest version of diff for key not greater than version_num. 
     * Returns nullptr if no such diff exists. 
     */
    Diff* traverseToVersion(K key, int version_num) {
        Diff* curr = key_value_store[key];

        // traverse to diff having prev_diff not greater than version_num
        while (curr && curr->prev_diff && version_num < curr->prev_diff->version) {
            curr = curr->prev_diff;
        }

        // get prev_diff if current diff has version_num greater than that requested
        if (curr && version_num < curr->version) {
            curr = curr->prev_diff;
        }
        return curr;
    }

    /** Hash table of diffs. */
    unordered_map<K, Diff*> key_value_store;

    /** Number of key value pairs for each saved version of the key value store. */
    vector<size_t> sizes;

public:
    /** Constructor. */
    VersionedKvStore() {
        sizes.push_back(0);
    }

    /** Destructor. */
    ~VersionedKvStore() {
        vector<Diff*> garbage;
        for (auto it = key_value_store.begin(); it != key_value_store.end(); ++it) {
            Diff* diff = it->second;
            while (diff) {
                garbage.push_back(diff);
                diff = diff->prev_diff;
            }
        }
        for (Diff* diff : garbage) {
            delete diff;
        }
    }

    /** Deletes the value stored for key. */
    void erase(K key) {
        if (!key_value_store[key]) {
            // key previously not instantiated
            return;
        } else if (key_value_store[key]->version != maxVersion()) {
            // key exists but not for current version
            Diff* diff = newDiff();
            diff->deleted = true;
            diff->prev_diff = key_value_store[key];
            key_value_store[key] = diff;
        } else {
            // key exists for current version
            key_value_store[key]->deleted = true;
        }
        sizes.back() -= 1;
        checkAndDeleteRedundantDiff(key);
    }

    /** Returns true if value exists for key. Returns false otherwise. */
    bool exists(K key) {
        return key_value_store[key] && !key_value_store[key]->deleted;
    }

    /** Returns true if value existed for key for corresponding version_num. */
    bool exists(K key, unsigned version_num) {
        Diff* diff = traverseToVersion(key, version_num);
        return diff && !diff->deleted;
    }

    /** Gets value for key. Returns default value for typename V if no value was set. */
    V get(K key) {
        if (!exists(key)) {
            return V();
        }
        return key_value_store[key]->value;
    }

    /** 
     * Returns value for key in snapshot corresponding to version_num. 
     * Returns current value for key if no such snapshot for version_num is found.
     */
    V get(K key, unsigned version_num) {
        if (!exists(key, version_num)) {
            return V();
        }
        Diff* diff = traverseToVersion(key, version_num);
        return diff->value;
    }

    /** Returns the current version number of the key value store. Version number starts at 0. */
    unsigned maxVersion() {
        return sizes.size() - 1;
    }

    /** Sets value for key. */
    void set(K key, V value) {
        if (!key_value_store[key]) {
            // key previously not instantiated
            Diff* diff = newDiff();
            diff->value = value;
            key_value_store[key] = diff;
            sizes.back() += 1;
        } else if (key_value_store[key]->version != maxVersion()) {
            // key exists but not for current version
            if (key_value_store[key]->deleted) {
                sizes.back() += 1;
            }
            Diff* diff = newDiff();
            diff->value = value;
            diff->prev_diff = key_value_store[key];
            key_value_store[key] = diff;
        } else {
            // key exists for current version
            key_value_store[key]->deleted = false;
            key_value_store[key]->value = value;
        }
        checkAndDeleteRedundantDiff(key);
    }

    /** Returns size of key value store. */
    size_t size() {
        return sizes.back();
    }

    /** 
    * Returns size of key value store for specific version. 
    * Returns size of current key value store if no such snapshot for version_num is found.
    */
    size_t size(unsigned version_num) {
        if (maxVersion() < version_num) {
            return size();
        }
        return sizes[version_num];
    }

    /** 
     * Saves snapshot of current key value store state. 
     * Returns corresponding version number for the snapshot. 
     */
    unsigned save() {
        unsigned version = maxVersion();
        sizes.push_back(size());
        return version;
    }
};

#endif // __VERSIONED_KV_STORE__
