//
// VersionedKvStore.h
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
public:
    /** Constructor. */
    VersionedKvStore();

    /** Destructor. */
    ~VersionedKvStore();

    /** Deletes the value stored for key. */
    void erase(K key);

    /** Gets value for key. Returns default constructor for typename V if no value was set. */
    V get(K key);

    /** 
     * Returns value for key in snapshot corresponding to snapshot_num. Returns value in latest snapshot
     * if no snapshot corresponding to snapshot_num is found.
     */
    V get(K key, unsigned snapshot_num);

    /** Returns the maximum snapshot number (returns -1 if no snapshots taken). */
    unsigned maxVersion();

    /** Sets value for key. */
    void set(K key, V value);

    /** Returns size of key value store. */
    size_t size();

    /** Saves snapshot for current key value state. Returns corresponding snapshot numbmer. */
    unsigned takeSnapshot();
private:
    unordered_map<K, V> key_value_store;

};

#endif __VERSIONED_KV_STORE__