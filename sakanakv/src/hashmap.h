#include <stddef.h>
#include <stdint.h>

/**
 * Implementation of a hashmap with chained collision resolution.
 */

// A single node in the hashmap
struct HashTableNode {
    HashTableNode *next = NULL;
    uint64_t hashcode = 0;
};

// Fixed size hashtable
struct HashTable {
    HashTableNode **table = NULL;
    size_t mask = 0;
    size_t size = 0;
};

// Hashtable with dynamic resizing
struct HashMap {
    HashTable ht1;
    HashTable ht2;
    size_t resizing_pos = 0;
};

HashTableNode *hm_get(
    HashMap *hm,
    HashTableNode *key,
    bool (*cmp)(HashTableNode *, HashTableNode *)
);

void hm_put(HashMap *hm, HashTableNode *node);

HashTableNode *hm_del(
    HashMap *hm,
    HashTableNode *key,
    bool (*cmp)(HashTableNode *, HashTableNode *)
);

void hm_destroy(HashMap *hm);

