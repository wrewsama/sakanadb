#include <stdlib.h>
#include "hashmap.h"

const size_t RESIZE_BATCH_SIZE = 128; 

// max ratio between hashtable size and num of buckets
const size_t RESIZE_THRESHOLD = 8; 

// initialise a fixed size hashtable
static void ht_init(HashTable *ht, size_t n) {
    ht->table = (HashTableNode **)calloc(sizeof(HashTableNode *), n);
    ht->mask = n - 1;
    ht->size = 0;
}

// insert into said hashtable
static void ht_insert(HashTable *ht, HashTableNode *node) {
    // group based on the prefix
    size_t position = node->hashcode & ht->mask;

    // append to the front
    HashTableNode *next = ht->table[position];
    node->next = next;
    ht->table[position] = node;
    ht->size++;
}

// hashtable lookup
static HashTableNode **ht_get(
        HashTable *ht,
        HashTableNode *key,
        bool (*cmp)(HashTableNode *, HashTableNode *)
    ) {
    if (!ht->table) {
        return NULL;
    }

    size_t position = key->hashcode & ht->mask;

    // set ptr to the head of the appropriate chain
    HashTableNode **ptr = &ht->table[position];

    // iterate through the linked list and return the ptr to the ptr to the
    // node we need
    while (*ptr) {
        if (cmp(*ptr, key)) {
            return ptr;
        }
        ptr = &(*ptr)->next;
    }
    return NULL;
}

static HashTableNode *ht_pop(HashTable *ht, HashTableNode **node) {
    HashTableNode *removed = *node;
    *node = (*node)->next;
    ht->size--;
    return removed;
}

// move 1 batch from ht2 to ht1
static void hm_move_batch(HashMap *hm) {
    if (hm->ht2.table == NULL) {
        return; // can't move anything if h2 is null
    }

    size_t moved_cnt = 0;
    while (moved_cnt < RESIZE_BATCH_SIZE && hm->ht2.size > 0) {
        HashTableNode **start = &hm->ht2.table[hm->resizing_pos];
        if (!*start) {
            // this bucket is empty, move on to the next one
            hm->resizing_pos++;
            continue;
        }

        // move the node
        ht_insert(&hm->ht1, ht_pop(&hm->ht2, start));
        moved_cnt++;
    }

    if (hm->ht2.size == 0) {
        // if there are no more records to move, free ht2's table
        free(hm->ht2.table);
        hm->ht2 = HashTable{};
    }
}

static void hm_resize(HashMap *hm) {
    // swap ht1 to ht2 for the batches to get moved
    hm->ht2 = hm->ht1;

    // double ht1 size
    ht_init(&hm->ht1, (hm->ht1.mask + 1) * 2);

    // reset the resizing_pos index
    hm->resizing_pos = 0;
}

HashTableNode *hm_get(
        HashMap *hm,
        HashTableNode *key,
        bool (*cmp)(HashTableNode *, HashTableNode *)
    ) {
    // trigger batch move
    hm_move_batch(hm);

    // get it from either ht1 or ht2
    HashTableNode **node = ht_get(&hm->ht1, key, cmp);
    if (!node) {
        node = ht_get(&hm->ht2, key, cmp);
    }
    
    // return node if exists else NULL
    return node ? *node : NULL;
}

void hm_put(HashMap *hm, HashTableNode *node) {
    if (!hm->ht1.table) {
        ht_init(&hm->ht1, 4);
    }
    ht_insert(&hm->ht1, node);
    if (!hm->ht2.table) {
        size_t load_factor = hm->ht1.size / (hm->ht1.mask + 1);
        if (load_factor >= RESIZE_THRESHOLD) {
            hm_resize(hm);
        }
    }
    hm_move_batch(hm);
}

HashTableNode *hm_del(
        HashMap *hm,
        HashTableNode *key,
        bool (*cmp)(HashTableNode *, HashTableNode *)
    ) {
    hm_move_batch(hm);
    HashTableNode **node = ht_get(&hm->ht1, key, cmp);
    if (node) {
        return ht_pop(&hm->ht1, node);
    }
    node = ht_get(&hm->ht2, key, cmp);
    if (node) {
        return ht_pop(&hm->ht2, node);
    }
    return NULL;
}

void hm_destroy(HashMap *hm) {
    free(hm->ht1.table);
    free(hm->ht2.table);
    *hm = HashMap{};
}
