#include "avltree.h"
#include "hashmap.h"

struct ZSet {
    AvlNode *tree = NULL;
    HashMap hm;
};

struct ZNode {
    AvlNode tree;
    HashTableNode hm;
    double score = 0;
    size_t len = 0;
    char name[0];
};

bool zset_add(ZSet *zset, const char *name, size_t len, double score);
ZNode *zset_lookup(ZSet *zset, const char *name, size_t len);
ZNode *zset_pop(ZSet * zset, const char *name, size_t len);
ZNode *zset_query(ZSet *zset, double score, const char *name, size_t len);
void zset_dispose(ZSet *zset);
ZNode *znode_offset(ZNode *node, int64_t offset);
void znode_del(ZNode *node);

