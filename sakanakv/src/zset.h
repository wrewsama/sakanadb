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

