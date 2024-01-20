#include <stddef.h>
#include <stdint.h>

struct AvlNode {
   uint32_t depth = 0; 
   uint32_t cnt = 0;
   AvlNode *left = NULL;
   AvlNode *right = NULL;
   AvlNode *parent = NULL;
};

inline void avl_init(AvlNode *node) {
    node->depth = 1;
    node->cnt = 1;
    node->left = NULL;
    node->right = NULL;
    node->parent = NULL;
}

AvlNode *avl_fix(AvlNode *node);
AvlNode *avl_del(AvlNode *node);
AvlNode *avl_offset(AvlNode *node, int64_t offset);
