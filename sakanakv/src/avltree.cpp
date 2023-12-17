#include <stddef.h>
#include <stdint.h>

struct AvlNode {
   uint32_t depth = 0; 
   uint32_t cnt = 0;
   AvlNode *left = NULL;
   AvlNode *right = NULL;
   AvlNode *parent = NULL;
};

static void avl_init(AvlNode *node) {
    node->depth = 1;
    node->cnt = 1;
    node->left = NULL;
    node->right = NULL;
    node->parent = NULL;
}

static uint32_t avl_depth(AvlNode *node) {
    return node ? node->depth: 0;
}

static uint32_t avl_cnt(AvlNode *node) {
    return node ? node->cnt : 0;
}

static uint32_t max(uint32_t lhs, uint32_t rhs) {
    return lhs < rhs ? rhs : lhs;
}
