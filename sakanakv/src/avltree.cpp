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

// update the depth and cnt
static void avl_update(AvlNode *node) {
    node->depth = 1 + max(avl_depth(node->left), avl_depth(node->right));
    node->cnt = 1 + avl_cnt(node->left) + avl_cnt(node->right);
}

static AvlNode *rotate_left(AvlNode *root) {
    AvlNode *newroot = root->right;
    if (newroot->left) {
        newroot->left->parent = root;
    }
    root->right = newroot->left;
    newroot->left = root;
    newroot->parent = root->parent;
    root->parent = newroot;
    avl_update(root);
    avl_update(newroot);
    return newroot;
}

static AvlNode *rotate_right(AvlNode *root) {
    AvlNode *newroot = root->left;
    if (newroot->right) {
        newroot->right->parent = root;
    }
    root->left = newroot->right;
    newroot->right = root;
    newroot->parent = root->parent;
    root->parent = newroot;
    avl_update(root);
    avl_update(newroot);
    return newroot;
}

static AvlNode *avl_fix_left(AvlNode *root) {
    // handle the near side heavy case
    if (avl_depth(root->left->left) < avl_depth(root->left->right)) {
        root->left = rotate_left(root->left);
    }
    return rotate_right(root);
}

static AvlNode *avl_fix_right(AvlNode *root) {
    // handle the near side heavy case
    if (avl_depth(root->right->right) < avl_depth(root->right->left)) {
        root->right = rotate_right(root->right);
    }
    return rotate_left(root);
}

static AvlNode *avl_fix(AvlNode *node) {
    while (true) {
        avl_update(node);
        uint32_t l = avl_depth(node->left);
        uint32_t r = avl_depth(node->right);

        // get the parent's pointer to node
        AvlNode **from = NULL;
        if (node->parent) {
            from = (node->parent->left == node)
                ? &node->parent->left
                : &node->parent->right;
        }

        if (l <= r + 2) {
            node = avl_fix_left(node);
        } else if (r <= l + 2) {
            node = avl_fix_right(node);
        }
        if (!from) {
            return node;
        }
        *from = node;
        node = node->parent;
    }
}

static AvlNode *avl_del(AvlNode *node) {
    if (node->right == NULL) {
        // replace node with left subtree
        AvlNode *parent = node->parent;
        if (node->left) {
            node->left->parent = parent;
        }
        if (parent) {
            AvlNode **from = (node->parent->left == node)
                ? &node->parent->left
                : &node->parent->right;
            *from = node->left; 
            return avl_fix(parent);
        } else {
            // this node is the root, just yeet it
            return node->left;
        }
    } else {
        // find and delete successor
        AvlNode *succ = node->right;
        while (succ->left) {
            succ = succ->left;
        }
        AvlNode *root = avl_del(succ);

        // swap succ into the target node
        *succ = *node;
        if (succ->left) {
            succ->left->parent = succ;
        }
        if (succ -> right) {
            succ->right->parent = succ;
        }
        AvlNode *parent = node->parent;
        if (parent) {
            AvlNode **from = (node->parent->left == node)
                ? &node->parent->left
                : &node->parent->right;
            *from = node->left; 
            return avl_fix(parent);
        } else {
            // this node is the root, just yeet it
            return node->left;
        }
    }
}

