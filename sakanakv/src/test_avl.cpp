#include <assert.h>
#include <stdio.h>
#include <stdlib.h>
#include <set>
#include "avltree.cpp"

#define container_of(ptr, type, member) ({                  \
    const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
    (type *)( (char *)__mptr - offsetof(type, member) );})

struct Data {
    AvlNode node;
    uint32_t val = 0;
};

struct Container {
    AvlNode *root = NULL;
};

static void insert(Container &container, uint32_t val) {
    Data *data = new Data();
    avl_init(&data->node);
    data->val = val;

    if (!container.root) {
        container.root = &data->node;
        return;
    }
    
    AvlNode *cur = container.root;
    while (true) {
        AvlNode **from = val < container_of(cur, Data, node)->val
            ? &cur->left
            : &cur->right;
        if (!*from) {
            *from = &data->node;
            data->node.parent = cur;
            container.root = avl_fix(&data->node);
            break;
        }
        cur = *from;
    }
}

static bool del(Container &container, uint32_t val) {
    // find the node
    AvlNode *cur = container.root;
    while (cur) {
        uint32_t node_val = container_of(cur, Data, node)->val;
        if (val == node_val) {
            break;
        }
        cur = val < node_val
            ? cur->left
            : cur->right;
    }

    // node doesn't exist
    if (!cur) {
        return false;
    }
    
    // delete
    container.root = avl_del(cur);
    delete container_of(cur, Data, node);
    return true;
}

static void avl_verify(AvlNode *parent, AvlNode *node) {
    if (!node) {
        return;
    }

    assert(node->parent == parent);
    avl_verify(node, node->left);
    avl_verify(node, node->right);

    assert(node->cnt == 1 + avl_cnt(node->left) + avl_cnt(node->right));

    uint32_t ldepth = avl_depth(node->left);
    uint32_t rdepth = avl_depth(node->right);
    assert(
            ldepth == rdepth
            || ldepth + 1 == rdepth
            || ldepth == rdepth + 1
    );
    assert(node->depth == 1 + max(ldepth, rdepth));

    uint32_t val = container_of(node, Data, node)->val;
    if (node->left) {
        assert(node->left->parent == node);
        assert(container_of(node->left, Data, node)-> val <= val);
    }
    if (node->right) {
        assert(node->right->parent == node);
        assert(container_of(node->right, Data, node)-> val >= val);
    }
}

static void inorder(AvlNode *node, std::multiset<uint32_t> &extracted) {
    if (!node) {
        return;
    }
    inorder(node->left, extracted);
    extracted.insert(container_of(node, Data, node)->val);
    inorder(node->right, extracted);
}

static void container_verify(
        Container &container,
        const std::multiset<uint32_t> &expected
    ) {
    avl_verify(NULL, container.root);
    assert(avl_cnt(container.root) == expected.size());
    std::multiset<uint32_t> actual;
    inorder(container.root, actual);
    assert(actual == expected);
}

static void cleanup(Container &container) {
    while (container.root) {
        AvlNode *node = container.root; 
        container.root = avl_del(container.root);
        delete container_of(node, Data, node);
    }
}

static void test_insert(uint32_t size) {
    for (uint32_t val = 0; val < size; val++) {
        Container container;
        std::multiset<uint32_t> ref;
        for (uint32_t i = 0; i < size; i++) {
            if (i == val) {
                continue;
            }
            insert(container, i);
            ref.insert(i);
        }
        container_verify(container, ref);

        insert(container, val);
        ref.insert(val);
        container_verify(container, ref);
        cleanup(container);
    }
}

static void test_insert_duplicate(uint32_t size) {
    for (uint32_t val = 0; val < size; val++) {
        Container container;
        std::multiset<uint32_t> ref;
        for (uint32_t i = 0; i < size; i++) {
            insert(container, i);
            ref.insert(i);
        }
        container_verify(container, ref);

        insert(container, val);
        ref.insert(val);
        container_verify(container, ref);
        cleanup(container);
    }
}

static void test_remove(uint32_t size) {
    for (uint32_t val = 0; val < size; val++) {
        Container container;
        std::multiset<uint32_t> ref;
        for (uint32_t i = 0; i < size; i++) {
            insert(container, i);
            ref.insert(i);
        }
        container_verify(container, ref);
        assert(del(container, val));
        ref.erase(val);
        container_verify(container, ref);
        cleanup(container);
    }
}

int main() {
    for (uint32_t i = 0; i < 200; i++) {
        test_insert(i);
        test_insert_duplicate(i);
        test_remove(i);
    }
    return 0;
}
