#include <stddef.h>
#include <stdint.h>

struct AvlNode {
   uint32_t depth = 0; 
   uint32_t cnt = 0;
   AvlNode *left = NULL;
   AvlNode *right = NULL;
   AvlNode *parent = NULL;
};
