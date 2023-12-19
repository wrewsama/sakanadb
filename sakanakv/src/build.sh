#!/usr/bin/env bash
g++ client.cpp -o client
g++ server.cpp hashmap.cpp -o server 

g++ avltree.cpp test_avl.cpp -o avltreetest
