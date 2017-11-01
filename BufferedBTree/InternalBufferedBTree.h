//
// Created by martin on 10/31/17.
//

#ifndef SPECIALE_INTERNALBUFFEREDBTREE_H
#define SPECIALE_INTERNALBUFFEREDBTREE_H


#include "keyValueTime.h"
#include "BufferedInternalNode.h"

class InternalBufferedBTree {
public:
    int B;
    int M;
    BufferedInternalNode* root;
    InternalBufferedBTree(int B, int M);
    ~InternalBufferedBTree();
    void insert(KeyValueTime* element);

};


#endif //SPECIALE_INTERNALBUFFEREDBTREE_H
