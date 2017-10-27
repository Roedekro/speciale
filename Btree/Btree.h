//
// Created by martin on 10/11/17.
//

#ifndef SPECIALE_BTREE_H
#define SPECIALE_BTREE_H

#include "KeyValue.h"

class Btree {
public:
    int B;
    int size;
    int root;
    int numberOfNodes;
    long iocounter;
    Btree(int B);
    virtual ~Btree();
    void insert(KeyValue* element);
    void insertIntoNonFull(KeyValue* element, int id, int height, int nodeSize, int* keys, int* values, bool write);
    void splitChild(int height, int nodeSize, int* keys, int* values, int childNumber, int* childSize,
                    int* cKeys, int* cValues, int* newKeys, int* newValues);
    int query(int element);
    void deleteElement(int element);
    void deleteNonSparse(int element, int id, int height, int nodeSize, int* keys, int* values, bool write);
    void fuseChild(int height, int* nodeSize, int* keys, int* values, int childNumber, int* childSize,
                    int* cKeys, int* cValues);
    void readNode(int id, int* height, int* nodeSize, int* keys, int* values);
    void writeNode(int id, int height, int nodeSize, int* keys, int* values);
    void cleanup();
};


#endif //SPECIALE_BTREE_H
