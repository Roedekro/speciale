//
// Created by martin on 10/26/17.
//

#ifndef SPECIALE_MODIFIEDBTREE_H
#define SPECIALE_MODIFIEDBTREE_H

#include "ModifiedInternalNode.h"
#include "KeyValue.h"

class ModifiedBtree {
public:
    int B;
    int M;
    int size;
    ModifiedInternalNode* root;
    int numberOfNodes;
    int internalNodeCounter;
    int maxInternalNodes;
    int externalNodeHeight;
    long iocounter;
    ModifiedBtree(int B, int M);
    virtual ~ModifiedBtree();
    void externalize();
    void recursiveExternalize(ModifiedInternalNode* node);
    void insert(KeyValue* element);
    void insertIntoNonFullInternal(KeyValue* element, ModifiedInternalNode* node);
    void insertIntoNonFull(KeyValue* element, int id, int height, int nodeSize, int* keys, int* values, bool write);
    void splitChildInternal(ModifiedInternalNode* parent, ModifiedInternalNode* child, int childNumber);
    void splitChildBorder(ModifiedInternalNode* parent, int childNumber, int* childSize,
                    int* cKeys, int* cValues, int* newKeys, int* newValues);
    void splitChild(int height, int nodeSize, int* keys, int* values, int childNumber, int* childSize,
                    int* cKeys, int* cValues, int* newKeys, int* newValues);
    int query(int element);
    void deleteElement(int element);
    void deleteNonSparseInternal(int element, ModifiedInternalNode* node);
    void deleteNonSparse(int element, int id, int height, int nodeSize, int* keys, int* values, bool write);
    void fuseChild(int height, int* nodeSize, int* keys, int* values, int childNumber, int* childSize,
                   int* cKeys, int* cValues);
    void readNode(int id, int* height, int* nodeSize, int* keys, int* values);
    void writeNode(int id, int height, int nodeSize, int* keys, int* values);
    void cleanup();

};


#endif //SPECIALE_MODIFIEDBTREE_H
