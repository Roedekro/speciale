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
    int numberOfNodes; // Created during lifetime. Used to avoid duplicates.
    int currentNumberOfNodes; // Actual number of nodes in the tree
    int internalNodeCounter; // Number of nodes internal memory
    int maxInternalNodes; // O(M/B). Can exceed temporarily by a factor O(log_B (M/B)).
    int minInternalNodes;
    int externalNodeHeight; // Height of the tree where all nodes, and below, are external
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
    void internalize();
    void recursiveInternalize(ModifiedInternalNode* node);
    void deleteElement(int element);
    void deleteNonSparseInternal(int element, ModifiedInternalNode* node);
    void deleteNonSparse(int element, int id, int height, int nodeSize, int* keys, int* values, bool write);
    void fuseChildInternal(ModifiedInternalNode* parent, ModifiedInternalNode* child, int childNumber);
    void fuseChildBorder(ModifiedInternalNode* parent, int childNumber, int* childSize,
                         int* cKeys, int* cValues);
    void fuseChild(int height, int* nodeSize, int* keys, int* values, int childNumber, int* childSize,
                   int* cKeys, int* cValues);
    void readNode(int id, int* height, int* nodeSize, int* keys, int* values);
    void writeNode(int id, int height, int nodeSize, int* keys, int* values);
    void printTree(ModifiedInternalNode* node);
    void printExternal(int node);
    void cleanup();

};


#endif //SPECIALE_MODIFIEDBTREE_H
