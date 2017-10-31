//
// Created by martin on 10/30/17.
//

#ifndef SPECIALE_BUFFEREDBTREE_H
#define SPECIALE_BUFFEREDBTREE_H


#include "BufferedInternalNode.h"
#include "keyValueTime.h"

class BufferedBTree {
public:
    int B;
    int M;
    int size;
    int bufferSize;
    BufferedInternalNode* root;
    int numberOfNodes; // Created during lifetime. Used to avoid duplicates.
    int currentNumberOfNodes; // Actual number of nodes in the tree
    int internalNodeCounter; // Number of nodes internal memory
    int maxInternalNodes; // O(M/B). Can exceed temporarily by a factor O(log_B (M/B)).
    int minInternalNodes;
    int externalNodeHeight; // Height of the tree where all nodes, and below, are external
    long iocounter;
    BufferedBTree(int B, int M);
    virtual ~BufferedBTree();
    void externalize();
    void recursiveExternalize(BufferedInternalNode* node);
    void insert(KeyValueTime* element);
    void insertIntoNonFullInternal(BufferedInternalNode* node);
    void insertIntoNonFull(KeyValueTime* element, int id, int height, int nodeSize, int* keys, int* values, bool write);
    void splitChildInternal(BufferedInternalNode* parent, BufferedInternalNode* child, int childNumber);
    void splitChildBorder(BufferedInternalNode* parent, int childNumber, int* childSize,
                          int* cKeys, int* cValues, int* newKeys, int* newValues);
    void splitChild(int height, int nodeSize, int* keys, int* values, int childNumber, int* childSize,
                    int* cKeys, int* cValues, int* newKeys, int* newValues);
    int query(int element);
    void internalize();
    void recursiveInternalize(BufferedInternalNode* node);
    void deleteElement(int element);
    void deleteNonSparseInternal(int element, BufferedInternalNode* node);
    void deleteNonSparse(int element, int id, int height, int nodeSize, int* keys, int* values, bool write);
    void fuseChildInternal(BufferedInternalNode* parent, BufferedInternalNode* child, int childNumber);
    void fuseChildBorder(BufferedInternalNode* parent, int childNumber, int* childSize,
                         int* cKeys, int* cValues);
    void fuseChild(int height, int* nodeSize, int* keys, int* values, int childNumber, int* childSize,
                   int* cKeys, int* cValues);
    void readNode(int id, int* height, int* nodeSize, int* keys, int* values);
    void writeNode(int id, int height, int nodeSize, int* keys, int* values);
    void printTree(BufferedInternalNode* node);
    void printExternal(int node);
    void cleanup();
    void sortInternal(std::vector<KeyValueTime>* buffer);
};


#endif //SPECIALE_BUFFEREDBTREE_H
