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
    int maxBufferSize;
    BufferedInternalNode* root;
    int numberOfNodes; // Created during lifetime. Used to avoid duplicates.
    int currentNumberOfNodes; // Actual number of nodes in the tree
    int internalNodeCounter; // Number of nodes internal memory
    int maxInternalNodes; // O(M/B). Can exceed temporarily by a factor O(log_B (M/B)).
    int minInternalNodes;
    int externalNodeHeight; // Height of the tree where all nodes, and below, are external
    long iocounter;

    /*
     * General Methods
     */
    BufferedBTree(int B, int M, int N, float delta);
    virtual ~BufferedBTree();
    void insert(KeyValueTime element);
    int query(int element);
    int specialQuery(int element);
    void deleteElement(int element);

    /*
     * Tree Structural Methods
     */
    int flushInternalNode(BufferedInternalNode* node);
    int flushExternalNode(int node);
    int splitInternalNodeInternalChildren(BufferedInternalNode* node, int childIndex);
    int splitInternalNodeExternalChildren(BufferedInternalNode* node, int childIndex, int cHeight, int cNodeSize,
                                          int* cBuffersize, std::vector<int>* cKeys, std::vector<int>* cValues);
    int splitExternalNodeExternalChildren(int nodeSize, std::vector<int>* keys, std::vector<int>* values, int childIndex,
                                          int cHeight, int cNodeSize, int* cBuffersize, std::vector<int>* cKeys,
                                          std::vector<int>* cValues);
    int splitLeafInternalParent(BufferedInternalNode* node, int leafNumber);
    int splitLeafExternalParent(int nodeSize, std::vector<int>* keys, std::vector<int>* values, int leafNumber,
                                std::vector<int>* leafSizes, std::vector<KeyValueTime>* leaf);
    void externalize();
    void recursiveExternalize(BufferedInternalNode* node);
    void internalize();
    void recursiveInternalize(BufferedInternalNode* node);

    /*
     * Buffer Handling Methods
     */
    void mergeVectors(std::vector<KeyValueTime>* v1, std::vector<KeyValueTime>* v2); // Places result in v1
    int appendBuffer(int node, std::vector<KeyValueTime>* kvts); // Returns resulting buffer size
    void writeBuffer(int id, std::vector<KeyValueTime>* kvts); // Will delete kvts
    void writeBufferNoDelete(int id, std::vector<KeyValueTime>* kvts);
    void readBuffer(int id, int bufferSize, std::vector<KeyValueTime>* buffer);

    /*
     * Utility Methods
     */
    void readNode(int id, int* height, int* nodeSize, int*bufferSize, std::vector<int>* keys, std::vector<int>* values);
    void readNodeInfo(int id, int* height, int* nodeSize, int*bufferSize);
    void writeNode(int id, int height, int nodeSize, int bufferSize, std::vector<int>* keys, std::vector<int>* values);
    void writeNodeInfo(int id, int height, int nodeSize, int bufferSize);
    void writeNodeNoDelete(BufferedInternalNode* node);
    void readLeaf(int leaf, std::vector<KeyValueTime>* ret, int leafSize);
    void writeLeaf(int leaf, std::vector<KeyValueTime>* kvts); // Deletes kvts
    int readLeafInfo(int id, std::vector<int>* leafs);
    void writeLeafInfo(int id, std::vector<int>* leafs);
    void printTree(BufferedInternalNode* node);
    void printExternal(int node);
    void printLeaf(int leaf, int leafSize);
    void cleanup();

};


#endif //SPECIALE_BUFFEREDBTREE_H
