//
// Created by martin on 11/15/17.
//

#ifndef SPECIALE_TRUNCATEDBUFFERTREE_H
#define SPECIALE_TRUNCATEDBUFFERTREE_H


#include "../Btree/KeyValue.h"

class TruncatedBufferTree {
public:
    int streamBuffer; // Buffersize on streams, fixed to count IOs.
    int B;
    int M;
    int delta;
    int size; // Nodes, up to 4x size.
    int leafSize;
    int maxBufferSize; // Max internal update size.
    int maxExternalBufferSize; // Max external buffer size.
    int maxBucketSize;
    long elementsInserted;
    int root;
    int rootBufferSize; // Also located in root, but keep track internally as well.
    int update_bufferSize; // Current amount of updates in our buffer.
    KeyValue** update_buffer; // Buffer of size O(B) updates.
    long iocounter;
    int numberOfNodes; // Total nodes in lifetime, used to avoid duplicates.
    int currentNumberOfNodes; // Current number of nodes in the tree.

    /*
     * General Methods
     */
    TruncatedBufferTree(int B, int M, int delta);
    ~TruncatedBufferTree();
    void insert(KeyValue* element);
    int query(int element);

    /*
     * Tree Structural Methods
     */
    void flushNode(int id, int height, int nodeSize, std::vector<int>* keys, std::vector<int>* values);
    void flushLeafNode(int id, int height, int nodeSize);
    int splitInternal(int height, int nodeSize, std::vector<int>* keys, std::vector<int>* values, int childNumber,
                      int cSize, std::vector<int>* cKeys, std::vector<int>* cValues);
    int splitLeaf(int nodeSize, std::vector<int>* keys, std::vector<int>* values, int childNumber);


    /*
     * Buffer Handling Methods
     */
    int readBuffer(int id, KeyValue** buffer, int type); // Returns buffer size
    void writeBuffer(int id, KeyValue** buffer, int bSize, int type);
    void appendBuffer(int id, KeyValue** buffer, int bSize, int type);
    void appendBufferNoDelete(int id, KeyValue** buffer, int bSize, int type);
    void sortInternalArray(KeyValue** buffer, int bufferSize);
    int sortAndRemoveDuplicatesExternalBuffer(int id, int bufferSize, int sortedSize);

    /*
     * Utility Methods
     */
    void readNode(int id, int* height, int* nodeSize, int*bufferSize, std::vector<int>* keys, std::vector<int>* values);
    void writeNode(int id, int height, int nodeSize, int bufferSize, std::vector<int>* keys, std::vector<int>* values);
    void readNodeInfo(int id, int* height, int* nodeSize, int*bufferSize);
    void writeNodeInfo(int id, int height, int nodeSize, int bufferSize);
    std::string getBufferName(int id, int type);
    std::string getListName(int id, int list);
    std::string getFracListName(int id, int list);
    void cleanUpExternallyAfterNodeOrLeaf(int id);
    void cleanUpTree();
    void cleanUpFromTo(int from, int to);
    void printTree();
    void printNode(int id);

};


#endif //SPECIALE_TRUNCATEDBUFFERTREE_H
