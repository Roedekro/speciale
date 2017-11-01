//
// Created by martin on 10/31/17.
//

#ifndef SPECIALE_EXTERNALBUFFEREDBTREE_H
#define SPECIALE_EXTERNALBUFFEREDBTREE_H


#include "../BufferedBTree/keyValueTime.h"

class ExternalBufferedBTree {
public:
    int streamBuffer; // Buffersize on streams, fixed to count IOs.
    int B;
    int M;
    int size; // Nodes, up to 4x size.
    int maxBufferSize; // Max internal update size.
    int maxExternalBufferSize; // Max external buffer size.
    int root;
    int rootBufferSize; // Also located in root, but keep track internally as well.
    int update_bufferSize; // Current amount of updates in our buffer.
    KeyValueTime** update_buffer; // Buffer of size O(B) updates.
    long iocounter;
    long numberOfNodes; // Total nodes in lifetime, used to avoid duplicates.
    long currentNumberOfNodes; // Current number of nodes in the tree.
    int time; // Sufficient to insert and delete over 5GB worth of key/values.
    void update(KeyValueTime* keyValueTime); // Negative value equals delete.
    ExternalBufferedBTree(int B, int M);
    ~ExternalBufferedBTree();
    void sortInternalArray(KeyValueTime** buffer, int bufferSize);
    int sortAndRemoveDuplicatesInternalArray(KeyValueTime** buffer, int bufferSize);
    void readNode(int id, int* height, int* nodeSize, int* keys, int* values);
    void writeNode(int id, int height, int nodeSize, int* keys, int* values);
    //int getBufferSize(int id); Deprecated, changed placement of size to node
    int readBuffer(int id, KeyValueTime** buffer, bool temp);
    void writeBuffer(int id, KeyValueTime** buffer, int bSize, bool temp);
    void appendBuffer(int id, KeyValueTime** buffer, int bSize, bool temp);
    //void mergeInternalBuffersAndOutput(int id, int bufSizeA, KeyValueTime** bufferA, int bufsizeB, KeyValueTime** bufferB);
    int loadSortRemoveDuplicatesOutputBuffer(int id, bool temp); // Returns buffer size
};


#endif //SPECIALE_EXTERNALBUFFEREDBTREE_H
