//
// Created by martin on 10/31/17.
//

#ifndef SPECIALE_EXTERNALBUFFEREDBTREE_H
#define SPECIALE_EXTERNALBUFFEREDBTREE_H


#include "../BufferedBTree/keyValueTime.h"
#include <string>
#include <vector>

class ExternalBufferedBTree {
public:

    /*
     * Class variables
     */
    int streamBuffer; // Buffersize on streams, fixed to count IOs.
    int B;
    int M;
    int size; // Nodes, up to 4x size.
    int leafSize;
    int maxBufferSize; // Max internal update size.
    int maxExternalBufferSize; // Max external buffer size.
    int root;
    int rootBufferSize; // Also located in root, but keep track internally as well.
    int update_bufferSize; // Current amount of updates in our buffer.
    KeyValueTime** update_buffer; // Buffer of size O(B) updates.
    long iocounter;
    int numberOfNodes; // Total nodes in lifetime, used to avoid duplicates.
    int currentNumberOfNodes; // Current number of nodes in the tree.
    int treeTime; // Sufficient to insert and delete over 5GB worth of key/values.
    bool forcedFlushed = false;

    /*
     * General Methods
     */
    ExternalBufferedBTree(int B, int M);
    ~ExternalBufferedBTree();
    void update(KeyValueTime* keyValueTime); // Negative value equals delete.
    int query(int element);

    /*
     * Tree Structural Methods
     */
    void flush(int id, int height, int nodeSize, std::vector<int>* keys, std::vector<int>* values);
    int splitInternal(int height, int nodeSize, std::vector<int>* keys, std::vector<int>* values, int childNumber,
               int cSize, std::vector<int>* cKeys, std::vector<int>* cValues);
    void splitLeaf(int nodeSize, std::vector<int>* keys, std::vector<int>* values, std::vector<int>* leafs, int leafNumber);
    int fuseInternal(int height, int nodeSize, std::vector<int>* keys, std::vector<int>* values, int childNumber,
                       int* cSize, std::vector<int>* cKeys, std::vector<int>* cValues);
    int fuseLeaf(int nodeSize, std::vector<int>* keys, std::vector<int>* values, std::vector<int>* leafs, int leafNumber);
    void flushEntireTree();
    void forcedFlush(int id, int height, int nodeSize, int bufferSize, std::vector<int>* keys, std::vector<int>* values);

    /*
     * Buffer Handling Methods
     */
    int readBuffer(int id, KeyValueTime** buffer, int type); // Returns buffer size
    void writeBuffer(int id, KeyValueTime** buffer, int bSize, int type);
    void appendBuffer(int id, KeyValueTime** buffer, int bSize, int type);
    void appendBufferNoDelete(int id, KeyValueTime** buffer, int bSize, int type);
    int sortAndRemoveDuplicatesExternalBuffer(int id, int bufferSize, int sortedSize);
    int sortAndRemoveDuplicatesExternalLeaf(int id, int bufferSize, int sortedSize);
    void sortInternalArray(KeyValueTime** buffer, int bufferSize);
    int sortAndRemoveDuplicatesInternalArray(KeyValueTime** buffer, int bufferSize);
    int sortAndRemoveDuplicatesInternalArrayLeaf(KeyValueTime** buffer, int bufferSize);
    int handleDeletesLeafOriginallyEmptyExternal(int id);

    /*
     * Utility Methods
     */
    void readNode(int id, int* height, int* nodeSize, int*bufferSize, std::vector<int>* keys, std::vector<int>* values);
    void writeNode(int id, int height, int nodeSize, int bufferSize, std::vector<int>* keys, std::vector<int>* values);
    void readNodeInfo(int id, int* height, int* nodeSize, int*bufferSize);
    void writeNodeInfo(int id, int height, int nodeSize, int bufferSize);
    std::string getBufferName(int id, int type);
    int readLeafInfo(int id, std::vector<int>* leafs);
    void writeLeafInfo(int id, std::vector<int>* leafs, int leafSize);
    int readLeaf(int id, KeyValueTime** buffer);
    void writeLeaf(int id, KeyValueTime** buffer, int bufferSize);
    void cleanUpExternallyAfterNodeOrLeaf(int id);
    void printTree();
    void printNode(int node);
    void printLeaf(int leaf);
    void cleanUpTree();

    /*
     * Deprecated methods, used in development / later replaced
     */
    void splitChild(int height, int nodeSize, int* keys, int* values, int childNumber, int* childSize,
                    int* childBufferSize, int* childKeys, int* childValues, int* newBufferSize, int* newKeys, int* newValues);
    int loadSortRemoveDuplicatesOutputBuffer(int id, int type); // Returns buffer size, deprecated.
    int mergeExternalBuffers(int id, int type1, int type2, int outType); // Returns buffer size, deprecated.
};


#endif //SPECIALE_EXTERNALBUFFEREDBTREE_H
