//
// Created by martin on 10/31/17.
//

#ifndef SPECIALE_EXTERNALBUFFEREDBTREE_H
#define SPECIALE_EXTERNALBUFFEREDBTREE_H


#include "../BufferedBTree/keyValueTime.h"
#include <string>

class ExternalBufferedBTree {
public:

    /*
     * Class variables
     */
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

    /*
     * General Methods
     */
    ExternalBufferedBTree(int B, int M);
    ~ExternalBufferedBTree();
    void update(KeyValueTime* keyValueTime); // Negative value equals delete.

    /*
     * Tree Structural Methods
     */
    void splitChild(int height, int nodeSize, int* keys, int* values, int childNumber, int* childSize,
        int* childBufferSize, int* childKeys, int* childValues, int* newBufferSize, int* newKeys, int* newValues);
    void flushToChildren(int id, int height, int nodeSize, int* keys, int* values);

    /*
     * Buffer Handling Methods
     */
    int readBuffer(int id, KeyValueTime** buffer, int type); // Returns buffer size
    void writeBuffer(int id, KeyValueTime** buffer, int bSize, int type);
    void appendBuffer(int id, KeyValueTime** buffer, int bSize, int type);
    int loadSortRemoveDuplicatesOutputBuffer(int id, int type); // Returns buffer size
    int mergeExternalBuffers(int id, int type1, int type2, int outType); // Returns buffer size

    /*
     * Utility Methods
     */
    void sortInternalArray(KeyValueTime** buffer, int bufferSize);
    int sortAndRemoveDuplicatesInternalArray(KeyValueTime** buffer, int bufferSize);
    void readNode(int id, int* height, int* nodeSize, int*bufferSize, int* tempSize, int* keys, int* values);
    void writeNode(int id, int height, int nodeSize, int bufferSize, int tempSize, int* keys, int* values);
    void readNodeInfo(int id, int* height, int* nodeSize, int*bufferSize, int* tempSize);
    void writeNodeInfo(int id, int height, int nodeSize, int bufferSize, int tempSize);
    std::string getBufferName(int id, int type);
};


#endif //SPECIALE_EXTERNALBUFFEREDBTREE_H
