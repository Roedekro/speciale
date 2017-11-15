//
// Created by martin on 11/15/17.
//

/*
 * Notice that the tree can not support deletion outside the
 * naive version of delta = Log_M/B (N/B).
 * This is because insertions are too cheap to cover the cost
 * of deletion, even via. global rebuilding.
 * Therefore, the meaning of time is irrelevant.
 * Furthermore we can not afford to update a key with a new value.
 */

#include <iostream>
#include <algorithm>
#include "TruncatedBufferTree.h"
#include "../Streams/OutputStream.h"
#include "../Streams/BufferedOutputStream.h"
#include "../Streams/InputStream.h"
#include "../Streams/BufferedInputStream.h"

using namespace std;

TruncatedBufferTree::TruncatedBufferTree(int B, int M, int delta) {

    streamBuffer = 4096;
    this->B = B;
    this->M = M;
    this->delta = delta;
    // Nodes have M/B fanout
    size = M/(sizeof(int)*2*B*4); // Max size of a node is 4x size, contains int keys and int values.
    cout << "Size is " << size << "\n";
    leafSize = B/(sizeof(KeyValue)*4); // Min leafSize, max 4x leafSize.
    // Internal update buffer, size O(B), consiting of KeyValueTimes.
    maxBufferSize = B/(sizeof(KeyValue));
    update_buffer = new KeyValue*[maxBufferSize];
    update_bufferSize = 0;
    // External buffers consists of KeyValueTimes
    maxExternalBufferSize = M/(sizeof(KeyValue));
    iocounter = 0;
    numberOfNodes = 2; // Root and single leaf
    currentNumberOfNodes = 1; // Only counts actual nodes.
    root = 1;
    rootBufferSize = 0;

    // Manually write out an empty root
    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string name = "B";
    name += to_string(1);
    os->create(name.c_str());
    int height = 1;
    os->write(&height); // Height
    int zero = 0;
    os->write(&zero); // Nodesize
    os->write(&zero); // BufferSize
    os->write(&zero); // No child
    /*int two = 2;
    os->write(&two);*/
    os->close();
    iocounter = os->iocounter;
    delete(os);

    cout << "Tree will have between " << size << " and " << (4*size-1) << " fanout\n";
    cout << "Tree will have between " << leafSize << " and " << 4*leafSize << " leaf size\n";
    cout << "Max insert buffer size is " << maxBufferSize << " and max external buffer size " << maxExternalBufferSize << "\n";

}

TruncatedBufferTree::~TruncatedBufferTree() {

}

void TruncatedBufferTree::insert(KeyValue *element) {

    update_buffer[update_bufferSize] = element;
    update_bufferSize++;

    if(update_bufferSize >= maxBufferSize) {

        // Sort - Not needed, will sort later
        //sortInternalArray(update_buffer,update_bufferSize);
        // Append to roots buffer
        appendBufferNoDelete(root,update_buffer,update_bufferSize,1);
        // Update rootBufferSize
        rootBufferSize = rootBufferSize + update_bufferSize;

        // Did roots buffer overflow?
        if(rootBufferSize >= maxExternalBufferSize) {

            // Sort root buffer, since we write B at a time without duplicates
            // we will enter this if statement at size O(M).
            // just sort internally.

            // Load in array
            KeyValue** rootBuffer = new KeyValue*[rootBufferSize];
            readBuffer(root,rootBuffer,1);
            // Sort
            sortInternalArray(rootBuffer,rootBufferSize);
            // Write out, will remove old buffer
            writeBuffer(root,rootBuffer,rootBufferSize,1);

            // Read in node
            int height, nodeSize, bufferSize;
            int* ptr_height = &height;
            int* ptr_nodeSize = &nodeSize;
            int* ptr_bufferSize = &bufferSize;
            vector<int>* keys = new vector<int>();
            keys->reserve(size*4-1);
            vector<int>* values = new vector<int>();
            values->reserve(size*4);

            readNode(root,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

            // Check delta
            if(height == delta) {
                flushLeafNode();
            }
            else {
                flush(root,height,nodeSize,keys,values);
            }

            // Rebalance




        }

    }
}


/***********************************************************************************************************************
 * Buffer Handling Methods
 **********************************************************************************************************************/

/*
 * Appends buffer of size bSize to the buffer of node id.
 * Will clean up and delete elements after output.
 */
void TruncatedBufferTree::appendBuffer(int id, KeyValue **buffer, int bSize, int type) {

    // Name
    string name = getBufferName(id,type);
    //cout << "Append Buffer name = " << name << "\n";

    /*cout << "Appending Buffer size " << bSize << "\n";
    for(int i = 0; i < bSize; i++) {
        cout << buffer[i]->key << " " << buffer[i]->value << " " << buffer[i]->time << "\n";
    }*/

    // Open file
    BufferedOutputStream* os = new BufferedOutputStream(streamBuffer);
    os->open(name.c_str());

    // Append buffer
    for(int i = 0; i < bSize; i++) {
        os->write(&(buffer[i]->key));
        os->write(&(buffer[i]->value));
    }

    // Cleanup
    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);
    for(int i = 0; i < bSize; i++) {
        delete(buffer[i]);
    }
    delete[] buffer;

}

/*
 * As above, but will not delete the buffer given, only its elements.
 * This is usefull for preserving the input buffer.
 */
void TruncatedBufferTree::appendBufferNoDelete(int id, KeyValue **buffer, int bSize, int type) {

    // Name
    string name = getBufferName(id,type);
    //cout << "Append Buffer name = " << name << "\n";

    /*cout << "Appending Buffer size " << bSize << "\n";
    for(int i = 0; i < bSize; i++) {
        cout << buffer[i]->key << " " << buffer[i]->value << " " << buffer[i]->time << "\n";
    }*/

    // Open file
    BufferedOutputStream* os = new BufferedOutputStream(streamBuffer);
    os->open(name.c_str());

    // Append buffer
    for(int i = 0; i < bSize; i++) {
        os->write(&(buffer[i]->key));
        os->write(&(buffer[i]->value));
    }

    // Cleanup
    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);
    for(int i = 0; i < bSize; i++) {
        delete(buffer[i]);
    }

    // No delete of buffer
}


/*
 * Sorts an array of KeyValues by key.
 */
void TruncatedBufferTree::sortInternalArray(KeyValue** buffer, int bufferSize) {


    std::sort(buffer, buffer+bufferSize,[](KeyValue * a, KeyValue * b) -> bool
    {
        return a->key < b->key;
    } );
}

/***********************************************************************************************************************
 * Utility Methods
 **********************************************************************************************************************/

void TruncatedBufferTree::writeNode(int id, int height, int nodeSize, int bufferSize, vector<int> *keys, vector<int> *values) {

    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string node = "B";
    node += to_string(id);
    os->create(node.c_str());
    os->write(&height);
    os->write(&nodeSize);
    os->write(&bufferSize);
    for(int k = 0; k < nodeSize; k++) {
        os->write(&(*keys)[k]);
    }
    for(int k = 0; k < nodeSize+1; k++) {
        os->write(&(*values)[k]);
    }

    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);
    delete(keys);
    delete(values);
}

void TruncatedBufferTree::writeNodeInfo(int id, int height, int nodeSize, int bufferSize) {

    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string node = "B";
    node += to_string(id);
    os->create(node.c_str());
    os->write(&height);
    os->write(&nodeSize);
    os->write(&bufferSize);
    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);
}

/*
 * Reads the node of id into the pointers height and size, as well as fills
 * out the arrays keys and values. Handles leafs (height == 1) appropriately.
 */
void TruncatedBufferTree::readNode(int id, int* height, int* nodeSize, int* bufferSize, vector<int> *keys, vector<int> *values) {


    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = "B";
    name += to_string(id);
    is->open(name.c_str());
    *height = is->readNext();
    *nodeSize = is->readNext();
    *bufferSize = is->readNext();

    for(int i = 0; i < *nodeSize; i++) {
        //keys[i] = is->readNext();
        keys->push_back(is->readNext());
    }

    for(int i = 0; i < *nodeSize+1; i++) {
        //values[i] = is->readNext();
        values->push_back(is->readNext());
    }

    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);
}

void TruncatedBufferTree::readNodeInfo(int id, int *height, int *nodeSize, int *bufferSize) {

    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = "B";
    name += to_string(id);
    is->open(name.c_str());
    *height = is->readNext();
    *nodeSize = is->readNext();
    *bufferSize = is->readNext();
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);
}

std::string TruncatedBufferTree::getBufferName(int id, int type) {

    string name = "Error, wrong type";
    if(type == 1) {
        name = "Buf";
        name += to_string(id);
    }
    else if(type == 2) {
        name = "TempBuf1";
    }
    else if(type == 3) {
        name = "TempBuf2";
    }
    else if(type == 4) {
        name = "Leaf";
        name += to_string(id);
    }
    return name;

}
