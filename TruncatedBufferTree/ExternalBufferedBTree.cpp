/*
 * Created by martin on 10/31/17.
 * Experiment to get the basics of an external
 * buffered BTree correct before implementing
 * the Truncated Buffered Tree.
 *
 * A node is laid out with its height, size, buffersize,
 * and temp buffer size, followed by its keys and its values.
 * A node will be stored in "B<id>".
 *
 * A buffer of node id consists of integers
 * denoting KeyValueTimes.
 * A buffer is stored in "Buf<id>".
 * A temp buffer may be stored in "Temp<id>".
 * A general temporary buffer is denoted "Temp",
 * belonging to no specific node.
 * The buffers are denoted as type 1, 2 and 3 respectively.
 * No buffer may exceed O(M) except temporarily during flushes,
 * when the primary buffer and a temporary buffer is merged,
 * both potentially of size O(M).
 *
 * NOTE: Buffers MUST be deleted after flush.
 * This way new elements can just be appended.
 *
 */


#include "ExternalBufferedBTree.h"
#include "../Streams/OutputStream.h"
#include "../Streams/BufferedOutputStream.h"
#include "../Streams/InputStream.h"
#include "../Streams/BufferedInputStream.h"
#include <sstream>
#include <iostream>
#include <unistd.h>
#include <cmath>
#include <algorithm>
#include <assert.h>

using namespace std;

/***********************************************************************************************************************
 * General Methods
 **********************************************************************************************************************/

ExternalBufferedBTree::ExternalBufferedBTree(int B, int M) {

    streamBuffer = 4096;
    this->B = B;
    this->M = M;
    // Nodes have M/B fanout
    size = M/(sizeof(int)*2*B*4); // Max size of a node is 4x size, contains int keys and int values.
    // Internal update buffer, size O(B), consiting of KeyValueTimes.
    maxBufferSize = B/(sizeof(KeyValueTime));
    update_buffer = new KeyValueTime*[maxBufferSize];
    update_bufferSize = 0;
    // External buffers consists of KeyValueTimes
    maxExternalBufferSize = M/(sizeof(KeyValueTime));
    iocounter = 0;
    numberOfNodes = 1;
    time = 0;
    root = 1;
    rootBufferSize = 0;

    // Manually write out an empty root
    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string name = "B";
    name += to_string(1);
    os->create(name.c_str());
    int height = 1;
    os->write(&height);
    int zero = 0;
    os->write(&zero);
    os->write(&zero);
    os->write(&zero);
    os->close();
    iocounter = os->iocounter;
    delete(os);

}

ExternalBufferedBTree::~ExternalBufferedBTree() {
    delete[] update_buffer;
}

/*
 * Updates the tree with the given KeyValueTime.
 * Time will be set according to the trees relative time.
 * A negative value denotes a delete.
 */
void ExternalBufferedBTree::update(KeyValueTime *keyValueTime) {

    time++;
    keyValueTime->time = time;
    update_buffer[update_bufferSize] = keyValueTime;
    update_bufferSize++;

    if(update_bufferSize >= maxBufferSize) {

        // Sort the buffer and remove duplicates
        update_bufferSize = sortAndRemoveDuplicatesInternalArray(update_buffer,update_bufferSize);

        // Flush to root
        if(rootBufferSize + update_bufferSize >= maxExternalBufferSize) {

            // Write update_buffer to roots temp buffer
            writeBuffer(root,update_buffer,update_bufferSize,2);

            // Load in roots buffer
            KeyValueTime** rootBuffer = new KeyValueTime*[rootBufferSize];
            readBuffer(root,rootBuffer,1);
            // Sort, remove, output
            rootBufferSize = sortAndRemoveDuplicatesInternalArray(rootBuffer,rootBufferSize);
            writeBuffer(root,rootBuffer,rootBufferSize,3); // Write to temp

            // Merge the two buffers into a new buffer.
            mergeExternalBuffers(root,3,2,1); // In from Temp<root> and Temp, out to Buf<root>.

            // Load in entire root, as regardless of the outcome we will need keys and values.
            int height, nodeSize, bufferSize, tempSize;
            int* ptr_height = &height;
            int* ptr_nodeSize = &nodeSize;
            int* ptr_bufferSize = &bufferSize;
            int* ptr_tempSize = &tempSize;
            int* keys = new int[size*4-1];
            int* values = new int[size*4];
            readNode(root,ptr_height,ptr_nodeSize,ptr_bufferSize,ptr_tempSize,keys,values);

            // We will delete buffer below.
            writeNodeInfo(root,height,nodeSize,0,0);

            // Flush to children.
            flushToChildren(root,height,nodeSize,keys,values);

        }
        else {
            // Append buffer to roots buffer
            appendBuffer(root,update_buffer,update_bufferSize,1);
            rootBufferSize = rootBufferSize + update_bufferSize;
        }

        // Reset internal buffer
        update_bufferSize = 0;
    }
}

/***********************************************************************************************************************
 * Tree Structural Methods
 **********************************************************************************************************************/

/*
 * Flushes the buffer of the node to its children.
 * The buffer can be larger than O(M).
 * The resulting buffers of the children may be larger than O(M).
 * The buffer of the parent will be deleted after the function completes.
 * Buffer of parent assumed to be in sorted order.
 */
void ExternalBufferedBTree::flushToChildren(int id, int height, int nodeSize, int *keys, int *values) {

    // TODO: Implement flushToChildren


    // Delete parents now empty buffer

}





/***********************************************************************************************************************
 * Buffer Handling Methods
 **********************************************************************************************************************/

/*
 * Convention that we will never read a file over size M (in bytes).
 * Will return actual size of buffer.
 */
int ExternalBufferedBTree::readBuffer(int id, KeyValueTime **buffer, int type) {

    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = getBufferName(id,type);
    //cout << "Read Buffer name = " << name << "\n";
    is->open(name.c_str());
    int newKey,newValue,newTime;
    //for(int i = 0; i < bSize; i++) {
    int i = 0;
    while(!is->endOfStream()) {
        newKey = is->readNext();
        newValue = is->readNext();
        newTime = is->readNext();
        buffer[i] = new KeyValueTime(newKey,newValue,newTime);
        i++;
    }
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);
    return i;
}

/*
 * Writes out buffer, cleans up.
 * Removes existing file, writes out new one.
 */
void ExternalBufferedBTree::writeBuffer(int id, KeyValueTime** buffer, int bSize, int type) {

    // Name
    string name = getBufferName(id,type);
    //cout << "Write Buffer name = " << name << "\n";

    // Remove existing buffer if present
    if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }

    // Create file again
    OutputStream* os = new BufferedOutputStream(streamBuffer);
    os->create(name.c_str());

    // Output buffer
    for(int i = 0; i < bSize; i++) {
        os->write(&buffer[i]->key);
        os->write(&buffer[i]->value);
        os->write(&buffer[i]->time);
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
 * Appends buffer of size bSize to the buffer of node id.
 * Will clean up and delete elements after output.
 */
void ExternalBufferedBTree::appendBuffer(int id, KeyValueTime **buffer, int bSize, int type) {

    // Name
    string name = getBufferName(id,type);
    //cout << "Append Buffer name = " << name << "\n";

    // Open file
    BufferedOutputStream* os = new BufferedOutputStream(streamBuffer);
    os->open(name.c_str());

    // Append buffer
    for(int i = 0; i < bSize; i++) {
        os->write(&buffer[i]->key);
        os->write(&buffer[i]->value);
        os->write(&buffer[i]->time);
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

int ExternalBufferedBTree::loadSortRemoveDuplicatesOutputBuffer(int id, int type) {

    // Load in buffer, max size M.
    KeyValueTime** temp_buffer = new KeyValueTime*[maxExternalBufferSize];
    int temp_bufferSize = readBuffer(id,temp_buffer,type);
    // Sort and remove duplicates, cleans up duplicates.
    int ret = sortAndRemoveDuplicatesInternalArray(temp_buffer,temp_bufferSize);
    // Output new buffer, cleans up remaining elements and buffer.
    writeBuffer(id,temp_buffer,ret,type);
    return ret;
}

/*
 * Will merge a nodes buffer and temp buffer, removing duplicates.
 * Will place the merged buffer into the nodes buffer file.
 * This might temporarily mean the buffer exceeds O(M).
 * Will return the number of elements in the buffer.
 */
int ExternalBufferedBTree::mergeExternalBuffers(int id, int inType1, int inType2, int outType) {

    InputStream* is1 = new BufferedInputStream(streamBuffer);
    InputStream* is2 = new BufferedInputStream(streamBuffer);
    OutputStream* os = new BufferedOutputStream(streamBuffer);

    // First inputstream
    string name1 = getBufferName(id,inType1);
    is1->open(name1.c_str());

    // Second inputstream
    string name2 = getBufferName(id,inType2);
    is2->open(name2.c_str());

    // Outputtream
    string name3 = getBufferName(id,outType);
    // Delete file if present
    if(FILE *file = fopen(name3.c_str(), "r")) {
        fclose(file);
        remove(name3.c_str());
    }
    // Create file
    os->create(name3.c_str());

    int outputsize = 0;
    // Read in initial elements
    int key1, value1, time1;
    int key2, value2, time2;
    key1 = is1->readNext();
    value1 = is1->readNext();
    time1 = is1->readNext();
    key2 = is2->readNext();
    value2 = is2->readNext();
    time2 = is2->readNext();

    // Merge files until we reach end of stream for one
    // Assumption: When the while loop terminates both
    // KeyValueTimes have been written to output (if it should),
    // and one of the streams will have terminated.
    while(true) {
        if(key1 == key2) {
            // Resolve duplicate
            // Largest time will always have priority
            // Discard other KeyValueTime
            if(time1 > time2) {
                os->write(&key1);
                os->write(&value1);
                os->write(&time1);
                outputsize++;
                // Read in new KeyValueTimes
                if(!is1->endOfStream() && !is2->endOfStream()) {
                    // Load in new element
                    key1 = is1->readNext();
                    value1 = is1->readNext();
                    time1 = is1->readNext();
                    // Discard other element
                    key2 = is2->readNext();
                    value2 = is2->readNext();
                    time2 = is2->readNext();
                }
                else {
                    break;
                }
            }
            else {
                os->write(&key2);
                os->write(&value2);
                os->write(&time2);
                outputsize++;
                // Read in new KeyValueTimes
                if(!is1->endOfStream() && !is2->endOfStream()) {
                    // Load in new element
                    key2 = is2->readNext();
                    value2 = is2->readNext();
                    time2 = is2->readNext();
                    // Discard other element
                    key1 = is1->readNext();
                    value1 = is1->readNext();
                    time1 = is1->readNext();
                }
                else {
                    break;
                }
            }
        }
        else {
            // Write out one of the KeyValueTimes, try and load in new
            // Break if one of the streams reached EOF.
            if(key1 < key2) {
                os->write(&key1);
                os->write(&value1);
                os->write(&time1);
                outputsize++;
                if(!is1->endOfStream()) {
                    // Load in new element
                    key1 = is1->readNext();
                    value1 = is1->readNext();
                    time1 = is1->readNext();
                }
                else {
                    // Write out other item
                    os->write(&key2);
                    os->write(&value2);
                    os->write(&time2);
                    outputsize++;
                    break;
                }
            }
            else {
                os->write(&key2);
                os->write(&value2);
                os->write(&time2);
                outputsize++;
                if(!is2->endOfStream()) {
                    // Load in new element
                    key2 = is2->readNext();
                    value2 = is2->readNext();
                    time2 = is2->readNext();
                }
                else {
                    // Write out other item
                    os->write(&key1);
                    os->write(&value1);
                    os->write(&time1);
                    outputsize++;
                    break;
                }
            }
        }
    }

    // Use Assumption: When the while loop terminates both
    // KeyValueTimes have been written to output,
    // and one of the streams will have terminated.
    if(!is1->endOfStream()) {
        // is1 did not finish
        while(!is1->endOfStream()) {
            key1 = is1->readNext();
            value1 = is1->readNext();
            time1 = is1->readNext();
            os->write(&key1);
            os->write(&value1);
            os->write(&time1);
            outputsize++;
        }
    }
    else {
        // is2 did not finish
        while(!is2->endOfStream()) {
            key2 = is2->readNext();
            value2 = is2->readNext();
            time2 = is2->readNext();
            os->write(&key2);
            os->write(&value2);
            os->write(&time2);
            outputsize++;
        }
    }

    // Clean up
    is1->close();
    iocounter = iocounter + is1->iocounter;
    delete(is1);
    is2->close();
    iocounter = iocounter + is2->iocounter;
    delete(is2);
    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);

    if(FILE *file = fopen(name1.c_str(), "r")) {
        fclose(file);
        remove(name1.c_str());
    }
    if(FILE *file = fopen(name2.c_str(), "r")) {
        fclose(file);
        remove(name2.c_str());
    }

    // Update node
    /*int height, nodeSize, bufferSize, tempSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* ptr_bufferSize = &bufferSize;
    int* ptr_tempSize = &tempSize;
    readNodeInfo(id,ptr_height,ptr_nodeSize,ptr_bufferSize,ptr_tempSize);
    bufferSize = outputsize;
    tempSize = 0;
    writeNodeInfo(id,height,nodeSize,bufferSize,tempSize);*/
    return outputsize;

}

/***********************************************************************************************************************
 * Utility Methods
 **********************************************************************************************************************/

/*
 * Writes out the node to file "B<id>".
 * Deletes key and value arrays.
 * Handles that leafs have size-1 values.
 */
void ExternalBufferedBTree::writeNode(int id, int height, int nodeSize, int bufferSize, int tempSize, int *keys, int *values) {

    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string node = "B";
    node += to_string(id);
    os->create(node.c_str());
    os->write(&height);
    os->write(&nodeSize);
    os->write(&bufferSize);
    os->write(&tempSize);
    for(int k = 0; k < nodeSize; k++) {
        os->write(&keys[k]);
    }
    if(height > 1) {
        for(int k = 0; k < nodeSize+1; k++) {
            os->write(&values[k]);
        }
    }
    else {
        // Leaf
        for(int k = 0; k < nodeSize; k++) {
            os->write(&values[k]);
        }
    }

    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);
    delete[] keys;
    delete[] values;
}

void ExternalBufferedBTree::writeNodeInfo(int id, int height, int nodeSize, int bufferSize, int tempSize) {

    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string node = "B";
    node += to_string(id);
    os->create(node.c_str());
    os->write(&height);
    os->write(&nodeSize);
    os->write(&bufferSize);
    os->write(&tempSize);
    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);
}

/*
 * Reads the node of id into the pointers height and size, as well as fills
 * out the arrays keys and values. Handles leafs (height == 1) appropriately.
 */
void ExternalBufferedBTree::readNode(int id, int* height, int* nodeSize, int* bufferSize, int* tempSize, int *keys, int *values) {


    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = "B";
    name += to_string(id);
    is->open(name.c_str());
    *height = is->readNext();
    *nodeSize = is->readNext();
    *bufferSize = is->readNext();
    *tempSize = is->readNext();

    for(int i = 0; i < *nodeSize; i++) {
        keys[i] = is->readNext();
    }

    if(*height > 1) {
        for(int i = 0; i < *nodeSize+1; i++) {
            values[i] = is->readNext();
        }
    }
    else {
        for(int i = 0; i < *nodeSize; i++) {
            values[i] = is->readNext();
        }
    }
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);
}

void ExternalBufferedBTree::readNodeInfo(int id, int *height, int *nodeSize, int *bufferSize, int *tempSize) {

    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = "B";
    name += to_string(id);
    is->open(name.c_str());
    *height = is->readNext();
    *nodeSize = is->readNext();
    *bufferSize = is->readNext();
    *tempSize = is->readNext();
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);
}

/* Deprecated
int ExternalBufferedBTree::getBufferSize(int id) {
    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = "Buf";
    name += to_string(id);
    is->open(name.c_str());
    int ret = is->readNext();
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);
    return ret;
}*/

/*
 * Sort an array of KeyValueTime by key and time
 */
void ExternalBufferedBTree::sortInternalArray(KeyValueTime** buffer, int bufferSize) {


    std::sort(buffer, buffer+bufferSize,[](KeyValueTime * a, KeyValueTime * b) -> bool
    {
        if(a->key == b->key) {
            return a->time < b->time;
        }
        else {
            return a->key < b->key;
        }
    } );
}

/*
 * Returns new size of the buffer after removing duplicates.
 * Will delete duplicates.
 */
int ExternalBufferedBTree::sortAndRemoveDuplicatesInternalArray(KeyValueTime **buffer, int bufferSize) {

    // Sort by key primary, time secondary
    sortInternalArray(buffer,bufferSize);
    // Remove duplicates
    int i = 1;
    int write = 0;
    KeyValueTime* kvt1 = buffer[0];
    KeyValueTime* kvt2;
    while(i < bufferSize) {

        kvt2 = buffer[i];
        if(kvt1->key == kvt2->key) {
            // Duplicate, resolve. Sorted by time as secondary.
            // Several combinations possible
            // Insert1 + delete2 = delete2
            // Insert1 + insert2 = insert2
            // delete1 + insert2 = insert2
            // delete1 + delete2 = delete2
            // Notice how second update always overwrite first
            delete(kvt1);
            kvt1 = kvt2;
            i++;
        }
        else {
            buffer[write] = kvt1;
            write++;
            kvt1 = kvt2;
            i++;
        }
    }
    buffer[write] = kvt1;
    write++;

    return write;
}

std::string ExternalBufferedBTree::getBufferName(int id, int type) {

    string name = "Error, wrong type";
    if(type == 1) {
        name = "Buf";
        name += to_string(id);
    }
    else if(type == 2) {
        name = "Temp";
        name += to_string(id);
    }
    else if(type == 3) {
        name = "Temp";
    }
    return name;

}

