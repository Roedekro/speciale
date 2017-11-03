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
 * A general temporary buffer is denoted "TempBuf1",
 * * A general temporary buffer is denoted "TempBuf2",
 * belonging to no specific node.
 * The buffers are denoted as type 1, 2 and 3 respectively.
 * No buffer may exceed O(M) except temporarily during flushes,
 * when the primary buffer and a temporary buffer is merged,
 * both potentially of size O(M).
 *
 * All leaf nodes (internal nodes with leafs)
 * will have a seperate file "Leaf<id>" with
 * info on the size of their leafs. O(M/B).
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
    numberOfNodes = 2; // Root and single leaf
    currentNumberOfNodes = 1; // Only counts actual nodes.
    treeTime = 0;
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

    treeTime++;
    keyValueTime->time = treeTime;
    update_buffer[update_bufferSize] = keyValueTime;
    update_bufferSize++;

    if(update_bufferSize >= maxBufferSize) {

        // Sort the buffer and remove duplicates
        update_bufferSize = sortAndRemoveDuplicatesInternalArray(update_buffer,update_bufferSize);

        // Append to root
        appendBuffer(root,update_buffer,update_bufferSize,1);
        rootBufferSize = rootBufferSize + update_bufferSize;

        // Did roots buffer overflow?
        if(rootBufferSize > maxExternalBufferSize) {

            // Sort buffer, remove duplicates
            sortAndRemoveDuplicatesExternalBuffer(root,rootBufferSize,update_bufferSize);

            // Load in entire root, as regardless of the outcome we will need keys and values.
            int height, nodeSize, bufferSize;
            int* ptr_height = &height;
            int* ptr_nodeSize = &nodeSize;
            int* ptr_bufferSize = &bufferSize;
            int* keys = new int[*2size*4-1]; // Leave room for each child to split
            int* values = new int[*2size*4]; // Leave room for each child to split
            readNode(root,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

            // Flush to children.
            flush(root,height,nodeSize,keys,values);

            // Check if we need to create a new root
            readNode(root,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);
            if(nodeSize > size*4-1) {
                // Create new root
                numberOfNodes++;
                currentNumberOfNodes++;
                int newRoot = numberOfNodes;
                int newHeight = height+1;
                int newSize = 0;
                int newBufferSize = 0;
                int* newKeys = new int[1];
                int* newValues = new int[2];
                newValues[0] = root;
                splitInternal(newHeight,newSize,newKeys,newValues,0,nodeSize,keys,values);
                newSize++;
                root = newRoot;
                writeNode(newRoot,newHeight,newSize,0,newKeys,newValues);
            }
            else if(nodeSize == 0 && height > 1) {
                // Delete this root, make only child new root
                root = values[0];
                currentNumberOfNodes--;
                delete[] keys;
                delete[] values;
            }
            else {
                delete[] keys;
                delete[] values;
            }
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
 * Buffer of node assumed to be in sorted order.
 * The buffer can be larger than O(M).
 * The resulting buffers of the children may be larger than O(M),
 * resulting in recursive flushes.
 * After flushes are resolved splits and fuses will be applied to
 * the relevant children.
 * The buffer of the node will be deleted after the function completes.
 * The info of the node will be updated and written to disk after function completes.
 * After function is completed check if the node needs to be split/fused.
 */
void ExternalBufferedBTree::flush(int id, int height, int nodeSize, int *keys, int *values) {

    if(height == 1) {
        // Children are leafs, special case
        InputStream *parentBuffer = new BufferedInputStream(streamBuffer);
        string name = getBufferName(id, 1);
        parentBuffer->open(name.c_str());

        int* appendedToLeafs = new int[nodeSize+1](); // Initialized to zero

        if(nodeSize == 0) {
            // Only one child, give it the entire buffer
            BufferedOutputStream* os = new BufferedOutputStream(streamBuffer);
            name = getBufferName(values[0],1);
            os->open(name.c_str());
            int key, value, time;
            while(!parentBuffer->endOfStream()) {
                key = parentBuffer->readNext();
                value = parentBuffer->readNext();
                time = parentBuffer->readNext();
                os->write(&key);
                os->write(&value);
                os->write(&time);
                (appendedToLeafs[0])++;
            }

            // Clean up
            os->close();
            iocounter = iocounter + os->iocounter;
            delete(os);
        }
        else {
            // Open streams to every childs buffer so we can append
            BufferedOutputStream **appendStreams = new BufferedOutputStream *[nodeSize + 1];
            BufferedOutputStream *os;
            for (int i = 0; i < nodeSize + 1; i++) {
                string name = getBufferName(values[i], 1);
                os = new BufferedOutputStream(streamBuffer);
                os->open(name.c_str());
            }

            int childIndex = 0;
            int key, value, time;
            while (!parentBuffer->endOfStream()) {
                key = parentBuffer->readNext();
                value = parentBuffer->readNext();
                time = parentBuffer->readNext();
                if (key > keys[childIndex]) {
                    // Find new index
                    while (key > keys[childIndex] && childIndex < nodeSize) {
                        childIndex++;
                    }
                }
                // Append to childs buffer
                appendStreams[childIndex]->write(&key);
                appendStreams[childIndex]->write(&value);
                appendStreams[childIndex]->write(&time);
                (appendedToLeafs[childIndex])++;
            }

            // Clean up appendStreams
            for (int i = 0; i < nodeSize + 1; i++) {
                os = appendStreams[i];
                os->close();
                iocounter = iocounter + os->iocounter;
                delete (os);
            }

        }

        // Close parent buffer
        parentBuffer->close();
        iocounter = iocounter + parentBuffer->iocounter;
        delete(parentBuffer);

        // Delete parents now empty buffer
        name = getBufferName(id,1);
        if(FILE *file = fopen(name.c_str(), "r")) {
            fclose(file);
            remove(name.c_str());
        }

        // Update size of every leaf, sort and remove duplicates
        // TODO:

        // Handle splits
        // TODO:

        // Handle fuses
        // TODO: HUSK AT FUSES KAN STADIG = SIZE 0 pga at children kan alle være 0

        // Write out parents info to disk
        writeNode(id,height,nodeSize,0,keys,values);

        // Write out updated leaf info
        // TODO: Write out leaf info.

    }
    else {
        // Children are internal nodes.
        // Flush parents buffer to childrens buffers.

        InputStream *parentBuffer = new BufferedInputStream(streamBuffer);
        string name = getBufferName(id, 1);
        parentBuffer->open(name.c_str());

        // Open streams to every childs buffer so we can append
        BufferedOutputStream** appendStreams = new BufferedOutputStream*[size*4];
        BufferedOutputStream* os;
        for(int i = 0; i < nodeSize+1; i++) {
            string name = getBufferName(values[i],1);
            os = new BufferedOutputStream(streamBuffer);
            os->open(name.c_str());
        }

        // Append KeyValueTimes to children
        // Remember how many KVTs we wrote to each
        int* appendedToChild = new int[nodeSize+1](); // Initialized to zero
        int childIndex = 0;
        int key, value, time;
        while(!parentBuffer->endOfStream()) {
            key = parentBuffer->readNext();
            value = parentBuffer->readNext();
            time = parentBuffer->readNext();
            if(key > keys[childIndex]) {
                // Find new index
                while(key > keys[childIndex] && childIndex < nodeSize) {
                    childIndex++;
                }
            }
            // Append to childs buffer
            appendStreams[childIndex]->write(&key);
            appendStreams[childIndex]->write(&value);
            appendStreams[childIndex]->write(&time);
            (appendedToChild[childIndex])++;
        }

        // Clean up appendStreams and parentBuffer
        for(int i = 0; i < nodeSize+1; i++) {
            os = appendStreams[i];
            os->close();
            iocounter = iocounter + os->iocounter;
            delete(os);
        }
        parentBuffer->close();
        iocounter = iocounter + parentBuffer->iocounter;
        delete(parentBuffer);

        // Delete parents now empty buffer
        name = getBufferName(id,1);
        if(FILE *file = fopen(name.c_str(), "r")) {
            fclose(file);
            remove(name.c_str());
        }

        // Update bufferSize of every child
        // Handle any buffer overflows in children
        int cHeight, cNodeSize, cBufferSize;
        int* ptr_cHeight = &cHeight;
        int* ptr_cNodeSize = &cNodeSize;
        int* ptr_cBufferSize = &cBufferSize;
        int* cKeys = new int[2*size*4-1]; // Leave room for each child to split
        int* cValues = new int[2*size*4]; // Leave room for each child to split

        // Update childrens bufferSizes and flush if required.
        for(int i = 0; i < nodeSize+1; i++) {
            readNode(values[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
            cBufferSize = cBufferSize + appendedToChild[i];
            if(cBufferSize > maxExternalBufferSize) {
                // Sort childs buffer
                sortAndRemoveDuplicatesExternalBuffer(values[i],cBufferSize,appendedToChild[i]);
                // Flush
                flush(values[i],cHeight,cNodeSize,cKeys,cValues);
                // Child will update its info at end of flush
            }
            else {
                // Just update bufferSize
                writeNodeInfo(values[i],cHeight,cNodeSize,cBufferSize);
            }
        }

        // Check if we need to split any children
        for(int i = 0; i < nodeSize+1; i++) {
            readNode(values[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
            if(cNodeSize > 4*size-1) {
                // Split
                splitInternal(height,nodeSize,keys,values,i,cNodeSize,cKeys,cValues);
                nodeSize++;
                i++; // Skip next node, we know its small enough.
            }
        }

        // Check if we need to fuse any children
        for(int i = 0; i < nodeSize+1; i++) {
            readNode(values[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
            if(cNodeSize < size) {
                // Fuse
                fuseInternal(height,nodeSize,keys,values,i,ptr_cNodeSize,cKeys,cValues);
                // TODO: If we fuse with previous neighbour, chaos!
                // TODO: Recheck fused child, because size could even be 0 if all children had
                // TODO: everything deleted!
                // Could also reduce nodeSize!

            }
        }

        // Write out parents info to disk
        writeNode(id,height,nodeSize,0,keys,values);

        // Clean up arrays
        delete[] cKeys;
        delete[] cValues;
    }
}

/*
 * Splits an internal node with internal children.
 * Writes out child and new child to disk.
 */
void ExternalBufferedBTree::splitInternal(int height, int nodeSize, int *keys, int *values, int childNumber, int cSize,
                                  int *cKeys, int *cValues) {

    numberOfNodes++;
    currentNumberOfNodes++;
    int newChild = numberOfNodes;
    int newHeight = height-1;
    int newSize = (cSize-1)/2;
    int* newKeys = new int[newSize];
    int* newValues = new int[newSize+1];
    cSize = cSize/2;

    // Fill out new childs keys
    for(int j = 0; j < newSize; j++) {
        newKeys[j] = cKeys[cSize+1+j];
    }

    // Copy values
    for(int j = 0; j < newSize; j++) {
        newValues[j] = cValues[cSize+1+j];
    }

    // Move parents keys by one
    for(int j = nodeSize; j > childNumber; j--) {
        keys[j] = keys[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    keys[childNumber] = cKeys[cSize-1];

    // Move parents values back by one
    for(int j = nodeSize+1; j > childNumber; j--) {
        values[j] = values[j-1];
    }
    // Insert pointer to new child
    values[childNumber+1] = newChild;

    // Write child and new child to disk
    writeNode(values[childNumber],newHeight,cSize,0,cKeys,cValues);
    writeNode(newChild,newHeight,newSize,0,newKeys,newValues);

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

/*
 * Takes the buffer of node <id> located  at "Buf<id>",
 * loads in the up to O(M) unsorted KeyValueTimes,
 * sorts and removes duplicates.
 * Then the newly sorted elements are merged with the
 * remaining (sorted) part of the buffer.
 * The final merged buffer is output to "Buf<id>" and
 * its size is returned by the method.
 */
int ExternalBufferedBTree::sortAndRemoveDuplicatesExternalBuffer(int id, int bufferSize, int sortedSize) {

    int unsorted = bufferSize - sortedSize;
    KeyValueTime** buffer = new KeyValueTime*[unsorted];
    InputStream* is1 = new BufferedInputStream(streamBuffer);
    string name1 = getBufferName(id,1);
    is1->open(name1.c_str());
    // Read in unsorted elements
    int key, value, time;
    KeyValueTime* kvt;
    for(int i = 0; i < unsorted; i++) {
        key = is1->readNext();
        value = is1->readNext();
        time = is1->readNext();
        kvt = new KeyValueTime(key,value,time);
        buffer[i] = kvt;
    }

    // Sort and remove duplicates
    int newBufferSize = sortAndRemoveDuplicatesInternalArray(buffer,unsorted);

    // Place the buffer in "TempBuf1"
    writeBuffer(id,buffer,newBufferSize,2);

    InputStream* is2 = new BufferedInputStream(streamBuffer);
    OutputStream* os = new BufferedOutputStream(streamBuffer);

    string name2 = getBufferName(id,2); // TempBuf1
    is2->open(name2.c_str());

    string name3 = getBufferName(id,3); // Write to TempBuf2, later rename
    if(FILE *file = fopen(name3.c_str(), "r")) {
        fclose(file);
        remove(name3.c_str());
    }
    // Create file
    os->create(name3.c_str());

    // Merge the two streams
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

    // Rename TempBuf2 to Buf<id>
    int result= rename( name3.c_str(), name1.c_str());
    if (result != 0) {
        cout << "Tried renaming " << name3 << " to " << name1 << "\n";
        perror("Error renaming file");
    }

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
void ExternalBufferedBTree::writeNode(int id, int height, int nodeSize, int bufferSize, int *keys, int *values) {

    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string node = "B";
    node += to_string(id);
    os->create(node.c_str());
    os->write(&height);
    os->write(&nodeSize);
    os->write(&bufferSize);
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

void ExternalBufferedBTree::writeNodeInfo(int id, int height, int nodeSize, int bufferSize) {

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
void ExternalBufferedBTree::readNode(int id, int* height, int* nodeSize, int* bufferSize, int *keys, int *values) {


    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = "B";
    name += to_string(id);
    is->open(name.c_str());
    *height = is->readNext();
    *nodeSize = is->readNext();
    *bufferSize = is->readNext();

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

void ExternalBufferedBTree::readNodeInfo(int id, int *height, int *nodeSize, int *bufferSize) {

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
        name = "TempBuf1";
    }
    else if(type == 3) {
        name = "TempBuf2";
    }
    else if(type == 3) {
        name = "Leaf";
        name += to_string(id);
    }
    return name;

}

/***********************************************************************************************************************
 * Deprecated Methods
 **********************************************************************************************************************/

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