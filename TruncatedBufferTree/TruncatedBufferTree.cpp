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
 *
 * Leaf nodes will save the number of lists in their nodeSize.
 * The lists will be saved in files labeled "Frac<node id>List<list id>".
 * Each list will contain elements of size 4. Let i be the ID of the list,
 * then we store Key, value, position in original list, position in list i-1.
 *
 * The original lists will be stored in "Node<node id>List<list id>",
 * to enable faster splits.
 */

#include <iostream>
#include <algorithm>
#include <fcntl.h>
#include <unistd.h>
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
    // Internal update buffer, size O(B), consiting of KeyValueTimes.
    maxBufferSize = B/(sizeof(KeyValue));
    update_buffer = new KeyValue*[maxBufferSize];
    update_bufferSize = 0;
    // External buffers consists of KeyValueTimes
    maxExternalBufferSize = M/(sizeof(KeyValue));
    iocounter = 0;
    numberOfNodes = 1; // Root
    currentNumberOfNodes = 1; // Only counts actual nodes.
    root = 1;
    rootBufferSize = 0;

    // TODO: Initiate maxBucketSize to something meaningfull
    // Remember that its denoted in nodeSize, that is each value
    // is multiplied with M to get actual size.
    maxBucketSize = 2;
    elementsInserted = 0;

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
    cout << "Max insert buffer size is " << maxBufferSize << " and max external buffer size " << maxExternalBufferSize << "\n";

}

TruncatedBufferTree::~TruncatedBufferTree() {

}

void TruncatedBufferTree::insert(KeyValue *element) {

    update_buffer[update_bufferSize] = element;
    update_bufferSize++;
    elementsInserted++;

    if(update_bufferSize >= maxBufferSize) {

        // Sort - Not needed, will sort later
        //sortInternalArray(update_buffer,update_bufferSize);
        // Append to roots buffer
        appendBufferNoDelete(root,update_buffer,update_bufferSize,1);
        // Update rootBufferSize
        rootBufferSize = rootBufferSize + update_bufferSize;

        // Did roots buffer overflow?
        if(rootBufferSize >= maxExternalBufferSize) {

            cout << "Root buffer overflow\n";

            // TODO: Update maxBucketSize according to N which is elementsInserted.
            elementsInserted = elementsInserted;
            maxBucketSize = 2;

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
            if(height == delta || height == 1) {
                delete(keys);
                delete(values);
                flushLeafNode(root,height,nodeSize);
            }
            else {
                flushNode(root,height,nodeSize,keys,values);
            }

            if(delta != 1) {

                // Read in node
                keys = new vector<int>();
                keys->reserve(size*4-1);
                values = new vector<int>();
                values->reserve(size*4);
                readNode(root,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

                // Check if we need to rebalance

                if(height == 1) {

                    delete(keys);
                    delete(values);

                    // Check size of leaf
                    if (nodeSize > maxBucketSize) {

                        cout << "Root was leaf, now creating new root node\n";

                        numberOfNodes++;
                        int newRoot = numberOfNodes;
                        int newHeight = height+1;
                        int newSize = 0;
                        int newBufferSize = 0;
                        vector<int>* newKeys = new vector<int>();
                        vector<int>* newValues = new vector<int>();
                        newValues->push_back(root);
                        int ret = splitLeaf(newSize,newKeys,newValues,0);
                        newSize++;
                        root = newRoot;
                        writeNode(root,2,newSize,0,newKeys,newValues);
                    }

                }
                else {
                    // Check size of node
                    if (nodeSize > 4 * size - 1) {

                        cout << "Root was internal, creating new root\n";

                        numberOfNodes++;
                        currentNumberOfNodes++;
                        int newRoot = numberOfNodes;
                        int newHeight = height+1;
                        int newSize = 0;
                        int newBufferSize = 0;
                        vector<int>* newKeys = new vector<int>();
                        vector<int>* newValues = new vector<int>();
                        newValues->push_back(root);
                        //cout << "Splitting old root\n";
                        int ret = splitInternal(newHeight,newSize,newKeys,newValues,0,nodeSize,keys,values);
                        //cout << "New root size " << ret << "\n";
                        newSize++;
                        root = newRoot;
                    }
                    else {
                        delete(keys);
                        delete(values);
                    }
                }
            }
            rootBufferSize = 0;
        }
        update_bufferSize = 0;
    }
}

/***********************************************************************************************************************
 * Tree Structural Methods Methods
 **********************************************************************************************************************/

void TruncatedBufferTree::flushNode(int id, int height, int nodeSize, std::vector<int> *keys,
                                    std::vector<int> *values) {

    cout << "Flushnode on node " << id << "\n";
    cout << "Height " << height << "\n";
    cout << "Nodesize " << nodeSize << "\n";
    cout << "--- Keys\n";
    for(int i = 0; i < nodeSize; i++) {
        cout << (*keys)[i] << "\n";
    }
    cout << "--- Values\n";
    for(int i = 0; i < nodeSize+1; i++) {
        cout << (*values)[i] << "\n";
    }
    cout << "---\n";



    // Children are internal nodes.
    // Flush parents buffer to childrens buffers.

    InputStream *parentBuffer = new BufferedInputStream(streamBuffer);
    string name = getBufferName(id, 1);
    parentBuffer->open(name.c_str());

    //cout << "Creating OutputStreams\n";
    // Open streams to every childs buffer so we can append
    BufferedOutputStream** appendStreams = new BufferedOutputStream*[nodeSize+1];
    for(int i = 0; i < nodeSize+1; i++) {
        string newname = getBufferName((*values)[i],1);
        const char* newchar = newname.c_str();
        appendStreams[i] = new BufferedOutputStream(streamBuffer);
        appendStreams[i]->open(newchar);
    }

    // Append KeyValueTimes to children
    // Remember how many KVTs we wrote to each
    int* appendedToChild = new int[nodeSize+1](); // Initialized to zero
    int childIndex = 0;
    int key, value;
    while(!parentBuffer->endOfStream()) {
        key = parentBuffer->readNext();
        value = parentBuffer->readNext();
        if(key > (*keys)[childIndex]) {
            // Find new index
            while(key > (*keys)[childIndex] && childIndex < nodeSize) {
                childIndex++;
            }
        }
        // Append to childs buffer
        appendStreams[childIndex]->write(&key);
        appendStreams[childIndex]->write(&value);
        (appendedToChild[childIndex])++;
    }

    // Clean up appendStreams and parentBuffer
    for(int i = 0; i < nodeSize+1; i++) {
        appendStreams[i]->close();
        iocounter = iocounter + appendStreams[i]->iocounter;
        delete(appendStreams[i]);
    }
    delete[] appendStreams;

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
    vector<int>* cKeys;
    vector<int>* cValues;

    //cout << "Flushing children of node " << id << "\n";
    // Update childrens bufferSizes and flush if required.
    for(int i = 0; i < nodeSize+1; i++) {
        if(height > 2) {
            cKeys = new vector<int>();
            cKeys->reserve(4*size-1);
            cValues = new vector<int>();
            cValues->reserve(4*size);
            // No reserve, in case of child being a leaf node.
            readNode((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
            cBufferSize = cBufferSize + appendedToChild[i];
            if(cBufferSize >= maxExternalBufferSize) {
                // Sort childs buffer
                if(cBufferSize - appendedToChild[i] != 0 ) {
                    sortAndRemoveDuplicatesExternalBuffer((*values)[i],cBufferSize,appendedToChild[i]);
                }
                // Flush
                flushNode((*values)[i],cHeight,cNodeSize,cKeys,cValues);
                // Child will update its info at end of flush
            }
            else {
                delete(cKeys);
                delete(cValues);
                // Just update bufferSize
                writeNodeInfo((*values)[i],cHeight,cNodeSize,cBufferSize);
            }
        }
        else {
            readNodeInfo((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize);
            cBufferSize = cBufferSize + appendedToChild[i];
            if(cBufferSize >= maxExternalBufferSize) {
                cout << "Flushing child " << (*values)[i] << " of node " << id << "\n";
                // Sort childs buffer
                if(cBufferSize - appendedToChild[i] != 0 ) {
                    sortAndRemoveDuplicatesExternalBuffer((*values)[i],cBufferSize,appendedToChild[i]);
                }
                // Flush
                flushLeafNode((*values)[i],cHeight,cNodeSize);
                cout << "Flushing of child " << (*values)[i] << " of node " << id << " completed!\n";
                // Child will update its info at end of flush
            }
            else {
                // Just update bufferSize
                writeNodeInfo((*values)[i],cHeight,cNodeSize,cBufferSize);
            }
        }
    }

    //cout << "Splitting children of node " << id << "\n";
    // Run through the children and split
    for(int i = 0; i < nodeSize+1; i++) {
        if(height > 2) {
            cKeys = new vector<int>();
            cKeys->reserve(4*size-1);
            cValues = new vector<int>();
            cValues->reserve(4*size);
            // No reserve in case child is a leaf node
            int child = (*values)[i];
            readNode((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
            if(cNodeSize > 4*size-1) {
                // Split
                int ret = splitInternal(height,nodeSize,keys,values,i,cNodeSize,cKeys,cValues);
                nodeSize++;
                if(ret > 4*size-1) {
                    i--; // Recurse upon this node
                }
                else {
                    i++; // Skip next node, we know its size is correct.
                }
            }
            else {
                // Clean up vectors
                delete(cKeys);
                delete(cValues);
            }
        }
        else {
            readNodeInfo((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize);
            if(cNodeSize > 4*size-1) {
                // Split
                int ret = splitLeaf(nodeSize,keys,values,i);
                nodeSize++;
                if(ret > 4*size-1) {
                    i--; // Recurse upon this node
                }
                else {
                    i++; // Skip next node, we know its size is correct.
                }
            }
        }
    }

    // Write out parents info to disk
    writeNode(id,height,nodeSize,0,keys,values);

}

/*
 * Assumes the buffer of the node has been sorted before this function is called.
 *
 * Places a new list in this leaf and updates its fractional cascading structure, UNLESS
 * this insertion will result in a split, in which case we simply insert the list without
 * updating the structure, as the structure will have to be rebuilt.
 */
void TruncatedBufferTree::flushLeafNode(int id, int height, int nodeSize) {

    nodeSize++;

    if(nodeSize > maxBucketSize) {
        // Only write out list
        string name1 = getBufferName(id,1); // Buf<id>
        string name2 = getListName(id,nodeSize); // Node<id>List<nodeSize>
        // Rename
        int result = rename( name1.c_str(), name2.c_str());
        if (result != 0) {
            cout << "Tried renaming " << name1 << " to " << name2 << "\n";
            perror("Error renaming file");
        }
    }
    else {
        if(nodeSize == 1) {
            // Special case
            string name1 = getBufferName(id,1); // Buf<id>
            string name2 = getListName(id,nodeSize); // Node<id>List<nodeSize>
            string name4 = getFracListName(id,nodeSize);

            InputStream* is1 = new BufferedInputStream(streamBuffer);
            is1->open(name1.c_str());

            if(FILE *file = fopen(name4.c_str(), "r")) {
                fclose(file);
                remove(name4.c_str());
            }
            OutputStream* os = new BufferedOutputStream(streamBuffer);
            os->create(name4.c_str());

            int bufCounter = 0;
            int key, value;
            int zero = 0;
            while(!is1->endOfStream()) {
                key = is1->readNext();
                value = is1->readNext();
                os->write(&key);
                os->write(&value);
                os->write(&bufCounter);
                os->write(&zero);
                bufCounter++;
            }

            // Cleanup
            os->close();
            iocounter = iocounter + os->iocounter;
            delete(os);

            is1->close();
            iocounter = iocounter + is1->iocounter;
            delete(is1);



            // Rename buffer to new list
            if(FILE *file = fopen(name2.c_str(), "r")) {
                fclose(file);
                remove(name2.c_str());
            }
            int result = rename( name1.c_str(), name2.c_str());
            if (result != 0) {
                cout << "Tried renaming " << name1 << " to " << name2 << "\n";
                perror("Error renaming file");
            }
        }
        else {
            // Write out list and build up fractional structure
            // Place list in appropriate position
            string name1 = getBufferName(id,1); // Buf<id>
            string name2 = getListName(id,nodeSize); // Node<id>List<nodeSize>
            string name3 = getFracListName(id,nodeSize-1); // Frac<id>List<nodeSize-1>
            string name4 = getFracListName(id,nodeSize);

            InputStream* is1 = new BufferedInputStream(streamBuffer);
            is1->open(name1.c_str());

            InputStream* is2 = new BufferedInputStream(streamBuffer);
            is2->open(name3.c_str());

            if(FILE *file = fopen(name4.c_str(), "r")) {
                fclose(file);
                remove(name4.c_str());
            }
            OutputStream* os = new BufferedOutputStream(streamBuffer);
            os->create(name4.c_str());

            // Write out to FracList, Buf will be renamed to List below
            int keyBuf, valueBuf;
            int keyFrac, valueFrac;
            bool run = true;
            keyBuf = is1->readNext();
            valueBuf = is1->readNext();

            keyFrac = is2->readNext();
            valueFrac = is2->readNext();
            is2->readNext();
            is2->readNext();

            int bufCounter = 0;
            int fracCounter = 0;

            // Create new frac list
            while(run) {

                if(keyBuf < keyFrac) {
                    os->write(&keyBuf);
                    os->write(&valueBuf);
                    os->write(&bufCounter); // Place in original list
                    os->write(&(fracCounter)); // Place if we searched list below
                    bufCounter++;
                    if(!is1->endOfStream()) {
                        keyBuf = is1->readNext();
                        valueBuf = is1->readNext();
                    }
                    else {
                        if(fracCounter % 2 == 1) {
                            os->write(&keyFrac);
                            os->write(&valueFrac);
                            os->write(&bufCounter);
                            os->write(&fracCounter);
                        }
                        fracCounter++;
                        break;
                    }
                }
                else {
                    if(fracCounter % 2 == 1) {
                        os->write(&keyFrac);
                        os->write(&valueFrac);
                        os->write(&bufCounter);
                        os->write(&fracCounter);
                    }
                    fracCounter++;
                    if(!is2->endOfStream()) {
                        keyFrac = is2->readNext();
                        valueFrac = is2->readNext();
                        is2->readNext();
                        is2->readNext();
                    }
                    else {
                        os->write(&keyBuf);
                        os->write(&valueBuf);
                        os->write(&bufCounter); // Place in original list
                        os->write(&(fracCounter)); // Place if we searched list below
                        bufCounter++;
                        break;
                    }
                }
            }

            // Finish writing out list
            if(!is1->endOfStream()) {
                while(!is1->endOfStream()) {
                    keyBuf = is1->readNext();
                    valueBuf = is1->readNext();
                    os->write(&keyBuf);
                    os->write(&valueBuf);
                    os->write(&bufCounter); // Place in original list
                    os->write(&(fracCounter)); // Place if we searched list below
                    bufCounter++;
                }

            }
            else if(!is2->endOfStream()) {
                while(!is2->endOfStream()) {
                    keyFrac = is2->readNext();
                    valueFrac = is2->readNext();
                    is2->readNext();
                    is2->readNext();
                    if(fracCounter % 2 == 1) {
                        os->write(&keyFrac);
                        os->write(&valueFrac);
                        os->write(&bufCounter);
                        os->write(&fracCounter);
                    }
                    fracCounter++;
                }
            }

            // Cleanup
            os->close();
            iocounter = iocounter + os->iocounter;
            delete(os);

            is1->close();
            iocounter = iocounter + is1->iocounter;
            delete(is1);

            is2->close();
            iocounter = iocounter + is2->iocounter;
            delete(is2);

            // Rename buffer to new list
            if(FILE *file = fopen(name2.c_str(), "r")) {
                fclose(file);
                remove(name2.c_str());
            }
            int result = rename( name1.c_str(), name2.c_str());
            if (result != 0) {
                cout << "Tried renaming " << name1 << " to " << name2 << "\n";
                perror("Error renaming file");
            }

        }
    }

    // Write out node
    writeNodeInfo(id,height,nodeSize,0);

}

/*
 * Works as in any other buffer tree.
 */
int TruncatedBufferTree::splitInternal(int height, int nodeSize, std::vector<int> *keys, std::vector<int> *values,
                                       int childNumber, int cSize, std::vector<int> *cKeys,
                                       std::vector<int> *cValues) {

    numberOfNodes++;
    currentNumberOfNodes++;

    //cout << "Split internal node " << (*values)[childNumber] << " into " << numberOfNodes << "\n";

    int newChild = numberOfNodes;
    int newHeight = height-1;
    int newSize = (cSize-1)/2;
    vector<int>* newKeys = new vector<int>();
    newKeys->reserve(newSize);
    vector<int>* newValues = new vector<int>();
    newValues->reserve(newSize+1);
    cSize = cSize/2;

    // Fill out new childs keys
    for(int j = 0; j < newSize; j++) {
        //newKeys[j] = cKeys[cSize+1+j];
        newKeys->push_back((*cKeys)[cSize+1+j]);
    }

    // Copy values
    for(int j = 0; j < newSize+1; j++) {
        //newValues[j] = cValues[cSize+1+j];
        newValues->push_back((*cValues)[cSize+1+j]);
    }

    // Move parents keys by one
    keys->push_back(0);
    for(int j = nodeSize; j > childNumber; j--) {
        (*keys)[j] = (*keys)[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    (*keys)[childNumber] = (*cKeys)[cSize]; // was cSize-1

    // Move parents values back by one
    values->push_back(0);
    for(int j = nodeSize+1; j > childNumber; j--) {
        (*values)[j] = (*values)[j-1];
    }
    // Insert pointer to new child
    (*values)[childNumber+1] = newChild;

    // Write child and new child to disk
    writeNode((*values)[childNumber],newHeight,cSize,0,cKeys,cValues);
    writeNode(newChild,newHeight,newSize,0,newKeys,newValues);

    return cSize;

}

/*
 * Splitleafs involves finding a split element.
 * Then we collect all the elements in the leaf and split them in two.
 * On top of these two sets we rebuild the fractional cascading structure.
 *
 * Notice that we can exploit the fact the elements are in sorted order in the lists
 * of size O(M). Thus we can just pick the mean element of each list, which is
 * max N/M < M (for all practical N and M) elements. These we can just sort,
 * and select the medians of medians, which is guaranteed to split all elements
 * in worst case 1/4 - 3/4. This guarantee is enough to amortize the cost of future splits.
 */
int TruncatedBufferTree::splitLeaf(int nodeSize, std::vector<int> *keys, std::vector<int> *values,
                                    int childNumber) {

    // First read in child
    int childID, cHeight,cSize,cBuffer;
    int* ptr_cHeight = &cHeight;
    int* ptr_cSize = &cSize;
    int* ptr_cBuffer = &cBuffer;
    childID = (*values)[childNumber];
    readNodeInfo(childID,ptr_cHeight,ptr_cSize,ptr_cBuffer);

    int numberOfLists = cSize;

    // Find all medians of all the lists
    int* medians = new int[numberOfLists];
    int median = 0;
    int offset = (maxExternalBufferSize-2)*sizeof(int); // Divide by 2, but each KeyValue is 2 ints.
    int* tempBuff = new int[4];
    for(int i = 1; i <= numberOfLists; i++) {

        // Find median of list
        // Use pread to read with an offset
        string temp = getListName(childID,i);
        int filedesc = ::open(temp.c_str(), O_RDONLY, 0666);
        int bytesRead = ::pread(filedesc, tempBuff, 4*sizeof(int),offset);
        median = tempBuff[0];
        ::close(filedesc);
        iocounter++;

        medians[i-1] = median;
    }
    delete[] tempBuff;

    // Sort medians
    sort(medians,medians+numberOfLists);

    int middle = numberOfLists/2; // 1 over with even numbers, because of 0 indexation.
    if(numberOfLists%2 == 0) {
        middle--;
    }

    // Found median of medians that we can split all KeyValues around!
    int medianOfMedians = medians[middle];

    // Clean up
    delete[] medians;

    InputStream* is;
    OutputStream* os1 = new BufferedOutputStream(streamBuffer);
    OutputStream* os2 = new BufferedOutputStream(streamBuffer);

    string tempbuf1 = getBufferName(1,2);
    string tempbuf2 = getBufferName(1,3);

    // Remove, just in case.
    if(FILE *file = fopen(tempbuf1.c_str(), "r")) {
        fclose(file);
        remove(tempbuf1.c_str());
    }
    if(FILE *file = fopen(tempbuf2.c_str(), "r")) {
        fclose(file);
        remove(tempbuf2.c_str());
    }

    os1->create(tempbuf1.c_str());
    os2->create(tempbuf2.c_str());

    // Split all KeyValues according to medianOfMedians
    // into the two temporary buffers
    long counter1 = 0;
    long counter2 = 0;
    int key, val;
    for(int i = 1; i <= numberOfLists; i++) {
        string tempString = getListName(childID,i);
        is = new BufferedInputStream(streamBuffer);
        is->open(tempString.c_str());

        while(!is->endOfStream()) {
            key = is->readNext();
            val = is->readNext();
            if(key <= medianOfMedians) {
                os1->write(&key);
                os1->write(&val);
                counter1++;
            }
            else {
                os2->write(&key);
                os2->write(&val);
                counter2++;
            }
        }

        // Cleanup
        is->close();
        iocounter = iocounter + is->iocounter;
        delete(is);

        // Remove old file
        if(FILE *file = fopen(tempString.c_str(), "r")) {
            fclose(file);
            remove(tempString.c_str());
        }
    }

    // Clean up
    os1->close();
    iocounter = iocounter + os1->iocounter;
    delete(os1);

    os2->close();
    iocounter = iocounter + os2->iocounter;
    delete(os2);

    // Now rebuild this nodes lists with tempbuf1
    InputStream* is1 = new BufferedInputStream(streamBuffer);
    is1->open(tempbuf1.c_str());
    cSize = counter1 / maxExternalBufferSize;
    int remainder = counter1 % maxExternalBufferSize;
    if(remainder != 0) {
        cSize++;
    }
    OutputStream* osList;
    // Iterate over all lists but the last one, special case.
    for(int i = 1; i < cSize; i++) {
        string tempString = getListName(childID,i);
        osList = new BufferedOutputStream(streamBuffer);
        osList->create(tempString.c_str());
        int counter = 0;
        if(i < cSize-1) {
            // Read in elements O(M)
            KeyValue** array = new KeyValue*[maxExternalBufferSize];
            while(counter < maxExternalBufferSize) {
                key = is1->readNext();
                val = is1->readNext();
                array[counter] = new KeyValue(key,val);
                counter++;
            }

            // Sort the internal array
            sortInternalArray(array,maxExternalBufferSize);

            // Output elements to new list
            for(int j = 0; j < maxExternalBufferSize; j++) {
                key = array[j]->key;
                val = array[j]->value;
                osList->write(&key);
                osList->write(&val);
            }

            // Clean up
            osList->close();
            iocounter = iocounter + osList->iocounter;
            delete(osList);

            for(int j = 0; j < maxExternalBufferSize; j++) {
                delete(array[j]);
            }
            delete[] array;

        }
        else {
            // Special case, second last list.
            // Split the remaining elements into two.
            int specialSize = (maxExternalBufferSize+remainder)/2;
            KeyValue** array = new KeyValue*[specialSize];

            while(counter < specialSize) {
                key = is1->readNext();
                val = is1->readNext();
                array[counter] = new KeyValue(key,val);
                counter++;
            }

            // Sort the internal array
            sortInternalArray(array,specialSize);

            // Output elements to new list
            for(int j = 0; j < specialSize; j++) {
                key = array[j]->key;
                val = array[j]->value;
                osList->write(&key);
                osList->write(&val);
            }

            osList->close();
            iocounter = iocounter + osList->iocounter;
            delete(osList);

            for(int j = 0; j < specialSize; j++) {
                delete(array[j]);
            }
            delete[] array;

            // Last list
            string specialString = getListName(childID,i+1);
            osList = new BufferedOutputStream(streamBuffer);
            osList->create(specialString.c_str());
            counter = 0;
            int verySpecialSize = (maxExternalBufferSize+remainder)-specialSize;
            array = new KeyValue*[verySpecialSize];

            while(counter < verySpecialSize) {
                key = is1->readNext();
                val = is1->readNext();
                array[counter] = new KeyValue(key,val);
                counter++;
            }

            // Sort the internal array
            sortInternalArray(array,verySpecialSize);

            // Output elements to new list
            for(int j = 0; j < verySpecialSize; j++) {
                key = array[j]->key;
                val = array[j]->value;
                osList->write(&key);
                osList->write(&val);
            }

            osList->close();
            iocounter = iocounter + osList->iocounter;
            delete(osList);

            for(int j = 0; j < verySpecialSize; j++) {
                delete(array[j]);
            }
            delete[] array;

        }

    }

    // Clean up inputstream
    is1->close();
    iocounter = iocounter + is1->iocounter;
    delete(is1);

    // ***** Rebuild this nodes fractional cascading

    // Special case for list 1
    is1 = new BufferedInputStream(streamBuffer);
    string firstString = getListName(childID,1);
    is1->open(firstString.c_str());

    string firstFrac = getFracListName(childID,1);
    if(FILE *file = fopen(firstFrac.c_str(), "r")) {
        fclose(file);
        remove(firstFrac.c_str());
    }
    os1 = new BufferedOutputStream(streamBuffer);
    os1->create(firstFrac.c_str());

    int counter = 0;
    int zero = 0;
    while(!is1->endOfStream()) {
        key = is1->readNext();
        val = is1->readNext();
        os1->write(&key);
        os1->write(&val);
        os1->write(&counter);
        os1->write(&zero);
        counter++;
    }

    os1->close();
    iocounter = iocounter + os1->iocounter;
    delete(os1);

    is1->close();
    iocounter = iocounter + is1->iocounter;
    delete(is1);

    // Remaining lists
    InputStream* list;
    InputStream* prevFrac;
    OutputStream* newFrac;
    for(int i = 2; i <= cSize; i++) {

        string listName = getListName(childID,i);
        list = new BufferedInputStream(streamBuffer);
        list->open(listName.c_str());

        string prevFracName = getFracListName(childID,i-1);
        prevFrac = new BufferedInputStream(streamBuffer);
        prevFrac->open(prevFracName.c_str());

        string newFracName = getFracListName(childID,i);
        if(FILE *file = fopen(newFracName.c_str(), "r")) {
            fclose(file);
            remove(newFracName.c_str());
        }
        newFrac = new BufferedOutputStream(streamBuffer);
        newFrac->create(newFracName.c_str());

        bool run = true;
        int keyList = list->readNext();
        int valueList = list->readNext();
        int keyFrac = prevFrac->readNext();
        int valueFrac = prevFrac->readNext();
        prevFrac->readNext();
        prevFrac->readNext();

        int listCounter = 0;
        int fracCounter = 0;

        // Now merge the two lists
        while(run) {

            if(keyList < keyFrac) {
                newFrac->write(&keyList);
                newFrac->write(&valueList);
                newFrac->write(&listCounter); // Place in original list
                newFrac->write(&(fracCounter)); // Place if we searched list below
                listCounter++;
                if(!list->endOfStream()) {
                    keyList = list->readNext();
                    valueList = list->readNext();
                }
                else {
                    if(fracCounter % 2 == 1) {
                        newFrac->write(&keyFrac);
                        newFrac->write(&valueFrac);
                        newFrac->write(&listCounter);
                        newFrac->write(&fracCounter);
                    }
                    fracCounter++;
                    break;
                }
            }
            else {
                if(fracCounter % 2 == 1) {
                    newFrac->write(&keyFrac);
                    newFrac->write(&valueFrac);
                    newFrac->write(&listCounter);
                    newFrac->write(&fracCounter);
                }
                fracCounter++;
                if(!prevFrac->endOfStream()) {
                    keyFrac = prevFrac->readNext();
                    valueFrac = prevFrac->readNext();
                    prevFrac->readNext();
                    prevFrac->readNext();
                }
                else {
                    newFrac->write(&keyList);
                    newFrac->write(&valueList);
                    newFrac->write(&listCounter); // Place in original list
                    newFrac->write(&(fracCounter)); // Place if we searched list below
                    listCounter++;
                    break;
                }
            }
        }

        // Finish writing out list
        if(!list->endOfStream()) {
            while(!list->endOfStream()) {
                keyList = list->readNext();
                valueList = list->readNext();
                newFrac->write(&keyList);
                newFrac->write(&valueList);
                newFrac->write(&listCounter); // Place in original list
                newFrac->write(&(fracCounter)); // Place if we searched list below
                listCounter++;
            }

        }
        else if(!prevFrac->endOfStream()) {
            while(!prevFrac->endOfStream()) {
                keyFrac = prevFrac->readNext();
                valueFrac = prevFrac->readNext();
                prevFrac->readNext();
                prevFrac->readNext();
                if(fracCounter % 2 == 1) {
                    newFrac->write(&keyFrac);
                    newFrac->write(&valueFrac);
                    newFrac->write(&listCounter);
                    newFrac->write(&fracCounter);
                }
                fracCounter++;
            }
        }

        // Clean up
        newFrac->close();
        iocounter = iocounter + newFrac->iocounter;
        delete(newFrac);

        list->close();
        iocounter = iocounter + list->iocounter;
        delete(list);

        prevFrac->close();
        iocounter = iocounter + prevFrac->iocounter;
        delete(prevFrac);
    }


    // Calculate newSize
    int newSize = counter2 / maxExternalBufferSize;
    remainder = counter2 % maxExternalBufferSize;
    if(remainder != 0) {
        newSize++;
    }

    numberOfNodes++;
    int newNodeID = numberOfNodes;

    // Build up new nodes lists with tempbuf2
    is1 = new BufferedInputStream(streamBuffer);
    is1->open(tempbuf2.c_str());
    // Iterate over all lists but the last one, special case.
    for(int i = 1; i < newSize; i++) {
        string tempString = getListName(newNodeID,i);
        if(FILE *file = fopen(tempString.c_str(), "r")) {
            fclose(file);
            remove(tempString.c_str());
        }
        osList = new BufferedOutputStream(streamBuffer);
        osList->create(tempString.c_str());
        int counter = 0;
        if(i < cSize-1) {
            // Read in elements O(M)
            KeyValue** array = new KeyValue*[maxExternalBufferSize];
            while(counter < maxExternalBufferSize) {
                key = is1->readNext();
                val = is1->readNext();
                array[counter] = new KeyValue(key,val);
                counter++;
            }

            // Sort the internal array
            sortInternalArray(array,maxExternalBufferSize);

            // Output elements to new list
            for(int j = 0; j < maxExternalBufferSize; j++) {
                key = array[j]->key;
                val = array[j]->value;
                osList->write(&key);
                osList->write(&val);
            }

            // Clean up
            osList->close();
            iocounter = iocounter + osList->iocounter;
            delete(osList);

            for(int j = 0; j < maxExternalBufferSize; j++) {
                delete(array[j]);
            }
            delete[] array;

        }
        else {
            // Special case, second last list.
            // Split the remaining elements into two.
            int specialSize = (maxExternalBufferSize+remainder)/2;
            KeyValue** array = new KeyValue*[specialSize];

            while(counter < specialSize) {
                key = is1->readNext();
                val = is1->readNext();
                array[counter] = new KeyValue(key,val);
                counter++;
            }

            // Sort the internal array
            sortInternalArray(array,specialSize);

            // Output elements to new list
            for(int j = 0; j < specialSize; j++) {
                key = array[j]->key;
                val = array[j]->value;
                osList->write(&key);
                osList->write(&val);
            }

            osList->close();
            iocounter = iocounter + osList->iocounter;
            delete(osList);

            for(int j = 0; j < specialSize; j++) {
                delete(array[j]);
            }
            delete[] array;

            // Last list
            string specialString = getListName(newNodeID,i+1);
            osList = new BufferedOutputStream(streamBuffer);
            osList->create(specialString.c_str());
            counter = 0;
            int verySpecialSize = (maxExternalBufferSize+remainder)-specialSize;
            array = new KeyValue*[verySpecialSize];

            while(counter < verySpecialSize) {
                key = is1->readNext();
                val = is1->readNext();
                array[counter] = new KeyValue(key,val);
                counter++;
            }

            // Sort the internal array
            sortInternalArray(array,verySpecialSize);

            // Output elements to new list
            for(int j = 0; j < verySpecialSize; j++) {
                key = array[j]->key;
                val = array[j]->value;
                osList->write(&key);
                osList->write(&val);
            }

            osList->close();
            iocounter = iocounter + osList->iocounter;
            delete(osList);

            for(int j = 0; j < verySpecialSize; j++) {
                delete(array[j]);
            }
            delete[] array;

        }

    }

    is1->close();
    iocounter = iocounter + is1->iocounter;
    delete(is1);

    // Build up new nodes fractional cascading

    // Special case for list 1
    is1 = new BufferedInputStream(streamBuffer);
    string first = getListName(newNodeID,1);
    is1->open(first.c_str());

    string firstF = getFracListName(newNodeID,1);
    if(FILE *file = fopen(firstF.c_str(), "r")) {
        fclose(file);
        remove(firstF.c_str());
    }
    os1 = new BufferedOutputStream(streamBuffer);
    os1->create(firstF.c_str());

    counter = 0;
    zero = 0;
    while(!is1->endOfStream()) {
        key = is1->readNext();
        val = is1->readNext();
        os1->write(&key);
        os1->write(&val);
        os1->write(&counter);
        os1->write(&zero);
        counter++;
    }

    os1->close();
    iocounter = iocounter + os1->iocounter;
    delete(os1);

    is1->close();
    iocounter = iocounter + is1->iocounter;
    delete(is1);

    for(int i = 2; i <= newSize; i++) {

        string listName = getListName(newNodeID,i);
        list = new BufferedInputStream(streamBuffer);
        list->open(listName.c_str());

        string prevFracName = getFracListName(newNodeID,i-1);
        prevFrac = new BufferedInputStream(streamBuffer);
        prevFrac->open(prevFracName.c_str());

        string newFracName = getFracListName(newNodeID,i);
        if(FILE *file = fopen(newFracName.c_str(), "r")) {
            fclose(file);
            remove(newFracName.c_str());
        }
        newFrac = new BufferedOutputStream(streamBuffer);
        newFrac->create(newFracName.c_str());

        bool run = true;
        int keyList = list->readNext();
        int valueList = list->readNext();
        int keyFrac = prevFrac->readNext();
        int valueFrac = prevFrac->readNext();
        prevFrac->readNext();
        prevFrac->readNext();

        int listCounter = 0;
        int fracCounter = 0;

        // Now merge the two lists
        while(run) {

            if(keyList < keyFrac) {
                newFrac->write(&keyList);
                newFrac->write(&valueList);
                newFrac->write(&listCounter); // Place in original list
                newFrac->write(&(fracCounter)); // Place if we searched list below
                listCounter++;
                if(!list->endOfStream()) {
                    keyList = list->readNext();
                    valueList = list->readNext();
                }
                else {
                    if(fracCounter % 2 == 1) {
                        newFrac->write(&keyFrac);
                        newFrac->write(&valueFrac);
                        newFrac->write(&listCounter);
                        newFrac->write(&fracCounter);
                    }
                    fracCounter++;
                    break;
                }
            }
            else {
                if(fracCounter % 2 == 1) {
                    newFrac->write(&keyFrac);
                    newFrac->write(&valueFrac);
                    newFrac->write(&listCounter);
                    newFrac->write(&fracCounter);
                }
                fracCounter++;
                if(!prevFrac->endOfStream()) {
                    keyFrac = prevFrac->readNext();
                    valueFrac = prevFrac->readNext();
                    prevFrac->readNext();
                    prevFrac->readNext();
                }
                else {
                    newFrac->write(&keyList);
                    newFrac->write(&valueList);
                    newFrac->write(&listCounter); // Place in original list
                    newFrac->write(&(fracCounter)); // Place if we searched list below
                    listCounter++;
                    break;
                }
            }
        }

        // Finish writing out list
        if(!list->endOfStream()) {
            while(!list->endOfStream()) {
                keyList = list->readNext();
                valueList = list->readNext();
                newFrac->write(&keyList);
                newFrac->write(&valueList);
                newFrac->write(&listCounter); // Place in original list
                newFrac->write(&(fracCounter)); // Place if we searched list below
                listCounter++;
            }

        }
        else if(!prevFrac->endOfStream()) {
            while(!prevFrac->endOfStream()) {
                keyFrac = prevFrac->readNext();
                valueFrac = prevFrac->readNext();
                prevFrac->readNext();
                prevFrac->readNext();
                if(fracCounter % 2 == 1) {
                    newFrac->write(&keyFrac);
                    newFrac->write(&valueFrac);
                    newFrac->write(&listCounter);
                    newFrac->write(&fracCounter);
                }
                fracCounter++;
            }
        }

        // Clean up
        newFrac->close();
        iocounter = iocounter + newFrac->iocounter;
        delete(newFrac);

        list->close();
        iocounter = iocounter + list->iocounter;
        delete(list);

        prevFrac->close();
        iocounter = iocounter + prevFrac->iocounter;
        delete(prevFrac);
    }


    // Clean up tempbuf1 and tempbuf2
    if(FILE *file = fopen(tempbuf1.c_str(), "r")) {
        fclose(file);
        remove(tempbuf1.c_str());
    }
    if(FILE *file = fopen(tempbuf2.c_str(), "r")) {
        fclose(file);
        remove(tempbuf2.c_str());
    }

    // Write out new child
    writeNodeInfo(newNodeID,1,newSize,0);

    // Add node to parent
    keys->push_back(0);
    for(int i = nodeSize; i > childNumber; i--) {
        (*keys)[i] = (*keys)[i-1];
    }
    (*keys)[childNumber] = medianOfMedians;

    values->push_back(0);
    for(int i = nodeSize+1; i > childNumber; i--) {
        (*values)[i] = (*values)[i-1];
    }
    (*values)[childNumber+1] = newNodeID;


    // Write out old child
    writeNodeInfo(childID,1,cSize,0);

    cout << "=== Finishing up SplitLeaf\n";
    cout << nodeSize << "\n";
    cout << cSize << "\n";
    cout << newSize << "\n";
    cout << "--- Keys\n";
    for(int i = 0; i < nodeSize+1; i++) {
        cout << (*keys)[i] << "\n";
    }
    cout << "--- Values\n";
    for(int i = 0; i < nodeSize+2; i++) {
        cout << (*values)[i] << "\n";
    }



    cout << "=== Finished SplitLeaf\n";

    return cSize;

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

int TruncatedBufferTree::readBuffer(int id, KeyValue **buffer, int type) {

    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = getBufferName(id,type);
    is->open(name.c_str());
    int newKey,newValue;
    int i = 0;
    while(!is->endOfStream()) {
        newKey = is->readNext();
        newValue = is->readNext();
        buffer[i] = new KeyValue(newKey,newValue);
        i++;
    }
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);
    return i;


}

void TruncatedBufferTree::writeBuffer(int id, KeyValue **buffer, int bSize, int type) {
    // TODO
}

int TruncatedBufferTree::sortAndRemoveDuplicatesExternalBuffer(int id, int bufferSize, int sortedSize) {
    // TODO
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

/*
 * Name of original lists associated with a node.
 */
std::string TruncatedBufferTree::getListName(int id, int list) {

    string name = "Node";
    name += to_string(id);
    name += "List";
    name += to_string(list);
    return name;
}

/*
 * Name of the fractional lists associated with a node.
 */
std::string TruncatedBufferTree::getFracListName(int id, int list) {

    string name = "Frac";
    name += to_string(id);
    name += "List";
    name += to_string(list);
    return name;
}

void TruncatedBufferTree::printTree() {
    printNode(root);
}

void TruncatedBufferTree::printNode(int id) {

    cout << "===== Node " << id << "\n";

    int height,nodeSize,bufferSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* ptr_bufferSize = &bufferSize;
    readNodeInfo(id,ptr_height,ptr_nodeSize,ptr_bufferSize);

    cout << "Height " << height << "\n";
    cout << "Nodesize " << nodeSize << "\n";

    if(height > 1) {

        vector<int>* keys = new vector<int>();
        vector<int>* values = new vector<int>();
        readNode(id,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

        cout << "--- Keys\n";
        for(int i = 0; i < nodeSize; i++) {
            cout << (*keys)[i] << "\n";
        }
        cout << "--- Values\n";
        for(int i = 0; i < nodeSize+1; i++) {
            cout << (*values)[i] << "\n";
        }

        cout << "--- Buffer\n";
        if(bufferSize > 0) {
            KeyValue** buffer = new KeyValue*[bufferSize];
            readBuffer(id,buffer,1);
            for(int i = 0; i < bufferSize; i++) {
                cout << buffer[i]->key << " " << buffer[i]->value <<"\n";
            }

            // Clean up
            for(int i = 0; i < bufferSize; i++) {
                delete(buffer[i]);
            }
            delete[] buffer;
        }

        delete(keys);

        if(nodeSize > 0 || (*values)[0] != 0) {
            for(int i = 0; i < nodeSize+1; i++) {
                printNode((*values)[i]);
            }
        }
        delete(values);
    }
    else {

        cout << "--- Buffer\n";
        if(bufferSize > 0) {
            KeyValue** buffer = new KeyValue*[bufferSize];
            readBuffer(id,buffer,1);
            for(int i = 0; i < bufferSize; i++) {
                cout << buffer[i]->key << " " << buffer[i]->value <<"\n";
            }

            // Clean up
            for(int i = 0; i < bufferSize; i++) {
                delete(buffer[i]);
            }
            delete[] buffer;
        }

        cout << "--- Lists\n";
        for(int i = 1; i <= nodeSize; i++) {
            cout << "--- List " << i << "\n";
            InputStream* is1 = new BufferedInputStream(streamBuffer);
            string temp = getListName(id,i);
            is1->open(temp.c_str());
            int key,value;
            while(!is1->endOfStream()) {
                key = is1->readNext();
                value = is1->readNext();
                cout << key << " " << value << "\n";
            }
            is1->close();
            delete(is1);
        }

        cout << "--- Fracs\n";
        for(int i = 1; i <= nodeSize; i++) {
            cout << "--- Frac " << i << "\n";
            InputStream* is1 = new BufferedInputStream(streamBuffer);
            string temp = getFracListName(id,i);
            is1->open(temp.c_str());
            int key,value,pointer1,pointer2;
            while(!is1->endOfStream()) {
                key = is1->readNext();
                value = is1->readNext();
                pointer1 = is1->readNext();
                pointer2 = is1->readNext();
                cout << key << " " << value << " " << pointer1 << " " << pointer2 << "\n";
            }
            is1->close();
            delete(is1);
        }
    }
}