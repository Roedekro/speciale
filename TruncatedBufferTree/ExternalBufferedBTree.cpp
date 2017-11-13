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
    cout << "Size is " << size << "\n";
    leafSize = B/(sizeof(KeyValueTime)*4); // Min leafSize, max 4x leafSize.
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
    os->write(&height); // Height
    int zero = 0;
    os->write(&zero); // Nodesize
    os->write(&zero); // BufferSize
    int two = 2;
    os->write(&two);
    os->close();
    iocounter = os->iocounter;
    delete(os);

    // Write leaf info for node
    vector<int>* leafs = new vector<int>(1);
    (*leafs)[0] = 0;
    writeLeafInfo(1,leafs,1);

    cout << "Tree will have between " << size << " and " << (4*size-1) << " fanout\n";
    cout << "Tree will have between " << leafSize << " and " << 4*leafSize << " leaf size\n";
    cout << "Max insert buffer size is " << maxBufferSize << " and max external buffer size " << maxExternalBufferSize << "\n";

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

    forcedFlushed = false;
    treeTime++;
    keyValueTime->time = treeTime;
    update_buffer[update_bufferSize] = keyValueTime;
    update_bufferSize++;

    if(update_bufferSize >= maxBufferSize) {

        //cout << "Insert buffer overflow\n";

        // Sort the buffer and remove duplicates
        update_bufferSize = sortAndRemoveDuplicatesInternalArray(update_buffer,update_bufferSize);

        // Append to root
        appendBufferNoDelete(root,update_buffer,update_bufferSize,1);
        rootBufferSize = rootBufferSize + update_bufferSize;

        // Did roots buffer overflow?
        if(rootBufferSize > maxExternalBufferSize) {

            //cout << "Root buffer overflow\n";
            //cout << "Root buffer size is " << rootBufferSize << "\n";

            // Sort buffer, remove duplicates
            int rootSize = sortAndRemoveDuplicatesExternalBuffer(root,rootBufferSize,update_bufferSize);
            //cout << "Roots buffer after sortAndRemoveDuplicatesExternalBuffer is " << rootSize << "\n";

            // Load in entire root, as regardless of the outcome we will need keys and values.
            int height, nodeSize, bufferSize;
            int* ptr_height = &height;
            int* ptr_nodeSize = &nodeSize;
            int* ptr_bufferSize = &bufferSize;
            vector<int>* keys = new vector<int>();
            keys->reserve(size*4-1);
            vector<int>* values = new vector<int>();
            values->reserve(size*4);

            readNode(root,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

            //cout << "Flushing to children from Root\n";
            // Flush to children.
            // Check special case empty leaf
            if(height == 1 && nodeSize == 0) {
                //cout << "Root empty \n";
                // Read in leaf sizes, just to be sure
                // In practice it will be 0 tho
                vector<int>* leafs = new vector<int>();
                readLeafInfo(root,leafs);
                if(leafs->at(0) == 0) {
                    handleRootEmptyLeafBufferOverflow(root,ptr_nodeSize,keys,values,leafs);
                    // Last leaf might be too small, check
                    if((*leafs)[nodeSize] < leafSize) {
                        // Fuse
                        fuseLeaf(nodeSize,keys,values,leafs,nodeSize);
                        nodeSize--;
                        // Check if split
                        if((*leafs)[nodeSize] > 4*leafSize) {
                            splitLeaf(nodeSize,keys,values,leafs,nodeSize);
                            nodeSize++;
                        }
                    }
                    /*cout << "Root updated to\n";
                    cout << nodeSize << "\n";
                    cout << "---Keys\n";
                    for(int i = 0; i < nodeSize; i++) {
                        cout << (*keys)[i] << "\n";
                    }

                    cout << "---Values\n";
                    for(int i = 0; i < nodeSize+1; i++) {
                        cout << (*values)[i] << "\n";
                    }

                    cout << "---Leafs\n";
                    for(int i = 0; i < nodeSize+1; i++) {
                        cout << (*leafs)[i] << "\n";
                    }*/

                    writeNode(root,1,nodeSize,0,keys,values);
                    writeLeafInfo(root,leafs,nodeSize+1);
                }
                else {
                    delete(leafs);
                    flush(root,height,nodeSize,keys,values);
                }
            }
            else {
                flush(root,height,nodeSize,keys,values);
            }

            //cout << "Finished root flush\n";

            // Check if we need to create a new root
            keys = new vector<int>();
            keys->reserve(size*4-1);
            values = new vector<int>();
            values->reserve(size*4);
            readNode(root,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);
            if(nodeSize > size*4-1) {
                //cout << "Creating new Root\n";
                // Create new root
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
                // We might have to split again, recheck
                if(ret > 4*size-1) {
                    //cout << "Splitting roots children\n";
                    // Loop over children until no more splits
                    for(int i = 0; i < newSize+1; i++) {
                        //cout << "Checking child " << i << " name " << (*newValues)[i] << "\n";
                        /*cout << "Root values ";
                        for(int j = 0; j < newSize+1; j++) {
                            cout << (*newValues)[j] << " ";
                        }
                        cout << "\n";*/

                        keys = new vector<int>();
                        keys->reserve(size*4-1);
                        values = new vector<int>();
                        values->reserve(size*4);
                        readNode((*newValues)[i],ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

                        /*cout << "Splitting child " << (*newValues)[i] << "\n";
                        cout << "Root updated to\n";
                        cout << nodeSize << "\n";
                        cout << "---Keys\n";
                        for(int i = 0; i < nodeSize; i++) {
                            cout << (*keys)[i] << "\n";
                        }

                        cout << "---Values\n";
                        for(int i = 0; i < nodeSize+1; i++) {
                            cout << (*values)[i] << "\n";
                        }

                        bool special = false;
                        if(nodeSize == 8 && (*newValues)[i] == 1) {
                            special = true;
                        }*/


                        if(nodeSize > 4*size-1) {
                            //cout << "Splitting child " << (*newValues)[i] << "\n";
                            ret = splitInternal(newHeight,newSize,newKeys,newValues,i,nodeSize,keys,values);
                            //cout << "Childs new size is " << ret << "\n";
                            newSize++;
                            if(ret > 4*size-1) {
                                i--; // Recurse
                            }
                            else {
                                i++; // We know the next child is fine.
                            }
                        }

                        /*if(special) {

                            keys = new vector<int>();
                            keys->reserve(size*4-1);
                            values = new vector<int>();
                            values->reserve(size*4);
                            readNode((*newValues)[i-1],ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

                            cout << "SPECIAL child " << (*newValues)[i-1] << "\n";
                            cout << "Root updated to\n";
                            cout << nodeSize << "\n";
                            cout << "---Keys\n";
                            for(int j = 0; j < nodeSize; j++) {
                                cout << (*keys)[j] << "\n";
                            }

                            cout << "---Values\n";
                            for(int j = 0; j < nodeSize+1; j++) {
                                cout << (*values)[j] << "\n";
                            }
                        }*/
                    }
                }
                //cout << "Writing new root to disk\n";
                writeNode(newRoot,newHeight,newSize,0,newKeys,newValues);
            }
            else if(nodeSize == 0 && height > 1) {
                cout << "Deleting Root\n";
                // Delete this root, make only child new root
                cleanUpExternallyAfterNodeOrLeaf(root);
                root = (*values)[0];
                currentNumberOfNodes--;
                delete(keys);
                delete(values);
            }
            else {
                delete(keys);
                delete(values);
            }
            rootBufferSize = 0;
        }
        // Reset internal buffer
        //update_buffer = new KeyValueTime*[maxBufferSize];
        update_bufferSize = 0;
    }
}

/*
 * Returns -1 if element not present.
 */
int ExternalBufferedBTree::query(int element) {

    // Flush the entire tree before we start to query.
    if(!forcedFlushed) {
        flushEntireTree();
    }

    int currentNode = root;
    int height, nodeSize, bufferSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* ptr_bufferSize = &bufferSize;
    vector<int>* keys = new vector<int>();
    keys->reserve(4*size-1);
    vector<int>* values = new vector<int>();
    values->reserve(4*size);
    readNode(currentNode, ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

    // Search for correct leaf node
    while(height != 1) {
        // Find matching key
        int i = nodeSize;
        while(i > 0 && element <= (*keys)[i-1]) {
            i--;
        }
        currentNode = (*values)[i];
        delete(keys);
        delete(values);
        keys = new vector<int>();
        keys->reserve(4*size-1);
        values = new vector<int>();
        values->reserve(4*size);
        readNode(currentNode, ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);
    }

    int i = nodeSize;
    while(i > 0 && element <= (*keys)[i-1]) {
        i--;
    }
    int leaf = (*values)[i];

    vector<int>* leafs = new vector<int>();
    leafs->reserve(size*4);
    readLeafInfo(currentNode,leafs);

    //cout << maxBufferSize << " " << (*leafs)[i] << "\n";

    KeyValueTime** buffer = new KeyValueTime*[maxBufferSize];
    readLeaf(leaf,buffer);

    int ret = -1;
    int index = 0;
    int leafSize = (*leafs)[i];
    while(index < leafSize  && element != buffer[index]->key) {
        index++;
    }
    //cout << "Leaf " << leaf << " " << (*leafs)[i] << " " << index << "\n";
    //cout << index << " " << (*leafs)[i] << "\n";
    if(index < (*leafs)[i] && element == buffer[index]->key) {
        ret = buffer[index]->value;
    }

    if(ret == -1) {
        cout << "Didnt find " << element << " in " << leaf << "\n";
        for(int j = 0; j < leafSize; j++) {
            cout << buffer[j]->key << "\n";
        }
        cout << "---\n";
    }

    // Clean up
    for(int j = 0; j < (*leafs)[i]; j++) {
        delete(buffer[j]);
    }
    delete[] buffer;
    delete(leafs);
    delete(keys);
    delete(values);

    return ret;

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
void ExternalBufferedBTree::flush(int id, int height, int nodeSize, vector<int> *keys, vector<int> *values) {

    //cout << "Flushing node " << id << "\n";

    if(height == 1) {

        vector<int>* leafs = new vector<int>();
        leafs->reserve(4*size);
        int* ptr_nodeSize = &nodeSize;
        readLeafInfo(id,leafs);
        handleLeafNodeBufferOverflow(id,ptr_nodeSize,keys,values,leafs);

        if((*leafs)[nodeSize] < leafSize) {
            fuseLeaf(nodeSize,keys,values,leafs,nodeSize);
            nodeSize--;
            if((*leafs)[nodeSize] > 4*leafSize) {
                splitLeaf(nodeSize,keys,values,leafs,nodeSize);
                nodeSize++;
            }
        }

        /*for(int i = 0; i < nodeSize+1; i++) {
            if((*leafs)[i] == 0 || (*leafs)[i] > 4*leafSize) {
                cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!! " << (*leafs)[i] << "\n";
            }
        }*/


        writeNode(id,height,nodeSize,0,keys,values);
        writeLeafInfo(id,leafs,nodeSize+1);


        /* Handle it differently now.



        //cout << "Node is a leaf node\n";
        // Children are leafs, special case
        InputStream *parentBuffer = new BufferedInputStream(streamBuffer);
        string name = getBufferName(id, 1);
        parentBuffer->open(name.c_str());
        //cout << "Opened parent stream\n";

        int* appendedToLeafs = new int[nodeSize+1](); // Initialized to zero

        if(nodeSize == 0) {
            // Only one child, give it the entire buffer
            //cout << "Only child is " << (*values)[0] << "\n";
            BufferedOutputStream* os = new BufferedOutputStream(streamBuffer);
            name = getBufferName((*values)[0],4);
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
            BufferedOutputStream **appendStreams = new BufferedOutputStream*[nodeSize + 1];
            BufferedOutputStream *os;
            for (int i = 0; i < nodeSize + 1; i++) {
                name = getBufferName((*values)[i], 4);
                os = new BufferedOutputStream(streamBuffer);
                os->open(name.c_str());
            }

            int childIndex = 0;
            int key, value, time;
            while (!parentBuffer->endOfStream()) {
                key = parentBuffer->readNext();
                value = parentBuffer->readNext();
                time = parentBuffer->readNext();
                if (key > (*keys)[childIndex]) {
                    // Find new index
                    while (key > (*keys)[childIndex] && childIndex < nodeSize) {
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

        //int* leafs = new int[4*size*4](); // Cant split into more than 4x max size
        vector<int>* leafs = new vector<int>();
        leafs->reserve(size*4); // Reserve M/B.

        readLeafInfo(id,leafs); // Leafs now contains previous size of each leaf

        //cout << "Flushing leafs\n";
        //cout << "Leaf info " << (*leafs)[0] << " " << leafs->size() << " " << leafs->capacity() << "\n";
        //cout << "Keys info " << keys->size() << " " << keys->capacity() << "\n";
        //cout << "Values info " << values->size() << " " << values->capacity() << "\n";

        // Update size of every leaf, sort and remove duplicates
        for(int i = 0; i < nodeSize+1; i++) {
            //cout << "Leaf " << i << " original size " << (*leafs)[i] << " appended " << appendedToLeafs[i] << "\n";
            // Update size
            (*leafs)[i] = appendedToLeafs[i] + (*leafs)[i];
            if(appendedToLeafs[i] > 0) {
                // Sort and remove duplicates
                // Check if it can be done internally
                //cout << (*leafs)[i] << " " << M/sizeof(KeyValueTime) << "\n";
                if((*leafs)[i] <= M/sizeof(KeyValueTime)) {
                    //cout << "Internal\n";
                    // Internal, load in elements
                    KeyValueTime** temp = new KeyValueTime*[(*leafs)[i]];
                    //cout << "Attempting to read in leaf " << (*values)[i] << " size " << (*leafs)[i] << "\n";
                    readLeaf((*values)[i], temp);
                    (*leafs)[i] = sortAndRemoveDuplicatesInternalArrayLeaf(temp,(*leafs)[i]);
                    writeLeaf((*values)[i],temp,(*leafs)[i]);
                }
                else {
                    //cout << "External\n";
                    // External
                    // Handle originally empty leaf
                    if((*leafs)[i] - appendedToLeafs[i] > 0) {
                        (*leafs)[i] = sortAndRemoveDuplicatesExternalLeaf(id,(*leafs)[i],appendedToLeafs[i]);
                        // Leaf was output by the above method.
                    }
                    else {
                        // There was no original elements in the leaf.
                        // Leaf now consits of appended elements in order of key and time,
                        // but we need to remove deletes and corresponding inserts.

                        handleDeletesLeafOriginallyEmptyExternal((*values)[i]);
                    }
                }
            }
        }

        delete[] appendedToLeafs;


        //cout << "=== Splittin\n";
        //cout << (*leafs)[0] << "\n";
        //cout << "Leaf capacity " << leafs->capacity() << "\n";
        // Split first, so we can guarantee fuses doesnt fuse with HUGE nodes.
        // Remove empty elements before fuse phase,
        // since they are hard to handle for fuses.
        for(int i = 0; i < nodeSize+1; i++) {
            if((*leafs)[i] > 4*leafSize) {
                //cout << "Split START " << i << "\n";
                splitLeaf(nodeSize, keys, values, leafs, i);
                //cout << "Split STOP\n";
                nodeSize++;
                // Leafs array will be updated in size
                // Check if we need to recurse upon leaf again
                if((*leafs)[i] > 4*leafSize) {
                    // Recheck child
                    i--;
                }
                else {
                    // Next child will be proper.
                    i++;
                }
            }
            else if((*leafs)[i] == 0) {
                //T-NOT-ODO: Check if this causes any issues
                // Erase this element, fuses below cant handle empty leafs.
                if(i != nodeSize) {
                    keys->erase(keys->begin() + i);
                }
                values->erase(values->begin() + i);
                leafs->erase(leafs->begin() + i);
                nodeSize--;
                i--;
            }
        }

        //cout << "=== Fusing\n";
        // Fuse
        for(int i = 0; i < nodeSize+1; i++) {
            if((*leafs)[i] < leafSize) {
                // Fuse
                int ret = fuseLeaf(nodeSize, keys, values, leafs, i);
                // -1 = fused from left neighbour
                // 0 = stole from one of the neighbours
                // 1 = fused with the right neighbour
                if (ret == -1) {
                    // Fused into previous child
                    // Recheck this position
                    nodeSize--;
                    i--; // Minimum recheck this position
                    // Check if previous child needs to be split
                    // With leftmost child safeguard
                    if (i >= 0 && (*leafs)[i] > leafSize * 4) {
                        splitLeaf(nodeSize, keys, values, leafs, i);
                        nodeSize++;
                        i++;
                    }
                } else if (ret == 1) {
                    // Fused next child into this one
                    nodeSize--;
                    // Check if we need to split or fuse
                    if ((*leafs)[i] > leafSize * 4) {
                        splitLeaf(nodeSize, keys, values, leafs, i);
                        nodeSize++;
                        i++;
                    } else if ((*leafs)[i] < leafSize) {
                        i--; // Fuse this leaf again
                    }
                }
            }
        }

        // Write out parents info to disk
        writeNode(id,height,nodeSize,0,keys,values);

        // Write out updated leaf info
        writeLeafInfo(id,leafs,nodeSize+1);

         */
    }
    else {
        // Children are internal nodes.
        // Flush parents buffer to childrens buffers.

        InputStream *parentBuffer = new BufferedInputStream(streamBuffer);
        string name = getBufferName(id, 1);
        parentBuffer->open(name.c_str());

        /*cout << "Writing out childrens buffer names\n";
        for(int i = 0; i < nodeSize+1; i++) {
            cout << getBufferName((*values)[i],1) << "\n";
        }*/


        //cout << "Creating OutputStreams\n";
        // Open streams to every childs buffer so we can append
        BufferedOutputStream** appendStreams = new BufferedOutputStream*[nodeSize+1];
        for(int i = 0; i < nodeSize+1; i++) {
            string newname = getBufferName((*values)[i],1);
            const char* newchar = newname.c_str();
            appendStreams[i] = new BufferedOutputStream(streamBuffer);
            appendStreams[i]->open(newchar);
            /*cout << newname << " " << newchar << "\n";
            if(FILE *file = fopen(newname.c_str(), "r")) {
                cout << "File " << newname << " exists\n";
                fclose(file);
            }*/
            /*os = new BufferedOutputStream(streamBuffer);
            os->open(name.c_str()); // Append
            appendStreams[i] = os;*/
        }

        /*cout << "Writing out streams\n";
        for(int i = 0; i < nodeSize+1; i++) {
            cout << appendStreams[i]->name << "\n";
        }*/

        // Append KeyValueTimes to children
        // Remember how many KVTs we wrote to each
        int* appendedToChild = new int[nodeSize+1](); // Initialized to zero
        int childIndex = 0;
        int key, value, time;
        while(!parentBuffer->endOfStream()) {
            key = parentBuffer->readNext();
            value = parentBuffer->readNext();
            time = parentBuffer->readNext();
            if(key > (*keys)[childIndex]) {
                // Find new index
                while(key > (*keys)[childIndex] && childIndex < nodeSize) {
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
            //cout << "Closing stream " << i << "\n";
            appendStreams[i]->close();
            iocounter = iocounter + appendStreams[i]->iocounter;
            delete(appendStreams[i]);
            /*os = appendStreams[i];
            os->close();
            iocounter = iocounter + os->iocounter;
            delete(os);*/
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
            cKeys = new vector<int>();
            cKeys->reserve(size*4-1);
            cValues = new vector<int>();
            cValues->reserve(size*4);
            readNode((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
            cBufferSize = cBufferSize + appendedToChild[i];
            if(cBufferSize > maxExternalBufferSize) {
                // Sort childs buffer
                sortAndRemoveDuplicatesExternalBuffer((*values)[i],cBufferSize,appendedToChild[i]);
                // Flush
                flush((*values)[i],cHeight,cNodeSize,cKeys,cValues);
                // Child will update its info at end of flush
            }
            else {
                // Just update bufferSize
                writeNodeInfo((*values)[i],cHeight,cNodeSize,cBufferSize);
            }
        }

        //cout << "Splitting children of node " << id << "\n";
        // Run through the children and split
        for(int i = 0; i < nodeSize+1; i++) {
            cKeys = new vector<int>();
            cKeys->reserve(size*4-1);
            cValues = new vector<int>();
            cValues->reserve(size*4);
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
            else if(cNodeSize == 0) {
                // Check if its truly empty, or has a single child we can fuse
                // TODO: Check if truly empty or has a single child we can fuse
            }
        }

        //cout << "Fusing children of node " << id << "\n";
        // Run through children and fuse
        for(int i = 0; i < nodeSize+1; i++) {
            cKeys = new vector<int>();
            cKeys->reserve(size*4-1);
            cValues = new vector<int>();
            cValues->reserve(size*4);
            readNode((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
            if(cNodeSize < size) {
                // Fuse
                int ret = fuseInternal(height,nodeSize,keys,values,i,ptr_cNodeSize,cKeys,cValues);
                // -1 = fused with left neighbour, check size.
                // 0 = stole elements from a neighbour, which is fine
                // 1 = fused with right neighbour, check size.
                if(ret == -1) {
                    // Fused with left neighbour, already checked.
                    // Therefore child can not be too small, check if too large
                    nodeSize--;
                    i--; // As a minimum we need to recheck this position with a new child.
                    if(cNodeSize > 4*size-1) {
                        // Split left neighbour
                        readNode((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
                        splitInternal(height,nodeSize,keys,values,i,cNodeSize,cKeys,cValues);
                        nodeSize++;
                        i++;
                    }
                }
                else if(ret == 1) {
                    // Fused with right child, check size.
                    nodeSize--;
                    if(cNodeSize > 4*size-1) {
                        readNode((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
                        splitInternal(height,nodeSize,keys,values,i,cNodeSize,cKeys,cValues);
                        nodeSize++;
                        i++;
                    }
                    else if(cNodeSize < size) {
                        i--; // Recurse
                    }
                }
            }
        }

        // Write out parents info to disk
        writeNode(id,height,nodeSize,0,keys,values);

        // Clean up vectors
        delete(cKeys);
        delete(cValues);
    }
}

/*
 * As above, except we continuously flush buffers in the entire tree.
 */
void ExternalBufferedBTree::forcedFlush(int id, int height, int nodeSize, int bufferSize, std::vector<int> *keys,
                                        std::vector<int> *values) {

    //cout << "Force Flushing node " << id << " with bufferSize " << bufferSize << "\n";

    if(height == 1) {

        if(bufferSize != 0) {

            vector<int>* leafs = new vector<int>();
            leafs->reserve(4*size);
            int* ptr_nodeSize = &nodeSize;
            readLeafInfo(id,leafs);

            //cout  << id << " " << leafs->size() << " " << nodeSize+1 << "\n";
            /*cout << "Prior to handleLeafBufferOverflow on node " << id << "\n";
            cout << "---Leaf info\n";
            for(int i = 0; i < leafs->size(); i++) {
                cout << leafs->at(i) << "\n";
            }
            cout << "---\n";*/

            handleLeafNodeBufferOverflow(id,ptr_nodeSize,keys,values,leafs);

            /*cout  << id << " " << leafs->size() << " " << nodeSize+1 << "\n";

            cout << "---Leaf info\n";
            for(int i = 0; i < leafs->size(); i++) {
                cout << leafs->at(i) << "\n";
            }
            cout << "---\n";
             */

            //cout << (*leafs)[nodeSize] << " " << (*values)[nodeSize] << "\n";
            if((*leafs)[nodeSize] < leafSize) {
                fuseLeaf(nodeSize,keys,values,leafs,nodeSize);
                nodeSize--;
                if((*leafs)[nodeSize] > 4*leafSize) {
                    //cout << (*leafs)[nodeSize] << " " << (*values)[nodeSize] << "\n";
                    splitLeaf(nodeSize,keys,values,leafs,nodeSize);
                    nodeSize++;
                }
            }

            /*if(leafs->size() != nodeSize+1) {
                cout << "Error after buffer overflow in node " << id <<"\n";
                for(int j = 0; j < nodeSize; j++) {
                    cout << (*keys)[j] << "\n";
                }

                cout << "---Values\n";
                for(int j = 0; j < nodeSize+1; j++) {
                    cout << (*values)[j] << "\n";
                }
            }
            cout << "---\n";*/

            writeNode(id,height,nodeSize,0,keys,values);
            //cout << leafs->size() << " " << nodeSize+1 << "\n";
            writeLeafInfo(id,leafs,nodeSize+1);
        }



        /* Handle it different now.

        //cout << "Node is a leaf node\n";

        int* appendedToLeafs = new int[nodeSize+1](); // Initialized to zero

        if(bufferSize != 0) {
            // Children are leafs, special case
            InputStream *parentBuffer = new BufferedInputStream(streamBuffer);
            string name = getBufferName(id, 1);
            parentBuffer->open(name.c_str());
            //cout << "Opened parent stream\n";

            if(nodeSize == 0) {
                // Only one child, give it the entire buffer
                //cout << "Only child is " << (*values)[0] << "\n";
                BufferedOutputStream* os = new BufferedOutputStream(streamBuffer);
                name = getBufferName((*values)[0],4);
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
                BufferedOutputStream **appendStreams = new BufferedOutputStream*[nodeSize + 1];
                BufferedOutputStream *os;
                for (int i = 0; i < nodeSize + 1; i++) {
                    string newname = getBufferName((*values)[i], 4);
                    const char* newchar = newname.c_str();
                    appendStreams[i] = new BufferedOutputStream(streamBuffer);
                    appendStreams[i]->open(newchar);
                    /*name = getBufferName((*values)[i], 4);
                    os = new BufferedOutputStream(streamBuffer);
                    os->open(name.c_str());*/
                /*}

                int childIndex = 0;
                int key, value, time;
                while (!parentBuffer->endOfStream()) {
                    key = parentBuffer->readNext();
                    value = parentBuffer->readNext();
                    time = parentBuffer->readNext();
                    if (key > (*keys)[childIndex]) {
                        // Find new index
                        while (key > (*keys)[childIndex] && childIndex < nodeSize) {
                            childIndex++;
                        }
                    }
                    //cout << "Writing to leaf " << (*values)[childIndex] << "\n";
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
        }

        //int* leafs = new int[4*size*4](); // Cant split into more than 4x max size
        vector<int>* leafs = new vector<int>();
        leafs->reserve(size*4); // Reserve M/B.

        readLeafInfo(id,leafs); // Leafs now contains previous size of each leaf

        //cout << "Flushing leafs\n";
        // Update size of every leaf, sort and remove duplicates
        for(int i = 0; i < nodeSize+1; i++) {
            // Update size
            (*leafs)[i] = appendedToLeafs[i] + (*leafs)[i];
            if(appendedToLeafs[i] > 0) {
                // Sort and remove duplicates
                // Check if it can be done internally
                //cout << (*leafs)[i] << " " << M/sizeof(KeyValueTime) << "\n";
                if((*leafs)[i] <= M/sizeof(KeyValueTime)) {
                    //cout << "Internal\n";
                    // Internal, load in elements
                    //cout << "Reading in leaf " << (*values)[i] << " of size " <<  (*leafs)[i] << "\n";
                    KeyValueTime** temp = new KeyValueTime*[(*leafs)[i]];
                    readLeaf((*values)[i], temp);
                    /*if(id == 73) {
                        cout << "Writing out leafs\n";
                        for(int i = 0; i < (*leafs)[i]; i++) {
                            cout << temp[i]->key << "\n";
                        }
                    }*/
                    /*(*leafs)[i] = sortAndRemoveDuplicatesInternalArrayLeaf(temp,(*leafs)[i]);
                    //cout << (*leafs)[i] << "\n";
                    writeLeaf((*values)[i],temp,(*leafs)[i]);
                }
                else {
                    // External
                    // Handle originally empty leaf
                    if((*leafs)[i] - appendedToLeafs[i] > 0) {
                        (*leafs)[i] = sortAndRemoveDuplicatesExternalLeaf(id,(*leafs)[i],appendedToLeafs[i]);
                        // Leaf was output by the above method.
                    }
                    else {
                        // There was no original elements in the leaf.
                        // Leaf now consits of appended elements in order of key and time,
                        // but we need to remove deletes and corresponding inserts.

                        handleDeletesLeafOriginallyEmptyExternal((*values)[i]);
                    }
                }
            }
        }

        delete[] appendedToLeafs;


        //cout << "=== Splitting\n";
        // Split first, so we can guarantee fuses doesnt fuse with HUGE nodes.
        // Remove empty elements before fuse phase,
        // since they are hard to handle for fuses.
        for(int i = 0; i < nodeSize+1; i++) {
            if((*leafs)[i] > 4*leafSize) {
                //cout << "Split START " << i << "\n";
                splitLeaf(nodeSize, keys, values, leafs, i);
                //cout << "Split STOP\n";
                nodeSize++;
                // Leafs array will be updated in size
                // Check if we need to recurse upon leaf again
                if((*leafs)[i] > 4*leafSize) {
                    // Recheck child
                    i--;
                }
                else {
                    // Next child will be proper.
                    i++;
                }
            }
            else if((*leafs)[i] == 0) {
                //T-NOT-ODO: Check if this causes any issues
                // Erase this element, fuses below cant handle empty leafs.
                if(i != nodeSize) {
                    keys->erase(keys->begin() + i);
                }
                values->erase(values->begin() + i);
                leafs->erase(leafs->begin() + i);
                nodeSize--;
                i--;
            }
        }

        //cout << "=== Fusing\n";
        // Fuse
        for(int i = 0; i < nodeSize+1; i++) {
            if((*leafs)[i] < leafSize) {
                // Fuse
                int ret = fuseLeaf(nodeSize, keys, values, leafs, i);
                // -1 = fused from left neighbour
                // 0 = stole from one of the neighbours
                // 1 = fused with the right neighbour
                if (ret == -1) {
                    // Fused into previous child
                    // Recheck this position
                    nodeSize--;
                    i--; // Minimum recheck this position
                    // Check if previous child needs to be split
                    // With leftmost child safeguard
                    if (i >= 0 && (*leafs)[i] > leafSize * 4) {
                        splitLeaf(nodeSize, keys, values, leafs, i);
                        nodeSize++;
                        i++;
                    }
                } else if (ret == 1) {
                    // Fused next child into this one
                    nodeSize--;
                    // Check if we need to split or fuse
                    if ((*leafs)[i] > leafSize * 4) {
                        splitLeaf(nodeSize, keys, values, leafs, i);
                        nodeSize++;
                        i++;
                    } else if ((*leafs)[i] < leafSize) {
                        i--; // Fuse this leaf again
                    }
                }
            }
        }

        // Write out parents info to disk
        writeNode(id,height,nodeSize,0,keys,values);

        // Write out updated leaf info
        writeLeafInfo(id,leafs,nodeSize+1);

        */

    }
    else {
        // Children are internal nodes.
        // Flush parents buffer to childrens buffers.
        int* appendedToChild = new int[nodeSize+1](); // Initialized to zero
        //cout << appendedToChild[0] << "\n";

        if(bufferSize != 0) {
            InputStream *parentBuffer = new BufferedInputStream(streamBuffer);
            string name = getBufferName(id, 1);
            parentBuffer->open(name.c_str());

            /*cout << "Writing out childrens buffer names\n";
            for(int i = 0; i < nodeSize+1; i++) {
                cout << getBufferName((*values)[i],1) << "\n";
            }*/

            //cout << "Creating OutputStreams\n";
            // Open streams to every childs buffer so we can append
            BufferedOutputStream** appendStreams = new BufferedOutputStream*[nodeSize+1];
            for(int i = 0; i < nodeSize+1; i++) {
                name = getBufferName((*values)[i],1);
                appendStreams[i] = new BufferedOutputStream(streamBuffer);
                appendStreams[i]->open(name.c_str());
            }

            // Append KeyValueTimes to children
            // Remember how many KVTs we wrote to each

            int childIndex = 0;
            int key, value, time;
            while(!parentBuffer->endOfStream()) {
                key = parentBuffer->readNext();
                value = parentBuffer->readNext();
                time = parentBuffer->readNext();
                if(key > (*keys)[childIndex]) {
                    // Find new index
                    while(key > (*keys)[childIndex] && childIndex < nodeSize) {
                        childIndex++;
                    }
                    //cout << "New childIndex " << childIndex << "\n";
                }
                // Append to childs buffer
                appendStreams[childIndex]->write(&key);
                appendStreams[childIndex]->write(&value);
                appendStreams[childIndex]->write(&time);
                /*if(childIndex != 8) {
                    cout << "Writing to child " << childIndex << "\n";
                    cout << key << " " << value << " " << time << "\n";
                }*/
                (appendedToChild[childIndex])++;
            }

            // Clean up appendStreams and parentBuffer
            for(int i = 0; i < nodeSize+1; i++) {
                //cout << "Closing stream " << i << "\n";
                appendStreams[i]->close();
                iocounter = iocounter + appendStreams[i]->iocounter;
                delete(appendStreams[i]);
                /*os = appendStreams[i];
                os->close();
                iocounter = iocounter + os->iocounter;
                delete(os);*/
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
            cKeys = new vector<int>();
            cKeys->reserve(size*4-1);
            cValues = new vector<int>();
            cValues->reserve(size*4);
            readNode((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
            cBufferSize = cBufferSize + appendedToChild[i];
            // FORCE FLUSH
            // Sort childs buffer
            if(cBufferSize != 0 && cBufferSize-appendedToChild[i] != 0) {
                sortAndRemoveDuplicatesExternalBuffer((*values)[i],cBufferSize,appendedToChild[i]);
            }
            // Flush
            //cout << "Force flushing child " << (*values)[i] << " bufferSize " << cBufferSize << "\n";
            //cout << id << " Force flushing child " << (*values)[i] << " with bufferSize " << cBufferSize << "\n";
            //cout << appendedToChild[i] << " " << cBufferSize-appendedToChild[i] << "\n";
            forcedFlush((*values)[i],cHeight,cNodeSize,cBufferSize,cKeys,cValues);
            // Child will update its info at end of flush
        }

        //cout << "Splitting children of node " << id << "\n";
        // Run through the children and split
        for(int i = 0; i < nodeSize+1; i++) {
            cKeys = new vector<int>();
            cKeys->reserve(size*4-1);
            cValues = new vector<int>();
            cValues->reserve(size*4);
            readNode((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
            if(cNodeSize > 4*size-1) {
                // Split
                /*cout << "Splitting child " << (*values)[i] << " of size " << cNodeSize << "\n";
                cout << "---cKeys\n";
                for(int j = 0; j < cNodeSize; j++) {
                    cout << cKeys->at(j) << "\n";
                }
                cout << "---Values\n";
                for(int j = 0; j < cNodeSize+1; j++) {
                    cout << cValues->at(j) << "\n";
                }*/
                int ret = splitInternal(height,nodeSize,keys,values,i,cNodeSize,cKeys,cValues);
                nodeSize++;
                if(ret > 4*size-1) {
                    i--; // Recurse upon this node
                }
                else {
                    i++; // Skip next node, we know its size is correct.
                }
            }
            else if(cNodeSize == 0) {
                // Check if its truly empty, or has a single child we can fuse
                // TODO: Check
            }
        }

        //cout << "Fusing children of node " << id << "\n";
        // Run through children and fuse
        for(int i = 0; i < nodeSize+1; i++) {
            cKeys = new vector<int>();
            cKeys->reserve(size*4-1);
            cValues = new vector<int>();
            cValues->reserve(size*4);
            readNode((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
            if(cNodeSize < size) {
                // Fuse
                int ret = fuseInternal(height,nodeSize,keys,values,i,ptr_cNodeSize,cKeys,cValues);
                // -1 = fused with left neighbour, check size.
                // 0 = stole elements from a neighbour, which is fine
                // 1 = fused with right neighbour, check size.
                if(ret == -1) {
                    // Fused with left neighbour, already checked.
                    // Therefore child can not be too small, check if too large
                    nodeSize--;
                    i--; // As a minimum we need to recheck this position with a new child.
                    if(cNodeSize > 4*size-1) {
                        // Split left neighbour
                        readNode((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
                        splitInternal(height,nodeSize,keys,values,i,cNodeSize,cKeys,cValues);
                        nodeSize++;
                        i++;
                    }
                }
                else if(ret == 1) {
                    // Fused with right child, check size.
                    nodeSize--;
                    if(cNodeSize > 4*size-1) {
                        readNode((*values)[i],ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);
                        splitInternal(height,nodeSize,keys,values,i,cNodeSize,cKeys,cValues);
                        nodeSize++;
                        i++;
                    }
                    else if(cNodeSize < size) {
                        i--; // Recurse
                    }
                }
            }
        }

        // Write out parents info to disk
        writeNode(id,height,nodeSize,0,keys,values);

        // Clean up vectors
        delete(cKeys);
        delete(cValues);
    }
}

/*
 * Flushes the entire tree into leaves.
 */
void ExternalBufferedBTree::flushEntireTree() {

    // Handle input buffer
    // Sort the buffer and remove duplicates
    if(update_bufferSize != 0) {
        //cout << "In FlushEntireTree, update_bufferSize was " << update_bufferSize << "\n";
        update_bufferSize = sortAndRemoveDuplicatesInternalArray(update_buffer,update_bufferSize);
        // Append to root
        appendBufferNoDelete(root,update_buffer,update_bufferSize,1);
        rootBufferSize = rootBufferSize + update_bufferSize;
    }

    //cout << "Old Root Buffer Size is " << rootBufferSize << "\n";

    // Merge input buffer with root buffer
    if(rootBufferSize != 0) {

        if(update_bufferSize == 0) {

            // Nothing was added.

            KeyValueTime** buffer = new KeyValueTime*[rootBufferSize];
            InputStream* is1 = new BufferedInputStream(streamBuffer);
            string name1 = getBufferName(root,1);
            is1->open(name1.c_str());
            // Read in unsorted elements
            int key, value, time;
            KeyValueTime* kvt;
            for(int i = 0; i < rootBufferSize; i++) {
                key = is1->readNext();
                value = is1->readNext();
                time = is1->readNext();
                kvt = new KeyValueTime(key,value,time);
                buffer[i] = kvt;
            }

            is1->close();
            iocounter = iocounter + is1->iocounter;
            delete(is1);

            // Sort and remove duplicates
            int newBufferSize = sortAndRemoveDuplicatesInternalArray(buffer,rootBufferSize);

            // Place the buffer in "TempBuf1"
            writeBuffer(root,buffer,newBufferSize,2);

            string name2 = getBufferName(root,2); // TempBuf1

            // Delete previous buffer
            if(FILE *file = fopen(name1.c_str(), "r")) {
                fclose(file);
                remove(name1.c_str());
            }

            // Rename
            int result= rename( name2.c_str(), name1.c_str());
            if (result != 0) {
                cout << "Tried renaming " << name2 << " to " << name1 << "\n";
                perror("Error renaming file");
            }

        }
        else {
            //cout << "Sorting and Removing duplicates from root buffer size " << rootBufferSize << "\n";
            rootBufferSize = sortAndRemoveDuplicatesExternalBuffer(root,rootBufferSize,update_bufferSize);
        }
    }

    //cout << "New Root Buffer Size is " << rootBufferSize << "\n";

    // Force flush root
    int height, nodeSize, bufferSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* ptr_bufferSize = &bufferSize;
    vector<int>* keys = new vector<int>();
    keys->reserve(4*size-1);
    vector<int>* values = new vector<int>();
    values->reserve(4*size);
    readNode(root,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);
    forcedFlush(root,height,nodeSize,rootBufferSize,keys,values);

    // Handle balancing of root
    keys = new vector<int>();
    keys->reserve(4*size-1);
    values = new vector<int>();
    values->reserve(4*size);
    readNode(root,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);
    if(nodeSize > size*4-1) {
        //cout << "Creating new Root\n";
        // Create new root
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
        // We might have to split again, recheck
        if(ret > 4*size-1) {
            //cout << "Splitting roots children\n";
            // Loop over children until no more splits
            for(int i = 0; i < newSize+1; i++) {
                //cout << "Checking child " << i << " name " << (*newValues)[i] << "\n";
                //cout << "Root values ";
                /*for(int j = 0; j < newSize+1; j++) {
                    cout << (*newValues)[j] << " ";
                }
                cout << "\n";*/

                keys = new vector<int>();
                keys->reserve(size*4-1);
                values = new vector<int>();
                values->reserve(size*4);
                readNode((*newValues)[i],ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);
                if(nodeSize > 4*size-1) {
                    //cout << "Splitting child " << (*newValues)[i] << "\n";
                    ret = splitInternal(newHeight,newSize,newKeys,newValues,i,nodeSize,keys,values);
                    //cout << "Childs new size is " << ret << "\n";
                    newSize++;
                    if(ret > 4*size-1) {
                        i--; // Recurse
                    }
                    else {
                        i++; // We know the next child is fine.
                    }
                }
            }
        }
        //cout << "Writing new root to disk\n";
        writeNode(newRoot,newHeight,newSize,0,newKeys,newValues);
    }
    else if(nodeSize == 0 && height > 1) {
        //cout << "Deleting Root\n";
        // Delete this root, make only child new root
        cleanUpExternallyAfterNodeOrLeaf(root);
        root = (*values)[0];
        currentNumberOfNodes--;
        delete(keys);
        delete(values);
    }
    else {
        delete(keys);
        delete(values);
    }
    rootBufferSize = 0;

    // Reset internal buffer
    update_bufferSize = 0;

    forcedFlushed = true;
}


/*
 * Splits an internal node with internal children.
 * Writes out child and new child to disk.
 * Returns the childs new size.
 */
int ExternalBufferedBTree::splitInternal(int height, int nodeSize, vector<int> *keys, vector<int> *values, int childNumber, int cSize,
                                          vector<int> *cKeys, vector<int> *cValues) {

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

    // If leaf node, split the leaf file
    if(height == 2) {
        InputStream* is = new BufferedInputStream(streamBuffer);
        string name1 = getBufferName((*values)[childNumber],4);
        is->open(name1.c_str());

        OutputStream* os = new BufferedOutputStream(streamBuffer);
        string name2 = getBufferName(1,2); // TempBuf1
        if(FILE *file = fopen(name2.c_str(), "r")) {
            fclose(file);
            remove(name2.c_str());
        }
        os->create(name2.c_str());

        int temp;
        for(int i = 0; i < cSize+1; i++) {
            temp = is->readNext();
            os->write(&temp);
        }

        os->close();
        iocounter = iocounter + os->iocounter;
        delete(os);

        string name3 = getBufferName(newChild,4);
        if(FILE *file = fopen(name3.c_str(), "r")) {
            fclose(file);
            remove(name3.c_str());
        }
        os = new BufferedOutputStream(streamBuffer);
        os->create(name3.c_str());

        while(!is->endOfStream()) {
            temp = is->readNext();
            os->write(&temp);
        }

        os->close();
        iocounter = iocounter + os->iocounter;
        delete(os);

        is->close();
        iocounter = iocounter + is->iocounter;
        delete(is);

        // Rename tempbuf1 to original leaf file
        int result = rename( name2.c_str(), name1.c_str());
        if (result != 0) {
            cout << "Tried renaming " << name2 << " to " << name1 << "\n";
            perror("Error renaming file");
        }

    }

    return cSize;

}

/*
 * Splits a leaf into two, and updates parents keys, values and leaf size.
 */
void ExternalBufferedBTree::splitLeaf(int nodeSize, vector<int> *keys, vector<int> *values, vector<int> *leafs, int leafNumber) {

    numberOfNodes++;
    int newLeaf = numberOfNodes;

    //cout << "Splitleaf on leaf " << (*values)[leafNumber] << " into " << newLeaf << "\n";
    //cout << "Leaf size is " << (*leafs)[leafNumber] << "\n";

    // Split leaf in half
    int newSize = (*leafs)[leafNumber]/2;

    InputStream* is = new BufferedInputStream(streamBuffer);
    string name1 = getBufferName((*values)[leafNumber],4);
    is->open(name1.c_str());

    //cout << "Writing to temp\n";
    string name2 = getBufferName(1,2); // TempBuf1
    //cout << name2 << "\n";
    if(FILE *file = fopen(name2.c_str(), "r")) {
        fclose(file);
        remove(name2.c_str());
        //cout << "Removed file " << name2 << "\n";
    }
    OutputStream* os = new BufferedOutputStream(streamBuffer);
    os->create(name2.c_str());

    // Write to temp file
    int key,value,time;
    for(int i = 0; i < newSize; i++) {
        key = is->readNext();
        value = is->readNext();
        time = is->readNext();
        os->write(&key);
        os->write(&value);
        os->write(&time);
    }

    int newKey = key;

    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);

    //cout << "Writing to new leaf\n";
    string name3 = getBufferName(newLeaf,4);
    if(FILE *file = fopen(name3.c_str(), "r")) {
        fclose(file);
        remove(name3.c_str());
        //cout << "Removed file " << name3 << "\n";
    }
    os = new BufferedOutputStream(streamBuffer);
    os->create(name3.c_str());
    while(!is->endOfStream()) {
        key = is->readNext();
        value = is->readNext();
        time = is->readNext();
        os->write(&key);
        os->write(&value);
        os->write(&time);
    }

    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);

    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);

    // Delete original file
    if(FILE *file = fopen(name1.c_str(), "r")) {
        fclose(file);
        remove(name1.c_str());
        //cout << "Removed file " << name1 << "\n";
    }

    // Rename temp file to original leaf file
    int result = rename( name2.c_str(), name1.c_str());
    if (result != 0) {
        cout << "Tried renaming " << name2 << " to " << name1 << "\n";
        perror("Error renaming file");
    }

    //cout << "Updating parents info\n";
    // Update parents info
    //cout << (*keys)[0] << " " << newKey << "\n";
    //cout << "--------------------------- " << keys->size() << " " << keys->capacity() << "\n";
    int zero = 0;
    keys->push_back(zero);
    //cout << nodeSize << " " << keys->size() << "\n";
    for(int i = nodeSize; i > leafNumber; i--) {
        (*keys)[i] = (*keys)[i-1];
    }
    (*keys)[leafNumber] = newKey;

    //cout << "Key size " << keys->size() << " " << (*keys)[0] << "\n";

    //cout << "Moving values and leafs\n";
    // Add element to leafs
    values->push_back(0);
    leafs->push_back(0);
    // Checked, size of values is now nodeSize+2.
    // Move values and leafs back by one
    for(int i = nodeSize+1; i > leafNumber; i--) {
        (*values)[i] = (*values)[i-1];
        (*leafs)[i] = (*leafs)[i-1];
    }
    //cout << "Updating values and leafs\n";
    (*values)[leafNumber+1] = newLeaf;
    (*leafs)[leafNumber] = newSize;
    (*leafs)[leafNumber+1] = (*leafs)[leafNumber+1] - newSize;
    //cout << "Done splitting\n";

}


/*
 * Fuses the child of a node.
 * Will either fuse with a neighbour or steal from its neighbours.
 * To do so it flushes its neighbours of this hasnt already been done.
 * Return values is as follows
 * -1 = fused with left neighbour
 * 0 = stole from one of the neighbours
 * 1 = fused with the right neighbour
 * cSize will be updated with the resulting size.
 * Writes out resulting changes to disk.
 */
int ExternalBufferedBTree::fuseInternal(int height, int nodeSize, vector<int> *keys, vector<int> *values, int childNumber, int *cSize,
                                        vector<int> *cKeys, vector<int> *cValues) {

    // First both neighbours must be flushed
    int nHeight, nNodeSize, nBufferSize;
    int* ptr_nHeight = &nHeight;
    int* ptr_nNodeSize = &nNodeSize;
    int* ptr_nBufferSize = &nBufferSize;
    vector<int>* nKeys = new vector<int>();
    nKeys->reserve(4*size);
    vector<int>* nValues = new vector<int>();

    bool leftNeighbour = false;

    if(childNumber == 0) {
        readNode(values->at(1),ptr_nHeight,ptr_nNodeSize,ptr_nBufferSize,nKeys,nValues);
    }
    else if(childNumber == nodeSize) {
        readNode(values->at(nodeSize-1),ptr_nHeight,ptr_nNodeSize,ptr_nBufferSize,nKeys,nValues);
        leftNeighbour = true;
    }
    else {
        // Just merge right into this
        readNode(values->at(childNumber+1),ptr_nHeight,ptr_nNodeSize,ptr_nBufferSize,nKeys,nValues);
    }

    if(leftNeighbour) {

        // Add this nodes keys to the left neighbour
        int newKey = keys->at(childNumber-1);
        nKeys->push_back(newKey);
        for(int i = 0; i < *cSize; i++) {
            int tempKey = cKeys->at(i);
            nKeys->push_back(tempKey);
        }

        // Values
        for(int i = 0; i < (*cSize)+1; i++) {
            int tempVal = cValues->at(i);
            nValues->push_back(tempVal);
        }

        // Leafs
        if(height == 2) {
            // Merge leafs
            string name1 = getBufferName((*values)[childNumber-1],4);
            string name2 = getBufferName((*values)[childNumber],4);
            BufferedOutputStream* os = new BufferedOutputStream(streamBuffer);
            os->open(name1.c_str()); // Append
            BufferedInputStream* is = new BufferedInputStream(streamBuffer);
            is->open(name2.c_str());

            while(!is->endOfStream()) {
                int temp = is->readNext();
                os->write(&temp);
            }

            os->close();
            iocounter = iocounter + os->iocounter;
            delete(os);

            is->close();
            iocounter = iocounter + is->iocounter;
            delete(is);


        }

        // Delete this node
        cleanUpExternallyAfterNodeOrLeaf((*values)[childNumber]);
        delete(cKeys);
        delete(cValues);

        // Update parent
        for(int i = childNumber-1; i < nodeSize-1; i++) {
            (*keys)[i] = (*keys)[i+1];
        }
        keys->pop_back();
        for(int i = childNumber; i < nodeSize; i++) {
            (*values)[i] = (*values)[i+1];
        }
        values->pop_back();

        nNodeSize = nNodeSize + 1 + (*cSize);
        *cSize = nNodeSize; // For return.

        // Write neighbour node to disk
        writeNode((*values)[childNumber-1],nHeight,nNodeSize,0,nKeys,nValues);

    }
    else {

        // Add neighbours keys and values to ours
        // Add this nodes keys to the left neighbour
        int newKey = keys->at(childNumber);
        cKeys->push_back(newKey);
        for(int i = 0; i < nNodeSize; i++) {
            int tempKey = nKeys->at(i);
            cKeys->push_back(tempKey);
        }

        // Values
        for(int i = 0; i < nNodeSize+1; i++) {
            int tempVal = nValues->at(i);
            cValues->push_back(tempVal);
        }

        // Leafs
        if(height == 2) {
            // Merge leafs
            string name1 = getBufferName((*values)[childNumber],4);
            string name2 = getBufferName((*values)[childNumber+1],4);
            BufferedOutputStream* os = new BufferedOutputStream(streamBuffer);
            os->open(name1.c_str()); // Append
            BufferedInputStream* is = new BufferedInputStream(streamBuffer);
            is->open(name2.c_str());

            while(!is->endOfStream()) {
                int temp = is->readNext();
                os->write(&temp);
            }

            os->close();
            iocounter = iocounter + os->iocounter;
            delete(os);

            is->close();
            iocounter = iocounter + is->iocounter;
            delete(is);


        }

        // Delete right neighbour
        cleanUpExternallyAfterNodeOrLeaf((*values)[childNumber+1]);
        delete(nKeys);
        delete(nValues);

        // Update parent
        for(int i = childNumber; i < nodeSize-1; i++) {
            (*keys)[i] = (*keys)[i+1];
        }
        keys->pop_back();
        for(int i = childNumber+1; i < nodeSize; i++) {
            (*values)[i] = (*values)[i+1];
        }
        values->pop_back();


        // Write this node to disk
        cSize = cSize + 1 + nNodeSize;

        writeNode((*values)[childNumber],height-1,*cSize,0,cKeys,cValues);

    }

    // Return
    if(leftNeighbour) {
        return -1;
    }
    else {
        return 1;
    }

}

/*
 * Fuses leaf.
 * Will fuse with a neighbour, unlike a normal
 * BTree where we try and steal elements from a neighbour.
 * Fuse possibly requires a following split
 * ASSUMES NO NEIGHBOUR IS EMPTY!
 * Will return -1 for fused with left neighbour.
 * 1 for fusing with right neighbour.
 */
int ExternalBufferedBTree::fuseLeaf(int nodeSize, vector<int> *keys, vector<int> *values, vector<int> *leafs, int leafNumber) {

    //cout << "Fuse Leaf " << (*values)[leafNumber] << "\n";
    /*
     * Two possible scenarios:
     * a) The child has a neighbouring sibling who also contains size-1 keys.
     * b) The child has a neighbouring sibling containing >= size keys.
     * We prefer case a over case b, since it allows for faster consecutive deletes.
     */

    bool leftSibling = false;
    int siblingID,siblingSize;

    // Check which neighbour to use, and under which condition.
    // We always fuse, and we always favour fusing with the right neighbour.
    // Left conditions for readability, only really need to check if we are
    // the rightmost leaf.
    if(leafNumber == 0) {
        // Leftmost child, check right sibling
    }
    else if (leafNumber == nodeSize) {
        leftSibling = true;
    }
    else {
        // Check both left and right sibling.
        // Favour left sibling, as right might be very large.
        leftSibling = true;
    }

    // Handle the actual fuse

    if(leftSibling) {
        // Append this node to our left sibling
        InputStream* is = new BufferedInputStream(streamBuffer);
        string name = getBufferName((*values)[leafNumber],4);
        is->open(name.c_str());

        BufferedOutputStream* os = new BufferedOutputStream(streamBuffer);
        name = getBufferName((*values)[leafNumber-1],4);
        os->open(name.c_str()); // Append, rather than create

        int key,value,time;
        while(!is->endOfStream()) {
            key = is->readNext();
            value = is->readNext();
            time = is->readNext();
            os->write(&key);
            os->write(&value);
            os->write(&time);
        }

        is->close();
        iocounter = iocounter + is->iocounter;
        delete(is);

        os->close();
        iocounter = iocounter + os->iocounter;
        delete(os);

        // Update new size
        (*leafs)[leafNumber-1] = (*leafs)[leafNumber-1] + (*leafs)[leafNumber];

        // Remove this nodes data
        cleanUpExternallyAfterNodeOrLeaf((*values)[leafNumber]);

        // Clean up data in parent

        // Move all keys back
        // Left sibling thus inherits current key
        for(int i = leafNumber-1; i < nodeSize-1; i++) {
            (*keys)[i] = (*keys)[i+1];
        }
        keys->pop_back();
        // Move values and leafs back, overwriting old values
        for(int i = leafNumber; i < nodeSize; i++) {
            (*values)[i] = (*values)[i+1];
            (*leafs)[i] = (*leafs)[i+1];
        }
        values->pop_back();
        leafs->pop_back();

    }
    else {
        // Append right sibling to this node
        InputStream* is = new BufferedInputStream(streamBuffer);
        string name = getBufferName((*values)[leafNumber+1],4);
        is->open(name.c_str());

        BufferedOutputStream* os = new BufferedOutputStream(streamBuffer);
        name = getBufferName((*values)[leafNumber],4);
        os->open(name.c_str()); // Append, rather than create

        int key,value,time;
        while(!is->endOfStream()) {
            key = is->readNext();
            value = is->readNext();
            time = is->readNext();
            os->write(&key);
            os->write(&value);
            os->write(&time);
        }

        is->close();
        iocounter = iocounter + is->iocounter;
        delete(is);

        os->close();
        iocounter = iocounter + os->iocounter;
        delete(os);

        // Update this size to new size, later overwrite right neighbours
        (*leafs)[leafNumber] = (*leafs)[leafNumber] + (*leafs)[leafNumber+1];

        // Remove this nodes data
        cleanUpExternallyAfterNodeOrLeaf((*values)[leafNumber+1]);

        // Clean up data in parent

        // Move all keys back
        // This node thus inherits right siblings key
        for(int i = leafNumber; i < nodeSize-1; i++) {
            (*keys)[i] = (*keys)[i+1];
        }
        keys->pop_back();
        // Move values and leafs back, overwriting right sibling
        for(int i = leafNumber+1; i < nodeSize; i++) {
            (*values)[i] = (*values)[i+1];
            (*leafs)[i] = (*leafs)[i+1];
        }
        values->pop_back();
        leafs->pop_back();

    }

    // Return -1 if we fused with left neighbour
    // Return 0 if we stole from a neighbour
    // Return 1 if we fused with right neighbour
    int ret = -1;
    if(!leftSibling) {
        ret = 1;
    }

    return ret;
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
        os->write(&(buffer[i]->key));
        os->write(&(buffer[i]->value));
        os->write(&(buffer[i]->time));
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
        os->write(&(buffer[i]->time));
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
void ExternalBufferedBTree::appendBufferNoDelete(int id, KeyValueTime **buffer, int bSize, int type) {

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
        os->write(&(buffer[i]->time));
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

/* Input consists of O(B) sorted elements, appended by some more sorted elements.
 * Merge the two parts of the file and handle duplicates.
 * Nearly identical to above method, but uses some leaf specific naming
 * and spacing, namely that we can keep the original elements in the leaf
 * in internal memory, and handle the appended elements externally.
 * This method also removes any inserted elements deleted by a delete (with appropriate timestamp).
 * Deletes can only be placed in the appended elements.
 */
int ExternalBufferedBTree::sortAndRemoveDuplicatesExternalLeaf(int id, int bufferSize, int sortedSize) {

    int sorted = bufferSize - sortedSize; // Sorted being the original elements.
    KeyValueTime** buffer = new KeyValueTime*[sorted];
    InputStream* is1 = new BufferedInputStream(streamBuffer);
    string name1 = getBufferName(id,4);
    is1->open(name1.c_str());
    // Read in orignal elements in leaf, they are sorted
    int key, value, time;
    KeyValueTime* kvt;
    for(int i = 0; i < sorted; i++) {
        key = is1->readNext();
        value = is1->readNext();
        time = is1->readNext();
        kvt = new KeyValueTime(key,value,time);
        buffer[i] = kvt;
    }

    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string name3 = getBufferName(id,2); // Write to TempBuf1, later rename
    if(FILE *file = fopen(name3.c_str(), "r")) {
        fclose(file);
        remove(name3.c_str());
    }
    // Create file
    os->create(name3.c_str());

    // Merge the two streams
    int outputsize = 0;
    int originalIndex = 0; // Current element loaded in from buffer
    // Read in initial elements
    KeyValueTime* original = buffer[0];
    int key2, value2, time2;
    key2 = is1->readNext();
    value2 = is1->readNext();
    time2 = is1->readNext();

    // Merge files until we reach end of stream for one
    // Assumption: When the while loop terminates both
    // KeyValueTimes have been written to output (if it should),
    // and one of the streams will have terminated.
    while(true) {
        if(original->key == key2) {
            // Resolve duplicate
            // Largest time will always have priority
            // Discard other KeyValueTime
            if(original->time > time2) {
                os->write(&(original->key));
                os->write(&(original->value));
                os->write(&(original->time));
                outputsize++;
                // Read in new KeyValueTimes
                if(originalIndex+1 < sorted && !is1->endOfStream()) {
                    // Load in new element
                    originalIndex++;
                    original = buffer[originalIndex];
                    // Discard other element
                    key2 = is1->readNext();
                    value2 = is1->readNext();
                    time2 = is1->readNext();
                }
                else {
                    break;
                }
            }
            else {
                // Handle delete
                if(value2 > 0) {
                    os->write(&key2);
                    os->write(&value2);
                    os->write(&time2);
                    outputsize++;
                }
                // Read in new KeyValueTimes
                if(originalIndex+1 < sorted && !is1->endOfStream()) {
                    // Load in new element
                    key2 = is1->readNext();
                    value2 = is1->readNext();
                    time2 = is1->readNext();
                    // Discard other element
                    originalIndex++;
                    original = buffer[originalIndex];
                }
                else {
                    break;
                }
            }
        }
        else {
            // Write out one of the KeyValueTimes, try and load in new
            // Break if one of the streams reached EOF.
            if(original->key < key2) {
                os->write(&(original->key));
                os->write(&(original->value));
                os->write(&(original->time));
                outputsize++;
                if(originalIndex+1 < sorted) {
                    // Load in new element
                    originalIndex++;
                    original = buffer[originalIndex];
                }
                else {
                    // Write out other item
                    if(value2 > 0) {
                        os->write(&key2);
                        os->write(&value2);
                        os->write(&time2);
                        outputsize++;
                    }

                    break;
                }
            }
            else {
                if(value2 > 0) {
                    os->write(&key2);
                    os->write(&value2);
                    os->write(&time2);
                    outputsize++;
                }
                if(!is1->endOfStream()) {
                    // Load in new element
                    key2 = is1->readNext();
                    value2 = is1->readNext();
                    time2 = is1->readNext();
                }
                else {
                    // Write out other item
                    os->write(&(original->key));
                    os->write(&(original->value));
                    os->write(&(original->time));
                    outputsize++;
                    break;
                }
            }
        }
    }

    // Use Assumption: When the while loop terminates both
    // KeyValueTimes have been written to output,
    // and one of the streams will have terminated.
    if(originalIndex+1 < sorted) {
        // Buffer not empty
        originalIndex++;
        for(int i = originalIndex; i < sorted; i++) {
            original = buffer[i];
            os->write(&(original->key));
            os->write(&(original->value));
            os->write(&(original->time));
            outputsize++;
        }
    }
    else {
        // is1 did not finish
        while(!is1->endOfStream()) {
            key2 = is1->readNext();
            value2 = is1->readNext();
            time2 = is1->readNext();
            // Handle delete
            if(value2 > 0) {
                os->write(&key2);
                os->write(&value2);
                os->write(&time2);
                outputsize++;
            }
        }
    }

    // Clean up
    is1->close();
    iocounter = iocounter + is1->iocounter;
    delete(is1);
    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);

    // Clean up buffer
    for(int i = 0; i < sorted; i++) {
        delete(buffer[i]);
    }
    delete[] buffer;

    // Remove old leaf file
    if(FILE *file = fopen(name1.c_str(), "r")) {
        fclose(file);
        remove(name1.c_str());
    }

    // Rename TempBuf1 to Leaf<id>
    int result = rename( name3.c_str(), name1.c_str());
    if (result != 0) {
        cout << "Tried renaming " << name3 << " to " << name1 << "\n";
        perror("Error renaming file");
    }

    return outputsize;
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

/*
 * As above but handles deletes.
 */
int ExternalBufferedBTree::sortAndRemoveDuplicatesInternalArrayLeaf(KeyValueTime **buffer, int bufferSize) {

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
            // Handle delete
            if(kvt1->value > 0) {
                buffer[write] = kvt1;
                write++;
            }
            else {
                delete(kvt1);
            }
            kvt1 = kvt2;
            i++;
        }
    }
    // Handle delete
    if(kvt1->value > 0) {
        buffer[write] = kvt1;
        write++;
    }
    else {
        delete(kvt1);
    }

    return write;
}

/*
 * Special method for when the leaf was originally empty and we appended elements to it.
 * Input may consists of similar inserts and deletes, remove relevant elements.
 * Input may be much larger than O(M).
 * Returns the new size of the leaf.
 */
int ExternalBufferedBTree::handleDeletesLeafOriginallyEmptyExternal(int id) {

    InputStream* is = new BufferedInputStream(streamBuffer);
    string name1 = getBufferName(id,4);
    is->open(name1.c_str());

    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string name2 = getBufferName(1,2); // TempBuf1
    if(FILE *file = fopen(name2.c_str(), "r")) {
        fclose(file);
        remove(name2.c_str());
    }
    os->create(name2.c_str());

    int counter = 0;

    int key1,value1,time1,key2,value2,time2;
    key1 = is->readNext();
    value1 = is->readNext();
    time1 = is->readNext();
    while(!is->endOfStream()) {
        key2 = is->readNext();
        value2 = is->readNext();
        time2 = is->readNext();
        if(key1 == key2) {
            // Later timestamp will always overwrite
            key1 = key2;
            value1 = value2;
            time1 = time2;
        }
        else {
            // Handle deletes
            if(value1 > 0) {
                os->write(&key1);
                os->write(&value1);
                os->write(&time1);
                counter++;
            }
            key1 = key2;
            value1 = value2;
            time1 = time2;
        }
    }
    if(value1 > 0) {
        os->write(&key1);
        os->write(&value1);
        os->write(&time1);
        counter++;
    }

    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);

    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);

    // Delete original leaf life
    if(FILE *file = fopen(name1.c_str(), "r")) {
        fclose(file);
        remove(name1.c_str());
    }

    // Rename TempBuf1 to Leaf<id>
    int result = rename( name2.c_str(), name1.c_str());
    if (result != 0) {
        cout << "Tried renaming " << name2 << " to " << name1 << "\n";
        perror("Error renaming file");
    }

    return counter;

}

/*
 * Handles a buffer overflow in an internal leaf node.
 * Will also handle flushing a non-overflowing buffer to its leafs.
 * Will handle empty leafs.
 *
 * This method was implemented to handle leafs more effeciently.
 * It will gather the leafs in a temporary buffer, delete the old leafs,
 * then merge the elements from the leafs with the elements of the buffer,
 * writing them out to new (or refused) full leafs.
 *
 * This saves handling splits and fuses on the leafs.
 *
 * Resulting leafs and node changes will NOT be written to disk.
 */
void ExternalBufferedBTree::handleLeafNodeBufferOverflow(int id, int* nodeSize, std::vector<int> *keys,
                                                         std::vector<int> *values, std::vector<int>* leafs) {

    // Gather the leafs in a temporary buffer
    string name1 = getBufferName(1,2); // TempBuf1
    if(FILE *file = fopen(name1.c_str(), "r")) {
        fclose(file);
        remove(name1.c_str());
    }
    OutputStream* os = new BufferedOutputStream(streamBuffer);
    os->create(name1.c_str());

    string tempName;
    int key,value,time;
    for(int i = 0; i < (*nodeSize)+1; i++) {
        tempName = getBufferName((*values)[i],4);
        InputStream* tempIS = new BufferedInputStream(streamBuffer);
        tempIS->open(tempName.c_str());
        while(!tempIS->endOfStream()) {
            key = tempIS->readNext();
            value = tempIS->readNext();
            time = tempIS->readNext();
            os->write(&key);
            os->write(&value);
            os->write(&time);
        }
        tempIS->close();
        iocounter = iocounter + tempIS->iocounter;
        delete(tempIS);

        // Clean up leaf
        if(FILE *file = fopen(tempName.c_str(), "r")) {
            fclose(file);
            remove(tempName.c_str());
        }
    }

    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);

    // Merge temp and buffer whilst writing out to reused or new leafs.
    InputStream* is1 = new BufferedInputStream(streamBuffer);
    is1->open(name1.c_str()); // TempBuf1
    string name2 = getBufferName(id,1); // Buffer<id>
    InputStream* is2 = new BufferedInputStream(streamBuffer);
    is2->open(name2.c_str());

    string name3 = getBufferName((*values)[0],4); // Reuse old leaf names.
    os = new BufferedOutputStream(streamBuffer);
    os->create(name3.c_str());
    //cout << "Writing to " << name3 << "\n";

    int outputsize = 0;
    int leafCounter = 0; // What leaf we are reusing
    int key1, value1, time1;
    int key2, value2, time2;
    key1 = is1->readNext();
    value1 = is1->readNext();
    time1 = is1->readNext();
    key2 = is2->readNext();
    value2 = is2->readNext();
    time2 = is2->readNext();

    int mostRecentKey = 0;
    bool run = true; // Use this instead of breaks to guarantee we execute bottom code.
    int fullLeaf = leafSize*4;
    bool specialCase = false; // Special case where a stream finishes on filling out a leaf.

    // Merge files until we reach end of stream for one
    // Assumption: When the while loop terminates both
    // KeyValueTimes have been written to output (if it should),
    // and one of the streams will have terminated.
    while(run) {
        if(key1 == key2) {
            //cout << "Duplicate " << key1 << "\n";
            // Resolve duplicate
            // Largest time will always have priority
            // Discard other KeyValueTime
            if(time1 > time2) {
                if(value1 > 0) {
                    os->write(&key1);
                    os->write(&value1);
                    os->write(&time1);
                    outputsize++;
                    mostRecentKey = key1;
                }
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
                    run = false;
                }
            }
            else {
                if(value2 > 0) {
                    os->write(&key2);
                    os->write(&value2);
                    os->write(&time2);
                    outputsize++;
                    mostRecentKey = key2;
                }
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
                    run = false;
                }
            }
        }
        else {
            // Write out one of the KeyValueTimes, try and load in new
            // Break if one of the streams reached EOF.
            if(key1 < key2) {
                if(value1 > 0) {
                    //cout << "Writing out " << key1 << "\n";
                    os->write(&key1);
                    os->write(&value1);
                    os->write(&time1);
                    outputsize++;
                    mostRecentKey = key1;
                }
                if(!is1->endOfStream()) {
                    // Load in new element
                    key1 = is1->readNext();
                    value1 = is1->readNext();
                    time1 = is1->readNext();
                }
                else {
                    // Write out other item
                    if(value2 > 0) {
                        if(outputsize != fullLeaf) {
                            os->write(&key2);
                            os->write(&value2);
                            os->write(&time2);
                            outputsize++;
                            mostRecentKey = key2;
                        }
                        else {
                            specialCase = true;
                        }
                    }
                    run = false;
                }
            }
            else {
                if(value2 > 0) {
                    //cout << "Writing out " << key2 << "\n";
                    os->write(&key2);
                    os->write(&value2);
                    os->write(&time2);
                    outputsize++;
                    mostRecentKey = key2;
                }
                if(!is2->endOfStream()) {
                    // Load in new element
                    key2 = is2->readNext();
                    value2 = is2->readNext();
                    time2 = is2->readNext();
                }
                else {
                    // Write out other item
                    if(value1 > 0) {
                        if(outputsize != fullLeaf) {
                            os->write(&key1);
                            os->write(&value1);
                            os->write(&time1);
                            mostRecentKey = key1;
                            outputsize++;
                        }
                        else {
                            specialCase = true;
                        }
                    }
                    run = false;
                }
            }
        }
        // Check if the leaf is full
        if(outputsize == fullLeaf) {
            //cout << "Filled out leaf\n";
            os->close();
            iocounter = iocounter + os->iocounter;
            delete(os);

            // Just finished leaf index leafcounter, update parent
            if(leafCounter < (*nodeSize)+1) {
                // We update the existing leaf value
                (*leafs)[leafCounter] = fullLeaf;
            }
            else {
                // Update value as well
                values->push_back(numberOfNodes);
                leafs->push_back(fullLeaf);
            }

            // Check keys
            if(leafCounter < (*nodeSize)) {
                (*keys)[leafCounter] = mostRecentKey;
            }
            else {
                keys->push_back(mostRecentKey);
            }

            // Check name
            string newname;
            // Check if we can reuse old name
            // and update values
            leafCounter++;
            if(leafCounter < (*nodeSize)+1) {
                newname = getBufferName((*values)[leafCounter],4);
            }
            else {
                numberOfNodes++;
                newname = getBufferName(numberOfNodes,4);
            }

            // New outputstream
            if(FILE *file = fopen(newname.c_str(), "r")) {
                fclose(file);
                remove(newname.c_str());
            }
            os = new BufferedOutputStream(streamBuffer);
            os->create(newname.c_str());
            //cout << "Writing to " << newname << "\n";
            outputsize = 0;
        }
    }

    //cout << "--- A stream finished\n";

    // Use Assumption: When the while loop terminates both
    // KeyValueTimes have been written to output,
    // and one of the streams will have terminated.
    // Furthermore the outputsize has been checked, and a new
    // outputstream created if necessary.
    if(!is1->endOfStream()) {
        if(specialCase) {
            if(value1 > 0) {
                os->write(&key1);
                os->write(&value1);
                os->write(&time1);
                outputsize++;
                mostRecentKey = key1;
            }

        }
        // is1 did not finish
        while(!is1->endOfStream()) {
            //cout << "Writing out " << key1 << "\n";
            key1 = is1->readNext();
            value1 = is1->readNext();
            time1 = is1->readNext();
            if(value1 > 0) {
                os->write(&key1);
                os->write(&value1);
                os->write(&time1);
                outputsize++;
                mostRecentKey = key1;
            }
            if(outputsize == fullLeaf) {
                os->close();
                iocounter = iocounter + os->iocounter;
                delete(os);
                // Just finished leaf index leafcounter, update parent
                if(leafCounter < (*nodeSize)+1) {
                    // We update the existing leaf value
                    (*leafs)[leafCounter] = fullLeaf;
                }
                else {
                    // Update value as well
                    values->push_back(numberOfNodes);
                    leafs->push_back(fullLeaf);
                }

                // Check keys
                if(leafCounter < (*nodeSize)) {
                    (*keys)[leafCounter] = mostRecentKey;
                }
                else {
                    keys->push_back(mostRecentKey);
                }

                string newname;
                // Check if we can reuse old name
                leafCounter++;
                if(!is1->endOfStream()) {
                    if(leafCounter < (*nodeSize)+1) {
                        newname = getBufferName((*values)[leafCounter],4);
                    }
                    else {
                        numberOfNodes++;
                        newname = getBufferName(numberOfNodes,4);
                    }

                    // New outputstream
                    if(FILE *file = fopen(newname.c_str(), "r")) {
                        fclose(file);
                        remove(newname.c_str());
                    }
                    os = new BufferedOutputStream(streamBuffer);
                    os->create(newname.c_str());
                    //cout << "Writing to " << newname << "\n";

                }

                outputsize = 0;
            }
        }
    }
    else {
        if(specialCase) {
            if(value2 > 0) {
                os->write(&key2);
                os->write(&value2);
                os->write(&time2);
                outputsize++;
                mostRecentKey = key2;
            }
        }
        // is2 did not finish
        while(!is2->endOfStream()) {
            key2 = is2->readNext();
            value2 = is2->readNext();
            time2 = is2->readNext();
            if(value2 > 0) {
                os->write(&key2);
                os->write(&value2);
                os->write(&time2);
                outputsize++;
                mostRecentKey = key2;
            }
            if(outputsize == fullLeaf) {
                os->close();
                iocounter = iocounter + os->iocounter;
                delete(os);
                // Just finished leaf index leafcounter, update parent
                if(leafCounter < (*nodeSize)+1) {
                    // We update the existing leaf value
                    (*leafs)[leafCounter] = fullLeaf;
                }
                else {
                    // Update value as well
                    values->push_back(numberOfNodes);
                    leafs->push_back(fullLeaf);
                }

                // Check keys
                if(leafCounter < (*nodeSize)) {
                    (*keys)[leafCounter] = mostRecentKey;
                }
                else {
                    keys->push_back(mostRecentKey);
                }

                string newname;
                leafCounter++;
                // Check if we can reuse old name
                if(!is2->endOfStream()) {
                    if(leafCounter < (*nodeSize)+1) {
                        newname = getBufferName((*values)[leafCounter],4);
                    }
                    else {
                        numberOfNodes++;
                        newname = getBufferName(numberOfNodes,4);
                    }

                    // New outputstream
                    if(FILE *file = fopen(newname.c_str(), "r")) {
                        fclose(file);
                        remove(newname.c_str());
                    }
                    os = new BufferedOutputStream(streamBuffer);
                    os->create(newname.c_str());

                }

                outputsize = 0;
            }
        }
    }

    // Update the last leaf
    if(outputsize > 0) {
        //cout << "Updating last leaf\n";
        // Just finished leaf index leafcounter, update parent
        if(leafCounter < (*nodeSize)+1) {
            // We update the existing leaf value
            (*leafs)[leafCounter] = fullLeaf;
        }
        else {
            // Update value as well
            values->push_back(numberOfNodes);
            leafs->push_back(outputsize);
        }

        // Check keys
        if(leafCounter < (*nodeSize)) {
            (*keys)[leafCounter] = mostRecentKey;
        }
        else {
            keys->push_back(mostRecentKey);
        }
        leafCounter++;
    }

    // Pop the last key
    keys->pop_back();

    // Clean up streams
    is1->close();
    iocounter = iocounter + is1->iocounter;
    delete(is1);
    is2->close();
    iocounter = iocounter + is2->iocounter;
    delete(is2);

    if(outputsize != 0) {
        os->close();
        iocounter = iocounter + os->iocounter;
        delete(os);
    }

    // Clean up temp files and buffers
    if(FILE *file = fopen(name1.c_str(), "r")) {
        fclose(file);
        remove(name1.c_str());
    }
    if(FILE *file = fopen(name2.c_str(), "r")) {
        fclose(file);
        remove(name2.c_str());
    }

    // Delete any unused original leafs
    if(leafCounter < (*nodeSize)+1) {
        // We did not fill out original leafs
        // Remove old leaf data
        for(int i = leafCounter; i < (*nodeSize)+1; i++) {
            string tempName = getBufferName((*values)[i],4);
            if(FILE *file = fopen(tempName.c_str(), "r")) {
                fclose(file);
                remove(tempName.c_str());
            }
        }
    }

    // Update nodeSize
    (*nodeSize) = leafCounter-1;

    //  Dont Write out node info and leaf info
    //writeNode(id,1,leafCounter-1,0,keys,values);
    //writeLeafInfo(id,leafs,leafCounter);

}

/*
 * Handles a special case when the tree is first created, where the root has a single empty leaf.
 */
void ExternalBufferedBTree::handleRootEmptyLeafBufferOverflow(int id, int* nodeSize, std::vector<int> *keys,
                                                              std::vector<int> *values, std::vector<int>* leafs) {

    // The buffer is alreay sorted, just ditribute it across the leafs
    InputStream* is = new BufferedInputStream(streamBuffer);
    string rootBufferName = getBufferName(id,1);
    is->open(rootBufferName.c_str());

    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string name1 = getBufferName((*values)[0],4); // Leaf2
    os->create(name1.c_str());

    if(leafs->size() != 0) {
        leafs->pop_back();
    }

    //cout << "NON = " << numberOfNodes << "\n";

    int outputsize = 0;
    int key,value,time;
    int mostRecentKey;
    int fullLeaf = leafSize*4;
    int leafCounter = 0;
    while(!is->endOfStream()) {
        key = is->readNext();
        value = is->readNext();
        time = is->readNext();
        if(value > 0) {
            os->write(&key);
            os->write(&value);
            os->write(&time);
            outputsize++;
            mostRecentKey = key;
        }
        if(outputsize == fullLeaf) {
            os->close();
            iocounter = iocounter + os->iocounter;
            delete(os);

            if(leafCounter > 0) {
                // Create new value
                //cout << "Pushing back " << numberOfNodes << "\n";
                values->push_back(numberOfNodes);
            }

            if(!is->endOfStream()) {
                // Must create new leaf to write to
                numberOfNodes++;
                string tempName = getBufferName(numberOfNodes,4);
                if(FILE *file = fopen(tempName.c_str(), "r")) {
                    fclose(file);
                    remove(tempName.c_str());
                }
                os = new BufferedOutputStream(streamBuffer);
                os->create(tempName.c_str());
            }

            keys->push_back(mostRecentKey);
            leafs->push_back(fullLeaf);

            leafCounter++;
            outputsize = 0;

        }
    }
    if(outputsize > 0) {
        os->close();
        iocounter = iocounter + os->iocounter;
        delete(os);
        values->push_back(numberOfNodes);
        keys->push_back(mostRecentKey);
        leafs->push_back(outputsize);
        leafCounter++;
    }

    // Last leaf doesnt require key
    keys->pop_back();

    // Update nodeSize
    (*nodeSize) = leafCounter-1;

    // Dont Write node and leaf data to disk

}




/***********************************************************************************************************************
 * Utility Methods
 **********************************************************************************************************************/

/*
 * Writes out the node to file "B<id>".
 * Deletes key and value arrays.
 * Handles that leafs have size-1 values.
 */
void ExternalBufferedBTree::writeNode(int id, int height, int nodeSize, int bufferSize, vector<int> *keys, vector<int> *values) {

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
void ExternalBufferedBTree::readNode(int id, int* height, int* nodeSize, int* bufferSize, vector<int> *keys, vector<int> *values) {


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
    else if(type == 4) {
        name = "Leaf";
        name += to_string(id);
    }
    return name;

}


/*
 * Reads a leaf into the given buffer.
 * Be sure that the buffer can contain the leaf!
 * Returns the size of the leaf.
 */
int ExternalBufferedBTree::readLeaf(int id, KeyValueTime **buffer) {

    int counter = 0;
    string name = getBufferName(id,4); // Leaf
    InputStream* is = new BufferedInputStream(streamBuffer);
    is->open(name.c_str());

    int key,value,time;
    KeyValueTime* kvt;
    while(!is->endOfStream()) {
        key = is->readNext();
        value = is->readNext();
        time = is->readNext();
        kvt = new KeyValueTime(key,value,time);
        buffer[counter] = kvt;
        counter++;
    }

    // Clean up
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);

    return counter;
}

/*
 * Deletes previous leaf file.
 * Writes out buffer to new leaf life.
 * Deletes elements in buffer.
 * Deletes buffer.
 */
void ExternalBufferedBTree::writeLeaf(int id, KeyValueTime **buffer, int bufferSize) {

    // Delete previous leaf life
    string name = getBufferName(id,4);
    if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }

    // Open outputstream
    OutputStream* os = new BufferedOutputStream(streamBuffer);
    os->create(name.c_str());

    // Write out buffer to new leaf life
    for(int i = 0; i < bufferSize; i++) {
        os->write(&(buffer[i]->key));
        os->write(&(buffer[i]->value));
        os->write(&(buffer[i]->time));
    }

    // Clean up stream
    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);


    // Delete kvt
    /*for(int i = 0; i < deleteSize; i++) {
        if(buffer[i] != nullptr) {
            delete(buffer[i]);
        }
    }*/
    for(int i = 0; i < bufferSize; i++) {
        delete(buffer[i]);
    }

    // Delete buffer
    // TODO: Why does this throw an error?
    delete[] buffer;



}

/*
 * Reads leaf into from "Leaf<id>", and returns its size
 */
//int ExternalBufferedBTree::readLeafInfo(int id, int *leafs) {
int ExternalBufferedBTree::readLeafInfo(int id, vector<int> *leafs) {

    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = getBufferName(id,4);
    is->open(name.c_str());
    int counter = 0;
    while(!is->endOfStream()) {
        //leafs[counter] = is->readNext();
        leafs->push_back(is->readNext());
        counter++;
    }

    // Clean up
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);

    return counter;
}

/*
 * Also deletes the array
 */
//void ExternalBufferedBTree::writeLeafInfo(int id, int *leafs, int leafSize) {
void ExternalBufferedBTree::writeLeafInfo(int id, vector<int> *leafs, int leafSize) {

    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string name = getBufferName(id,4);
    if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }
    os->create(name.c_str());
    for(int i = 0; i < leafSize; i++) {
        //os->write(&(leafs[i]));
        os->write(&(leafs->at(i)));
    }

    // Clean up
    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);
    delete(leafs);
    //delete[] leafs;
}

void ExternalBufferedBTree::printTree() {
    cout << "Print Tree!\n";
    cout << "Root is " << root << "\n";
    cout << "Number of nodes is " << numberOfNodes << "\n";
    cout << "--- Update Buffer\n";
    for(int i = 0; i < update_bufferSize; i++) {
        cout << update_buffer[i]->key << " " << update_buffer[i]->value << " " << update_buffer[i]->time << "\n";
    }
    printNode(root);
}

void ExternalBufferedBTree::printNode(int node) {

    cout << "==== Node " << node << "\n";

    int height, nodeSize, bufferSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* ptr_bufferSize = &bufferSize;
    vector<int>* keys = new vector<int>();
    keys->reserve(size*4-1);
    vector<int>* values = new vector<int>();
    values->reserve(size*4);
    readNode(node,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

    if(node == root) {
        bufferSize = rootBufferSize;
    }

    cout << "Height " << height << "\n";
    cout << "NodeSize " << nodeSize << "\n";
    cout << "BufferSize " << bufferSize << "\n";
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
        KeyValueTime** buffer = new KeyValueTime*[bufferSize];
        readBuffer(node,buffer,1);
        for(int i = 0; i < bufferSize; i++) {
            cout << buffer[i]->key << " " << buffer[i]->value << " " << buffer[i]->time << " " <<"\n";
        }

        // Clean up
        for(int i = 0; i < bufferSize; i++) {
            delete(buffer[i]);
        }
        delete[] buffer;
    }

    delete(keys);

    if(nodeSize > 0 || (*values)[0] != 0) {
        if(height == 1) {
            vector<int>* leafs = new vector<int>();
            readLeafInfo(node,leafs);
            cout << "--- Leafs\n";
            for(int i = 0; i < nodeSize+1; i++) {
                cout << (*leafs)[i] << "\n";
            }
            delete(leafs);
            for(int i = 0; i < nodeSize+1; i++) {
                printLeaf((*values)[i]);
            }
        }
        else {
            for(int i = 0; i < nodeSize+1; i++) {
                printNode((*values)[i]);
            }
        }
    }

    delete(values);

}

void ExternalBufferedBTree::printLeaf(int leaf) {

    cout << "==== Leaf " << leaf << "\n";
    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = getBufferName(leaf,4);
    is->open(name.c_str());
    while(!is->endOfStream()) {
        cout << is->readNext() << " " << is->readNext() << " " << is->readNext() << "\n";
    }
    is->close();
    delete(is);
}

void ExternalBufferedBTree::cleanUpExternallyAfterNodeOrLeaf(int id) {

    // Cleanup "Buf<id>".
    string name = getBufferName(id,1);
    if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }

    // Cleanup "Leaf<id>"
    name = getBufferName(id,4);
    if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }

    name = "B";
    name += to_string(id);
    if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }


}

void ExternalBufferedBTree::cleanUpTree() {
    string name = getBufferName(1,2);
    if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }
    name = getBufferName(1,3);
    if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }

    for(int i = 1; i <= numberOfNodes; i++) {
        cleanUpExternallyAfterNodeOrLeaf(i);
    }
}

void ExternalBufferedBTree::cleanUpFromTo(int from, int to) {

    for(int i = from; i <= to; i++) {
        cleanUpExternallyAfterNodeOrLeaf(i);
    }
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