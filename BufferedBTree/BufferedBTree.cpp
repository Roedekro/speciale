//
// Created by martin on 10/30/17.
// Buffered BTree with the top of the tree stored in
// internal memory. The buffer size is O(B), and
// a fanout of O(delta/log(N)) where delta
// log(N) < delta <= B log(N).
// Delta can then be used as a tradeoff between
// insert and query time.
//
// NOTE: Each node will use max 2*B space + Buffer of B.
//

#include "BufferedBTree.h"
#include "../Streams/OutputStream.h"
#include "../Streams/BufferedOutputStream.h"
#include "../Streams/InputStream.h"
#include "../Streams/BufferedInputStream.h"
#include <sstream>
#include <iostream>
#include <unistd.h>
#include <cmath>
#include <algorithm>

using namespace std;

/*
 * General Methods
 */

/*
 * Create a B-tree by creating an empty root.
 */
BufferedBTree::BufferedBTree(int B, int M, int N, float delta) {
    // Create root
    this->B = B;
    this->M = M;

    // Fanout must be Theta(delta/log N)
    size = delta / log2(N);
    // Each node takes up max 3B space, 2B for keys/values and B for the buffer.
    size = (B/sizeof(int))/4; // #values. Max #keys will be 4*size-1. Min = size.

    bufferSize = (int) B/sizeof(KeyValueTime);
    numberOfNodes = 1;
    currentNumberOfNodes = 1;
    root = new BufferedInternalNode(numberOfNodes,1,size,true,bufferSize);
    internalNodeCounter=1;
    externalNodeHeight = -1; // Height the external nodes begin <--------------------------------------------------------------------------!!!!!!!!!!!!!!
    // How many nodes can we store in internal Memory?
    // Three ints, an array of keys, and an array of pointers, pr. Node. + a buffer.
    int internalNodeSize = ((size*4-1)*sizeof(int)) + (size*4*sizeof(BufferedInternalNode*)) + B; // Keys, children, buffer
    maxInternalNodes = M/internalNodeSize;
    int internalHeight = (int) (log(maxInternalNodes) / log(4*size));
    if(internalHeight - (log(maxInternalNodes) / log(4*size)) != 0) {
        //
        internalHeight++;
        //cout << "Increased height\n";
    }
    minInternalNodes = (int) pow(4*size,internalHeight-1);
    iocounter = 0;
    /*cout << "Created root " << root->id << "\n";
    cout << "Tree will have size = " << size << " resulting in #keys = " << 4*size-1 << " with int size = " << sizeof(int) << " and buffer size "
            << bufferSize << "\n";
    cout << "Max internal nodes = " << maxInternalNodes << " with internal node size = " << internalNodeSize << "\n";
    cout << "Internal height is " << internalHeight << " and min internal nodes is " << minInternalNodes << "\n";*/
}

BufferedBTree::~BufferedBTree() {

}






/*
 * Tree Structural Methods
 */




/*
 * Buffer Handling Methods
 */




/*
 * Utility Methods
 */









/*
 * OLD METHODS BELOW
 * DEPRECATED
 * ETC.
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 */



/*
 * Responsible for externalizing the lowest level of the internal nodes.
 * Should be called when internalNodeCount > maxInternalNodes.
 * The extra nodes will never be greater than O(log(M/B)), and
 * will in practice be much smaller.
 */
/*void BufferedBTree::externalize() {

    cout << "Externalizing #internalNodes " << internalNodeCounter << " maxInternalNodes " << maxInternalNodes << "\n";
    cout << "#nodes in tree " << currentNumberOfNodes << " External node height is " << externalNodeHeight << "\n";

    if(externalNodeHeight == -1) {
        // Special case, first time we externalize
        externalNodeHeight = 1;
    }
    else {
        externalNodeHeight++;
    }

    // Recursively travel down to the external node height +1
    // and externalize the children of the nodes at this level.
    recursiveExternalize(root);

    cout << "Externalization completed, new #internalNodes " << internalNodeCounter << " external node height " << externalNodeHeight << "\n";
}

void BufferedBTree::recursiveExternalize(BufferedInternalNode *node) {

    if(node->height != externalNodeHeight+1) {
        // Recurse on children
        for(int i = 0; i < node->nodeSize+1; i++) {
            recursiveExternalize(node->children->at(i));
        }
    }
    else {
        // Externalize children
        node->values = new vector<int>();
        node->values->reserve(4*size);
        int cHeight, cSize;
        vector<int>* cKeys = new vector<int>();
        cKeys->reserve(size*4);
        vector<int>* cValues = new vector<int>();
        cValues->reserve(4*size);
        BufferedInternalNode* child;
        for(int i = 0; i < node->nodeSize+1; i++) {
            child = node->children->at(i);
            writeNode(child->id,child->height,child->nodeSize,child->keys,child->values);
            //cout << "Externalized node " << child->id << " from parent " << node->id << "\n";
            (*node->values)[i] = child->id;
            delete(child);
            internalNodeCounter--;
        }
        delete[] node->children;
    }
}

/*
 * Inserts key/value pair.
 * If key already present it updates value.
 */
/*void BufferedBTree::insert(KeyValueTime element) {

    // Commented out, work in progress

    /*root->buffer->push_back(*element);

    if(root->buffer->size() < bufferSize) {
        // Do nothing
        return;
    }
    else {
        // Sort the buffer of the root
        sortInternal(root->buffer);

        // Check if we need to split the root
        // In which case the buffer also gets split,
        // and no further action is needed.
        if(root->nodeSize == 4*size-1) {
            numberOfNodes++;
            internalNodeCounter++;
            currentNumberOfNodes++;
            BufferedInternalNode* newRoot = new BufferedInternalNode(numberOfNodes,root->height+1,size,false,bufferSize);
            (*newRoot->children)[0] = root;
            // Note that children of the new root will always be internal
            // Even if they are supposed to be external we dont externalize them
            // until after the insertion
            splitChildInternal(newRoot,root,0);
            root = newRoot;

            // Special case if root was leaf???

        }
        else {
            insertIntoNonFullInternal(root);
        }

        // Check if we need to externalize
        if(internalNodeCounter > maxInternalNodes) {
            externalize();
        }
    }
     */
/*}

/*
 * Assumes the input nodes buffer is sorted.
 */
/*void BufferedBTree::insertIntoNonFullInternal(KeyValueTime element, BufferedInternalNode *node) {

    // commented out because work in progress


    // Split the sorted buffer of node to its children
    /*if(node->height == 2) {
        // Children are leafs

        if(externalNodeHeight <= 0) {
            // Internal children who are leafs

            // Use two temporary vectors to merge
            // childrens buffer with ours
            std::vector<KeyValueTime>* toChild = new std::vector<KeyValueTime>();
            toChild->reserve(bufferSize);
            std::vector<KeyValueTime>* temp;

            // Distribute to children
            BufferedInternalNode* child;
            int bufferIndex = 0;
            for(int i = 0; i < node->nodeSize; i++) {
                child = node->children->at(i);
                int divider = node->values->at(i);
                // Run through buffer, adding to toChild
                while(bufferIndex < node->buffer->size() && (*node->buffer)[bufferIndex].key <= divider) {
                    toChild->push_back((*node->buffer)[bufferIndex]);
                }
                temp = child->buffer;
                child->buffer = new std::vector<KeyValueTime>();
                child->buffer->reserve(bufferSize);
                // Merge toChild and temp into new child buffer
                int toChildIndex = 0;
                int tempIndex = 0;
                while(toChildIndex < toChild->size() || tempIndex < temp->size()) {
                    // Pick
                    if(toChildIndex >= toChild->size()) {
                        // toChild done, add from temp
                        KeyValueTime kvt = temp->at(tempIndex);
                        child->buffer->push_back(kvt);
                        tempIndex++;
                    }
                    else if(tempIndex >= temp->size()) {
                        KeyValueTime kvt = toChild->at(toChildIndex);
                        child->buffer->push_back(kvt);
                        toChildIndex++;
                    }
                    else {
                        if(temp->at(tempIndex).key == toChild->at(toChildIndex).key) {
                            // Special case, update

                        }
                        else if(temp->at(tempIndex).key < toChild->at(toChildIndex).key) {
                            KeyValueTime kvt = temp->at(tempIndex);
                            child->buffer->push_back(kvt);
                            tempIndex++;
                        }
                        else {
                            KeyValueTime kvt = toChild->at(toChildIndex);
                            child->buffer->push_back(kvt);
                            toChildIndex++;
                        }
                    }
                }
            }
            // Last child


        }
        else {
            // Children are external leafs

        }


    }
    else {

    }

    //cout << "Insert Non Full Internal " << element->key << " Node ID = " << node->id << "\n";

    /*int i = node->nodeSize;
    if(node->height == 1) {
        // Insert into this leaf

        // Check if key is present
        int j = 0;
        while(j < node->nodeSize && node->keys[j] != element->key) {
            j++;
        }
        if(node->keys[j] == element->key) {
            // Special case, just update value
            node->values[j] = element->value;
            //cout << "Node = " <<  node->id << " Updated key = " << element->key << " to value = " << element->value << "\n";
            return;
        }
        else {
            // Move keys and values back, insert new pair
            while(i > 0 && element->key < node->keys[i-1]) {
                node->keys[i] = node->keys[i-1];
                node->values[i] = node->values[i-1];
                i--;
            }
            node->keys[i] = element->key;
            node->values[i] = element->value;
            (node->nodeSize)++;
            //cout << "Node = " <<  node->id << " Inserted key = " << element->key << " with value = " << element->value << "\n";
            return;
        }


    }
    else {
        // Search for node to insert into

        // Find child
        while(i > 0 && element->key <= node->keys[i-1]) {
            i--;
        }
        //int child = node->values[i];


        if(node->height != externalNodeHeight+1) {
            // Internal children

            BufferedInternalNode* child = node->children[i];
            if(child->nodeSize == size*4-1) {
                // Split child
                splitChildInternal(node,child,i);
                //(node->nodeSize)++;

                if(element->key > node->keys[i]) {
                    child = node->children[i+1];
                }
            }
            insertIntoNonFullInternal(element,child);

        }
        else {
            // External children

            int child = node->values[i];
            // Load in child
            int cHeight, cSize;
            int* ptr_cHeight = &cHeight;
            int* ptr_cSize = &cSize;
            int* cKeys = new int[size*4-1];
            int* cValues = new int[size*4];
            readNode(child,ptr_cHeight,ptr_cSize,cKeys,cValues);
            bool cWrite = false;

            // Check if we need to split the child
            if(cSize == 4*size-1) {

                cWrite = true;
                int newSize = 2*size-1;
                int* newKeys = new int[size*4-1];
                int* newValues = new int[size*4];

                // Special
                splitChildBorder(node,i,ptr_cSize, cKeys, cValues,
                                 newKeys, newValues);

                //cout << "Split children new sizes " << cSize << " " << newSize << "\n";

                // Check which of the two children we need to recurse upon.
                if(element->key > node->keys[i]) {

                    // Write out old child
                    //cout << "Writing out old child " << child << " with size " << cSize << "\n";
                    writeNode(child,cHeight,cSize,cKeys,cValues);

                    // Switch child to new child.
                    i++;
                    child = node->values[i];
                    cKeys = newKeys;
                    cValues = newValues;
                    cSize = newSize;

                }
                else {
                    // Write out new child
                    writeNode(numberOfNodes,cHeight,newSize,newKeys,newValues);
                }
            }

            // Insert into child - will switch to external functions
            insertIntoNonFull(element,child,cHeight,cSize,cKeys,cValues,cWrite);
        }
    }
    */
/*}

/*
 * Inserts into an internal node or a leaf.
 * Write denotes whether a node was changed, ans thus needs to be written to disk.
 */

/*void BufferedBTree::insertIntoNonFull(KeyValueTime element, int id, int height, int nodeSize, int *keys, int *values, bool write) {

    int i = nodeSize;
    if(height == 1) {
        // Insert into this leaf

        // Check if key is present
        int j = 0;
        while(j < nodeSize && keys[j] != element.key) {
            j++;
        }
        /*if(keys[j] == element.key) {
            // Special case, just update value
            values[j] = element.value;
            writeNode(id,height,nodeSize,keys,values);
            //cout << "Node = " <<  id << " Updated key = " << element->key << " to value = " << element->value << "\n";
            return;
        }
        else {
            // Move keys and values back, insert new pair
            while(i > 0 && element.key < keys[i-1]) {
                keys[i] = keys[i-1];
                values[i] = values[i-1];
                i--;
            }
            keys[i] = element.key;
            values[i] = element.value;
            nodeSize++;
            writeNode(id,height,nodeSize,keys,values);
            //cout << "Node = " <<  id << " Inserted key = " << element->key << " with value = " << element->value << "\n";
            return;
        }*/
        // Move keys and values back, insert new pair
        /*while(i > 0 && element.key < keys[i-1]) {
            keys[i] = keys[i-1];
            values[i] = values[i-1];
            i--;
        }
        keys[i] = element.key;
        values[i] = element.value;
        nodeSize++;
        writeNode(id,height,nodeSize,keys,values);
        //cout << "Node = " <<  id << " Inserted key = " << element->key << " with value = " << element->value << "\n";
        return;
    }
    else {
        // Internal node

        // Find child
        while(i > 0 && element.key <= keys[i-1]) {
            i--;
        }
        int child = values[i];

        // Load in child
        int cHeight, cSize;
        int* ptr_cHeight = &cHeight;
        int* ptr_cSize = &cSize;
        int* cKeys = new int[size*4-1];
        int* cValues = new int[size*4];
        readNode(child,ptr_cHeight,ptr_cSize,cKeys,cValues);
        bool cWrite = false;

        // Check if we need to split the child
        if(cSize == 4*size-1) {

            write = true;
            cWrite = true;

            int newSize = 2*size-1;
            int* newKeys = new int[size*4-1];
            int* newValues = new int[size*4];

            splitChild(height, nodeSize, keys, values, i, ptr_cSize, cKeys, cValues,
                       newKeys, newValues);
            nodeSize++;

            //cout << "Split children new sizes " << cSize << " " << newSize << "\n";

            // Check which of the two children we need to recurse upon.
            if(element.key > keys[i]) {

                // Write out old child
                //cout << "Writing out old child " << child << " with size " << cSize << "\n";
                writeNode(child,cHeight,cSize,cKeys,cValues);

                // Switch child to new child.
                i++;
                child = values[i];
                cKeys = newKeys;
                cValues = newValues;
                cSize = newSize;

            }
            else {
                // Write out new child
                writeNode(numberOfNodes,cHeight,newSize,newKeys,newValues);
            }
        }

        // Write node to disk, if necessary
        if(write) {
            writeNode(id,height,nodeSize,keys,values);
        }

        // Insert into child
        insertIntoNonFull(element,child,cHeight,cSize,cKeys,cValues,cWrite);
    }
}

/*
 * Split internal children.
 * Will handle if childrens children are external.
 */
/*void BufferedBTree::splitChildInternal(BufferedInternalNode* parent, BufferedInternalNode *child, int childNumber) {

    //cout << "Split Internal Child on parent " << parent->id << " child " << child->id << " childnumber " << childNumber <<"\n";
    //cout << parent->nodeSize << " " << externalNodeHeight << " " << child->height << "\n";

    numberOfNodes++;
    currentNumberOfNodes++;
    internalNodeCounter++;
    bool externalChildren = (child->height == externalNodeHeight+1);
    BufferedInternalNode* newChild;
    // Special case if this is a leaf
    if(child->height == 1) {
        newChild = new BufferedInternalNode(numberOfNodes,child->height,size,true,bufferSize);
    }
    else {
        newChild = new BufferedInternalNode(numberOfNodes,child->height,size,externalChildren,bufferSize);
    }

    // Fill out new childs keys
    for(int j = 0; j < 2*size-1; j++) {
        newChild->keys[j] = child->keys[2*size+j];
    }

    // Fill out values
    if(!externalChildren) {
        if(child->height == 1) {
            // Children are internal leafs
            for(int j = 0; j < 2*size-1; j++) {
                newChild->values[j] = child->values[2*size+j];
            }
        }
        else {
            // Children are internal nodes
            for(int j = 0; j < 2*size; j++) {
                newChild->children[j] = child->children[2*size+j];
            }
        }
    }
    else {
        // Childrens children are external nodes
        for(int j = 0; j < 2*size; j++) {
            newChild->values[j] = child->values[2*size+j];
        }

    }

    // Move parents keys by one
    for(int j = parent->nodeSize; j > childNumber; j--) {
        parent->keys[j] = parent->keys[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    parent->keys[childNumber] = child->keys[2*size-1];

    // Also update size of children.
    if(child->height == 1) {
        child->nodeSize = 2*size;
    }
    else {
        child->nodeSize = 2*size-1;
    }
    newChild->nodeSize = 2*size-1;

    // Move parents values back by one
    for(int j = parent->nodeSize+1; j > childNumber; j--) {
        parent->children[j] = parent->children[j-1];
    }
    // Insert pointer to new child
    parent->children[childNumber+1] = newChild;

    // Increase parent size
    (parent->nodeSize)++;

}

/*
 * Special case where the parent is a node in internal memory and the children are external nodes
 */
/*void BufferedBTree::splitChildBorder(BufferedInternalNode *parent, int childNumber, int *childSize, int *cKeys,
                                     int *cValues, int *newKeys, int *newValues) {

    numberOfNodes++;
    currentNumberOfNodes++;
    int newChild = numberOfNodes;

    // Fill out new childs keys
    for(int j = 0; j < 2*size-1; j++) {
        newKeys[j] = cKeys[2*size+j];
    }

    // Copy values
    if(parent->height == 2) {
        // Children are leafs
        for(int j = 0; j < 2*size-1; j++) {
            newValues[j] = cValues[2*size+j]; // <---?
        }
    }
    else {
        // Children are internal nodes
        for(int j = 0; j < 2*size; j++) {
            newValues[j] = cValues[2*size+j];
        }
    }

    // Move parents keys by one
    for(int j = parent->nodeSize; j > childNumber; j--) {
        parent->keys[j] = parent->keys[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    parent->keys[childNumber] = cKeys[2*size-1];

    // Also update size of old child.
    if(parent->height == 2) {
        *childSize = 2*size;
    }
    else {
        *childSize = 2*size-1;
    }

    // Move parents values back by one
    for(int j = parent->nodeSize+1; j > childNumber; j--) {
        parent->values[j] = parent->values[j-1];
    }
    // Insert pointer to new child
    parent->values[childNumber+1] = newChild;

    (parent->nodeSize)++;

}

/*
 * Splits a child of a node into a new child.
 * Does not clean up parent, existing child or new child, responsibility is left to calling function.
 * You must increase parents size by one, and set the old child and new childs size to size-1;
 * Will update nodeSize of original child correctly, depending on if its a leaf or not.
 * New child will have nodeSize = size-1;
 */
/*void BufferedBTree::splitChild(int height, int nodeSize, int *keys, int *values, int childNumber, int* childSize,
                               int *cKeys, int *cValues, int *newKeys, int *newValues) {

    numberOfNodes++;
    currentNumberOfNodes++;
    int newChild = numberOfNodes;

    // Fill out new childs keys
    for(int j = 0; j < 2*size-1; j++) {
        newKeys[j] = cKeys[2*size+j];
    }

    // Copy values
    if(height == 2) {
        // Children are leafs
        for(int j = 0; j < 2*size-1; j++) {
            newValues[j] = cValues[2*size+j]; // <---?
        }
    }
    else {
        // Children are internal nodes
        for(int j = 0; j < 2*size; j++) {
            newValues[j] = cValues[2*size+j];
        }
    }

    // Move parents keys by one
    for(int j = nodeSize; j > childNumber; j--) {
        keys[j] = keys[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    keys[childNumber] = cKeys[2*size-1];

    // Also update size of old child.
    if(height == 2) {
        *childSize = 2*size;
    }
    else {
        *childSize = 2*size-1;
    }

    // Move parents values back by one
    for(int j = nodeSize+1; j > childNumber; j--) {
        values[j] = values[j-1];
    }
    // Insert pointer to new child
    values[childNumber+1] = newChild;

}

/*int BufferedBTree::query(int element) {

    // Root is in internal memory
    BufferedInternalNode* node = root;
    while(node->height > externalNodeHeight+1 && node->height != 1) {
        // Find position
        int i = node->nodeSize;
        while(i > 0 && element <= node->keys[i-1]) {
            i--;
        }
        node = node->children[i];
    }

    // Two cases
    // 1) Hit internal leaf
    // 2) Hit internal node on height externalNodeHeight+1

    if(node->height == 1) {
        // Internal leaf, just report element
        int j = 0;
        while(j < node->nodeSize && node->keys[j] != element) {
            j++;
        }
        return node->values[j];
    }
    else {

        // Search externally

        // First find the approriate child as root of the subtree
        int i = node->nodeSize;
        while(i > 0 && element <= node->keys[i-1]) {
            i--;
        }
        int currentNode = node->values[i];

        // Load in child
        int height, nodeSize;
        int* ptr_height = &height;
        int* ptr_nodeSize = &nodeSize;
        int* keys = new int[size*4-1];
        int* values = new int[size*4];
        readNode(currentNode,ptr_height,ptr_nodeSize,keys,values);

        // Go down in tree until we hit the leaf
        while(height != 1) {

            //cout << "---Inside node " << currentNode << "\n";

            // Find position
            int i = nodeSize;
            while(i > 0 && element <= keys[i-1]) {
                i--;
            }
            currentNode = values[i];

            //cout << "Found node " << currentNode << " in place " << i << "\n";

            readNode(currentNode,ptr_height,ptr_nodeSize,keys,values);
        }

        //cout << "Searching leaf, node " << currentNode << "\n";

        // In leaf
        int j = 0;
        while(j < nodeSize && keys[j] != element) {
            j++;
        }

        // Postpone return, clean up first.
        int ret = -1;
        if(keys[j] == element) {
            ret = values[j];
        }

        delete[] keys;
        delete[] values;

        return ret;
    }
}

void BufferedBTree::internalize() {

    if(currentNumberOfNodes == internalNodeCounter) {
        // Do nothing, no external nodes to internalize
        return;
    }
    else if(externalNodeHeight > 0) {
        cout << "Internalize, #internalNodes = " << internalNodeCounter << "\n";
        externalNodeHeight--;
        recursiveInternalize(root);
    }
}

void BufferedBTree::recursiveInternalize(BufferedInternalNode *node) {

    if(node->height != externalNodeHeight+2) {
        // Recurse internally
        for(int i = 0; i < node->nodeSize+1; i++) {
            recursiveInternalize(node->children[i]);
        }
    }
    else {
        // Children are external, internalize them
        cout << "=Internalizing children of node " << node->id << "\n";
        node->children = new BufferedInternalNode*[4*size];
        for(int i = 0; i < node->nodeSize+1; i++) {
            int id = node->values[i];
            cout << "Internalizing node " << id << "\n";
            BufferedInternalNode* child = new BufferedInternalNode(id,0,size,true,bufferSize);
            int* ptr_height = &(child->height);
            int* ptr_nodeSize = &(child->nodeSize);
            readNode(id,ptr_height,ptr_nodeSize,child->keys,child->values);
            node->children[i] = child;
            internalNodeCounter++;
        }
        delete[] node->values;
    }
}

void BufferedBTree::deleteElement(int element) {

    // Root always in internal memory
    // Perform check to see if we need to shrink the tree.
    if(root->height != 1 && root->nodeSize == 0) {
        cout << "Deleting root " << root->id << "\n";
        BufferedInternalNode* oldRoot = root;
        if(root->height == externalNodeHeight+1) {
            // Special case
            cout << "Special case " << root->id << " " << root->values[0] << "\n";
            // Special case where root is the only node in internal memory,
            // and we must internalize its child
            int child = root->values[0];
            root = new BufferedInternalNode(root->values[0],root->height-1,size,true,bufferSize);
            int height, nodeSize;
            int* ptr_height = &height;
            int* ptr_nodeSize = &nodeSize;
            readNode(root->id,ptr_height,ptr_nodeSize,root->keys,root->values);
            root->height = height;
            root->nodeSize = nodeSize;
            cout << root->id << " " << root->height << " " << root->nodeSize << "\n";
            delete[] oldRoot->keys;
            delete[] oldRoot->values;
            delete(oldRoot);
            externalNodeHeight--;
        }
        else {
            root = root->children[0];
            delete[] oldRoot->keys;
            delete[] oldRoot->children;
            delete(oldRoot);
        }
        internalNodeCounter--;
        currentNumberOfNodes--;
        cout << "New root is node " << root->id << "\n";
        deleteElement(element);
    }
    else {
        deleteNonSparseInternal(element,root);
    }

    if(internalNodeCounter < minInternalNodes) {
        internalize();
    }
}

void BufferedBTree::deleteNonSparseInternal(int element, BufferedInternalNode *node) {

    cout << "Delete Non Sparse Internal " << node->id << "\n";
    if(node->height == 1) {
        cout << "Found internal leaf\n";
        // Leaf
        // Find key position
        int i = 0;
        while(i < node->nodeSize && node->keys[i] != element) {
            i++;
        }
        if(node->keys[i] == element) {
            (node->nodeSize)--;
            while(i < node->nodeSize) {
                // Move array indexes forward one place
                node->keys[i] = node->keys[i+1];
                node->values[i] = node->values[i+1];
                i++;
            }
            //cout << "Deleted key = " << element << " from node " << id << "\n";
            return;
        }
        else {
            // Key was not present
            //cout << "Key not present in node " << id << "\n";
            return;
        }
    }
    else if(node->height > externalNodeHeight+1){
        cout << "Internal child\n";
        // Children are internal
        int i = node->nodeSize;
        while(i > 0 && element <= node->keys[i-1]) {
            i--;
        }
        BufferedInternalNode* child = node->children[i];

        // Check if child is too small
        if(child->nodeSize == size-1) {
            // Fuse child
            fuseChildInternal(node,child,i);

            // Recheck child to recurse upon
            int i = node->nodeSize;
            while(i > 0 && element <= node->keys[i-1]) {
                i--;
            }
            child = node->children[i];
        }
        deleteNonSparseInternal(element,child);
    }
    else {

        // Children are external
        int i = node->nodeSize;
        while(i > 0 && element <= node->keys[i-1]) {
            i--;
        }
        int child = node->values[i];

        cout << "External child " << child << " " << i << " " << element << " " << node->nodeSize << "\n";



        // Load in child
        int cHeight, cSize;
        int* ptr_cHeight = &cHeight;
        int* ptr_cSize = &cSize;
        int* cKeys = new int[size*4-1];
        int* cValues = new int[size*4];
        readNode(child,ptr_cHeight,ptr_cSize,cKeys,cValues);
        bool cWrite = false;

        if(cSize == size-1) {

            cWrite = true;
            cout << "!!!!!!\n";
            // Fuse child to ensure place for deletion
            fuseChildBorder(node,i,ptr_cSize,cKeys,cValues);

            // Recheck what child to recurse upon
            // Fusechild will update cKeys and cValues
            i = node->nodeSize;
            while(i > 0 && element <= node->keys[i-1]) {
                i--;
            }
            child = node->values[i];
        }
        deleteNonSparse(element,child,cHeight,cSize,cKeys,cValues,cWrite);
    }
}

/*
 * Responsible for cleaning up arguments!
 * Write indicates whether a node was changed, and thus needs to be written to disk.
 */
/*void BufferedBTree::deleteNonSparse(int element, int id, int height, int nodeSize, int *keys, int *values, bool write) {

    cout << "Delete on node " << id << " height " << height << " of size " << nodeSize << "\n";

    if(height == 1) {
        // Leaf, delete

        // Find key position
        int i = 0;
        while(i < nodeSize && keys[i] != element) {
            i++;
        }
        if(keys[i] == element) {
            nodeSize--;
            while(i < nodeSize) {
                // Move array indexes forward one place
                keys[i] = keys[i+1];
                values[i] = values[i+1];
                i++;
            }
            writeNode(id,height,nodeSize,keys,values);
            //cout << "Deleted key = " << element << " from node " << id << "\n";
            return;
        }
        else {
            // Key was not present
            if(write) {
                writeNode(id,height,nodeSize,keys,values);
            }
            //cout << "Key not present in node " << id << "\n";
            return;
        }
    }
    else {
        // Internal node, find child to recurse upon
        int i = nodeSize;
        while(i > 0 && element <= keys[i-1]) {
            i--;
        }
        int child = values[i];

        // Load in child
        int cHeight, cSize;
        int* ptr_cHeight = &cHeight;
        int* ptr_cSize = &cSize;
        int* cKeys = new int[size*4-1];
        int* cValues = new int[size*4];
        readNode(child,ptr_cHeight,ptr_cSize,cKeys,cValues);
        bool cWrite = false;

        if(cSize == size-1) {

            write = true;
            cWrite = true;

            // Fuse child to ensure place for deletion
            int* ptr_nodeSize = &nodeSize;
            fuseChild(height,ptr_nodeSize,keys,values,i,ptr_cSize,cKeys,cValues);

            // Recheck what child to recurse upon
            // Fusechild will update cKeys and cValues
            i = nodeSize;
            while(i > 0 && element <= keys[i-1]) {
                i--;
            }
            child = values[i];
        }

        if(write) {
            writeNode(id,height,nodeSize,keys,values);
        }
        deleteNonSparse(element,child,cHeight,cSize,cKeys,cValues,cWrite);
    }
}


// Reponsible for fusing internal children, cleans up and adjusts all values in involved nodes.
// Must handle children having external children.
void BufferedBTree::fuseChildInternal(BufferedInternalNode *parent, BufferedInternalNode *child, int childNumber) {

    /*
     * Two possible scenarios:
     * a) The child has a neighbouring sibling who also contains size-1 keys.
     * b) The child has a neighbouring sibling containing >= size keys.
     * We prefer case a over case b, since it allows for faster consecutive deletes.
     */

    /*cout << "Fuse child internal " << child->id << "\n";

    // Check case and placement
    BufferedInternalNode* sibling;
    bool caseA = false;
    bool leftSibling = false;
    if(childNumber == 0) {
        // Leftmost child, check right sibling
        sibling = parent->children[1];
        if(sibling->nodeSize == size-1) {
            caseA = true;
        }
    }
    else if (childNumber == parent->nodeSize) {
        // Rightmost child, check left sibling
        sibling = parent->children[(parent->nodeSize)-1];
        if(sibling->nodeSize == size-1) {
            caseA = true;
        }
        leftSibling = true;
    }
    else {
        // Check both left and right sibling
        sibling = parent->children[childNumber+1];
        if(sibling->nodeSize == size-1) {
            caseA = true;
        }
        else {
            sibling = parent->children[childNumber-1];
            if(sibling->nodeSize == size-1) {
                caseA = true;
            }
            leftSibling = true;
        }
    }

    if(caseA) {
        // Case a, fuse child with its sibling
        // Fuse right child over into left child
        if(leftSibling) {
            if(parent->height != 2) {
                if(child->height != externalNodeHeight+1) {
                    // Internal children
                    // Fuse child into left sibling

                    // We lack one key to separate the two sets of keys.
                    // This key is the left siblings rightmost childs
                    // rightmost key. This key can be found separating
                    // the two children in the parent!

                    sibling->keys[sibling->nodeSize] = parent->keys[childNumber-1];

                    for(int i = 0; i < child->nodeSize; i++) {
                        sibling->keys[sibling->nodeSize+1+i] = child->keys[i];
                    }
                    for(int i = 0; i < child->nodeSize+1; i++) {
                        sibling->children[sibling->nodeSize+1+i] = child->children[i];
                    }
                    sibling->nodeSize = sibling->nodeSize+1+child->nodeSize;
                }
                else {
                    sibling->keys[sibling->nodeSize] = parent->keys[childNumber-1];

                    for(int i = 0; i < child->nodeSize; i++) {
                        sibling->keys[sibling->nodeSize+1+i] = child->keys[i];
                    }
                    for(int i = 0; i < child->nodeSize+1; i++) {
                        sibling->values[sibling->nodeSize+1+i] = child->values[i];
                    }
                    sibling->nodeSize = sibling->nodeSize+1+child->nodeSize;
                }
            }
            else {
                // Children are leafs, and can not be external
                // Fuse child into left sibling
                for(int i = 0; i < child->nodeSize; i++) {
                    sibling->keys[sibling->nodeSize+i] = child->keys[i];
                    sibling->values[sibling->nodeSize+i] = child->values[i];
                }
                sibling->nodeSize = sibling->nodeSize+child->nodeSize;
            }

            // Update parent
            for(int i = childNumber-1; i < parent->nodeSize; i++) {
                parent->keys[i] = parent->keys[i+1];
            }
            for(int i = childNumber; i < parent->nodeSize; i++) {
                parent->children[i] = parent->children[i+1];
            }
            (parent->nodeSize)--;

            // Delete original child
            delete[] child->keys;
            if(child->height != externalNodeHeight+1) {
                delete[] child->children;
            }
            else {
                delete[] child->values;
            }
            delete(child);
            return;
        }
        else {
            if(parent->height != 2) {
                if(child->height != externalNodeHeight+1) {
                    // Internal children with internal children
                    // Fuse right sibling into child
                    child->keys[child->nodeSize] = parent->keys[childNumber];

                    for(int i = 0; i < sibling->nodeSize; i++) {
                        child->keys[child->nodeSize+1+i] = sibling->keys[i];
                    }
                    for(int i = 0; i < sibling->nodeSize+1; i++) {
                        child->children[child->nodeSize+1+i] = sibling->children[i];
                    }
                    child->nodeSize = child->nodeSize+1+sibling->nodeSize;
                }
                else {
                    // Internal children with external children
                    // Fuse right sibling into child
                    child->keys[child->nodeSize] = parent->keys[childNumber];

                    for(int i = 0; i < sibling->nodeSize; i++) {
                        child->keys[child->nodeSize+1+i] = sibling->keys[i];
                    }
                    for(int i = 0; i < sibling->nodeSize+1; i++) {
                        child->values[child->nodeSize+1+i] = sibling->values[i];
                    }
                    child->nodeSize = child->nodeSize+1+sibling->nodeSize;
                }
            }
            else {
                // Children are leafs, and can not be external
                // Fuse right sibling into child
                for(int i = 0; i < sibling->nodeSize; i++) {
                    child->keys[child->nodeSize+i] = sibling->keys[i];
                    child->values[child->nodeSize+i] = sibling->values[i];
                }
                child->nodeSize = child->nodeSize+sibling->nodeSize;
            }

            // Update parent
            for(int i = childNumber; i < parent->nodeSize; i++) {
                parent->keys[i] = parent->keys[i+1];
            }
            for(int i = childNumber+1; i < parent->nodeSize+1; i++) {
                parent->children[+i] = parent->children[i+1];
            }
            (parent->nodeSize)--;

            // Effectively deleted right sibling
            delete[] sibling->keys;
            if(sibling->height != externalNodeHeight+1) {
                delete[] sibling->children;
            }
            else {
                delete[] sibling->values;
            }
            delete(sibling);
            return;
        }
        // Fused two internal children
        internalNodeCounter--;
        currentNumberOfNodes--;
    }
    else {
        // Case b, steal a key and a value from the sibling
        if(leftSibling) {
            // Steal rightmost value from left sibling
            if(parent->height != 2) {
                if(child->height != externalNodeHeight+1) {
                    // Internal nodes with internal children
                    // Make room for new value and key
                    for(int i = child->nodeSize; i > 0; i--) {
                        child->keys[i] = child->keys[i-1];
                    }
                    for(int i = child->nodeSize+1; i > 0; i--) {
                        child->children[i] = child->children[i-1];
                    }
                    child->keys[0] = parent->keys[childNumber-1];
                    child->children[0] = sibling->children[sibling->nodeSize];
                    (child->nodeSize)++;
                    (sibling->nodeSize)--;
                    parent->keys[childNumber-1] = sibling->keys[sibling->nodeSize];
                }
                else {
                    // Internal nodes with external children
                    // Make room for new value and key
                    for(int i = child->nodeSize; i > 0; i--) {
                        child->keys[i] = child->keys[i-1];
                    }
                    for(int i = child->nodeSize+1; i > 0; i--) {
                        child->values[i] = child->values[i-1];
                    }
                    child->keys[0] = parent->keys[childNumber-1];
                    child->values[0] = sibling->values[sibling->nodeSize];
                    (child->nodeSize)++;
                    (sibling->nodeSize)--;
                    parent->keys[childNumber-1] = sibling->keys[sibling->nodeSize];
                }
            }
            else {
                // Leafs, cant be external
                for(int i = child->nodeSize; i > 0; i--) {
                    child->keys[i] = child->keys[i-1];
                }
                for(int i = child->nodeSize+1; i > 0; i--) {
                    child->values[i] = child->values[i-1];
                }
                //cKeys[0] = keys[childNumber-1];
                child->keys[0] = sibling->keys[sibling->nodeSize-1];
                child->values[0] = sibling->values[sibling->nodeSize-1];
                (child->nodeSize)++;
                (sibling->nodeSize)--;
                parent->keys[childNumber-1] = sibling->keys[sibling->nodeSize-1];
            }
        }
        else {
            // Steal leftmost value from right sibling
            if(parent->height != 2) {
                if(child->height != externalNodeHeight+1) {
                    child->keys[child->nodeSize] = parent->keys[childNumber];
                    child->children[child->nodeSize+1] = sibling->children[0];
                    (child->nodeSize)++;
                    parent->keys[childNumber] = sibling->keys[0];
                    // Move siblings keys and values forward in arrays
                    for(int i = 0; i < sibling->nodeSize-1; i++) {
                        sibling->keys[i] = sibling->keys[i+1];
                    }
                    for(int i = 0; i < sibling->nodeSize; i++) {
                        sibling->children[i] = sibling->children[i+1];
                    }
                    (sibling->nodeSize)--;
                }
                else {
                    child->keys[child->nodeSize] = parent->keys[childNumber];
                    child->values[child->nodeSize+1] = sibling->values[0];
                    (child->nodeSize)++;
                    parent->keys[childNumber] = sibling->keys[0];
                    // Move siblings keys and values forward in arrays
                    for(int i = 0; i < sibling->nodeSize-1; i++) {
                        sibling->keys[i] = sibling->keys[i+1];
                    }
                    for(int i = 0; i < sibling->nodeSize; i++) {
                        sibling->values[i] = sibling->values[i+1];
                    }
                    (sibling->nodeSize)--;
                }

            }
            else {
                // Leafs
                //cKeys[*childSize] = keys[childNumber];
                child->keys[child->nodeSize] = sibling->keys[0];
                child->values[child->nodeSize] = sibling->values[0];
                (child->nodeSize)++;
                parent->keys[childNumber] = sibling->keys[0];
                // Move siblings keys and values forward in arrays
                for(int i = 0; i < sibling->nodeSize-1; i++) {
                    sibling->keys[i] = sibling->keys[i+1];
                    sibling->values[i] = sibling->values[i+1];
                }
                (sibling->nodeSize)--;
            }
        }
        return;
    }
}

// Internal node with external children
void BufferedBTree::fuseChildBorder(BufferedInternalNode *parent, int childNumber, int *childSize, int *cKeys,
                                    int *cValues) {

    /*
     * Two possible scenarios:
     * a) The child has a neighbouring sibling who also contains size-1 keys.
     * b) The child has a neighbouring sibling containing >= size keys.
     * We prefer case a over case b, since it allows for faster consecutive deletes.
     */

    /*cout << "Fuse child border " << parent->values[childNumber] << "\n";

    int siblingID;
    int siblingHeight, siblingSize;
    int* ptr_siblingHeight = &siblingHeight;
    int* ptr_siblingSize = &siblingSize;
    int* siblingKeys = new int[size*4-1];
    int* siblingValues = new int[size*4];

    // Check case and placement
    bool caseA = false;
    bool leftSibling = false;
    if(childNumber == 0) {
        // Leftmost child, check right sibling
        siblingID = parent->values[1];
        readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
        if(siblingSize == size-1) {
            caseA = true;
        }
    }
    else if (childNumber == (parent->nodeSize)) {
        // Rightmost child, check left sibling
        siblingID = parent->values[(parent->nodeSize)-1];
        readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
        if(siblingSize == size-1) {
            caseA = true;
        }
        leftSibling = true;
    }
    else {
        // Check both left and right sibling
        siblingID = parent->values[childNumber+1];
        readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
        if(siblingSize == size-1) {
            caseA = true;
        }
        else {
            siblingID = parent->values[childNumber-1];
            readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
            if(siblingSize == size-1) {
                caseA = true;
            }
            leftSibling = true;
        }
    }

    //cout << "CaseA = " << caseA << " leftSibling = " << leftSibling << " siblingSize = " << siblingSize << "\n";

    // Perform the necessary adjustments
    if(caseA) {
        // Case a, fuse child with its sibling
        // Fuse right child over into left child
        if(leftSibling) {
            if(parent->height != 2) {
                // Internal children
                // Fuse child into left sibling

                // We lack one key to separate the two sets of keys.
                // This key is the left siblings rightmost childs
                // rightmost key. This key can be found separating
                // the two children in the parent!

                siblingKeys[siblingSize] = parent->keys[childNumber-1];

                for(int i = 0; i < *childSize; i++) {
                    siblingKeys[siblingSize+1+i] = cKeys[i];
                }
                for(int i = 0; i < (*childSize)+1; i++) {
                    siblingValues[siblingSize+1+i] = cValues[i];
                }
                siblingSize = siblingSize+1+(*childSize);
            }
            else {
                // Children are leafs
                // Fuse child into left sibling
                for(int i = 0; i < *childSize; i++) {
                    siblingKeys[siblingSize+i] = cKeys[i];
                    siblingValues[siblingSize+i] = cValues[i];
                }
                siblingSize = siblingSize+(*childSize);
            }

            // Update parent
            for(int i = childNumber-1; i < parent->nodeSize; i++) {
                parent->keys[i] = parent->keys[i+1];
            }
            for(int i = childNumber; i < (parent->nodeSize); i++) {
                parent->values[i] = parent->values[i+1];
            }
            (parent->nodeSize)--;

            // "Return" left sibling
            *childSize = siblingSize;

            if(parent->height != 2) {
                for(int i = 0; i < siblingSize; i++) {
                    cKeys[i] = siblingKeys[i];
                }
                for(int i = 0; i < siblingSize+1; i++) {
                    cValues[i] = siblingValues[i];
                }
            }
            else {
                for(int i = 0; i < siblingSize; i++) {
                    cKeys[i] = siblingKeys[i];
                    cValues[i] = siblingValues[i];
                }
            }
            delete[] siblingKeys;
            delete[] siblingValues;
            return;
        }
        else {
            if(parent->height != 2) {
                // Internal children
                // Fuse right sibling into child
                cKeys[*childSize] = parent->keys[childNumber];

                for(int i = 0; i < siblingSize; i++) {
                    cKeys[(*childSize)+1+i] = siblingKeys[i];
                }
                for(int i = 0; i < siblingSize+1; i++) {
                    cValues[(*childSize)+1+i] = siblingValues[i];
                }
                (*childSize) = (*childSize)+1+siblingSize;
            }
            else {
                // Children are leafs
                // Fuse right sibling into child
                for(int i = 0; i < siblingSize; i++) {
                    cKeys[(*childSize)+i] = siblingKeys[i];
                    cValues[(*childSize)+i] = siblingValues[i];
                }
                (*childSize) = (*childSize)+siblingSize;
            }

            // Update parent
            for(int i = childNumber; i < parent->nodeSize; i++) {
                parent->keys[i] = parent->keys[i+1];
            }
            for(int i = childNumber+1; i < (parent->nodeSize)+1; i++) {
                parent->values[+i] = parent->values[i+1];
            }
            (parent->nodeSize)--;

            // Effectively deleted right sibling
            delete[] siblingKeys;
            delete[] siblingValues;
            return;
        }
        currentNumberOfNodes--;
    }
    else {
        // Case b, steal a key and a value from the sibling
        if(leftSibling) {
            // Steal rightmost value from left sibling
            if(parent->height != 2) {
                // Internal nodes
                // Make room for new value and key
                for(int i = *childSize; i > 0; i--) {
                    cKeys[i] = cKeys[i-1];
                }
                for(int i = (*childSize)+1; i > 0; i--) {
                    cValues[i] = cValues[i-1];
                }
                cKeys[0] = parent->keys[childNumber-1];
                cValues[0] = siblingValues[siblingSize];
                (*childSize)++;
                siblingSize--;
                parent->keys[childNumber-1] = siblingKeys[siblingSize];
            }
            else {
                // Leafs
                for(int i = *childSize; i > 0; i--) {
                    cKeys[i] = cKeys[i-1];
                }
                for(int i = (*childSize)+1; i > 0; i--) {
                    cValues[i] = cValues[i-1];
                }
                //cKeys[0] = keys[childNumber-1];
                cKeys[0] = siblingKeys[siblingSize-1];
                cValues[0] = siblingValues[siblingSize-1];
                (*childSize)++;
                siblingSize--;
                parent->keys[childNumber-1] = siblingKeys[siblingSize-1];
            }
        }
        else {
            // Steal leftmost value from right sibling
            if(parent->height != 2) {
                cKeys[*childSize] = parent->keys[childNumber];
                cValues[(*childSize)+1] = siblingValues[0];
                (*childSize)++;
                parent->keys[childNumber] = siblingKeys[0];
                // Move siblings keys and values forward in arrays
                for(int i = 0; i < siblingSize-1; i++) {
                    siblingKeys[i] = siblingKeys[i+1];
                }
                for(int i = 0; i < siblingSize; i++) {
                    siblingValues[i] = siblingValues[i+1];
                }
                siblingSize--;
            }
            else {
                //cKeys[*childSize] = keys[childNumber];
                cKeys[*childSize] = siblingKeys[0];
                cValues[*childSize] = siblingValues[0];
                (*childSize)++;
                parent->keys[childNumber] = siblingKeys->at(0);
                // Move siblings keys and values forward in arrays
                for(int i = 0; i < siblingSize-1; i++) {
                    siblingKeys[i] = siblingKeys[i+1];
                    siblingValues[i] = siblingValues[i+1];
                }
                siblingSize--;
            }
        }
        // Write out sibling
        writeNode(siblingID,siblingHeight,siblingSize,siblingKeys,siblingValues);
        return;
    }
}

/*
 * Fuses a child with an appropriate neighbour.
 * Will update parent as appropriate.
 * If child fused into other child, keys and values are replaced.
 */
/*void BufferedBTree::fuseChild(int height, int* nodeSize, int *keys, int *values, int childNumber, int *childSize,
                              int *cKeys, int *cValues) {

    /*
     * Two possible scenarios:
     * a) The child has a neighbouring sibling who also contains size-1 keys.
     * b) The child has a neighbouring sibling containing >= size keys.
     * We prefer case a over case b, since it allows for faster consecutive deletes.
     */

    /*int siblingID;
    int siblingHeight, siblingSize;
    int* ptr_siblingHeight = &siblingHeight;
    int* ptr_siblingSize = &siblingSize;
    int* siblingKeys = new int[size*4-1];
    int* siblingValues = new int[size*4];

    // Check case and placement
    bool caseA = false;
    bool leftSibling = false;
    if(childNumber == 0) {
        // Leftmost child, check right sibling
        siblingID = values[1];
        readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
        if(siblingSize == size-1) {
            caseA = true;
        }
    }
    else if (childNumber == (*nodeSize)) {
        // Rightmost child, check left sibling
        siblingID = values[(*nodeSize)-1];
        readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
        if(siblingSize == size-1) {
            caseA = true;
        }
        leftSibling = true;
    }
    else {
        // Check both left and right sibling
        siblingID = values[childNumber+1];
        readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
        if(siblingSize == size-1) {
            caseA = true;
        }
        else {
            siblingID = values[childNumber-1];
            readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
            if(siblingSize == size-1) {
                caseA = true;
            }
            leftSibling = true;
        }
    }

    //cout << "CaseA = " << caseA << " leftSibling = " << leftSibling << " siblingSize = " << siblingSize << "\n";

    // Perform the necessary adjustments
    if(caseA) {
        // Case a, fuse child with its sibling
        // Fuse right child over into left child
        if(leftSibling) {
            if(height != 2) {
                // Internal children
                // Fuse child into left sibling

                // We lack one key to separate the two sets of keys.
                // This key is the left siblings rightmost childs
                // rightmost key. This key can be found separating
                // the two children in the parent!

                siblingKeys[siblingSize] = keys[childNumber-1];

                for(int i = 0; i < *childSize; i++) {
                    siblingKeys[siblingSize+1+i] = cKeys[i];
                }
                for(int i = 0; i < (*childSize)+1; i++) {
                    siblingValues[siblingSize+1+i] = cValues[i];
                }
                siblingSize = siblingSize+1+(*childSize);
            }
            else {
                // Children are leafs
                // Fuse child into left sibling
                for(int i = 0; i < *childSize; i++) {
                    siblingKeys[siblingSize+i] = cKeys[i];
                    siblingValues[siblingSize+i] = cValues[i];
                }
                siblingSize = siblingSize+(*childSize);
            }

            // Update parent
            for(int i = childNumber-1; i < *nodeSize; i++) {
                keys[i] = keys[i+1];
            }
            for(int i = childNumber; i < (*nodeSize); i++) {
                values[i] = values[i+1];
            }
            (*nodeSize)--;

            /*cout << "===Printing out child\n";
            cout << height-1 << "\n";
            cout << *childSize << "\n";
            for(int j = 0; j < *childSize; j++) {
                cout << cKeys[j] << "\n";
            }
            for(int j = 0; j < *childSize; j++) {
                cout << cValues[j] << "\n";
            }
            cout << "===Printing out sibling\n";
            cout << height-1 << "\n";
            cout << siblingSize << "\n";
            for(int j = 0; j < siblingSize; j++) {
                cout << siblingKeys[j] << "\n";
            }
            for(int j = 0; j < siblingSize; j++) {
                cout << siblingValues[j] << "\n";
            }*/

            // Dont delete and reassign, copy over and delete instead
            //delete[] cKeys;
            //delete[] cValues;
            //cKeys = siblingKeys;
            //cValues = siblingValues;

            // "Return" left sibling
/*
            *childSize = siblingSize;

            if(height != 2) {
                for(int i = 0; i < siblingSize; i++) {
                    cKeys[i] = siblingKeys[i];
                }
                for(int i = 0; i < siblingSize+1; i++) {
                    cValues[i] = siblingValues[i];
                }
            }
            else {
                for(int i = 0; i < siblingSize; i++) {
                    cKeys[i] = siblingKeys[i];
                    cValues[i] = siblingValues[i];
                }
            }
            delete[] siblingKeys;
            delete[] siblingValues;
            return;
        }
        else {
            if(height != 2) {
                // Internal children
                // Fuse right sibling into child
                cKeys[*childSize] = keys[childNumber];

                for(int i = 0; i < siblingSize; i++) {
                    cKeys[(*childSize)+1+i] = siblingKeys[i];
                }
                for(int i = 0; i < siblingSize+1; i++) {
                    cValues[(*childSize)+1+i] = siblingValues[i];
                }
                (*childSize) = (*childSize)+1+siblingSize;
            }
            else {
                // Children are leafs
                // Fuse right sibling into child
                for(int i = 0; i < siblingSize; i++) {
                    cKeys[(*childSize)+i] = siblingKeys[i];
                    cValues[(*childSize)+i] = siblingValues[i];
                }
                (*childSize) = (*childSize)+siblingSize;
            }

            // Update parent
            for(int i = childNumber; i < *nodeSize; i++) {
                keys[i] = keys[i+1];
            }
            for(int i = childNumber+1; i < (*nodeSize)+1; i++) {
                values[+i] = values[i+1];
            }
            (*nodeSize)--;

            // Effectively deleted right sibling
            delete[] siblingKeys;
            delete[] siblingValues;
            return;
        }
        currentNumberOfNodes--;
    }
    else {
        // Case b, steal a key and a value from the sibling
        if(leftSibling) {
            // Steal rightmost value from left sibling
            if(height != 2) {
                // Internal nodes
                // Make room for new value and key
                for(int i = *childSize; i > 0; i--) {
                    cKeys[i] = cKeys[i-1];
                }
                for(int i = (*childSize)+1; i > 0; i--) {
                    cValues[i] = cValues[i-1];
                }
                cKeys[0] = keys[childNumber-1];
                cValues[0] = siblingValues[siblingSize];
                (*childSize)++;
                siblingSize--;
                keys[childNumber-1] = siblingKeys[siblingSize];
            }
            else {
                // Leafs
                for(int i = *childSize; i > 0; i--) {
                    cKeys[i] = cKeys[i-1];
                }
                for(int i = (*childSize)+1; i > 0; i--) {
                    cValues[i] = cValues[i-1];
                }
                //cKeys[0] = keys[childNumber-1];
                cKeys[0] = siblingKeys[siblingSize-1];
                cValues[0] = siblingValues[siblingSize-1];
                (*childSize)++;
                siblingSize--;
                keys[childNumber-1] = siblingKeys[siblingSize-1];
            }
        }
        else {
            // Steal leftmost value from right sibling
            if(height != 2) {
                cKeys[*childSize] = keys[childNumber];
                cValues[(*childSize)+1] = siblingValues[0];
                (*childSize)++;
                keys[childNumber] = siblingKeys[0];
                // Move siblings keys and values forward in arrays
                for(int i = 0; i < siblingSize-1; i++) {
                    siblingKeys[i] = siblingKeys[i+1];
                }
                for(int i = 0; i < siblingSize; i++) {
                    siblingValues[i] = siblingValues[i+1];
                }
                siblingSize--;
            }
            else {
                //cKeys[*childSize] = keys[childNumber];
                cKeys[*childSize] = siblingKeys[0];
                cValues[*childSize] = siblingValues[0];
                (*childSize)++;
                keys[childNumber] = siblingKeys[0];
                // Move siblings keys and values forward in arrays
                for(int i = 0; i < siblingSize-1; i++) {
                    siblingKeys[i] = siblingKeys[i+1];
                    siblingValues[i] = siblingValues[i+1];
                }
                siblingSize--;
            }
        }

        // Write out sibling
        writeNode(siblingID,siblingHeight,siblingSize,siblingKeys,siblingValues);
        return;
    }
}

/*
 * Writes out the node to file "B<id>".
 * Deletes key and value arrays.
 * Handles that leafs have size-1 values.
 */
/*void BufferedBTree::writeNode(int id, int height, int nodeSize, int *keys, int *values) {

    OutputStream* os = new BufferedOutputStream(B);
    string node = "B";
    node += to_string(id);
    os->create(node.c_str());
    os->write(&height);
    os->write(&nodeSize);
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

/*
 * Reads the node of id into the pointers height and size, as well as fills
 * out the arrays keys and values. Handles leafs (height == 1) appropriately.
 */
/*void BufferedBTree::readNode(int id, int* height, int* nodeSize, int *keys, int *values) {


    InputStream* is = new BufferedInputStream(B);
    string name = "B";
    name += to_string(id);
    is->open(name.c_str());
    *height = is->readNext();
    *nodeSize = is->readNext();

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

void BufferedBTree::printTree(BufferedInternalNode *node) {

    cout << "=== I " << node->id << "\n";
    cout << node->height << "\n";
    cout << node->nodeSize << "\n";
    cout << "---\n";
    for(int i = 0; i < node->nodeSize; i++) {
        cout << node->keys->at(i) << "\n";
    }
    if(node->height > 1 && node->height != externalNodeHeight+1) {
        cout << "---C\n";
        for(int i = 0; i < node->nodeSize+1; i++) {
            cout << node->children->at(i)->id << "\n";
        }
        for(int i = 0; i < node->nodeSize+1; i++) {
            printTree(node->children->at(i));
        }
    }
    else if( node->height == 1 && externalNodeHeight <= 0) {
        cout << "---V1\n";
        for(int i = 0; i < node->nodeSize; i++) {
            cout << node->values->at(i) << "\n";
        }
    }
    else {
        cout << "---V\n";
        for(int i = 0; i < node->nodeSize+1; i++) {
            cout << node->values->at(i) << "\n";
        }
        for(int i = 0; i < node->nodeSize+1; i++) {
            printExternal(node->values->at(i));
        }
    }

}

void BufferedBTree::printExternal(int node) {

    int height, nodeSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* keys = new int[size*4-1];
    int* values = new int[size*4];
    readNode(node,ptr_height,ptr_nodeSize,keys,values);

    cout << "=== E " << node << "\n";
    cout << height << "\n";
    cout << nodeSize << "\n";
    cout << "---\n";
    for(int i = 0; i < nodeSize; i++) {
        cout << keys[i] << "\n";
    }
    cout << "---\n";
    if(height == 1) {
        for(int i = 0; i < nodeSize; i++) {
            cout << values[i] << "\n";
        }
    }
    else {
        for(int i = 0; i < nodeSize+1; i++) {
            cout << values[i] << "\n";
        }
        for(int i = 0; i < nodeSize+1; i++) {
            printExternal(values[i]);
        }
    }
}

// Clean up external files
void BufferedBTree::cleanup() {
    for(int i = 1; i <= numberOfNodes; i++) {
        string name = "B";
        name += to_string(i);
        // Doesnt always work
        /*if(access( name.c_str(), F_OK ) != -1) {
            remove(name.c_str());
        }*/
        // This always works
        /*if(FILE *file = fopen(name.c_str(), "r")) {
            fclose(file);
            remove(name.c_str());
        }
    }
}

/*
 * Sort an array of KeyValueTime by key and time
 */
/*void BufferedBTree::sortInternal(std::vector<KeyValueTime>* buffer) {


    std::sort(buffer->begin(), buffer->end(),[](KeyValueTime a, KeyValueTime b) -> bool
    {
        if(a.key == b.key) {
            return a.time < b.time;
        }
        else {
            return a.key < b.key;
        }
    } );

    /*std::sort(buffer->begin(), buffer->end(),[](KeyValueTime a, KeyValueTime b) -> bool
    {
        if(a->key == b->key) {
            return a->time < b->time;
        }
        else {
            return a->key < b->key;
        }
    } );

    /*bool ValueCmp(KeyValueTime const & a, KeyValueTime const & b)
    {
        return a.key < b.key;
    }
    std::sort(buffer,buffer+bSize,ValueCmp);*/
/*}