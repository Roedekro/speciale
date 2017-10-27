//
// Created by martin on 10/11/17.
//

/*
 * Based on Cormen et. al. Introduction to Algorithms 3rd Ed.
 * Chapter 18 B-Trees.
 *
 * A node in a B-tree has the following layout:
 * It is a single file consisting of O(1) blocks of data.
 * Each block of data consists of integers.
 * The first two integers of the node are reserved for the following data:
 * 1) Height in the tree (1 being a leaf)
 * 2) Number of keys in the node
 *
 * This is followed by a list of keys of size k,
 * which in turn is followed by a list of
 * values/pointers of size k+1. Leafs have k values.
 */

#include "Btree.h"
#include "../Streams/BufferedOutputStream.h"
#include "../Streams/BufferedInputStream.h"
#include <sstream>
#include <iostream>

using namespace std;

/*
 * Create a B-tree by creating an empty root.
 */
Btree::Btree(int B) {
    // Create root
    this->B = B;
    size = ((B/sizeof(int))-2)/2; // #values. Max #keys will be 2*size-1. Min = size.
                                // Result is that one node can fill out 2*B.
    numberOfNodes = 1;
    root = 1;
    // Exception for use of writeNode, since this node is empty.
    OutputStream* os = new BufferedOutputStream(B);
    string name = "B";
    name += to_string(1);
    os->create(name.c_str());
    int height = 1;
    os->write(&height);
    int zero = 0;
    os->write(&zero);
    os->close();
    iocounter = os->iocounter;
    delete(os);
    //cout << "Created root " << root << " " << name << "\n";
    //cout << "Tree will have size = " << size << " resulting in #keys = " << 2*size-1 << " with int size = " << sizeof(int) << "\n";
}

Btree::~Btree() {}

/*
 * Insert a (key,value) pair.
 * If key is already present update value.
*/

void Btree::insert(KeyValue* element) {

    // Read in root
    int height, nodeSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* keys = new int[size*2-1];
    int* values = new int[size*2];
    readNode(root,ptr_height,ptr_nodeSize,keys,values);

    // Check if we need to split the root
    if(nodeSize == 2*size-1) {

        numberOfNodes++;
        int oldRoot = root;
        root = numberOfNodes;

        // New root
        int newRootHeight = height+1;
        int* newRootKeys = new int[size*2-1];
        int* newRootValues = new int[size*2];
        newRootValues[0] = oldRoot;

        // New Child
        int newChildHeight = height;
        int newChildSize = size-1;
        int* newChildKeys = new int[size*2-1];
        int* newChildValues = new int[size*2];

        splitChild(newRootHeight,0,newRootKeys,newRootValues,0,ptr_nodeSize,keys,values,newChildKeys,newChildValues);

        // Save both children
        writeNode(oldRoot,height,nodeSize,keys,values);
        writeNode(numberOfNodes,newChildHeight,newChildSize,newChildKeys,newChildValues);

        // Insert into new root
        insertIntoNonFull(element,root,newRootHeight,1,newRootKeys,newRootValues,true);
    }
    else {
        insertIntoNonFull(element,root,height,nodeSize,keys,values,false);
    }

}

/*
 * Inserts into an internal node or a leaf.
 * Write denotes whether a node was changed, ans thus needs to be written to disk.
 */

void Btree::insertIntoNonFull(KeyValue* element, int id, int height, int nodeSize, int *keys, int *values, bool write) {

    int i = nodeSize;
    if(height == 1) {
        // Insert into this leaf

        // Check if key is present
        int j = 0;
        while(j < nodeSize && keys[j] != element->key) {
            j++;
        }
        if(keys[j] == element->key) {
            // Special case, just update value
            values[j] = element->value;
            writeNode(id,height,nodeSize,keys,values);
            //cout << "Node = " <<  id << " Updated key = " << element->key << " to value = " << element->value << "\n";
            return;
        }
        else {
            // Move keys and values back, insert new pair
            while(i > 0 && element->key < keys[i-1]) {
                keys[i] = keys[i-1];
                values[i] = values[i-1];
                i--;
            }
            keys[i] = element->key;
            values[i] = element->value;
            nodeSize++;
            writeNode(id,height,nodeSize,keys,values);
            //cout << "Node = " <<  id << " Inserted key = " << element->key << " with value = " << element->value << "\n";
            return;
        }
    }
    else {
        // Internal node

        // Find child
        while(i > 0 && element->key <= keys[i-1]) {
            i--;
        }
        int child = values[i];

        // Load in child
        int cHeight, cSize;
        int* ptr_cHeight = &cHeight;
        int* ptr_cSize = &cSize;
        int* cKeys = new int[size*2-1];
        int* cValues = new int[size*2];
        readNode(child,ptr_cHeight,ptr_cSize,cKeys,cValues);
        bool cWrite = false;

        // Check if we need to split the child
        if(cSize == 2*size-1) {

            write = true;
            cWrite = true;

            int newSize = size-1;
            int* newKeys = new int[size*2-1];
            int* newValues = new int[size*2];

            splitChild(height, nodeSize, keys, values, i, ptr_cSize, cKeys, cValues,
            newKeys, newValues);
            nodeSize++;

            //cout << "Split children new sizes " << cSize << " " << newSize << "\n";

            // Check which of the two children we need to recurse upon.
            if(element->key > keys[i]) {

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
 * Splits a child of a node into a new child.
 * Does not clean up parent, existing child or new child, responsibility is left to calling function.
 * You must increase parents size by one, and set the old child and new childs size to size-1;
 * Will update nodeSize of original child correctly, depending on if its a leaf or not.
 * New child will have nodeSize = size-1;
 */
void Btree::splitChild(int height, int nodeSize, int *keys, int *values, int childNumber, int* childSize,
                       int *cKeys, int *cValues, int *newKeys, int *newValues) {

    numberOfNodes++;
    int newChild = numberOfNodes;

    // Fill out new childs keys
    for(int j = 0; j < size-1; j++) {
        newKeys[j] = cKeys[size+j];
    }

    // Copy values
    if(height == 2) {
        // Children are leafs
        for(int j = 0; j < size-1; j++) {
            newValues[j] = cValues[size+j]; // <---?
        }
    }
    else {
        // Children are internal nodes
        for(int j = 0; j < size; j++) {
            newValues[j] = cValues[size+j];
        }
    }

    // Move parents keys by one
    for(int j = nodeSize; j > childNumber; j--) {
        keys[j] = keys[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    keys[childNumber] = cKeys[size-1];

    // Also update size of old child.
    if(height == 2) {
        *childSize = size;
    }
    else {
        *childSize = size-1;
    }

    // Move parents values back by one
    for(int j = nodeSize+1; j > childNumber; j--) {
        values[j] = values[j-1];
    }
    // Insert pointer to new child
    values[childNumber+1] = newChild;

}

int Btree::query(int element) {

    int height, nodeSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* keys = new int[size*2-1];
    int* values = new int[size*2];

    // Load in root
    int currentNode = root;
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

/*
 * Deletes the key and value denoted by element.
 * If no key matching element is present it does nothing.
 */
void Btree::deleteElement(int element) {

    //cout << "DeleteElement " << element << "\n";

    // Read in root
    int height, nodeSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* keys = new int[size*2-1];
    int* values = new int[size*2];
    readNode(root,ptr_height,ptr_nodeSize,keys,values);

    // Perform check to see if we need to shrink the tree.
    if(height != 1 && nodeSize == 0) {
        root = values[0];
        delete[] keys;
        delete[] values;
        deleteElement(element);
    }
    else {
        deleteNonSparse(element,root,height,nodeSize,keys,values,false);
    }
}

/*
 * Responsible for cleaning up arguments!
 * Write indicates whether a node was changed, and thus needs to be written to disk.
 */
void Btree::deleteNonSparse(int element, int id, int height, int nodeSize, int *keys, int *values, bool write) {

    //cout << "Delete on node " << id << " height " << height << " of size " << nodeSize << "\n";

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
        int* cKeys = new int[size*2-1];
        int* cValues = new int[size*2];
        readNode(child,ptr_cHeight,ptr_cSize,cKeys,cValues);
        bool cWrite = false;

        if(cSize == size-1) {

            write = true;
            cWrite = true;

            // Fuse child to ensure place for deletion
            int* ptr_nodeSize = &nodeSize;
            //cout << "Fusing " << child << "\n";
            fuseChild(height,ptr_nodeSize,keys,values,i,ptr_cSize,cKeys,cValues);
            //cout << "Fusing completed\n";

            // Recheck what child to recurse upon
            // Fusechild will update cKeys and cValues
            i = nodeSize;
            while(i > 0 && element <= keys[i-1]) {
                i--;
            }
            child = values[i];

            /*cout << "===Printing out parent\n";
            cout << height << "\n";
            cout << nodeSize << "\n";
            for(int j = 0; j < nodeSize; j++) {
                cout << keys[j] << "\n";
            }
            for(int j = 0; j < nodeSize+1; j++) {
                cout << values[j] << "\n";
            }
            cout << "===Printing out child\n";
            cout << cHeight << "\n";
            cout << cSize << "\n";
            for(int j = 0; j < cSize; j++) {
                cout << cKeys[j] << "\n";
            }
            for(int j = 0; j < cSize+1; j++) {
                cout << cValues[j] << "\n";
            }*/
        }

        if(write) {
            writeNode(id,height,nodeSize,keys,values);
        }
        deleteNonSparse(element,child,cHeight,cSize,cKeys,cValues,cWrite);
    }
}

/*
 * Fuses a child with an appropriate neighbour.
 * Will update parent as appropriate.
 * If child fused into other child, keys and values are replaced.
 */
void Btree::fuseChild(int height, int* nodeSize, int *keys, int *values, int childNumber, int *childSize,
                      int *cKeys, int *cValues) {

    /*
     * Two possible scenarios:
     * a) The child has a neighbouring sibling who also contains size-1 keys.
     * b) The child has a neighbouring sibling containing >= size keys.
     * We prefer case a over case b, since it allows for faster consecutive deletes.
     */

    int siblingID;
    int siblingHeight, siblingSize;
    int* ptr_siblingHeight = &siblingHeight;
    int* ptr_siblingSize = &siblingSize;
    int* siblingKeys = new int[size*2-1];
    int* siblingValues = new int[size*2];

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
void Btree::writeNode(int id, int height, int nodeSize, int *keys, int *values) {

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
void Btree::readNode(int id, int* height, int* nodeSize, int *keys, int *values) {


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

/*
 * Removes all node files used.
 */
void Btree::cleanup() {

    for(int i = 1; i <= numberOfNodes; i++) {
        string name = "B";
        name += to_string(i);
        remove(name.c_str());
    }
}