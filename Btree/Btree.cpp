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
 * values/pointers of size k+1
 */

#include "Btree.h"
#include "../Streams/BufferedOutputStream.h"
#include "../Streams/BufferedInputStream.h"
#include <sstream>
#include <vector>
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
    delete(os);
    cout << "Created root " << root << " " << name << "\n";
    cout << "Tree will have size = " << size << " resulting in #keys = " << 2*size-1 << " with int size = " << sizeof(int) << "\n";
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
        insertIntoNonFull(element,root,newRootHeight,1,newRootKeys,newRootValues);
    }
    else {
        insertIntoNonFull(element,root,height,nodeSize,keys,values);
    }

}

/*
 * Inserts into an internal node or a leaf.
 */

void Btree::insertIntoNonFull(KeyValue* element, int id, int height, int nodeSize, int *keys, int *values) {

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
            cout << "Node = " <<  id << " Updated key = " << element->key << " to value = " << element->value << "\n";
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
            cout << "Node = " <<  id << " Inserted key = " << element->key << " with value = " << element->value << "\n";
            return;
        }
    }
    else {
        // Internal node

        // Find child
        while(i > 0 && element->key < keys[i-1]) {
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

        // Check if we need to split the child
        if(cSize == 2*size-1) {

            int newSize = size-1;
            int* newKeys = new int[size*2-1];
            int* newValues = new int[size*2];

            splitChild(height, nodeSize, keys, values, i, ptr_cSize, cKeys, cValues,
            newKeys, newValues);
            nodeSize++;

            cout << "Split children new sizes " << cSize << " " << newSize << "\n";

            // Check which of the two children we need to recurse upon.
            if(element->key > keys[i]) {

                // Write out old child
                cout << "Writing out old child " << child << " with size " << cSize << "\n";
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

        // Write node to disk - Possible optimization, find out if write is necessary for this node.
        writeNode(id,height,nodeSize,keys,values);

        // Insert into child
        insertIntoNonFull(element,child,cHeight,cSize,cKeys,cValues);
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

    // Move parents keys and values back by one
    for(int j = nodeSize; j > childNumber+1; j--) {
        keys[j] = keys[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    // Also update size of old child.
    if(height == 2) {
        // Leaf, old child got extra key
        keys[childNumber] = cKeys[size-1];
        *childSize = size;
    }
    else {
        keys[childNumber] = cKeys[size-2];
        *childSize = size-1;
    }

    for(int j = nodeSize+1; j > childNumber+1; j--) {
        values[j] = values[j-1];
    }
    // Insert pointer to new child
    values[childNumber+1] = newChild;

}

int Btree::query(int element) {

    return 0;
}

void Btree::deleteElement(int element) {

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
    delete(is);
}