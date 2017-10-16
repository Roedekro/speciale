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
    size = ((B/sizeof(int))-3)/2; // #keys. Max #keys will be 2*size-1. Min = size.
    numberOfNodes = 1;
    root = 1;
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
    cout << "Tree will have #keys = " << size << "\n";
}

Btree::~Btree() {}

/*
 * Insert a (key,value) pair.
 * If key is already present update value.
*/

void Btree::insert(keyValue element) {

    // Read in root
    InputStream* is = new BufferedInputStream(B);
    string name = "B";
    name += to_string(root);
    is->open(name.c_str());

    // Read in root data
    int height = is->readNext();
    int nodeSize = is->readNext();

    // Make extra room to insert, just in case.
    int* keys = new int[nodeSize+1];
    int* values = new int[nodeSize+2];

   // Read in keys
    int i = 0;
    while(i < nodeSize) {
        keys[i] = is->readNext();
        i++;
    }

    // Read in values. If internal +1;
    int to = nodeSize;
    if(height != 1) {
        nodeSize++;
    }
    while(i < nodeSize) {
        values[i] = is->readNext();
        i++;
    }

    is->close();
    delete(is);

    if(nodeSize == 2*size-1) {

        // Create new root
        int rootHeight = height+1;
        int rootSize = 0;
        int* rootKeys = new int[1];
        int* rootValues = new int[2];
        rootValues[0] = root;
        int oldRoot = root;
        root = numberOfNodes++;

        //// <---------------------------------------------------------------------------------------------------------!!!!!!!!!!!!!!!!!!!!!!!
        //splitChild(root,rootHeight,rootSize,rootKeys,rootValues,oldRoot,height,&nodeSize,keys,values);
        rootSize++;

        // Write old root to disk
        OutputStream* os = new BufferedOutputStream(B);
        os->create(name.c_str());
        os->write(&height);
        os->write(&nodeSize);
        for(int j = 0; j < nodeSize; j++) {
            os->write(&keys[j]);
        }
        for(int j = 0; j < nodeSize+1; j++) {
            os->write(&values[j]);
        }
        os->close();
        delete(os);

        // Clean up
        delete[] keys;
        delete[] values;

        // Recurse
        insertIntoNonFull(element,root,rootHeight,rootSize,rootKeys,rootValues);

    }
    else {
        insertIntoNonFull(element,root,height,nodeSize,keys,values);
    }


}

/*
 * Inserts into an internal node or a leaf.
 * Will clean up after itself.
 */

void Btree::insertIntoNonFull(keyValue element, int id, int height, int nodeSize, int *keys, int *values) {

    int i = nodeSize;
    if(height == 1) {
        // Insert into this leaf

        // Check if key is present
        int j = 0;
        while(j < nodeSize && keys[j] != element.key) {
            j++;
        }
        if(keys[j] == element.key) {
            // Special case, just update value
            values[j] = element.value;

            // Write node to disk
            OutputStream* os = new BufferedOutputStream(B);
            string name = "B";
            name += to_string(id);
            os->create(name.c_str());
            os->write(&height);
            os->write(&nodeSize);
            for(int k = 0; k < nodeSize; k++) {
                os->write(&keys[k]);
            }
            for(int k = 0; k < nodeSize; k++) {
                os->write(&values[k]);
            }
            os->close();
            delete(os);
            delete[] keys;
            delete[] values;
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

            // Write node to disk
            OutputStream* os = new BufferedOutputStream(B);
            string name = "B";
            name += to_string(id);
            os->create(name.c_str());
            os->write(&height);
            os->write(&nodeSize);
            for(int k = 0; k < nodeSize; k++) {
                os->write(&keys[k]);
            }
            for(int k = 0; k < nodeSize; k++) {
                os->write(&values[k]);
            }
            os->close();
            delete(os);
            delete[] keys;
            delete[] values;
            return;
        }
    }
    else {
        // Internal node

        // Find child
        while(i > 0 && element.key < keys[i-1]) {
            i--;
        }
        int child = values[i];

        // Load in child
        InputStream* is = new BufferedInputStream(B);
        string name = "B";
        name += to_string(child);
        is->open(name.c_str());
        int cHeight = is->readNext();
        int cSize = is->readNext();
        // Make room to potentially insert
        int* cKeys = new int[cSize+1];
        int* cValues = new int[cSize+2];
        int j = 0;
        while(j < cSize) {
            cKeys[j] = is->readNext();
            j++;
        }
        j = 0;
        while(j < cSize+1) {
            cValues[j] = is->readNext();
        }
        is->close();
        delete(is);

        // Check if we need to split the child
        if(cSize == 2*size-1) {

            int newSize = size-1;
            int* newKeys = new int[newSize];
            int* newValues = new int[newSize+1];

            splitChild(height, nodeSize, keys, values, i, cKeys, cValues,
            newKeys, newValues);
            nodeSize++;
            cSize = size-1;

            // Check which of the two children we need to recurse upon.
            if(element.key > keys[i]) {
                i++;
                child = values[i];

                // Write out old child
                OutputStream* os = new BufferedOutputStream(B);
                string node = "B";
                node += to_string(child);
                os->create(node.c_str());
                os->write(&cHeight);
                os->write(&cSize);
                for(int k = 0; k < cSize; k++) {
                    os->write(&keys[k]);
                }
                for(int k = 0; k < cSize+1; k++) {
                    os->write(&values[k]);
                }
                os->close();
                delete(os);
                delete[] cKeys;
                delete[] cValues;

                // Switch child to new child.
                cKeys = newKeys;
                cValues = newValues;
                cSize = newSize;

            }
            else {
                // Write out new child
                OutputStream* os = new BufferedOutputStream(B);
                string node = "B";
                node += to_string(numberOfNodes);
                os->create(node.c_str());
                os->write(&cHeight);
                os->write(&newSize);
                for(int k = 0; k < newSize; k++) {
                    os->write(&keys[k]);
                }
                for(int k = 0; k < newSize+1; k++) {
                    os->write(&values[k]);
                }
                os->close();
                delete(os);
                delete[] newKeys;
                delete[] newValues;
            }
        }

        // Write node to disk
        OutputStream* os = new BufferedOutputStream(B);
        string node = "B";
        node += to_string(id);
        os->create(node.c_str());
        os->write(&height);
        os->write(&nodeSize);
        for(int k = 0; k < nodeSize; k++) {
            os->write(&keys[k]);
        }
        for(int k = 0; k < nodeSize+1; k++) {
            os->write(&values[k]);
        }
        os->close();
        delete(os);
        delete[] keys;
        delete[] values;

        // Insert into child
        insertIntoNonFull(element,child,cHeight,cSize,cKeys,cValues);
    }
}

/*
 * Splits a child of a node into a new child.
 * Assumes values is an even number (if B is even, this is true).
 * Does not clean up parent, existing child or new child, responsibility is left to calling function.
 * You must increase parents size by one, and set the old child and new childs size to size;
 */
void Btree::splitChild(int height, int nodeSize, int *keys, int *values, int childNumber,
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
    keys[childNumber+1] = newValues[size-1];

    for(int j = nodeSize+1; j > childNumber+1; j--) {
        values[j] = values[j-1];
    }
    values[childNumber+1] = newChild;

}

int Btree::query(int element) {

    return 0;
}

keyValue Btree::deleteElement(int element) {

}