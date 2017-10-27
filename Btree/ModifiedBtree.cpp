//
// Created by martin on 10/26/17.
// Modification of the normal B-tree where we spend O(M)
// internal memory on storing the top of the tree.
// This gives faster insertion and query times.
// Root must be internal, but the rest of the nodes depends
// on the size of memory allocated to internal nodes.
//
// Some comments copied over, internal has double meaning.
// Either internal memory, or a non-leaf node (potentially external).
//

#include "ModifiedBtree.h"
#include "../Streams/OutputStream.h"
#include "../Streams/BufferedOutputStream.h"
#include "../Streams/InputStream.h"
#include "../Streams/BufferedInputStream.h"
#include <sstream>
#include <iostream>
#include <unistd.h>

using namespace std;

/*
 * Create a B-tree by creating an empty root.
 */
ModifiedBtree::ModifiedBtree(int B, int M) {
    // Create root
    this->B = B;
    this->M = M;
    size = ((B/sizeof(int))-2)/2; // #values. Max #keys will be 2*size-1. Min = size.
    // Result is that one node can fill out 2*B.
    numberOfNodes = 1;
    root = new ModifiedInternalNode(numberOfNodes,1,size,true);
    internalNodeCounter=1;
    externalNodeHeight = -1; // Height the external nodes begin <--------------------------------------------------------------------------!!!!!!!!!!!!!!
    // How many nodes can we store in internal Memory?
    // Three ints, an array of keys, and an array of pointers, pr. Node.
    int internalNodeSize = ((3*sizeof(int)) + ((size*2-1)*sizeof(int)) + (size*2*sizeof(ModifiedInternalNode*)));
    maxInternalNodes = M/internalNodeSize;
    iocounter = 0;
    cout << "Created root " << root->id << "\n";
    cout << "Tree will have size = " << size << " resulting in #keys = " << 2*size-1 << " with int size = " << sizeof(int) << "\n";
    cout << "Max internal nodes = " << maxInternalNodes << " with internal node size = " << internalNodeSize << "\n";
}

ModifiedBtree::~ModifiedBtree() {

}

/*
 * Responsible for externalizing the lowest level of the internal nodes.
 * Should be called when internalNodeCount > maxInternalNodes.
 * The extra nodes will never be greater than O(log(M/B)), and
 * will in practice be much smaller.
 */
void ModifiedBtree::externalize() {

    cout << "Externalizing #internalNodes " << internalNodeCounter << " maxInternalNodes " << maxInternalNodes << "\n";

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
}

void ModifiedBtree::recursiveExternalize(ModifiedInternalNode *node) {

    if(node->height != externalNodeHeight+1) {
        // Recurse on children
        for(int i = 0; i < node->nodeSize+1; i++) {
            recursiveExternalize(node->children[i]);
        }
    }
    else {
        // Externalize children
        node->values = new int[size*2];
        int cHeight, cSize;
        int* cKeys = new int[size*2-1];
        int* cValues = new int[size*2];
        ModifiedInternalNode* child;
        for(int i = 0; i < node->nodeSize+1; i++) {
            child = node->children[i];
            writeNode(child->id,child->height,child->nodeSize,child->keys,child->values);
            cout << "Externalized node " << child->id << " from parent " << node->id << "\n";
            node->values[i] = child->id;
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
void ModifiedBtree::insert(KeyValue *element) {

    // Check if we need to split the root
    if(root->nodeSize == 2*size-1) {
        numberOfNodes++;
        internalNodeCounter++;
        ModifiedInternalNode* newRoot = new ModifiedInternalNode(numberOfNodes,root->height+1,size,false);
        newRoot->children[0] = root;
        // Note that children of the new root will always be internal
        // Even if they are supposed to be external we dont externalize them
        // until after the insertion
        splitChildInternal(newRoot,root,0);
        root = newRoot;
        insertIntoNonFullInternal(element,root);
    }
    else {
     insertIntoNonFullInternal(element,root);
    }

    // Check if we need to externalize
    if(internalNodeCounter > maxInternalNodes) {
        externalize();
    }
}

void ModifiedBtree::insertIntoNonFullInternal(KeyValue *element, ModifiedInternalNode *node) {

    cout << "Insert Non Full Internal " << element->key << " Node ID = " << node->id << "\n";

    int i = node->nodeSize;
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
            cout << "Node = " <<  node->id << " Updated key = " << element->key << " to value = " << element->value << "\n";
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
            cout << "Node = " <<  node->id << " Inserted key = " << element->key << " with value = " << element->value << "\n";
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

            ModifiedInternalNode* child = node->children[i];
            if(child->nodeSize == size*2-1) {
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
            int* cKeys = new int[size*2-1];
            int* cValues = new int[size*2];
            readNode(child,ptr_cHeight,ptr_cSize,cKeys,cValues);
            bool cWrite = false;

            // Check if we need to split the child
            if(cSize == 2*size-1) {

                cWrite = true;
                int newSize = size-1;
                int* newKeys = new int[size*2-1];
                int* newValues = new int[size*2];

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
}

/*
 * Inserts into an internal node or a leaf.
 * Write denotes whether a node was changed, ans thus needs to be written to disk.
 */

void ModifiedBtree::insertIntoNonFull(KeyValue* element, int id, int height, int nodeSize, int *keys, int *values, bool write) {

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
 * Split internal children.
 * Will handle if childrens children are external.
 */
void ModifiedBtree::splitChildInternal(ModifiedInternalNode* parent, ModifiedInternalNode *child, int childNumber) {

    cout << "Split Internal Child on parent " << parent->id << " child " << child->id << " childnumber " << childNumber <<"\n";
    cout << parent->nodeSize << " " << externalNodeHeight << " " << child->height << "\n";

    numberOfNodes++;
    bool externalChildren = (child->height == externalNodeHeight+1);
    ModifiedInternalNode* newChild;
    // Special case if this is a leaf
    if(child->height == 1) {
        newChild = new ModifiedInternalNode(numberOfNodes,child->height,size,true);
    }
    else {
        newChild = new ModifiedInternalNode(numberOfNodes,child->height,size,externalChildren);
    }

    // Fill out new childs keys
    for(int j = 0; j < size-1; j++) {
        newChild->keys[j] = child->keys[size+j];
    }

    // Fill out values
    if(!externalChildren) {
        if(child->height == 1) {
            // Children are internal leafs
            for(int j = 0; j < size-1; j++) {
                newChild->values[j] = child->values[size+j];
            }
        }
        else {
            // Children are internal nodes
            for(int j = 0; j < size; j++) {
                newChild->children[j] = child->children[size+j];
            }
        }
    }
    else {
        // Childrens children are external nodes
        for(int j = 0; j < size; j++) {
            newChild->values[j] = child->values[size+j];
        }

    }

    // Move parents keys by one
    for(int j = parent->nodeSize; j > childNumber; j--) {
        parent->keys[j] = parent->keys[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    parent->keys[childNumber] = child->keys[size-1];

    // Also update size of children.
    if(child->height == 1) {
        child->nodeSize = size;
    }
    else {
        child->nodeSize = size-1;
    }
    newChild->nodeSize = size-1;

    // Move parents values back by one
    for(int j = parent->nodeSize+1; j > childNumber; j--) {
        parent->children[j] = parent->children[j-1];
    }
    // Insert pointer to new child
    parent->children[childNumber+1] = newChild;

    // Increase parent size
    (parent->nodeSize)++;

    internalNodeCounter++;

}

/*
 * Special case where the parent is a node in internal memory and the children are external nodes
 */
void ModifiedBtree::splitChildBorder(ModifiedInternalNode *parent, int childNumber, int *childSize, int *cKeys,
                                     int *cValues, int *newKeys, int *newValues) {

    numberOfNodes++;
    int newChild = numberOfNodes;

    // Fill out new childs keys
    for(int j = 0; j < size-1; j++) {
        newKeys[j] = cKeys[size+j];
    }

    // Copy values
    if(parent->height == 2) {
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
    for(int j = parent->nodeSize; j > childNumber; j--) {
        parent->keys[j] = parent->keys[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    parent->keys[childNumber] = cKeys[size-1];

    // Also update size of old child.
    if(parent->height == 2) {
        *childSize = size;
    }
    else {
        *childSize = size-1;
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
void ModifiedBtree::splitChild(int height, int nodeSize, int *keys, int *values, int childNumber, int* childSize,
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

int ModifiedBtree::query(int element) {

    // Root is in internal memory
    ModifiedInternalNode* node = root;
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
        int* keys = new int[size*2-1];
        int* values = new int[size*2];
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


/*
 * Writes out the node to file "B<id>".
 * Deletes key and value arrays.
 * Handles that leafs have size-1 values.
 */
void ModifiedBtree::writeNode(int id, int height, int nodeSize, int *keys, int *values) {

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
void ModifiedBtree::readNode(int id, int* height, int* nodeSize, int *keys, int *values) {


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

void ModifiedBtree::printTree(ModifiedInternalNode *node) {

    cout << "=== I " << node->id << "\n";
    cout << node->height << "\n";
    cout << node->nodeSize << "\n";
    cout << "---\n";
    for(int i = 0; i < node->nodeSize; i++) {
        cout << node->keys[i] << "\n";
    }
    cout << "---\n";
    if(node->height > 1 && node->height != externalNodeHeight+1) {
        for(int i = 0; i < node->nodeSize+1; i++) {
            cout << node->children[i]->id << "\n";
        }
        for(int i = 0; i < node->nodeSize+1; i++) {
            printTree(node->children[i]);
        }
    }
    else if( node->height == 1 && externalNodeHeight == -1) {
        for(int i = 0; i < node->nodeSize; i++) {
            cout << node->values[i] << "\n";
        }
    }
    else {
        for(int i = 0; i < node->nodeSize+1; i++) {
            cout << node->values[i] << "\n";
        }
        for(int i = 0; i < node->nodeSize+1; i++) {
            printExternal(node->values[i]);
        }
    }

}

void ModifiedBtree::printExternal(int node) {

    int height, nodeSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* keys = new int[size*2-1];
    int* values = new int[size*2];
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
void ModifiedBtree::cleanup() {
    for(int i = 1; i <= numberOfNodes; i++) {
        string name = "B";
        name += to_string(i);
        // Doesnt always work
        /*if(access( name.c_str(), F_OK ) != -1) {
            remove(name.c_str());
        }*/
        // This always works
        if(FILE *file = fopen(name.c_str(), "r")) {
            fclose(file);
            remove(name.c_str());
        }
    }
}