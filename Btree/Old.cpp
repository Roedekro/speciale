//
// Created by martin on 10/16/17.
//


/*
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

/*
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
 * Does not clean up parent, existing child or new child, responsibility is left to calling function.
 * You must increase parents size by one, and set the old child and new childs size to size-1;
 * Special case for leafs, where number of values becomes size-1 instead of size, but
 * there is space left to write out a dummmy value, so just treat it as if it had #size values.
 */

/*
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
    // New child inherits key from old child. Assign new key to old child.
    if(height == 2) {
        // Leaf, old child got extra key
        keys[childNumber] = cKeys[size-1];
    }
    else {
        keys[childNumber] = cKeys[size-2];
    }

    for(int j = nodeSize+1; j > childNumber+1; j--) {
        values[j] = values[j-1];
    }
    // Insert pointer to new child
    values[childNumber+1] = newChild;

}


























#include "Old.h"




/*
//
// Created by martin on 10/11/17.
//

/*
 * A node in a B-tree has the following layout:
 * It is a single file consisting of O(1) blocks of data.
 * Each block of data consists of integers.
 * The first four integers of the node are reserved for the following data:
 * 1) ID of the node.
 * 2) ID of the nodes parent.
 * 3) Leaf 0/1, where 1 indicate the node is a leaf.
 * 4) Number of elements in the node.
 *
 * Depending on if the node is internal or is a leaf
 * it either stores an element followed by an integer,
 * denoting the name of the child represented by the element,
 * or if a leaf it will only store elements.
 *
 * Elements denoting the maximum value of a child will channel
 * queries of <= element to that child, as opposed to < element,
 * which would send equal elements into different children.
 * However elements are assumed to be unique.
 *
 * The rightmost element may not be the largest element from that
 * subtree/child, but will be updated correctly upon split/fuse.
 * Ex. largest element is 4, insert 5, which is not <= any element,
 * so it goes into the child denoted by 4. Updating 4 to 5  in the parent
 * would cost an unnecesary I/O, since elements > 4 will go to that child
 * regardless until we split/fuse, whereupon we update the element.
 *
 * The tree only support unique keys.
 */

/*

#include "Btree.h"
#include "../Streams/BufferedOutputStream.h"
#include "../Streams/BufferedInputStream.h"
#include <sstream>
#include <vector>
#include <iostream>


using namespace std;

Btree::Btree(int B) {
    // Create root
    this->B = B;
    size = B/sizeof(int);
    numberOfNodes = 1;
    root = 1;
    OutputStream* os = new BufferedOutputStream(B);
    ostringstream oss;
    oss << root;
    string s = "B"+oss.str();
    const char* name = s.c_str();
    os->create(name);
    os->write(&root);
    int parent = 0;
    os->write(&parent);
    int leaf = 1;
    os->write(&leaf);
    // Write 0 to indicate 0 elements
    os->write(&parent);
    os->close();
    delete(os);
    cout << "Created root " << root << " " << name << "\n";
    cout << "Tree will have size " << size << "\n";
}

Btree::~Btree() {}

void Btree::insert(int element) {
    cout << "Inserting element " << element << "\n";
    int currentNode = root;
    bool reachedLeaf = false;
    int* data;
    while(!reachedLeaf) {
        data = new int[size+1];
        InputStream* is = new BufferedInputStream(B);
        ostringstream oss;
        oss << currentNode;
        string s = "B"+oss.str();
        const char* name = s.c_str();
        is->open(name);
        int i = 0;
        while(!is->endOfStream() && i < size) {
            data[i] = is->readNext();
            i++;
        }
        cout << "Read in " << i << " integers from node " << currentNode << "\n";
        if(data[2] == 1) {
            reachedLeaf = true;
            cout << "Element " << element << " Reached leaf in node" << currentNode << "\n";
            /*cout << data[0] << " " << data[1] << " " << data[2] << " " << data[4]
                 << " " << data[5] << " " << data[6]
                 << " " << data[7] <<"\n";*/ /*
            for(int v = 0; v < size; v++) {
                cout << data[v] << " ";
            }
            cout << "\n";
        }
        else {
            cout << "Element " << element << " Finding leaf in node" << currentNode << "\n";
            /*cout << data[0] << " " << data[1] << " " << data[2] << " " << data[4]
                    << " " << data[5] << " " << data[6]
                    << " " << data[7] <<"\n"; */ /*
            for(int v = 0; v < size; v++) {
                cout << data[v] << " ";
            }
            cout << "\n";
            i = 4;
            int j = data[3]*2+4; // number of elements times two
            int ele;
            while(i < j) {
                ele = data[i];
                currentNode = data[i+1];
                if(ele >= element) {
                    break;
                }
                i = i+2;
            }
        }
        if(!reachedLeaf) {
            delete[] data;
        }
        is->close();
        delete(is);
        cout << "Proceeding to node " << currentNode << "\n";
    }

    // Reached leaf where we will insert the element

    // If we have room for an extra element
    if(data[3]+4 < size) {
        int y = data[3]+4;
        int i = 4;
        while(element >= data[i] && i < y) {
            i++;
        }
        // Insert element and move all other elements back in the array
        y = y+1;
        int j;
        while(i < y) {
            j = data[i];
            data[i] = element;
            element = j;
            i++;
        }

        data[3] = y-4; // Which is data[3] = data[3]+1;
        ostringstream oss;
        oss << currentNode;
        string s = "B"+oss.str();
        const char* name = s.c_str();
        OutputStream* os = new BufferedOutputStream(B);
        os->create(name);
        for(int x = 0; x < size; x++) {
            os->write(&data[x]);
        }
        os->close();
        delete(os);

        delete[] data;
        cout << "Successfully inserted element in node " << currentNode << "\n";
    }
    else {
        cout << "No room for extra element \n";
        // Just kidding, we have room for ONE more in data, but then we
        // split it into two nodes with half the elements.

        // Find where to insert the element and move back the following elements.
        int y = data[3];
        int i = 4;
        while(element >= data[i] && i < size) {
            i++;
        }

        cout << "Inserting in place " << i << " size " << size << "\n";

        // Insert element and move all other elements back in the array
        int j;
        while(i <= size) {
            j = data[i];
            data[i] = element;
            cout << "Placed " << element << " in array place " << i << "\n";
            element = j;
            i++;
        }

        int split = (y+1)/2;
        cout << "Splitting node " << currentNode << " into two with #" << split << "\n";
        data[3] = split;
        bool parent = true;
        if(data[1] == 0) {
            parent = false;
            data[1] = numberOfNodes+2;
            // Will update below
        }
        // Write out original file
        ostringstream oss;
        oss << currentNode;
        string s = "B"+oss.str();
        const char* name = s.c_str();
        OutputStream* os = new BufferedOutputStream(B);
        os->create(name);
        for(int x = 0; x < split+4; x++) {
            os->write(&data[x]);
        }
        os->close();
        delete(os);

        // Create new node
        numberOfNodes++;
        ostringstream oss2;
        oss2 << numberOfNodes;
        string s2 = "B"+oss2.str();
        const char* name2 = s2.c_str();
        OutputStream* os2 = new BufferedOutputStream(B);
        os2->create(name2);
        os2->write(&numberOfNodes); // ID
        os2->write(&data[1]); // Parent
        os2->write(&data[2]); // Leaf
        int otherSide = split+1;
        os2->write(&otherSide); // #Elements after split
        for(int x = split+4; x <= size; x++) {
            os2->write(&data[x]);
        }
        os2->close();
        delete(os2);

        // Split

        if(!parent) {
            // Create new root
            numberOfNodes++;
            root = numberOfNodes;
            OutputStream* os3 = new BufferedOutputStream(B);
            ostringstream oss3;
            oss3 << numberOfNodes;
            string s3 = "B"+oss3.str();
            const char* name3 = s3.c_str();
            os3->create(name3);
            os3->write(&numberOfNodes);
            int zero = 0;
            os3->write(&zero); // No parent
            os3->write(&zero); // Not a leaf
            int two = 2;
            os3->write(&two); // Two elements
            os3->write(&data[split+3]);
            os3->write(&currentNode);
            os3->write(&data[size]);
            int newChild = numberOfNodes-1;
            os3->write(&newChild);
            os3->close();
            delete(os3);
            cout << "Created new root " << root << " with " << data[split+3] << " " << data[size] << "\n";
            delete[] data;

        }
        else {
            int ele1 = data[split+3];
            int ele2 = data[size];
            int par = data[1];
            delete[] data;
            insertInternal(par,currentNode,numberOfNodes,ele1,ele2);
        }
    }
}

void Btree::insertInternal(int node, int child1, int child2, int max1, int max2) {

    cout << "Insert internal on node " << node << "\n";
    cout << node << " " << child1 << " " << child2 << " " << max1 << " " << max2 << "\n";

    // Read in data
    int* data = new int[size+2];
    InputStream* is = new BufferedInputStream(B);
    ostringstream oss;
    oss << node;
    string s = "B"+oss.str();
    const char* name = s.c_str();
    is->open(name);
    int i = 0;
    while(!is->endOfStream()) {
        data[i] = is->readNext();
        i++;
    }
    is->close();
    delete(is);

    if(data[3]*2+4 < size) {
        // Space enough to insert

        // Find original child node
        int i = 5;
        while(!(data[i] == child1)) {
            i=i+2; // Skip elements, look at pointers.
        }
        // Update childs element, keep pointer at i
        data[i-1] = max1;
        i++;

        // Insert second child after first, and move
        // remaining elements and children back.
        int count = data[3]+1;
        int x,y;
        int element = max2;
        int child = child2;
        while(i < count) {
            x = data[i];
            y = data[i+1];
            data[i] = element;
            data[i+1] = child;
            element = x;
            child = y;
            i=i+2;
        }
        data[3] = count;

        // Write out node
        OutputStream* os = new BufferedOutputStream(B);
        ostringstream oss;
        oss << node;
        string s = "B"+oss.str();
        const char* name = s.c_str();
        os->create(name);
        for(int z = 0; z < size; z++) {
            os->write(&data[z]);
        }
        os->close();
        delete(os);
        delete[] data;

    }
    else {
        // Insert new element, then split
        // Find original child node
        int i = 5;
        while(!(data[i] == child1)) {
            i=i+2; // Skip elements, look at pointers.
        }
        // Update childs element, keep pointer at i
        data[i-1] = max1;
        i++;

        // Insert second child after first, and move
        // remaining elements and children back.
        int split = (data[3]+1)/2;
        cout << "Split = " << split << "\n";
        int x,y;
        int element = max2;
        int child = child2;
        while(i <= size+1) {
            x = data[i];
            y = data[i+1];
            data[i] = element;
            data[i+1] = child;
            element = x;
            child = y;
            i=i+2;
        }
        data[3] = split;

        bool parent = true;
        if(data[1] == 0) {
            parent = false;
            data[1] = numberOfNodes+2;
            // Will update below
        }
        // Write out original node
        OutputStream* os = new BufferedOutputStream(B);
        ostringstream oss;
        oss << node;
        string s = "B"+oss.str();
        const char* name = s.c_str();
        os->create(name);
        for(int z = 0; z < split*2+4; z++) {
            os->write(&data[z]);
        }
        os->close();
        delete(os);

        cout << "Updated node " << node << " to ";
        for(int z = 0; z < split*2+4; z++) {
            cout << data[z] << " ";
        }
        cout << "\n";

        // Write new node
        numberOfNodes++;
        OutputStream* os2 = new BufferedOutputStream(B);
        ostringstream oss2;
        oss2 << numberOfNodes;
        string s2 = "B"+oss2.str();
        const char* name2 = s2.c_str();
        os2->create(name2);
        os2->write(&numberOfNodes);
        os2->write(&data[1]);
        os2->write(&data[2]);
        int otherSide = split+1;
        os2->write(&otherSide);
        for(int z = split*2+4; z <= size+1; z++) {
            os2->write(&data[z]);
        }
        os2->close();
        delete(os2);

        cout << "Created node " << numberOfNodes << " with ";
        cout << numberOfNodes << " ";
        cout << data[1] << " ";
        cout << data[2] << " ";
        cout << otherSide << " ";

        for(int z = split*2+4; z <= size+1; z++) {
            cout << data[z] << " ";
        }
        cout << "\n";

        // Check if we need to update the left childs parent pointer
        bool updateParentPointer = true;
        for(int z = 5; z < split*2+4; z=z+2) {
            if(data[z] == child1) {
                updateParentPointer = false; // Left child went to original parent
            }
        }

        if(!parent) {
            // Create new root
            numberOfNodes++;
            cout << "Creating new root " << numberOfNodes << "\n";
            root = numberOfNodes;
            OutputStream* os3 = new BufferedOutputStream(B);
            ostringstream oss3;
            oss3 << numberOfNodes;
            string s3 = "B"+oss3.str();
            const char* name3 = s3.c_str();
            os3->create(name3);
            os3->write(&numberOfNodes);
            int zero = 0;
            os3->write(&zero); // No parent
            os3->write(&zero); // Not a leaf
            int two = 2; // Two elements
            os3->write(&two);
            os3->write(&data[split+3]);
            os3->write(&node);
            os3->write(&data[size]);
            int newNode = numberOfNodes-1;
            os3->write(&newNode);
            os3->close();
            cout << "test\n";
            delete(os3);
            cout << "test2\n";
            //delete[] data;
            cout << "test3\n";

        }
        else {
            // Recurse
            int ele1 = data[split+3];
            int ele2 = data[size];
            int par = data[1];
            delete[] data;
            insertInternal(par, node, numberOfNodes,ele1,ele2);
        }
    }


}

int Btree::query(int element) {

    return 0;
}

void Btree::deleteElement(int element) {

}
 */
