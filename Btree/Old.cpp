//
// Created by martin on 10/16/17.
//

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
