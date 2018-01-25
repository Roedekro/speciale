//
// Created by martin on 11/8/17.
//

#include "DevelopmentTester.h"
#include <iostream>
#include "../Streams/OutputStream.h"
#include "../Streams/InputStream.h"
#include "../Streams/BufferedOutputStream.h"
#include "../Streams/NaiveInputStream.h"
#include "../Streams/SystemInputStream.h"
#include "../Streams/BufferedInputStream.h"
#include "../Streams/MapInputStream.h"
#include "../Streams/NaiveOutputStream.h"
#include "../Streams/SystemOutputStream.h"
#include "../Streams/MapOutputStream.h"
#include "../Btree/Btree.h"
#include "../Btree/ModifiedInternalNode.h"
#include "../Btree/ModifiedBtree.h"
#include "../BufferedBTree/BufferedBTree.h"
#include "../TruncatedBufferTree/ExternalBufferedBTree.h"
#include "../TruncatedBufferTree/TruncatedBufferTree.h"
#include "../Btree/ModifiedBuilder.h"
#include "../BufferedBTree/BufferedBTreeBuilder.h"
#include "../xDict/XDict.h"

#include <ctime>
#include <ratio>
#include <chrono>
#include <sstream>
#include <cmath>
#include <iostream>
#include <sstream>
#include <unistd.h>
#include <string>
#include <fcntl.h>
#include <math.h>
#include <unistd.h>
#include <algorithm>
#include <fcntl.h>
#include <sys/stat.h>
#include <fstream>
#include <sys/resource.h>

using namespace std;

DevelopmentTester::DevelopmentTester() {

}

DevelopmentTester::~DevelopmentTester() {

}

void DevelopmentTester::test() {
    // Test
    //manipulateArrayTest();
    //streamtestread(1000000000,100000000);
    //streamtestread(10000000,1000000);
    //streamtestwrite(10000000,1000000);
    //BtreeInsertTest();
    //writeToFileTest();
    //simpleInsertAndQueryTest();
    //insertAndQueryTest();
    //insertDeleteAndQueryTest();
    //pointerSize();
    //internalModifiedBtreeInsertAndQueryTest();
    //internalModifiedBtreeInsertDeleteAndQueryTest();
    //internalNodeCalculation();
    //BufferedBtreeInsertDeleteAndQueryTest();
    //sortTest();
    //sizeTest();
    //sortAndRemoveTest();
    //BufferWriteReadAppendTest();
    //sortAndRemoveDuplicatesExternalBufferTest();
    //testLeafReadWriteAndSortRemove();
    //testHandleDeletesExternalLeaf();
    //testNewLeafBufferOverflowMethod();
    //handleEmptyRootLeafBufferOverflow();
    //externalBufferedBTreeInsertDeleteQuery(); // <------ ExternalBufferedBTree Main test!
    //testMedianOfMedians();
    //testTruncatedInsertQueryDelete(); // Main Truncated Buffer Tree!
    //testBinarySearchFracList();
    //readDiskstatsTest();
    //buildModifiedBTreeTest();
    //initialTestBufferedBTree();
    //advancedTestBufferedBTree();
    //buildBufferedBTreeTest();
    xDictBasicTest();
}

int DevelopmentTester::streamtestread(long n, int increment) {

    using namespace std::chrono;

    long* time = new long[4];
    long counter = increment;
    while(counter <= n) {

        // Create file to be read
        OutputStream* os = new BufferedOutputStream(4096);
        os->create("testRead");
        int write = 2147483647;
        for(int i = 0; i < counter; i++) {
            os->write(&write);
        }
        os->close();

        InputStream* is = new NaiveInputStream();
        is->open("testRead");
        high_resolution_clock::time_point t1 = high_resolution_clock::now();
        for(int i = 0; i < counter; i++) {
            is->readNext();
        }
        high_resolution_clock::time_point t2 = high_resolution_clock::now();
        is->close();
        time[0] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        is = new SystemInputStream();
        is->open("testRead");
        t1 = high_resolution_clock::now();
        for(int i = 0; i < counter; i++) {
            is->readNext();
        }
        t2 = high_resolution_clock::now();
        is->close();
        time[1] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        is = new BufferedInputStream(4096);
        is->open("testRead");
        t1 = high_resolution_clock::now();
        for(int i = 0; i < counter; i++) {
            is->readNext();
        }
        t2 = high_resolution_clock::now();
        is->close();
        time[2] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        is = new MapInputStream(4096,counter);
        is->open("testRead");
        t1 = high_resolution_clock::now();
        for(int i = 0; i < counter; i++) {
            is->readNext();
        }
        t2 = high_resolution_clock::now();
        is->close();
        time[3] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();


        cout << counter << " " << time[0] << " " << time[1]
             << " " << time[2] << " " << time[3] << "\n";

        counter = counter + increment;
        remove("testRead");
    }
    return 0;
}

int DevelopmentTester::streamtestread2(long n, int increment) {

    using namespace std::chrono;

    long *time = new long[3];
    long counter = increment;
    while (counter <= n) {

        // Create file to be read
        OutputStream *os = new BufferedOutputStream(4096);
        os->create("testRead");
        int write = 2147483647;
        for (int i = 0; i < counter; i++) {
            os->write(&write);
        }
        os->close();

        InputStream* is = new SystemInputStream();
        is->open("testRead");
        high_resolution_clock::time_point t1 = high_resolution_clock::now();
        for (int i = 0; i < counter; i++) {
            is->readNext();
        }
        high_resolution_clock::time_point t2 = high_resolution_clock::now();
        is->close();
        time[0] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        is = new BufferedInputStream(4096);
        is->open("testRead");
        t1 = high_resolution_clock::now();
        for (int i = 0; i < counter; i++) {
            is->readNext();
        }
        t2 = high_resolution_clock::now();
        is->close();
        time[1] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        is = new MapInputStream(4096, counter);
        is->open("testRead");
        t1 = high_resolution_clock::now();
        for (int i = 0; i < counter; i++) {
            is->readNext();
        }
        t2 = high_resolution_clock::now();
        is->close();
        time[2] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();


        cout << counter << " " << time[0] << " " << time[1]
             << " " << time[2] << "\n";

        counter = counter + increment;
        remove("testRead");
    }
    return 0;
}

int DevelopmentTester::streamtestwrite(long n, int increment) {

    using namespace std::chrono;

    long* time = new long[4];
    long counter = increment;
    int write = 2147483647;
    while(counter <= n) {

        OutputStream* os = new NaiveOutputStream();
        os->create("testWrite");
        high_resolution_clock::time_point t1 = high_resolution_clock::now();
        for(int i = 0; i < counter; i++) {
            os->write(&write);
        }
        high_resolution_clock::time_point t2 = high_resolution_clock::now();
        os->close();
        remove("testWrite");
        time[0] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        os = new SystemOutputStream();
        os->create("testWrite");
        t1 = high_resolution_clock::now();
        for(int i = 0; i < counter; i++) {
            os->write(&write);
        }
        t2 = high_resolution_clock::now();
        os->close();
        remove("testWrite");
        time[1] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        os = new BufferedOutputStream(4096);
        os->create("testWrite");
        t1 = high_resolution_clock::now();
        for(int i = 0; i < counter; i++) {
            os->write(&write);
        }
        t2 = high_resolution_clock::now();
        os->close();
        remove("testWrite");
        time[2] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        os = new MapOutputStream(4096,counter);
        os->create("testWrite");
        t1 = high_resolution_clock::now();
        for(int i = 0; i < counter; i++) {
            os->write(&write);
        }
        t2 = high_resolution_clock::now();
        os->close();
        remove("testWrite");
        time[3] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();


        cout << counter << " " << time[0] << " " << time[1]
             << " " << time[2] << " " << time[3] << "\n";

        counter = counter + increment;
    }
    return 0;
}

int DevelopmentTester::streamtestwrite2(long n, int increment) {

    using namespace std::chrono;

    long* time = new long[3];
    long counter = increment;
    int write = 2147483647;
    while(counter <= n) {

        OutputStream* os = new SystemOutputStream();
        os->create("testWrite");
        high_resolution_clock::time_point t1 = high_resolution_clock::now();
        for(int i = 0; i < counter; i++) {
            os->write(&write);
        }
        high_resolution_clock::time_point t2 = high_resolution_clock::now();
        os->close();
        remove("testWrite");
        time[0] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        os = new BufferedOutputStream(4096);
        os->create("testWrite");
        t1 = high_resolution_clock::now();
        for(int i = 0; i < counter; i++) {
            os->write(&write);
        }
        t2 = high_resolution_clock::now();
        os->close();
        remove("testWrite");
        time[1] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        os = new MapOutputStream(4096,counter);
        os->create("testWrite");
        t1 = high_resolution_clock::now();
        for(int i = 0; i < counter; i++) {
            os->write(&write);
        }
        t2 = high_resolution_clock::now();
        os->close();
        remove("testWrite");
        time[2] = chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();


        cout << counter << " " << time[0] << " " << time[1]
             << " " << time[2] << "\n";

        counter = counter + increment;
    }
    return 0;
}

void DevelopmentTester::BtreeInsertTest() {

    Btree* btree = new Btree(24);
    KeyValue* kV = new KeyValue(1,1);
    cout << "Inserting 1\n";
    btree->insert(kV);


    kV->key = 3;
    kV->value = 3;
    cout << "Inserting 3\n";
    btree->insert(kV);
    kV->key = 4;
    kV->value = 4;
    cout << "Inserting 4\n";
    btree->insert(kV);

    kV->key = 5;
    kV->value = 5;
    cout << "Inserting 5\n";
    btree->insert(kV);
    kV->key = 6;
    kV->value = 6;
    cout << "Inserting 6\n";
    btree->insert(kV);


    kV->key = 2;
    kV->value = 2;
    cout << "Inserting 2\n";
    btree->insert(kV);

    kV->key = 7;
    kV->value = 7;
    cout << "Inserting 7\n";
    btree->insert(kV);

    // Write out nodes
    for(int i = 1; i < 5; i++) {
        cout << "-----\n";
        InputStream* is2 = new BufferedInputStream(32);
        ostringstream oss2;
        oss2 << i;
        string s2 = "B"+oss2.str();
        const char* name2 = s2.c_str();
        is2->open(name2);
        int element = 0;
        while(!is2->endOfStream()) {
            element = is2->readNext();
            cout << element << "\n";
        }
        is2->close();
        delete(is2);
    }

    /*Btree* btree = new Btree(32);
    InputStream* is = new BufferedInputStream(32);
    ostringstream oss;
    oss << 1;
    string s = "B"+oss.str();
    const char* name = s.c_str();
    is->open(name);
    int element;
    //cout << is->readNext() << "\n";
    while(!is->endOfStream()) {
        element = is->readNext();
        cout << element << "\n";
    }
    is->close();
    delete(is);

    cout << "Done!\n";

    btree->insert(1);
    btree->insert(2);
    btree->insert(3);
    btree->insert(4);
    btree->insert(5);
    btree->insert(6); // Korrekt til og med 6
    btree->insert(7);
    /*btree->insert(8);
    */



}

void DevelopmentTester::simpleInsertAndQueryTest() {

    Btree* btree = new Btree(24);
    KeyValue* kV = new KeyValue(1,1);
    cout << "Inserting 1\n";
    btree->insert(kV);


    kV->key = 3;
    kV->value = 3;
    cout << "Inserting 3\n";
    btree->insert(kV);
    kV->key = 4;
    kV->value = 4;
    cout << "Inserting 4\n";
    btree->insert(kV);

    kV->key = 5;
    kV->value = 5;
    cout << "Inserting 5\n";
    btree->insert(kV);
    kV->key = 6;
    kV->value = 6;
    cout << "Inserting 6\n";
    btree->insert(kV);


    kV->key = 2;
    kV->value = 2;
    cout << "Inserting 2\n";
    btree->insert(kV);

    kV->key = 7;
    kV->value = 7;
    cout << "Inserting 7\n";
    btree->insert(kV);

    int ret;

    for(int i = 1; i <= 7; i++) {
        ret = btree->query(i);
        cout << ret << "\n";
    }

    // Write out nodes
    for(int i = 1; i < 5; i++) {
        cout << "-----\n";
        InputStream* is2 = new BufferedInputStream(32);
        ostringstream oss2;
        oss2 << i;
        string s2 = "B"+oss2.str();
        const char* name2 = s2.c_str();
        is2->open(name2);
        int element = 0;
        while(!is2->endOfStream()) {
            element = is2->readNext();
            cout << element << "\n";
        }
        is2->close();
        delete(is2);
    }

}

void DevelopmentTester::insertAndQueryTest() {

    //int number = 10;
    int number = 200;

    Btree* btree = new Btree(24);
    KeyValue* kV = new KeyValue(1,1);

    // Generate random key values
    int* keys = new int[number];
    int* values = new int[number];
    srand(time(NULL));
    //srand(100000);
    for(int i = 0; i < number; i++) {
        //keys[i] = rand()%1000;
        //values[i] = rand()%1000;
        keys[i] = rand();
        values[i] = rand();
    }

    for(int i = 0; i < number; i++) {
        kV->key = keys[i];
        kV->value = values[i];
        btree->insert(kV);
    }

    int res;
    for(int i = 0; i < number; i++) {
        //cout << "===== Query for " << keys[i] << "\n";
        res = btree->query(keys[i]);
        if(res != values[i]) {
            cout << "Query for " << keys[i] << " should have returned "
                 << values[i] << " but returned " << res << "\n";
        }
    }

    /*cout << "Root is " << btree->root << "\n";

    // Write out nodes
    for(int i = 1; i < 10; i++) {
        cout << "-----\n";
        InputStream* is2 = new BufferedInputStream(32);
        ostringstream oss2;
        oss2 << i;
        string s2 = "B"+oss2.str();
        const char* name2 = s2.c_str();
        is2->open(name2);
        int element = 0;
        while(!is2->endOfStream()) {
            element = is2->readNext();
            cout << element << "\n";
        }
        is2->close();
        delete(is2);
    }
    */

    cout << "Done!";

}

void DevelopmentTester::insertDeleteAndQueryTest() {

    //int number = 10;
    // 200, 10 giver problemer
    // 80, 0 giver problemer med 184
    int inserts = 200;
    int deletes = 40;

    Btree* btree = new Btree(24);
    KeyValue* kV = new KeyValue(1,1);

    // Generate random key values
    int* keys = new int[inserts];
    int* values = new int[inserts];
    //srand(time(NULL));
    srand(100000);
    for(int i = 0; i < inserts; i++) {
        keys[i] = rand()%1000;
        values[i] = rand()%1000;
        //keys[i] = rand();
        //values[i] = rand();
    }

    for(int i = 0; i < inserts; i++) {
        kV->key = keys[i];
        kV->value = values[i];
        btree->insert(kV);
        cout << "Inserted " << keys[i] << " with value " << values[i] << "\n";
    }

    cout << "Inserted " << inserts << " elements into tree\n";

    for(int i = 0; i < deletes; i++) {
        cout << "=== Deleting element " <<keys[i] << "\n";
        btree->deleteElement(keys[i]);
    }

    cout << "Querying remaining elements\n";
    int res;
    for(int i = deletes; i < inserts; i++) {
        //cout << "===== Query for " << keys[i] << "\n";
        res = btree->query(keys[i]);
        if(res != values[i]) {
            bool updated = false;
            int j = i+1;
            while(j < inserts) {
                if(keys[i] == keys[j]) {
                    updated = true;
                    break;
                }
                j++;
            }
            if(updated) {
                //cout << "Updated " << keys[i] << "\n";
                if(res != values[j]) {
                    cout << "Query for " << keys[i] << " should have returned "
                         << values[j] << " but returned " << res << " i = " << i << "\n";
                }
            }
            else {
                bool deleted = false;
                int h = 0;
                while(h < deletes) {
                    if(keys[i] == keys[h]) {
                        deleted = true;
                        break;
                    }
                    h++;
                }
                if(!deleted) {
                    cout << "Query for " << keys[i] << " should have returned "
                         << values[i] << " but returned " << res << " i = " << i << "\n";
                }
            }
        }
    }

    cout << "Root is " << btree->root << "\n";

    // Write out nodes
    /*for(int i = 1; i < 56; i++) {
        cout << "-----\n";
        cout << "Node " << i << "\n";
        InputStream* is2 = new BufferedInputStream(32);
        ostringstream oss2;
        oss2 << i;
        string s2 = "B"+oss2.str();
        const char* name2 = s2.c_str();
        is2->open(name2);
        int element = 0;
        while(!is2->endOfStream()) {
            element = is2->readNext();
            cout << element << "\n";
        }
        is2->close();
        delete(is2);
    }*/

    cout << "Done!";

}

// Should give output 3 | 3 2 | 3 1
void DevelopmentTester::writeToFileTest() {

    OutputStream* os = new BufferedOutputStream(32);
    /*ostringstream oss;
    oss << 1;
    string s = "test"+oss.str();
    const char* name = s.c_str();
    os->create(name)*/
    string name = "test";
    name += to_string(1);
    os->create(name.c_str());
    int element = 3;
    os->write(&element);
    os->close();

    InputStream* is = new BufferedInputStream(32);
    is->open(name.c_str());
    cout << "---\n";
    while(!is->endOfStream()) {
        element = is->readNext();
        cout << element << "\n";
    }
    is->close();

    os = new BufferedOutputStream(32);
    os->create(name.c_str());
    os->write(&element);
    element = 2;
    os->write(&element);
    os->close();

    is = new BufferedInputStream(32);
    is->open(name.c_str());
    cout << "---\n";
    while(!is->endOfStream()) {
        element = is->readNext();
        cout << element << "\n";
    }
    is->close();

    element = 1;
    os = new BufferedOutputStream(32);
    os->create(name.c_str());
    os->write(&element);
    os->close();

    is = new BufferedInputStream(32);
    is->open(name.c_str());
    cout << "---\n";
    while(!is->endOfStream()) {
        element = is->readNext();
        cout << element << "\n";
    }
    is->close();

    cout << name;
    remove(name.c_str());
}

void DevelopmentTester::manipulateArray(int* array) {
    array[1] = 2;
}

void DevelopmentTester::manipulateArrayTest() {

    int* array = new int[2];
    array[0] = 1;
    array[1] = 1;

    cout << array[0] << array[1] << "\n";

    manipulateArray(array);

    cout << array[0] << array[1] << "\n";
}

void DevelopmentTester::pointerSize() {
    /*
    ModifiedInternalNode* node = new ModifiedInternalNode(1,1,1);
    int size = sizeof(node);
    cout << size << "\n";

    node->values[0] = (ModifiedInternalNode*) 10;

    int res = (int) node->values[0];

    cout << res << "\n";

    delete(node);
     */

}

// Only works for internal tree
void DevelopmentTester::recursivePrintModifiedNode(ModifiedInternalNode* node) {
    cout << "---\n";
    cout << node->id << "\n";
    cout << node->height << "\n";
    cout << node->nodeSize << "\n";
    for(int i = 0; i < node->nodeSize; i++) {
        cout << node->keys[i] << "\n";
    }
    if(node->height > 1) {
        cout << "=== Start " << node->id << "\n";
        for(int i = 0; i <= node->nodeSize; i++) {
            recursivePrintModifiedNode(node->children[i]);
        }
        cout << "=== Stop " << node->id << "\n";
    }
    else {
        for(int i = 0; i < node->nodeSize; i++) {
            cout << node->values[i] << "\n";
        }
    }
}

void DevelopmentTester::internalModifiedBtreeInsertAndQueryTest() {

    int number = 100;

    // Internal node size = 56
    ModifiedBtree* btree = new ModifiedBtree(24,56);
    KeyValue* keyVal = new KeyValue(1,1);
    for(int i = 1; i <= number; i++) {
        keyVal->key = i;
        keyVal->value = i;
        cout << "Inserting " << keyVal->key << "\n";
        btree->insert(keyVal);
    }

    // Write out internal tree
    //recursivePrintModifiedNode(btree->root);
    btree->printTree(btree->root);

    cout << "===Queries\n";
    for(int i = 1; i <= number; i++) {
        int ret = btree->query(i);
        if(ret != i) {
            cout << ret << "\n";
        }
    }

    cout << "Number of nodes " << btree->numberOfNodes;
    btree->cleanup();

}

void DevelopmentTester::internalModifiedBtreeInsertDeleteAndQueryTest() {

    int number = 100;
    int deletes = number/3*2;
    int* keys = new int[number];
    int* values = new int[number];

    // Internal node size = 56
    //ModifiedBtree* btree = new ModifiedBtree(24,224); // 4
    //ModifiedBtree* btree = new ModifiedBtree(24,2240); // 40
    ModifiedBtree* btree = new ModifiedBtree(24,1120); // 20
    KeyValue* keyVal = new KeyValue(1,1);
    for(int i = 1; i <= number; i++) {
        keyVal->key = i;
        keyVal->value = i;
        keys[i] = keyVal->key;
        values[i] = keyVal->value;
        //cout << "Inserting " << keyVal->key << "\n";
        btree->insert(keyVal);
    }

    // Write out internal tree
    //recursivePrintModifiedNode(btree->root);
    //btree->printTree(btree->root);

    /*for(int i = 1; i <= deletes; i++) {
        cout << "=== Deleting " << i << "\n";
        btree->deleteElement(i);
        //cout << btree->internalNodeCounter << "\n";
    }*/

    for(int i = 1; i <= 57; i++) {
        cout << "=== Deleting " << i << "\n";
        btree->deleteElement(i);
        //cout << btree->internalNodeCounter << "\n";
    }
    btree->printTree(btree->root);
    cout << "External node height is " << btree->externalNodeHeight << "\n";
    cout << "Tree height height is " << btree->root->height << "\n";

    // Reinsertion works perfectly
    /*for(int i = 1; i <= number/2; i++) {
        keyVal->key = i;
        keyVal->value = i;
        keys[i] = keyVal->key;
        values[i] = keyVal->value;
        cout << "Inserting " << keyVal->key << "\n";
        btree->insert(keyVal);
    }*/

    cout << "=== Queries\n";
    for(int i = deletes+1; i <= number; i++) {
        int ret = btree->query(i);
        if(ret != i) {
            cout << keys[i] << " returned " << ret << " should have returned " << values[i] << "\n";
        }
    }

    cout << "Number of nodes " << btree->currentNumberOfNodes << "\n";
    cout << btree->numberOfNodes << " " << btree->internalNodeCounter;
    btree->cleanup();

}

void DevelopmentTester::BufferedBtreeInsertDeleteAndQueryTest() {

    /*int number = 100;
    int deletes = number/3*2;
    int* keys = new int[number];
    int* values = new int[number];

    BufferedBTree* btree = new BufferedBTree(40,1120); // 20
    KeyValueTime* keyVal = new KeyValueTime(1,1,1);
    for(int i = 1; i <= number; i++) {
        keyVal->key = i;
        keyVal->value = i;
        keys[i] = keyVal->key;
        values[i] = keyVal->value;
        cout << "Inserting " << keyVal->key << "\n";
        btree->insert(keyVal);
    }

    // Write out internal tree
    //recursivePrintModifiedNode(btree->root);
    //btree->printTree(btree->root);

    for(int i = 1; i <= deletes; i++) {
        cout << "=== Deleting " << i << "\n";
        btree->deleteElement(i);
        //cout << btree->internalNodeCounter << "\n";
    }

    /*for(int i = 1; i <= 57; i++) {
        cout << "=== Deleting " << i << "\n";
        btree->deleteElement(i);
        //cout << btree->internalNodeCounter << "\n";
    }*/
    /*cout << "Root is " << btree->root->id << "\n";
    btree->printTree(btree->root);
    cout << "External node height is " << btree->externalNodeHeight << "\n";
    cout << "Tree height height is " << btree->root->height << "\n";

    //btree->deleteElement(1);

    cout << "=== Queries\n";
    for(int i = deletes+1; i <= number; i++) {
        int ret = btree->query(i);
        if(ret != i) {
            cout << keys[i] << " returned " << ret << " should have returned " << values[i] << "\n";
        }
    }
    /*for(int i = 1; i <= number; i++) {
        int ret = btree->query(i);
        if(ret != i) {
            cout << keys[i] << " returned " << ret << " should have returned " << values[i] << "\n";
        }
    }

    cout << "External node height is " << btree->externalNodeHeight << "\n";
    cout << "Tree height height is " << btree->root->height << "\n";
    btree->printTree(btree->root); */

    /*cout << "Number of nodes " << btree->currentNumberOfNodes << "\n";
    cout << btree->numberOfNodes << " " << btree->internalNodeCounter;
    btree->cleanup();
*/
}

void DevelopmentTester::internalNodeCalculation() {

    int B = 24;
    //int M = 224; // 4 nodes of size 56 = min 1
    //int M = 2240; // 40 nodes = min 16
    //int M = 1120; // 20 nodes = min 16
    int M = 896; // 20 nodes = min 4
    int size = ((B/sizeof(int))-2)/2;
    int internalNodeSize = ((3*sizeof(int)) + ((size*2-1)*sizeof(int)) + (size*2*sizeof(ModifiedInternalNode*)));
    int maxInternalNodes = M/internalNodeSize;
    int internalHeight = (int) (log(maxInternalNodes) / log(2*size));
    if(internalHeight - (log(maxInternalNodes) / log(2*size)) != 0) {
        //
        internalHeight++;
        cout << "Increased height\n";
    }
    int minInternalNodes = (int) pow(2*size,internalHeight-1);
    cout << "Tree will have size = " << size << " resulting in #keys = " << 2*size-1 << " with int size = " << sizeof(int) << "\n";
    cout << "Max internal nodes = " << maxInternalNodes << " with internal node size = " << internalNodeSize << "\n";
    cout << "Internal height is " << internalHeight << " and min internal nodes is " << minInternalNodes << "\n";

}

void DevelopmentTester::sortTest() {


    KeyValueTime** array = new KeyValueTime*[10];
    for(int i = 1; i <= 10; i++) {
        array[i-1] = new KeyValueTime(11-i,11-i,11-i);
    }

    array[8]->key = 1;

    for(int i = 0; i < 10; i++) {
        cout << array[i]->key << " " << array[i]->time << "\n";
    }

    ExternalBufferedBTree* btree = new ExternalBufferedBTree(1,1);
    btree->sortInternalArray(array,10);

    cout << "---\n";
    for(int i = 0; i < 10; i++) {
        cout << array[i]->key << " " << array[i]->time << "\n";
    }

    /*std::vector<KeyValueTime>* buffer = new std::vector<KeyValueTime>();
    buffer->reserve(10);
    KeyValueTime* keyValueTime = new KeyValueTime(1,1,1);
    for(int i = 1; i <= 10; i++) {
        keyValueTime->key = 11-i;
        keyValueTime->time = 11-i;
        buffer->push_back(*keyValueTime);
    }

    for(int i = 0; i < 10; i++) {
        cout << buffer->at(i).key << " " << buffer->at(i).time << "\n";
    }

    BufferedBTree* bTree = new BufferedBTree(40,1120);
    bTree->sortInternal(buffer);

    cout << "---\n";
    for(int i = 0; i < 10; i++) {
        cout << buffer->at(i).key << " " << buffer->at(i).time << "\n";
    }*/


    /*KeyValueTime** array = new KeyValueTime*[10];
    for(int i = 1; i <= 10; i++) {
        array[i-1] = new KeyValueTime(11-i,11-i,11-i);
    }

    array[8]->key = 1;

    for(int i = 0; i < 10; i++) {
        cout << array[i]->key << " " << array[i]->time << "\n";
    }

    BufferedBTree* bTree = new BufferedBTree(40,1120);
    bTree->sortInternal(array,10);

    cout << "---\n";
    for(int i = 0; i < 10; i++) {
        cout << array[i]->key << " " << array[i]->time << "\n";
    }*/


}

void DevelopmentTester::sizeTest() {

    KeyValueTime* keyValueTime = new KeyValueTime(1,1,1);
    cout << "size of KeyValueTime is " << sizeof(*keyValueTime) << "\n";
    cout << "size of KeyValueTime is " << sizeof(KeyValueTime) << "\n";
}

void DevelopmentTester::sortAndRemoveTest() {

    KeyValueTime** array = new KeyValueTime*[10];
    for(int i = 1; i <= 10; i++) {
        array[i-1] = new KeyValueTime(11-i,11-i,11-i);
    }

    array[8]->key = 1;
    array[4]->key = 5;

    for(int i = 0; i < 10; i++) {
        cout << array[i]->key << " " << array[i]->time << "\n";
    }

    ExternalBufferedBTree* btree = new ExternalBufferedBTree(1,1);
    int ret = btree->sortAndRemoveDuplicatesInternalArray(array,10);

    cout << "--- " << ret << " \n";
    for(int i = 0; i < 10; i++) {
        cout << array[i]->key << " " << array[i]->time << "\n";
    }
}

void DevelopmentTester::BufferWriteReadAppendTest() {

    KeyValueTime** array = new KeyValueTime*[10];
    for(int i = 1; i <= 10; i++) {
        array[i-1] = new KeyValueTime(11-i,11-i,11-i);
    }

    ExternalBufferedBTree* btree = new ExternalBufferedBTree(1,1);

    btree->writeBuffer(42,array,10,1);

    array = new KeyValueTime*[10];

    btree->readBuffer(42,array,1);

    for(int i = 0; i < 10; i++) {
        cout << array[i]->key << " " << array[i]->time << "\n";
    }
    cout << "---\n";

    array[0]->time = 20;

    btree->appendBuffer(42,array,10,1);

    array = new KeyValueTime*[20];

    btree->readBuffer(42,array,1);

    for(int i = 0; i < 20; i++) {
        cout << array[i]->key << " " << array[i]->time << "\n";
    }

    // Delete file, append (create), read
    string name = "Buf";
    name += to_string(42);
    if(std::FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }

    cout << "---\n";

    array = new KeyValueTime*[10];
    for(int i = 1; i <= 10; i++) {
        array[i-1] = new KeyValueTime(11-i,11-i,11-i);
    }

    btree->appendBuffer(42,array,10,1);

    array = new KeyValueTime*[10];

    btree->readBuffer(42,array,1);

    for(int i = 0; i < 10; i++) {
        cout << array[i]->key << " " << array[i]->time << "\n";
    }




}

void DevelopmentTester::sortAndRemoveDuplicatesExternalBufferTest() {

    KeyValueTime** array = new KeyValueTime*[10];
    for(int i = 1; i <= 10; i++) {
        array[i-1] = new KeyValueTime(11-i,11-i,11-i);
    }

    array[8]->key = 1;
    array[4]->key = 5;

    for(int i = 0; i < 10; i++) {
        cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
    }
    cout << "---\n";

    ExternalBufferedBTree* btree = new ExternalBufferedBTree(1,1);

    btree->writeBuffer(42,array,10,1);

    array = new KeyValueTime*[10];
    for(int i = 0; i < 10; i++) {
        array[i] = new KeyValueTime(i+5,i+6,i+7);
    }

    for(int i = 0; i < 10; i++) {
        cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
    }
    cout << "---\n";

    btree->appendBuffer(42,array,10,1);

    int newSize = btree->sortAndRemoveDuplicatesExternalBuffer(42,20,10);

    array = new KeyValueTime*[newSize];

    btree->readBuffer(42,array,1);

    for(int i = 0; i < newSize; i++) {
        cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
    }

}

void DevelopmentTester::testLeafReadWriteAndSortRemove() {





    KeyValueTime** array = new KeyValueTime*[20];
    for(int i = 1; i <= 10; i++) {
        array[i-1] = new KeyValueTime(i,i,i);
    }

    //array[1]->value = -2;
    //array[1]->time = 5;

    for(int i = 11; i <= 20; i++) {
        array[i-1] = new KeyValueTime(i-9,i-8,i-7);
    }

    array[10]->value = -2;

    cout << "Original array:\n";

    for(int i = 0; i < 20; i++) {
        cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
    }
    cout << "---\n";

    cout << "Array after write and read:\n";

    ExternalBufferedBTree* btree = new ExternalBufferedBTree(1,1);

    btree->writeLeaf(42,array,20);

    array = new KeyValueTime*[20];

    int ret = btree->readLeaf(42,array);

    for(int i = 0; i < ret; i++) {
        cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
    }
    cout << "---\n";

    // Clean up
    for(int i = 0; i < ret; i++) {
        delete(array[i]);
    }
    delete[] array;

    btree->sortAndRemoveDuplicatesExternalLeaf(42,20,10);

    array = new KeyValueTime*[20];
    ret = btree->readLeaf(42,array);

    cout << "Array after external sort/remove:\n";
    for(int i = 0; i < ret; i++) {
        cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
    }
    cout << "---\n";
    cout << "Generated original array again\n";
    cout << "---\n";

    // Clean up
    for(int i = 0; i < ret; i++) {
        delete(array[i]);
    }
    delete[] array;

    array = new KeyValueTime*[20];
    for(int i = 1; i <= 10; i++) {
        array[i-1] = new KeyValueTime(i,i,i);
    }

    //array[1]->value = -2;
    //array[1]->time = 5;

    for(int i = 11; i <= 20; i++) {
        array[i-1] = new KeyValueTime(i-9,i-8,i-7);
    }

    array[10]->value = -2;

    ret = btree->sortAndRemoveDuplicatesInternalArrayLeaf(array,20);

    cout << "Array after internal sort/remove:\n";
    for(int i = 0; i < ret; i++) {
        cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
    }
}

void DevelopmentTester::externalBufferedBTreeInsertDeleteQuery() {

    // Insert elements
    KeyValueTime** array = new KeyValueTime*[20];
    for(int i = 1; i <= 10; i++) {
        array[i-1] = new KeyValueTime(i,i,0);
    }

    for(int i = 11; i <= 20; i++) {
        array[i-1] = new KeyValueTime(i-9,i-8,0);
    }

    int B = 96;
    int M = 6144;

    //cout << (M/(sizeof(int)*2*B*4)) << "\n";

    ExternalBufferedBTree* bTree = new ExternalBufferedBTree(B,M);
    bTree->cleanUpFromTo(3,300);
    cout << "Cleaned up\n";


    //int updates = 4;
    /*for(int i = 0; i < updates; i++) {
        bTree->update(array[i]);
    }*/

    int updates = 600;
    updates = updates - 7;
    for(int i = 1; i <= updates; i++) {
        bTree->update(new KeyValueTime(i,i,0));
    }

    cout << "============================================= DONE INSERTING\n";

    //bTree->printTree();

    //bTree->printTree();
    //bTree->cleanUpTree();

    cout << "Update buffer size is " << bTree->update_bufferSize << "\n";
    cout << "Root buffer size is " << bTree->rootBufferSize << "\n";


    cout << "============================================= Queries\n";

    //bTree->flushEntireTree();
    //bTree->printTree();


    int ret;
    /*int ret = bTree->query(1);
    cout << "Query returned " << ret << "\n";*/

    //bTree->printTree();
    //bTree->query(1);

    ret = 0;
    for(int i = 1; i <= updates; i++) {
        ret = bTree->query(i);
        if(ret != i) {
            cout << i << " " << ret << "\n";
        }
    }

    //bTree->printTree();

    //cout << "============================================= Clean up\n";

    //bTree->cleanUpTree();



    cout << "============================================= DELETING\n";

    int spareTheseKVTs = 10;

    for(int i = 1; i <= updates-spareTheseKVTs; i++) {
        bTree->update(new KeyValueTime(i,-i,0));
    }

    cout << "============================================= DONE DELETING\n";

    bTree->printTree();

    cout << "Flushing\n";
    bTree->flushEntireTree();
    cout << "Done Flushing\n";

    //cout << "Cleaning up\n";*/

    bTree->printTree();

    bTree->cleanUpTree();



    cout << "Done!\n";

    //bTree->printTree();

}

void DevelopmentTester::testHandleDeletesExternalLeaf() {

    KeyValueTime** array = new KeyValueTime*[10];
    for(int i = 1; i <= 10; i++) {
        array[i-1] = new KeyValueTime(i,i,i);
    }
    array[1]->key = 1;
    array[5]->key = 5;
    array[5]->value = -5;
    array[7]->key = 9;
    array[7]->value = -8;

    for(int i = 0; i < 10; i++) {
        cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
    }

    ExternalBufferedBTree* bTree = new ExternalBufferedBTree(1,1);

    bTree->writeLeaf(42,array,10);

    int ret = bTree->handleDeletesLeafOriginallyEmptyExternal(42);

    cout << ret << "\n";

    array = new KeyValueTime*[10];

    ret = bTree->readLeaf(42,array);
    for(int i = 0; i < ret; i++) {
        cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
    }

}

void DevelopmentTester::testNewLeafBufferOverflowMethod() {

    // Create new buffer to overflow
    KeyValueTime** array = new KeyValueTime*[10];
    cout << "Buffer will consist of\n";
    for(int i = 1; i <= 10; i++) {
        array[i-1] = new KeyValueTime(i,i,i+10);
    }

    array[7]->key = 12;
    array[8]->key = 14;
    array[8]->value = -14;
    array[9]->key = 19;
    array[9]->value = -1;

    for(int i = 1; i <= 10; i++) {
        cout << array[i-1]->key << " " << array[i-1]->value << " " << array[i-1]->time << "\n";
    }

    int B = 96;
    int M = 6144;
    ExternalBufferedBTree* bTree = new ExternalBufferedBTree(B,M);

    bTree->writeBuffer(42,array,10,1); // Buf42

    // Create new leaf
    array = new KeyValueTime*[5];
    cout << "Leaf421 will consist of\n";
    for(int i = 1; i <= 5; i++) {
        array[i-1] = new KeyValueTime(i+10,i+10,i);
        cout << i+10 << " " << i+10 << " " << i << "\n";
    }

    bTree->writeLeaf(421,array,5);

    // Create new leaf
    cout << "Leaf422 will consist of\n";
    array = new KeyValueTime*[5];
    for(int i = 1; i <= 5; i++) {
        array[i-1] = new KeyValueTime(i+15,i+15,i+5);
        cout << i+15 << " " << i+15 << " " << i+5 << "\n";
    }


    bTree->writeLeaf(422,array,5);

    // Create node
    vector<int>* keys = new vector<int>();
    vector<int>* values = new vector<int>();
    values->push_back(421);
    values->push_back(422);
    keys->push_back(15);
    bTree->writeNode(42,1,1,10,keys,values);

    // Write leaf info
    vector<int>* leafs = new vector<int>();
    leafs->push_back(5);
    leafs->push_back(5);
    bTree->writeLeafInfo(42,leafs,2);


    keys = new vector<int>();
    values = new vector<int>();
    values->push_back(421);
    values->push_back(422);
    keys->push_back(15);
    leafs = new vector<int>();
    leafs->push_back(5);
    leafs->push_back(5);
    int nodeSize = 1;
    int* ptr_nodeSize = &nodeSize;
    bTree->handleLeafNodeBufferOverflow(42,ptr_nodeSize,keys,values,leafs);

    cout << "---Keys\n";
    for(int i = 0; i < nodeSize; i++) {
        cout << (*keys)[i] << "\n";
    }

    cout << "---Values\n";
    for(int i = 0; i < nodeSize+1; i++) {
        cout << (*values)[i] << "\n";
    }

    for(int i = 0; i < nodeSize+1; i++) {
        array = new KeyValueTime*[8];
        int ret = bTree->readLeaf((*values)[i],array);
        string name = "Leaf";
        name += to_string((*values)[i]);
        cout << "---" << name << "\n";
        for(int i = 0; i < ret; i++) {
            cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
        }
    }

    /*array = new KeyValueTime*[8];
    int ret = bTree->readLeaf(421,array);

    cout << "---Leaf421\n";
    for(int i = 0; i < ret; i++) {
        cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
    }

    array = new KeyValueTime*[8];
    ret = bTree->readLeaf(422,array);

    cout << "---Leaf422\n";
    for(int i = 0; i < ret; i++) {
        cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
    }

    /*array = new KeyValueTime*[8];
    ret = bTree->readLeaf(3,array);

    cout << "---Leaf3\n";
    for(int i = 0; i < ret; i++) {
        cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
    }
     */


}

void DevelopmentTester::handleEmptyRootLeafBufferOverflow() {

    int B = 96;
    int M = 6144;
    ExternalBufferedBTree* bTree = new ExternalBufferedBTree(B,M);

    // Create node
    vector<int>* keys = new vector<int>();
    vector<int>* values = new vector<int>();
    values->push_back(200);
    int nodeSize = 0;
    int* ptr_nodeSize = &nodeSize;
    //bTree->writeNode(42,1,0,10,keys,values);

    // Write leaf info
    vector<int>* leafs = new vector<int>();
    leafs->push_back(0);
    //bTree->writeLeafInfo(42,leafs,1);

    KeyValueTime** array = new KeyValueTime*[10];
    cout << "Buffer will consist of\n";
    for(int i = 1; i <= 10; i++) {
        array[i-1] = new KeyValueTime(i,i,i+10);
    }

    /*array[7]->key = 12;
    array[8]->key = 14;
    array[8]->value = -14;
    array[9]->key = 19;
    array[9]->value = -1;*/

    for(int i = 1; i <= 10; i++) {
        cout << array[i-1]->key << " " << array[i-1]->value << " " << array[i-1]->time << "\n";
    }

    bTree->writeBuffer(42,array,10,1); // Buf42

    bTree->handleRootEmptyLeafBufferOverflow(42,ptr_nodeSize,keys,values,leafs);

    cout << "Printing node\n";
    cout << nodeSize << "\n";
    cout << "---Keys\n";
    for(int i = 0; i < nodeSize; i++) {
        cout << (*keys)[i] << "\n";
    }

    cout << "---Values\n";
    for(int i = 0; i < nodeSize+1; i++) {
        cout << (*values)[i] << "\n";
    }


    for(int i = 0; i < nodeSize+1; i++) {
        array = new KeyValueTime*[8];
        int ret = bTree->readLeaf((*values)[i],array);
        string name = "Leaf";
        name += to_string((*values)[i]);
        cout << "---" << name << "\n";
        for(int i = 0; i < ret; i++) {
            cout << array[i]->key << " " << array[i]->value << " " << array[i]->time << "\n";
        }
    }

}

void DevelopmentTester::testMedianOfMedians() {

    string temp = "tempfile";
    OutputStream* os = new BufferedOutputStream(4096);
    os->create(temp.c_str());
    int tempInt;
    for(int i = 1; i <= 10; i++) {
        os->write(&i);
        tempInt = -1 * i;
        os->write(&tempInt);
    }
    os->close();
    delete(os);

    int offset = (10-2)*sizeof(int);

    int* tempBuff = new int[4];
    int filedesc = ::open(temp.c_str(), O_RDONLY, 0666);
    int bytesRead = ::pread(filedesc, tempBuff, 4*sizeof(int),offset);
    int median = tempBuff[0];
    ::close(filedesc);

    cout << median << "\n";
    cout << tempBuff[3];
}

void DevelopmentTester::testTruncatedInsertQueryDelete() {

    int testB = 131072; // 128KB
    int testM = 8388608; // 8 MB
    int testN = 2000000000;
    int testDelta = 2;
    int size = testM/(sizeof(int)*2*testB*4);
    cout << "Test size would be " << size << "\n";
    int testBucket = ( testN / (pow(size*4,testDelta) / 2 ) );
    cout << "Test number of lists pr. bucket will be " << testBucket << "\n";
    int bucketSize = testM/sizeof(KeyValue);
    cout << "And elements pr. bucket will be " << bucketSize << "\n";

    long testTreeSize = (pow(size*4,testDelta) * testBucket * bucketSize);
    cout << "Test tree would have size of " << testTreeSize << "\n";
    long totalDivBuckets = testN/(testBucket*bucketSize);
    cout << totalDivBuckets << "\n";
    double log1 = log(testN/(testBucket*bucketSize));
    double log2 = log(size*4);
    cout << log1 << " " << log2 << "\n";
    double h = (log1  /   log2 );
    cout << "Height of test tree would be " << h << "\n";


    cout << "============================================= TEST STARTING\n";

    int inserts = 319;
    inserts = 10240;


    int B = 16;
    int M = 64;
    // Want size of 2. Size = M/B*4 -> M = 2*4*B
    int delta = 1;
    delta = 4;
    TruncatedBufferTree* tree = new TruncatedBufferTree(B,M,delta,inserts);


    KeyValue kv;
    for(int i = 1; i <= inserts; i++) {
        kv = KeyValue(i,i);
        tree->insert(kv);
    }

    cout << "============================================= DONE INSERTING\n";

    //tree->printTree();

    cout << "Number of nodes is " << tree->numberOfNodes << "\n";
    cout << "Root is " << tree->root << "\n";

    cout << "============================================= Printing\n";

    //tree->printTree();

    cout << "============================================= QUERIES\n";

    //cout << tree->query(3) << "\n";

    int ret = 0;
    for(int i = 1; i <= inserts; i++) {
        ret = tree->query(i);
        if(ret != i) {
            cout << "Error " << i << " " << ret << "\n";
        }
    }


    cout << "============================================= CLEANING UP\n";

    int height,nodeSize,bufferSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* ptr_bufferSize = &bufferSize;
    tree->readNodeInfo(tree->root,ptr_height,ptr_nodeSize,ptr_bufferSize);

    cout << "Height maxed out at " << height << "\n";

    //tree->printTree();

    tree->cleanUpTree();
}

void DevelopmentTester::testBinarySearchFracList() {

    // Write out temp file
    BufferedOutputStream* os = new BufferedOutputStream(4096);
    string testFile = "testFile";
    os->create(testFile.c_str());

    int element = 3;

    int elements = 11;
    int key, value, pointer1, pointer2;
    for(int i = 1; i <= elements; i++) {

        key = i;
        value = i+5;
        pointer1 = i+1;
        pointer2 = i+2;

        os->write(&key);
        os->write(&value);
        os->write(&pointer1);
        os->write(&pointer2);
    }

    os->close();
    delete(os);


    struct stat stat_buf;
    int rc = stat(testFile.c_str(), &stat_buf);
    long fileSize = stat_buf.st_size;
    // Convert to number of elements, each element consisting of 4 integers
    fileSize = fileSize / (sizeof(int)*4);

    cout << "Filesize is " << fileSize << "\n";

    for(int i = 1; i <= elements; i++) {

        element = i;

        int left = 0;
        int right = fileSize-1; // Will fit in int
        int middle;

        int* tempBuff = new int[4];

        while(left <= right) {

            middle = (left+right)/2;

            // Fetch element at position middle
            int filedesc = ::open(testFile.c_str(), O_RDONLY, 0666);
            ::pread(filedesc, tempBuff, 4*sizeof(int),middle*4*sizeof(int));
            ::close(filedesc);

            if(tempBuff[0] < element) {
                left = middle+1;
            }
            else if(tempBuff[0] > element) {
                right = middle-1;
            }
            else {
                break;
            }

        }

        cout << "Found " << tempBuff[0] << " " << tempBuff[1] << " " << tempBuff[2] << " " << tempBuff[3] << "\n";

        delete[] tempBuff;

    }
}

void DevelopmentTester::readDiskstatsTest() {

    // /proc/diskstats

    string diskstats = "/proc/diskstats";

    /*if(FILE *file = fopen(diskstats.c_str(), "r")) {
        fclose(file);
        cout << diskstats << " exists\n";
    }*/

    std::ifstream file(diskstats);
    std::string str;
    while (std::getline(file, str))
    {
        cout << str << "\n";

        /*1 - major number
        2 - minor mumber
        3 - device name
        4 - reads completed successfully
        5 - reads merged
        6 - sectors read
        7 - time spent reading (ms)
        8 - writes completed
        9 - writes merged
        10 - sectors written
        11 - time spent writing (ms)
        12 - I/Os currently in progress
        13 - time spent doing I/Os (ms)
        14 - weighted time spent doing I/Os (ms)*/

        istringstream iss(str);

        string s;
        iss >> s;
        if(s.compare("8") == 0) { // If major number = 8.
            iss >> s;
            iss >> s;
            cout << s << " "; // Device name
            iss >> s;
            iss >> s;
            iss >> s;
            cout << s << " "; // Sectors read
            iss >> s;
            iss >> s;
            iss >> s;
            iss >> s;
            cout << s << "\n"; // Sectors written
        }


        /* Works
        while(iss) {
            string subs;
            iss >> subs;
            cout << "Substring: " << subs << endl;
        }*/


        /*string type =str.substr(3,4);
        if(atoi(type.c_str()) == 8) {
            cout << type << "\n";
        }


        string::size_type pos;
        pos=str.find(' ',0);
        string second=str.substr(pos+1);
        str=str.substr(0,pos);*/

    }
    file.close();
}

void DevelopmentTester::buildModifiedBTreeTest() {

    int B = 131072;
    int M = 8388608; //1024
    int N = 50000000;

    ModifiedBuilder* builder = new ModifiedBuilder();
    int root = builder->build(N,B,M);
    ModifiedBtree* tree = new ModifiedBtree(B,M,root);
    delete(builder);
    cout << "================= Finished building!\n";
    cout << tree->externalNodeHeight << " " << tree->numberOfNodes << "\n";

    srand (time(NULL));

    long tempMod = 2*N;
    int modulus;
    if(tempMod > 2147483647) {
        modulus = 2147483647;
    }
    else {
        modulus = tempMod;
    }
    struct rusage r_usage;
    getrusage(RUSAGE_SELF,&r_usage);
    cout << "Memory before insertion " << r_usage.ru_maxrss << "\n";

    KeyValue* keyVal;
    for(int i = 0; i < 1000000; i++) {
        if(i%1000==0) {
            cout << i << "\n";
            struct rusage r_usage;
            getrusage(RUSAGE_SELF,&r_usage);
            cout << "Memory during insertion " << r_usage.ru_maxrss << "\n";
        }
        int ins = rand() % modulus + 1;
        keyVal = new KeyValue(ins,ins);
        tree->insert(keyVal);
        delete(keyVal);
    }

    cout << tree->root->height << "\n";
    /*tree->externalize();

    for(int i = 0; i < 10000; i++) {
        if(i%1000==0)cout << i << "\n";
        int ins = rand() % modulus + 1;
        tree->insert(new KeyValue(ins,ins));
    }*/



    cout << "================= Finished inserting!\n";

    for(int i = 0; i < 10000; i++) {
        int q =  rand() % modulus + 1;
        int ret = tree->query(q);
        if(q != ret) {
            //cout << q << " " << ret << "\n";
        }
    }
    cout << "================= Printing!\n";
    //tree->printTree(tree->root);
    tree->cleanup();
}

void DevelopmentTester::initialTestBufferedBTree() {

    int B = 32;
    int M = 128;
    int N = 100;
    float delta = 1.0;

    int insert = N;
    //int insert = 50;

    BufferedBTree* tree = new BufferedBTree(B,M,N,delta);

    for(int i = 1; i <= insert; i++) {
        cout << "Inserting " << i << "\n";
        tree->insert(KeyValueTime(i,i,i));
    }

    cout << "============================================= DONE INSERTING\n";

    for(int i = 1; i <= insert; i++) {
        int ret = tree->query(i);
        if(ret != i) {
            cout << i << " " << ret << "\n";
        }
    }

    cout << "============================================= PRINTING\n";

    tree->printTree(tree->root);

    cout << "============================================= CLEANING UP\n";

    tree->cleanup();
    delete(tree);

    cout << "Done!";
}

void DevelopmentTester::advancedTestBufferedBTree() {

    // Quick test
    /*int testN = 10;
    vector<KeyValueTime>* buffer = new vector<KeyValueTime>();
    for(int i = 0; i < testN; i++) {
        int r = rand() % testN + 1;
        buffer->push_back(KeyValueTime(r,r,i));
        cout << r << " " << r << " " << i << "\n";
    }

    sort(buffer->begin(),buffer->end());

    cout << "=============================================\n";

    for(int i = 0; i < testN; i++) {
        KeyValueTime kvt = buffer->at(i);
        cout << kvt.key << " " << kvt.value << " " << kvt.time << "\n";
    }

    cout << "=============================================\n";

    buffer->erase(buffer->begin()+2,buffer->begin()+2);

    for(int i = 0; i < buffer->size(); i++) {
        KeyValueTime kvt = buffer->at(i);
        cout << kvt.key << " " << kvt.value << " " << kvt.time << "\n";
    }

    cout << "=============================================\n";
     */

    int B = 64;
    int M = 256;
    int N = 2500;
    //float delta = 1.0;
    float delta = 100;

    int insert = N;
    //int insert = 50;

    //srand (time(NULL));

    BufferedBTree* tree = new BufferedBTree(B,M,N,delta);

    vector<int> store;

    for(int i = 1; i <= insert; i++) {
        int ins = rand() % N + 1;
        cout << "---Inserting " << ins << " " << i << "\n";

        store.push_back(ins);
        tree->insert(KeyValueTime(ins,ins,i));
    }

    cout << "============================================= DONE INSERTING\n";

    /*for(int i = 1; i <= insert; i++) {
        int ret = tree->query(i);
        if(ret != i) {
            cout << i << " " << ret << "\n";
        }
    }*/

    for(int i = 0; i < store.size(); i++) {
        int ret = tree->query(store.at(i));
        if(ret != store.at(i)) {
            cout << store.at(i) << " " << ret << "\n";
        }
    }

    cout << "============================================= PRINTING\n";

    //tree->printTree(tree->root);

    cout << "============================================= ELEMENTS\n";

    /*sort(store.begin(),store.end());

    for(int i = 0; i < store.size(); i++) {
        cout << store.at(i) << "\n";
    }*/

    cout << "============================================= CLEANING UP\n";

    tree->cleanup();
    delete(tree);

    cout << "Done!";
}

void DevelopmentTester::buildBufferedBTreeTest() {



    /*int B = 64;
    int M = 256;
    int N = 25600;*/

    int B = 131000;
    int M = 8000000;
    int N = 50000000;
    //float delta = 1.0;
    float delta = 100;
    //int insert = N;
    int insert = 20000000;

    struct rusage r_usage;
    getrusage(RUSAGE_SELF,&r_usage);
    cout << "Memory before build " << r_usage.ru_maxrss << "\n";

    /*BufferedBTree* tree2 = new BufferedBTree(B,M,N,delta);
    for(int i = 1; i <= insert; i++) {
        int ins = rand() % N + 1;
        //cout << "---Inserting " << ins << " " << i << "\n";

        //store.push_back(ins);
        tree2->insert(KeyValueTime(ins,ins,i));
    }

    getrusage(RUSAGE_SELF,&r_usage);
    cout << "Memory after build " << r_usage.ru_maxrss << "\n";


    return;*/



    cout << "============================================= BUILDING\n";
    BufferedBTreeBuilder builder;
    int newRoot = builder.build(N,B,M,delta);

    BufferedBTree* tree = new BufferedBTree(B,M,N,delta,newRoot);

    getrusage(RUSAGE_SELF,&r_usage);
    cout << "Memory after build " << r_usage.ru_maxrss << "\n";

    cout << tree->maxInternalNodes << " " << tree->internalNodeCounter << "\n";

    cout << "============================================= INSERTING\n";

    //vector<int> store;

    for(int i = 1; i <= insert; i++) {
        int ins = rand() % N + 1;
        //cout << "---Inserting " << ins << " " << i << "\n";

        //store.push_back(ins);
        tree->insert(KeyValueTime(ins,ins,i));
    }

    getrusage(RUSAGE_SELF,&r_usage);
    cout << "Memory after insertions " << r_usage.ru_maxrss << "\n";

    cout << tree->maxInternalNodes << " " << tree->internalNodeCounter << "\n";

    cout << "Size of internal nodes = " << tree->calculateSize(tree->root) << "\n";

    cout << "============================================= DONE INSERTING\n";

    /*for(int i = 0; i < store.size(); i++) {
        int ret = tree->query(store.at(i));
        if(ret != store.at(i)) {
            cout << store.at(i) << " " << ret << "\n";
        }
    }*/

    cout << "============================================= SPECIAL\n";

    /*for(int i = 0; i < store.size(); i++) {
        int ret = tree->specialQuery(store.at(i));
        if(ret != store.at(i)) {
            cout << store.at(i) << " " << ret << "\n";
        }
    }*/

    cout << "============================================= CLEANING UP\n";

    tree->cleanup();
    delete(tree);

    cout << "Done!\n";


}

/*
 * Test the basic functionality of an xDict / xBox.
 */
void DevelopmentTester::xDictBasicTest() {

    long x = 64;

    // Create the most basic of xDicts, a single xBox with minSize
    float alpha = 1;

    cout << "============================================= BUILDING\n";

    XDict* xDict = new XDict(alpha);

    // We have a subbox of size 64, create one of size 64^2 = 4096
    /*x = 4096;

    long firstXboxSize = xDict->recursivelyCalculateSize(64);
    long secondXBoxSize = xDict->recursivelyCalculateSize(4096);
    long newSize = firstXboxSize + secondXBoxSize;

    cout << "First xBox had size " << firstXboxSize << " second xBox has size " << secondXBoxSize
         << " totalSize is " << newSize << "\n";*/

    /*xDict->extendMapTo(newSize);
    xDict->map[newSize] = 1;

    cout << "Laying out new xBox\n";

    xDict->layoutXBox(firstXboxSize,4096);*/

    xDict->addXBox();

    cout << "Latest x is " << xDict->latestX << " fileSize is " << xDict->fileSize << "\n";

    cout << "============================================= FLUSH TEST\n";
    /*
     * Flush test, place some numbers in the second xBox and flush it.
     */

    vector<long> insertions;

    long pointerToXBox = xDict->xBoxes->at(1);

    /*xDict->addXBox();
    pointerToXBox = xDict->xBoxes->at(2);*/

    long pointerToInput = pointerToXBox+xDict->infoOffset;
    int number = 0;
    long lastNumber = 0;
    int toInsert = 10;
    toInsert = 64;
    long bufferModifier = 8;

    for(int i = 0; i < toInsert; i++) {
        // Insert increasing numbers
        long index = pointerToInput+i*4;
        long ins = rand() % 100 + 1 + lastNumber;
        xDict->map[index] = ins;
        xDict->map[index+1] = ins;
        lastNumber = ins;
        //cout << ins << " ";
        insertions.push_back(ins);
    }
    xDict->map[pointerToInput+4*toInsert] = -1;
    cout << "\n";

    long pointerToMiddle = xDict->map[pointerToXBox+7];
    lastNumber = 0;
    for(int i = 0; i < toInsert*bufferModifier; i++) {
        // Insert increasing numbers
        long index = pointerToMiddle+i*4;
        long ins = rand() % 100 + 1 + lastNumber;
        xDict->map[index] = ins;
        xDict->map[index+1] = ins;
        lastNumber = ins;
        //cout << ins << " ";
        insertions.push_back(ins);
    }
    xDict->map[pointerToMiddle+4*toInsert*bufferModifier] = -1;
    cout << "\n";

    long pointerToOutput = xDict->map[pointerToXBox+9];
    lastNumber = 0;
    for(int i = 0; i < toInsert*bufferModifier; i++) {
        // Insert increasing numbers
        long index = pointerToOutput+i*4;
        long ins = rand() % 100 + 1 + lastNumber;
        xDict->map[index] = ins;
        xDict->map[index+1] = ins;
        lastNumber = ins;
        //cout << ins << " ";
        insertions.push_back(ins);
    }
    xDict->map[pointerToOutput+4*toInsert*bufferModifier] = -1;
    cout << "\n";

    // Create and insert into two upper level subboxes
    long sizeOfSubbox = xDict->map[pointerToXBox+3];
    long numberOfUpperLevel = xDict->map[pointerToXBox+4];
    long pointerToUpperBool = xDict->map[pointerToXBox+6];
    long placementOfFirst = pointerToUpperBool + numberOfUpperLevel;
    long placementOfSecond = placementOfFirst+sizeOfSubbox;

    // Boolean update

    xDict->map[pointerToUpperBool] = placementOfFirst;
    xDict->map[pointerToUpperBool+1] = placementOfSecond;

    // Layout
    xDict->layoutXBox(placementOfFirst,xDict->minX);
    xDict->layoutXBox(placementOfSecond,xDict->minX);

    // Insert into first
    long pointerToFirstArray = placementOfFirst+2;
    //pointerToFirstArray = xDict->map[placementOfFirst+9];
    lastNumber = 0;
    for(int i = 0; i < toInsert; i++) {
        // Insert increasing numbers
        long index = pointerToFirstArray+i*4;
        long ins = rand() % 100 + 1 + lastNumber;
        xDict->map[index] = ins;
        xDict->map[index+1] = ins;
        lastNumber = ins;
        //cout << ins << " ";
        insertions.push_back(ins);
    }
    xDict->map[pointerToFirstArray+4*toInsert] = -1;
    xDict->map[placementOfFirst+1] = toInsert;
    cout << "\n";

    long pointerToSecondArray = placementOfSecond+2;
    //pointerToSecondArray = xDict->map[placementOfSecond+9];
    for(int i = 0; i < toInsert; i++) {
        // Insert increasing numbers
        long index = pointerToSecondArray+i*4;
        long ins = rand() % 100 + 1 + lastNumber;
        xDict->map[index] = ins;
        xDict->map[index+1] = ins;
        lastNumber = ins;
        //cout << ins << " ";
        insertions.push_back(ins);
    }
    xDict->map[pointerToSecondArray+4*toInsert] = -1;
    xDict->map[placementOfSecond+1] = toInsert;

    cout << "\n";

    cout << "Placed subboxes in bool = " << pointerToUpperBool << " boxes = " << placementOfFirst << " and " << placementOfSecond << "\n";

    xDict->map[pointerToXBox+1] = 3*toInsert+2*toInsert*bufferModifier; // TODO Number of real elements.

    xDict->flush(pointerToXBox,false);

    sort(insertions.begin(),insertions.end());

    /*cout << "Output:\n";
    for(int i = 0; i < insertions.size(); i++) {
        cout << insertions.at(i) << " ";
    }
    cout << "\n";*/

    bool correct = true;

    long key = 0, value = 0;
    long readIndex = 0;
    while(key != -1) {
        key = xDict->map[pointerToOutput+readIndex*4];
        if(key != -1 && key != insertions.at(readIndex)) {
            correct = false;
        }
        //cout << key << " ";
        readIndex++;

        // Safeguard
        if(readIndex > 10*toInsert*bufferModifier) {
            break;
        }
    }
    cout << "\n";

    if(correct) {
        cout << "Flush works correct\n";
    }
    else {
        cout << "Error in Flush\n";
    }

    cout << "Real elements in xBox is now " << xDict->map[pointerToXBox+1] << "\n";

    cout << "============================================= SAMPLE UP AFTER FLUSH\n";

    //cout << pointerToXBox << "\n";

    xDict->sampleUpAfterFlush(pointerToXBox);

    //cout << pointerToXBox << "\n";

    cout << "============================================= SEARCH TEST\n";

    // Insert into the minimum xBox we didnt use above
    for(int i = 0; i < 32; i++) {
        xDict->map[2+i*4] = i+1;
        xDict->map[2+i*4+1] = i+1;
    }

    /* Works
    for(int i = 0; i < 32; i++) {
        long ret = xDict->search(0,2,i+1);
        if(ret != i+1) {
            cout << "Error\n";
        }
    }*/

    cout << pointerToXBox << "\n";

    // Now search the normal xBox we created for 37
    long forwardPointer = pointerToXBox + xDict->infoOffset;
    cout << "Forward pointer points to " << xDict->map[forwardPointer] << " " << xDict->map[forwardPointer+1] << "\n";
    long ret = xDict->search(pointerToXBox,forwardPointer,37);


    cout << "============================================= CLEANING UP\n";

    delete(xDict);

    cout << "Done!\n";
}
