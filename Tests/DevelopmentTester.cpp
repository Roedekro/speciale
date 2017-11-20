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
    testTruncatedInsertQueryDelete();
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

    int number = 100;
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
    cout << "Root is " << btree->root->id << "\n";
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

    cout << "Number of nodes " << btree->currentNumberOfNodes << "\n";
    cout << btree->numberOfNodes << " " << btree->internalNodeCounter;
    btree->cleanup();

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
    inserts = 512;


    int B = 16;
    int M = 64;
    // Want size of 2. Size = M/B*4 -> M = 2*4*B
    int delta = 1;
    delta = 4;
    TruncatedBufferTree* tree = new TruncatedBufferTree(B,M,delta,inserts);


    KeyValue* kv;
    for(int i = 1; i <= inserts; i++) {
        kv = new KeyValue(i,i);
        tree->insert(kv);
    }

    cout << "============================================= DONE INSERTING\n";

    //tree->printTree();

    cout << "Number of nodes is " << tree->numberOfNodes << "\n";
    cout << "Root is " << tree->root << "\n";


    cout << "============================================= QUERIES\n";




    cout << "============================================= CLEANING UP\n";

    tree->cleanUpTree();
}
