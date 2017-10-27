#include <iostream>
#include "Streams/OutputStream.h"
#include "Streams/InputStream.h"
#include "Streams/BufferedOutputStream.h"
#include "Streams/NaiveInputStream.h"
#include "Streams/SystemInputStream.h"
#include "Streams/BufferedInputStream.h"
#include "Streams/MapInputStream.h"
#include "Streams/NaiveOutputStream.h"
#include "Streams/SystemOutputStream.h"
#include "Streams/MapOutputStream.h"
#include "Btree/Btree.h"
#include "Btree/ModifiedInternalNode.h"
#include "Btree/ModifiedBtree.h"

#include <ctime>
#include <ratio>
#include <chrono>
#include <sstream>

using namespace std;

int streamtestread(long n, int increment) {

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

int streamtestread2(long n, int increment) {

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

int streamtestwrite(long n, int increment) {

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

int streamtestwrite2(long n, int increment) {

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

void BtreeInsertTest() {

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

void simpleInsertAndQueryTest() {

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

void insertAndQueryTest() {

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

void insertDeleteAndQueryTest() {

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
void writeToFileTest() {

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

void manipulateArray(int* array) {
    array[1] = 2;
}

void manipulateArrayTest() {

    int* array = new int[2];
    array[0] = 1;
    array[1] = 1;

    cout << array[0] << array[1] << "\n";

    manipulateArray(array);

    cout << array[0] << array[1] << "\n";
}

void pointerSize() {
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

void recursivePrintModifiedNode(ModifiedInternalNode* node) {
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

void internalModifiedBtreeInsertAndQueryTest() {

    int number = 10;

    ModifiedBtree* btree = new ModifiedBtree(24,10000000);
    KeyValue* keyVal = new KeyValue(1,1);
    for(int i = 1; i <= number; i++) {
        keyVal->key = i;
        keyVal->value = i;
        cout << "Inserting " << keyVal->key << "\n";
        btree->insert(keyVal);
    }

    // Write out internal tree
    //recursivePrintModifiedNode(btree->root);

    cout << "===Queries\n";
    for(int i = 1; i <= number; i++) {
        int ret = btree->query(i);
        if(ret != i) {
            cout << ret << "\n";
        }
    }


}






int main(int argc, char* argv[]) {

    if(argc == 1) {
        cout << "Give arguments, try again \n";

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
        internalModifiedBtreeInsertAndQueryTest();
        return 0;
    }
    int test = atoi(argv[1]);
    if(test == 1) {
        streamtestread(atol(argv[2]),atoi(argv[3]));
    }
    else if(test == 2) {
        streamtestwrite(atol(argv[2]),atoi(argv[3]));
    }
    else if(test == 3) {
        streamtestread2(atol(argv[2]),atoi(argv[3]));
    }
    else if(test == 4) {
        streamtestwrite2(atol(argv[2]),atoi(argv[3]));
    }


    return 0;
}