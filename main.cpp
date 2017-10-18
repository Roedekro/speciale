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
        insertAndQueryTest();
        return 0;
    }
    int test = atoi(argv[1]);
    if(test == 1) {
        streamtestread(atol(argv[2]),atoi(argv[3]));
    }
    else if(test == 2) {
        streamtestwrite(atol(argv[2]),atoi(argv[3]));
    }


    return 0;
}