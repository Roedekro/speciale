


#include <cstdlib>
#include <iostream>
#include <vector>
#include "Tests/DevelopmentTester.h"
#include "Tests/TreeTester.h"
#include "Btree/KeyValue.h"

using namespace std;

void test() {

    /*TreeTester* treeTester = new TreeTester();
    treeTester->modifiedBTreeTest(32,256,10000,2);*/

    DevelopmentTester* dev = new DevelopmentTester();
    dev->test();
    delete(dev);

    /*TreeTester* test = new TreeTester();
    // B = 128KB, M = 8MB, N = 100mil, runs = 2
    //test->truncatedDeltaTest(131072,8388608,10000000,2);
    test->bufferedBTreeDeltaTest(131072,8388608,10000000,2);
    delete(test);*/


    /*
     * Quick experiment
     */
    /*std::vector<int> vector;
    vector.push_back(1);
    vector.push_back(2);
    vector.push_back(3);
    vector.push_back(4);
    vector.push_back(5);

    //vector.erase(vector.begin() + 2, vector.begin() + 4);
    vector.erase(vector.begin() + 2, vector.end());

    for(int i = 0; i < vector.size(); i++) {
        cout << vector.at(i);
    }*/

    // Modified BTree test - Insert and Query
    /*TreeTester* test = new TreeTester();
    // int B, int M, int N, int runs
    test->modifiedBTreeTest(131072,8388608,50000000,10);
    delete(test);*/

    /*TreeTester* test = new TreeTester();
    // B = 128KB, M = 8MB, N = 100mil, runs = 2
    //test->truncatedDeltaTest(131072,8388608,10000000,2);
    test->truncatedDeltaTest(1024,8196,1000000,2);
    delete(test);*/

    /*TreeTester* test = new TreeTester();
    // B = 128KB, M = 8MB, N = 100mil, runs = 2
    //test->truncatedDeltaTest(131072,8388608,10000000,2);
    test->truncatedDeltaTest(1024,8196,100000,2);
    delete(test);*/


    // Actual delta test
    /*TreeTester* test = new TreeTester();
    // B = 128KB, M = 8MB, N = 800mil, runs = 10
    // 10k queries.
    test->truncatedDeltaTest(131072,8388608,800000000,10);
    delete(test);*/

    /*cout << sizeof(KeyValue) << "\n";
    KeyValue* ptr;
    cout << sizeof(ptr) << "\n";

    KeyValue** array = new KeyValue*[4];
    array[0] = new KeyValue(1,1);
    array[1] = new KeyValue(1,1);
    array[2] = new KeyValue(1,1);
    array[3] = new KeyValue(1,1);

    cout << sizeof(array) << "\n";
    cout << sizeof(*array) << "\n";

    cout << "---\n";
    for(int i = 0; i < 4; i++) {
        int k = sizeof(array[i]) + sizeof(*(array[i]));
        cout << k << "\n";
    }*/

    //cout << RAND_MAX;


}


int main(int argc, char* argv[]) {

    if(argc == 1) {
        cout << "Give arguments, try again \n";

        test();

        return 0;
    }
    int test = atoi(argv[1]);
    if(test == 1) {
        //streamtestread(atol(argv[2]),atoi(argv[3]));
    }
    else if(test == 2) {
        //streamtestwrite(atol(argv[2]),atoi(argv[3]));
    }
    else if(test == 3) {
        //streamtestread2(atol(argv[2]),atoi(argv[3]));
    }
    else if(test == 4) {
        //streamtestwrite2(atol(argv[2]),atoi(argv[3]));
    }
    else if(test == 5) {
        // Truncated delta test - Insert and Query
        TreeTester* test = new TreeTester();
        // int B, int M, int delta, int N, int runs
        test->truncatedBufferTree(131072,8388608,4,atoi(argv[2]),10);
        delete(test);
    }
    else if(test == 6) {
        // Modified BTree test - Insert and Query
        TreeTester* test = new TreeTester();
        // int B, int M, int N, int runs
        test->modifiedBTreeTest(131072,8388608,atoi(argv[2]),10);
        delete(test);
    }


    return 0;
}