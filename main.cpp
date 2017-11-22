


#include <cstdlib>
#include <iostream>
#include "Tests/DevelopmentTester.h"
#include "Tests/TreeTester.h"

using namespace std;

void test() {

    /*DevelopmentTester* dev = new DevelopmentTester();
    dev->test();
    delete(dev);*/

    TreeTester* test = new TreeTester();
    // B = 128KB, M = 8MB, N = 100mil, runs = 2
    //test->truncatedDeltaTest(131072,8388608,10000000,2);
    test->truncatedDeltaTest(1024,8196,1000000,2);
    delete(test);
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
        // Truncated delta test
        TreeTester* test = new TreeTester();
        // B, M, N, runs
        test->truncatedDeltaTest(atoi(argv[2]),atoi(argv[3]),atoi(argv[4]),atoi(argv[5]));
        delete(test);
    }


    return 0;
}