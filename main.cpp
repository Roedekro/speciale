


#include <cstdlib>
#include <iostream>
#include "Tests/DevelopmentTester.h"

using namespace std;

void test() {

    DevelopmentTester* dev = new DevelopmentTester();
    dev->test();
    delete(dev);
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


    return 0;
}