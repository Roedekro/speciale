//
// Created by martin on 11/21/17.
//

#include "TreeTester.h"
#include "../TruncatedBufferTree/TruncatedBufferTree.h"
#include <ratio>
#include <chrono>
#include <iostream>
#include <ctime>
#include <stdlib.h>
#include <time.h>
#include <vector>
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
//#include <papi.h>

using namespace std;

TreeTester::TreeTester() {

}

TreeTester::~TreeTester() {

}

void TreeTester::truncatedBufferTree(int B, int M, int delta, int N, int runs) {

    // Insert, query, iocounter, papi io
    int outputlength = 4;
    long* output = new long[outputlength];

    for(int i = 0; i < runs; i++) {

        TruncatedBufferTree* tree = new TruncatedBufferTree(B,M,delta,N);





        tree->cleanUpTree();
        delete(tree);
    }


    for(int i = 0; i < outputlength; i++) {
        output[i] = output[i]/runs;
        cout << output[i] << " ";
    }
    cout << "\n";

    delete[] output;
}

void TreeTester::truncatedDeltaTest(int B, int M, int N, int runs) {

    int numberOfDeltas = 4;
    long* insertTime = new long[numberOfDeltas];
    long* insertIO = new long[numberOfDeltas];
    long* queryTime = new long[numberOfDeltas];
    long* queryIO = new long[numberOfDeltas];
    int* heights = new int[numberOfDeltas];

    for(int i = 1; i <= numberOfDeltas; i++) {

        int delta = i;

        for(int r = 0; r < runs; r++) {

            srand (time(NULL));

            using namespace std::chrono;

            TruncatedBufferTree* tree = new TruncatedBufferTree(B,M,delta,N);

            high_resolution_clock::time_point t1 = high_resolution_clock::now();
            int modulus = 2*N;
            int number;
            for(int i = 1; i <= N; i++) {
                number = rand() % modulus +1;
                tree->insert(new KeyValue(number,number));
            }
            high_resolution_clock::time_point t2 = high_resolution_clock::now();
            insertTime[i-1] = insertTime[i-1] + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

            t1 = high_resolution_clock::now();
            for(int i = 1; i <= N/100; i++) {
                number = rand() % modulus +1;
                tree->query(number);
            }
            t2 = high_resolution_clock::now();
            queryTime[i-1] = queryTime[i-1] + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();


            int height,nodeSize,bufferSize;
            int* ptr_height = &height;
            int* ptr_nodeSize = &nodeSize;
            int* ptr_bufferSize = &bufferSize;
            tree->readNodeInfo(tree->root,ptr_height,ptr_nodeSize,ptr_bufferSize);
            heights[i-1] = height;

            tree->cleanUpTree();
            delete(tree);
        }

        for(int j = 0; j < numberOfDeltas; j++) {
            insertTime[j] = insertTime[j]/runs;
            insertIO[j] = insertIO[j]/runs;
            queryTime[j] = queryTime[j]/runs;
            queryIO[j] = queryIO[j]/runs;
        }

    }

    // Write out results
    for(int j = 0; j < numberOfDeltas; j++) {
        cout << "Delta=" << j+1 << " " << insertTime[j] << " " << insertIO[j] << " " << queryTime[j] << " " << queryIO[j] << " " << heights[j] << "\n";
    }

    delete[] insertTime;
    delete[] insertIO;
    delete[] queryTime;
    delete[] queryIO;
    delete[] heights;
}