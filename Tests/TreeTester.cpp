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
#include <fstream>
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

    int numberOfQueries = 10000;

    int numberOfDeltas = 4;
    long* insertTime = new long[numberOfDeltas]();
    long* insertIO = new long[numberOfDeltas]();
    long* queryTime = new long[numberOfDeltas]();
    long* queryIO = new long[numberOfDeltas]();
    int* heights = new int[numberOfDeltas];

    for(int i = 1; i <= numberOfDeltas; i++) {

        //cout << "Delta = " << i << "\n";

        int delta = i;

        long diskReads1 = 0;
        long diskReads2 = 0;
        long diskReads3 = 0;
        long diskWrites1 = 0;
        long diskWrites2 = 0;
        long diskWrites3 = 0;
        long temp = 0;

        for(int r = 0; r < runs; r++) {

            //cout << "Run " << r+1 << "\n";

            srand (time(NULL));

            using namespace std::chrono;

            // GET DISK STATS
            sleep(10);
            string diskstats = "/proc/diskstats";

            //cout << "Diskread1 ";
            diskReads1 = 0;
            diskWrites1 = 0;
            std::string str;
            std::ifstream file1(diskstats);
            while (std::getline(file1, str))
            {
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
                    iss >> s; // Device name
                    iss >> s;
                    iss >> s;
                    iss >> s; // Sectors read
                    //cout << s << " ";
                    diskReads1 = diskReads1 + stol(s);
                    iss >> s;
                    iss >> s;
                    iss >> s;
                    iss >> s; // Sectors written
                    diskWrites1 = diskWrites1 + stol(s);
                }
            }
            file1.close();
            //cout << "\n";

            TruncatedBufferTree* tree = new TruncatedBufferTree(B,M,delta,N);

            //cout << "Inserting\n";

            // Insert N elements
            int modulus = 2*N;
            int number;
            high_resolution_clock::time_point t1 = high_resolution_clock::now();
            for(int j = 1; j <= N; j++) {
                number = rand() % modulus +1;
                tree->insert(KeyValue(number,number));
            }
            high_resolution_clock::time_point t2 = high_resolution_clock::now();
            insertTime[i-1] = insertTime[i-1] + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

            // Write out diskstats again
            sleep(10);
            //cout << "Diskread2 ";
            diskReads2 = 0;
            diskWrites2 = 0;
            std::ifstream file2(diskstats);
            while (std::getline(file2, str))
            {

                istringstream iss(str);
                string s;
                iss >> s;
                if(s.compare("8") == 0) { // If major number = 8.
                    iss >> s;
                    iss >> s; // Device name
                    iss >> s;
                    iss >> s;
                    iss >> s; // Sectors read
                    //cout << s << " ";
                    diskReads2 = diskReads2 + stol(s);
                    iss >> s;
                    iss >> s;
                    iss >> s;
                    iss >> s; // Sectors written
                    diskWrites2 = diskWrites2 + stol(s);
                }
            }
            file2.close();
            //cout << "\n";

            //cout << "Insert diskreads = " << (diskReads2-diskReads1) << "\n";
            //cout << diskReads2 << " " << diskReads1 << "\n";
            //cout << "Insert diskwrites = " << (diskWrites2 - diskWrites1) << "\n";
            //cout << diskWrites2 << " " << diskWrites1 << "\n";

            temp = (diskReads2-diskReads1) + (diskWrites2 - diskWrites1);
            insertIO[i-1] = insertIO[i-1] + temp;

            //cout << "Query\n";
            // Query N/100 elements
            t1 = high_resolution_clock::now();
            for(int j = 1; j <= numberOfQueries; j++) {
                number = rand() % modulus +1;
                tree->query(number);
            }
            t2 = high_resolution_clock::now();
            queryTime[i-1] = queryTime[i-1] + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

            //cout << "Query Done\n";

            // Write out diskstats a final time
            sleep(10);
            //cout << "Diskread3 ";
            diskReads3 = 0;
            diskWrites3 = 0;
            std::ifstream file3(diskstats);
            while (std::getline(file3, str))
            {

                istringstream iss(str);
                string s;
                iss >> s;
                if(s.compare("8") == 0) { // If major number = 8.
                    iss >> s;
                    iss >> s; // Device name
                    iss >> s;
                    iss >> s;
                    iss >> s; // Sectors read
                    //cout << s << " ";
                    diskReads3 = diskReads3 + stol(s);
                    iss >> s;
                    iss >> s;
                    iss >> s;
                    iss >> s; // Sectors written
                    diskWrites3 =  diskWrites3 + stol(s);
                }
            }
            file3.close();
            //cout << "\n";

            //cout << "Query diskreads = " << (diskReads3-diskReads2) << "\n";
            //cout << diskReads3 << " " << diskReads2 << "\n";
            //cout << "Query diskwrites = " << (diskWrites3 - diskWrites2) << "\n";
            //cout << diskWrites3 << " " << diskWrites2 << "\n";

            temp = (diskReads3-diskReads2) + (diskWrites3 - diskWrites2);
            queryIO[i-1] = queryIO[i-1] + temp;

            //cout << "Cleaning up\n";
            // Get tree height
            int height,nodeSize,bufferSize;
            int* ptr_height = &height;
            int* ptr_nodeSize = &nodeSize;
            int* ptr_bufferSize = &bufferSize;
            tree->readNodeInfo(tree->root,ptr_height,ptr_nodeSize,ptr_bufferSize);
            if(height > heights[i-1]) {
                heights[i-1] = height;
            }

            tree->cleanUpTree();
            delete(tree);

            //cout << "Done!\n";
        }

        cout << "Temp data Delta=" << i << " " << insertTime[i-1] << " " << insertIO[i-1] << " " << queryTime[i-1] << " " << queryIO[i-1] << " " << heights[i-1] << "\n";
    }

    // Divide by runs
    for(int j = 0; j < numberOfDeltas; j++) {
        insertTime[j] = insertTime[j]/runs;
        insertIO[j] = insertIO[j]/runs;
        queryTime[j] = queryTime[j]/runs;
        queryIO[j] = queryIO[j]/runs;
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