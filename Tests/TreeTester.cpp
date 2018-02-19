//
// Created by martin on 11/21/17.
//

#include "TreeTester.h"
#include "../TruncatedBufferTree/TruncatedBufferTree.h"
#include "../Btree/ModifiedBtree.h"
#include "../Btree/ModifiedBuilder.h"
#include "../BufferedBTree/BufferedBTree.h"
#include "../BufferedBTree/BufferedBTreeBuilder.h"
#include "../xDict/XDict.h"
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
#include <random>
#include <sys/resource.h>

using namespace std;

TreeTester::TreeTester() {

}

TreeTester::~TreeTester() {

}

void TreeTester::truncatedBufferTree(int B, int M, int delta, int N, int runs) {

    int numberOfQueries = 10000;

    // Insert, query, iocounter, papi io
    long insertTime = 0;
    long insertIO = 0;
    long queryTime = 0;
    long queryIO = 0;

    long diskReads1 = 0;
    long diskReads2 = 0;
    long diskReads3 = 0;
    long diskWrites1 = 0;
    long diskWrites2 = 0;
    long diskWrites3 = 0;
    long temp = 0;

    for(int i = 0; i < runs; i++) {

        srand (time(NULL));

        using namespace std::chrono;

        // GET DISK STATS
        sleep(10);
        string diskstats = "/proc/diskstats";

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

        TruncatedBufferTree* tree = new TruncatedBufferTree(B,M,delta,N);

        // Insert N elements
        long tempMod = 2*N;
        int modulus;
        if(tempMod > 2147483647) {
            modulus = 2147483647;
        }
        else {
            modulus = tempMod;
        }

        int number;
        high_resolution_clock::time_point t1 = high_resolution_clock::now();
        for(int j = 1; j <= N; j++) {
            number = rand() % modulus +1;
            tree->insert(KeyValue(number,number));
        }
        high_resolution_clock::time_point t2 = high_resolution_clock::now();
        insertTime = insertTime + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        // Write out diskstats again
        sleep(10);
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

        temp = (diskReads2-diskReads1) + (diskWrites2 - diskWrites1);
        insertIO = insertIO + temp;

        t1 = high_resolution_clock::now();
        for(int j = 1; j <= numberOfQueries; j++) {
            number = rand() % modulus +1;
            tree->query(number);
        }
        t2 = high_resolution_clock::now();
        queryTime = queryTime + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();


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

        temp = (diskReads3-diskReads2) + (diskWrites3 - diskWrites2);
        queryIO = queryIO + temp;

        // Write out temp result
        cout << "run=" << i << " " << insertTime << " " << insertIO << " " << queryTime << " " << queryIO << "\n";

        tree->cleanUpTree();
        delete(tree);
    }

    // Divide by runs
    insertTime = insertTime/runs;
    insertIO = insertIO/runs;
    queryTime = queryTime/runs;
    queryIO = queryIO/runs;

    // Write out results

    cout << insertTime << " " << insertIO << " " << queryTime << " " << queryIO << "\n";


}

void TreeTester::truncatedDeltaTest(int B, int M, int N, int runs) {

    int numberOfQueries = 10000;

    int numberOfDeltas = 4;
    long* insertTime = new long[numberOfDeltas]();
    long* insertIO = new long[numberOfDeltas]();
    long* queryTime = new long[numberOfDeltas]();
    long* queryIO = new long[numberOfDeltas]();
    int* heights = new int[numberOfDeltas]();

    for(int i = 2; i <= numberOfDeltas; i++) {

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

void TreeTester::modifiedBTreeTest(int B, int M, int N, int runs) {

    int numberOfQueries = 10000;
    int extraInserts = 1000000;

    // Insert, query, iocounter
    long insertTime = 0;
    long insertIO = 0;
    long queryTime = 0;
    long queryIO = 0;

    long diskReads1 = 0;
    long diskReads2 = 0;
    long diskReads3 = 0;
    long diskWrites1 = 0;
    long diskWrites2 = 0;
    long diskWrites3 = 0;
    long temp = 0;


    for(int i = 0; i < runs; i++) {

        srand (time(NULL));

        using namespace std::chrono;

        // GET DISK STATS
        sleep(10);
        string diskstats = "/proc/diskstats";

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

        long tempMod = 2*N;
        int modulus;
        if(tempMod > 2147483647) {
            modulus = 2147483647;
        }
        else {
            modulus = tempMod;
        }

        //ModifiedBtree* tree = new ModifiedBtree(B,M);

        // Build REALISTIC tree in O(sort), much faster than O(N log_B( N/B)).
        cout << "Building\n";
        ModifiedBuilder* builder = new ModifiedBuilder();
        int root = builder->build(N,B,M);
        ModifiedBtree* tree = new ModifiedBtree(B,M,root);
        delete(builder);
        //cout << "================= Finished building!\n";

        int number;
        KeyValue* keyVal;
        // Insert 1mil elements to make the constructed tree realistic
        // REMOVED: Now building realistic tree from the start!
        /*for(int j = 1; j <= 1000000; j++) {
            number = rand() % modulus +1;
            keyVal = new KeyValue(number,number);
            tree->insert(keyVal);
            delete(keyVal);
        }*/

        cout << "Inserting\n";
        // Now insert 1mil elements that we time
        high_resolution_clock::time_point t1 = high_resolution_clock::now();
        high_resolution_clock::time_point ttemp1 = high_resolution_clock::now();
        high_resolution_clock::time_point ttemp2 = high_resolution_clock::now();
        for(int j = 0; j < extraInserts; j++) {
            if(j%10000==0) {
                ttemp2 = high_resolution_clock::now();
                long temp_time = chrono::duration_cast<chrono::milliseconds>(ttemp2 - ttemp1).count();
                // Force result to be written in real time
                cout << j << " " << temp_time << "\n"; // << std::flush;
                ttemp1 = high_resolution_clock::now();
                /*cout << j << "\n";
                struct rusage r_usage;
                getrusage(RUSAGE_SELF,&r_usage);
                cout << "Memory during insertion " << r_usage.ru_maxrss << "\n";*/
            }
            number = rand() % modulus +1;
            keyVal = new KeyValue(number,number);
            tree->insert(keyVal);
            delete(keyVal);
        }
        high_resolution_clock::time_point t2 = high_resolution_clock::now();
        insertTime = insertTime + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        // Write out diskstats again
        sleep(10);
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

        temp = (diskReads2-diskReads1) + (diskWrites2 - diskWrites1);
        insertIO = insertIO + temp;


        cout << "Query\n";
        t1 = high_resolution_clock::now();
        for(int j = 1; j <= numberOfQueries; j++) {
            number = rand() % modulus +1;
            tree->query(number);
        }
        t2 = high_resolution_clock::now();
        queryTime = queryTime + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();


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

        temp = (diskReads3-diskReads2) + (diskWrites3 - diskWrites2);
        queryIO = queryIO + temp;

        // Write out temp result, force flush
        cout << "run=" << i << " " << insertTime << " " << insertIO << " " << queryTime << " " << queryIO << "\n" << std::flush;

        tree->cleanup();
        delete(tree);
    }

    // Divide by runs
    insertTime = insertTime/runs;
    insertIO = insertIO/runs;
    queryTime = queryTime/runs;
    queryIO = queryIO/runs;

    // Write out results

    cout << insertTime << " " << insertIO << " " << queryTime << " " << queryIO << "\n";




}

/*
 * Test various (fixed) values of Delta, and measure the impact on insertion and query.
 */
void TreeTester::bufferedBTreeDeltaTest(int B, int M, int N, int runs) {

    int numberOfQueries = 10000;
    int insertionsDuringBuild = 1000000; // 1mil to fill internal buffers
    int insertions = 1000000; // 1mil to measure

    int min = log2(N);
    int max = B * log2(N) + 1;
    int interval = (max - min) / 4; // 5 values linearly distributed between min and max

    // TODO: Change deltaRuns to 0 again. Its 4 because a test got killed at 4 (unrelated to program).
    for (int deltaRuns = 4; deltaRuns < 5; deltaRuns++) {

        // Insert, query, iocounter
        long insertTime = 0;
        long insertIO = 0;
        long queryTime = 0;
        long queryIO = 0;

        long diskReads1 = 0;
        long diskReads2 = 0;
        long diskReads3 = 0;
        long diskWrites1 = 0;
        long diskWrites2 = 0;
        long diskWrites3 = 0;
        long temp = 0;

        int delta = min + (interval * deltaRuns);

        for (int r = 0; r < runs; r++) {

            srand(time(NULL));

            long tempMod = 2 * N;
            int modulus;
            if (tempMod > 2147483647) {
                modulus = 2147483647;
            } else {
                modulus = tempMod;
            }

            struct rusage r_usage;
            getrusage(RUSAGE_SELF,&r_usage);
            cout << "Memory before build " << r_usage.ru_maxrss << "\n";

            //cout << "Building\n";
            //BufferedBTree* tree = new BufferedBTree(B,M,N,delta);
            BufferedBTreeBuilder* builder = new BufferedBTreeBuilder();
            int exRoot = builder->build(N-insertionsDuringBuild,B,M,delta);
            delete(builder);
            //cout << "Building tree\n";
            BufferedBTree* tree = new BufferedBTree(B,M,N,delta,exRoot);

            getrusage(RUSAGE_SELF,&r_usage);
            cout << "Memory after build " << r_usage.ru_maxrss << "\n";

            int number;

            //cout << "Building insertion\n";
            // Insert the 100k missing elements
            for (int j = 0; j < insertionsDuringBuild; j++) {
                number = rand() % modulus + 1;
                tree->insert(KeyValueTime(number, number,j+1));
            }

            getrusage(RUSAGE_SELF,&r_usage);
            cout << "Memory after initial insertions " << r_usage.ru_maxrss << "\n";

            using namespace std::chrono;

            // GET DISK STATS
            sleep(10);
            string diskstats = "/proc/diskstats";

            diskReads1 = 0;
            diskWrites1 = 0;
            std::string str;
            std::ifstream file1(diskstats);
            while (std::getline(file1, str)) {
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
                if (s.compare("8") == 0) { // If major number = 8.
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

            //cout << "Inserting\n";
            // Now insert 1mil elements that we time
            high_resolution_clock::time_point t1 = high_resolution_clock::now();
            for (int j = 0; j < insertions; j++) {
                number = rand() % modulus + 1;
                tree->insert(KeyValueTime(number, number,j+1));
            }
            high_resolution_clock::time_point t2 = high_resolution_clock::now();
            insertTime = insertTime + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

            getrusage(RUSAGE_SELF,&r_usage);
            cout << "Memory after insertions " << r_usage.ru_maxrss << "\n";

            // Write out diskstats again
            sleep(10);
            diskReads2 = 0;
            diskWrites2 = 0;
            std::ifstream file2(diskstats);
            while (std::getline(file2, str)) {

                istringstream iss(str);
                string s;
                iss >> s;
                if (s.compare("8") == 0) { // If major number = 8.
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

            temp = (diskReads2 - diskReads1) + (diskWrites2 - diskWrites1);
            insertIO = insertIO + temp;


            //cout << "Query\n";
            t1 = high_resolution_clock::now();
            for (int j = 1; j <= numberOfQueries; j++) {
                number = rand() % modulus + 1;
                tree->query(number);
            }
            t2 = high_resolution_clock::now();
            queryTime = queryTime + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

            getrusage(RUSAGE_SELF,&r_usage);
            cout << "Memory after queries " << r_usage.ru_maxrss << "\n";

            // Write out diskstats a final time
            sleep(10);
            //cout << "Diskread3 ";
            diskReads3 = 0;
            diskWrites3 = 0;
            std::ifstream file3(diskstats);
            while (std::getline(file3, str)) {

                istringstream iss(str);
                string s;
                iss >> s;
                if (s.compare("8") == 0) { // If major number = 8.
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
                    diskWrites3 = diskWrites3 + stol(s);
                }
            }
            file3.close();

            temp = (diskReads3 - diskReads2) + (diskWrites3 - diskWrites2);
            queryIO = queryIO + temp;

            // Quick hack to write out time, could also have used chrono like above
            time_t now = time(0);
            struct tm tstruct;
            char buf[80];
            tstruct = *localtime(&now);
            strftime(buf, sizeof(buf), "%Y-%m-%d.%X", &tstruct);
            cout << "Time " << buf << "\n";

            // Write out temp result, force flush
            cout << "run=" << r << " " << insertTime << " " << insertIO << " " << queryTime << " " << queryIO << "\n"
                 << std::flush;

            tree->cleanup();
            delete (tree);

            getrusage(RUSAGE_SELF,&r_usage);
            cout << "Memory after cleanup " << r_usage.ru_maxrss << "\n";
        }

        // Divide by runs
        insertTime = insertTime / runs;
        insertIO = insertIO / runs;
        queryTime = queryTime / runs;
        queryIO = queryIO / runs;

        // Write out results

        cout << "=== " << deltaRuns << " " << delta << " " << insertTime << " " << insertIO << " " << queryTime << " " << queryIO << "\n" << std::flush;


    }

}

void TreeTester::bufferedBTreeTest(int B, int M, int N, int runs, float delta) {

    int numberOfQueries = 10000;
    int insertionsDuringBuild = 1000000; // 1 mil to fill internal buffers
    int insertions = 1000000; // 1mil to measure

    // Insert, query, iocounter
    long insertTime = 0;
    long insertIO = 0;
    long queryTime = 0;
    long queryIO = 0;

    long diskReads1 = 0;
    long diskReads2 = 0;
    long diskReads3 = 0;
    long diskWrites1 = 0;
    long diskWrites2 = 0;
    long diskWrites3 = 0;
    long temp = 0;


    for(int i = 0; i < runs; i++) {

        srand (time(NULL));

        using namespace std::chrono;

        BufferedBTreeBuilder* builder = new BufferedBTreeBuilder();
        int exRoot = builder->build(N-insertionsDuringBuild,B,M,delta);
        delete(builder);
        BufferedBTree* tree = new BufferedBTree(B,M,N,delta,exRoot);

        int number;
        long tempMod = 2*N;
        int modulus;
        if(tempMod > 2147483647) {
            modulus = 2147483647;
        }
        else {
            modulus = tempMod;
        }

        //cout << "Building insertion\n";
        // Insert the 100k missing elements
        for (int j = 0; j < insertionsDuringBuild; j++) {
            number = rand() % modulus + 1;
            tree->insert(KeyValueTime(number, number,j+1));
        }

        // GET DISK STATS
        sleep(10);
        string diskstats = "/proc/diskstats";

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

        //cout << "Inserting\n";
        // Now insert 1mil elements that we time
        high_resolution_clock::time_point t1 = high_resolution_clock::now();
        for (int j = 0; j < insertions; j++) {
            number = rand() % modulus + 1;
            tree->insert(KeyValueTime(number, number,j+1));
        }
        high_resolution_clock::time_point t2 = high_resolution_clock::now();
        insertTime = insertTime + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        // Write out diskstats again
        sleep(10);
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

        temp = (diskReads2-diskReads1) + (diskWrites2 - diskWrites1);
        insertIO = insertIO + temp;


        //cout << "Query\n";
        t1 = high_resolution_clock::now();
        for(int j = 1; j <= numberOfQueries; j++) {
            number = rand() % modulus +1;
            tree->query(number);
        }
        t2 = high_resolution_clock::now();
        queryTime = queryTime + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();


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

        temp = (diskReads3-diskReads2) + (diskWrites3 - diskWrites2);
        queryIO = queryIO + temp;

        // Write out temp result, force flush
        cout << "run=" << i << " " << insertTime << " " << insertIO << " " << queryTime << " " << queryIO << "\n" << std::flush;

        tree->cleanup();
        delete(tree);
    }

    // Divide by runs
    insertTime = insertTime/runs;
    insertIO = insertIO/runs;
    queryTime = queryTime/runs;
    queryIO = queryIO/runs;

    // Write out results

    cout << insertTime << " " << insertIO << " " << queryTime << " " << queryIO << "\n";

}

void TreeTester::bufferedBTreeTestSpecialQuery(int B, int M, int N, int runs, float delta) {

    int numberOfQueries = 10000;
    int insertionsDuringBuild = 1000000; // 1 mil to fill internal buffers
    int insertions = 1000000; // 1mil to measure

    // Insert, query, iocounter
    long insertTime = 0;
    long insertIO = 0;
    long queryTime = 0;
    long queryIO = 0;
    long specialQueryTime = 0;
    long specialQueryIO = 0;

    long diskReads1 = 0;
    long diskReads2 = 0;
    long diskReads3 = 0;
    long diskReads4 = 0;
    long diskWrites1 = 0;
    long diskWrites2 = 0;
    long diskWrites3 = 0;
    long diskWrites4 = 0;
    long temp = 0;


    for(int i = 0; i < runs; i++) {

        srand (time(NULL));

        using namespace std::chrono;

        BufferedBTreeBuilder* builder = new BufferedBTreeBuilder();
        int exRoot = builder->build(N-insertionsDuringBuild,B,M,delta);
        delete(builder);
        BufferedBTree* tree = new BufferedBTree(B,M,N,delta,exRoot);

        int number;
        long tempMod = 2*N;
        int modulus;
        if(tempMod > 2147483647) {
            modulus = 2147483647;
        }
        else {
            modulus = tempMod;
        }

        //cout << "Building insertion\n";
        // Insert the 100k missing elements
        for (int j = 0; j < insertionsDuringBuild; j++) {
            number = rand() % modulus + 1;
            tree->insert(KeyValueTime(number, number,j+1));
        }

        // GET DISK STATS
        sleep(10);
        string diskstats = "/proc/diskstats";

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

        //cout << "Inserting\n";
        // Now insert 1mil elements that we time
        high_resolution_clock::time_point t1 = high_resolution_clock::now();
        for (int j = 0; j < insertions; j++) {
            number = rand() % modulus + 1;
            tree->insert(KeyValueTime(number, number,j+1));
        }
        high_resolution_clock::time_point t2 = high_resolution_clock::now();
        insertTime = insertTime + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

        // Write out diskstats again
        sleep(10);
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

        temp = (diskReads2-diskReads1) + (diskWrites2 - diskWrites1);
        insertIO = insertIO + temp;


        //cout << "Query\n";
        t1 = high_resolution_clock::now();
        for(int j = 1; j <= numberOfQueries; j++) {
            number = rand() % modulus +1;
            tree->query(number);
        }
        t2 = high_resolution_clock::now();
        queryTime = queryTime + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();


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


        temp = (diskReads3-diskReads2) + (diskWrites3 - diskWrites2);
        queryIO = queryIO + temp;


        // SPECIAL QUERY
        t1 = high_resolution_clock::now();
        for(int j = 1; j <= numberOfQueries; j++) {
            number = rand() % modulus +1;
            tree->specialQuery(number);
        }
        t2 = high_resolution_clock::now();
        specialQueryTime = specialQueryTime + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();


        sleep(10);
        //cout << "Diskread3 ";
        diskReads4 = 0;
        diskWrites4 = 0;
        std::ifstream file4(diskstats);
        while (std::getline(file4, str))
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
                diskReads4 = diskReads4 + stol(s);
                iss >> s;
                iss >> s;
                iss >> s;
                iss >> s; // Sectors written
                diskWrites4 =  diskWrites4 + stol(s);
            }
        }
        file4.close();


        temp = (diskReads4-diskReads3) + (diskWrites4 - diskWrites3);
        specialQueryIO = specialQueryIO + temp;



        // Write out temp result, force flush
        cout << "run=" << i << " " << insertTime << " " << insertIO << " " << queryTime << " " << queryIO << " " << specialQueryTime
             << " " << specialQueryIO << "\n" << std::flush;

        tree->cleanup();
        delete(tree);
    }

    // Divide by runs
    insertTime = insertTime/runs;
    insertIO = insertIO/runs;
    queryTime = queryTime/runs;
    queryIO = queryIO/runs;
    specialQueryTime = specialQueryTime/runs;
    specialQueryIO = specialQueryIO/runs;

    // Write out results

    cout << insertTime << " " << insertIO << " " << queryTime << " " << queryIO << " " << specialQueryTime << " " << specialQueryIO << "\n";
}

void TreeTester::xDictAlphaTest(int N, int runs) {

    int numberOfQueries = 10000; // 10k

    long insertionTime[10];
    long insertionIO[10];
    long queryTime[10];
    long queryIO[10];

    for(int i = 0; i < 10; i++) {
        insertionTime[i] = 0;
        insertionIO[0] = 0;
        queryTime[i] = 0;
        queryIO[i] = 0;
    }

    long diskReads1 = 0;
    long diskReads2 = 0;
    long diskReads3 = 0;
    long diskReads4 = 0;
    long diskWrites1 = 0;
    long diskWrites2 = 0;
    long diskWrites3 = 0;
    long diskWrites4 = 0;
    long temp = 0;

    //runs = 1;

    srand (time(NULL));

    for(int i  = 0 ; i< 10; i++) {

        /* Inaccurate
        double alpha = 0.1*i;
        /*if(i == 2) {
            continue;
           alpha = 0.22;
        }*/

        double alpha;
        if(i == 0) {
            alpha = 0.1;
        }
        else if(i == 1) {
            alpha = 0.2;
        }
        else if(i == 2) {
            alpha = 0.3;
        }
        else if(i == 3) {
            alpha = 0.4;
        }
        else if(i == 4) {
            alpha = 0.5;
        }
        else if(i == 5) {
            alpha = 0.6;
        }
        else if(i == 6) {
            alpha = 0.7;
        }
        else if(i == 7) {
            alpha = 0.8;
        }
        else if(i == 8) {
            alpha = 0.9;
        }
        else if(i == 9) {
            alpha = 1;
        }

        for(int r = 0; r < runs; r++) {

            //srand (time(NULL));

            using namespace std::chrono;

            XDict* xDict = new XDict(alpha);

            int number;
            long tempMod = 2*N;
            int modulus;
            if(tempMod > 2147483647) {
                modulus = 2147483647;
            }
            else {
                modulus = tempMod;
            }

            // GET DISK STATS
            sleep(10);
            string diskstats = "/proc/diskstats";

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

            //cout << "Inserting\n";
            // Now insert 1mil elements that we time
            high_resolution_clock::time_point t1 = high_resolution_clock::now();
            for (int j = 0; j < N; j++) {
                number = rand() % modulus + 1;
                xDict->insert(KeyValue(number,number));
            }
            high_resolution_clock::time_point t2 = high_resolution_clock::now();
            insertionTime[i] = insertionTime[i] + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();

            // Write out diskstats again
            sleep(10);
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

            temp = (diskReads2-diskReads1) + (diskWrites2 - diskWrites1);
            insertionIO[i] = insertionIO[i] + temp;


            //cout << "Query\n";
            t1 = high_resolution_clock::now();
            for(int j = 1; j <= numberOfQueries; j++) {
                number = rand() % modulus +1;
                xDict->query(number);
            }
            t2 = high_resolution_clock::now();
            queryTime[i] = queryTime[i] + chrono::duration_cast<chrono::milliseconds>(t2 - t1).count();


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

            temp = (diskReads3-diskReads2) + (diskWrites3 - diskWrites2);
            queryIO[i] = queryIO[i] + temp;

            delete(xDict);

        }

        cout << "Run " << alpha << " " << insertionTime[i] << " " << insertionIO[i] << " " << queryTime[i] << " " << queryIO[i] << "\n" << std::flush;

    }

    // Divide by runs
    for(int i = 0; i < 10; i++) {
        insertionTime[i] = insertionTime[i] / runs;
        insertionIO[i] = insertionIO[i] / runs;
        queryTime[i] = queryTime[i] / runs;
        queryIO[i] = queryIO[i] / runs;
    }

    cout << "Results:\n";

    for(int i = 0; i < 10; i++) {
        cout << (0.1*(i+1)) << " " << insertionTime[i] << " " << insertionIO[i] << " " << queryTime[i] << " " << queryIO[i] << "\n";
    }


}

