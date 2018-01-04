//
// Created by martin on 12/18/17.
//

#include <cstdlib>
#include <string>
#include <vector>
#include <queue>
#include <iostream>
#include <algorithm>
#include "ModifiedBuilder.h"
#include "../Streams/OutputStream.h"
#include "../Streams/BufferedOutputStream.h"
#include "../Streams/InputStream.h"
#include "../Streams/BufferedInputStream.h"
#include "KeyValue.h"
#include "Heap.h"
#include "ModifiedBtree.h"

using namespace std;

ModifiedBuilder::ModifiedBuilder() {
    numberOfNodes = 0;

}

ModifiedBuilder::~ModifiedBuilder() {

}

/*
 * Builds up the structure of a full ModifiedBTree.
 * You should insert elements afterwards to make it
 * more realistic, before you measure the insertions.
 */
int ModifiedBuilder::build(int N, int B, int M) {

    //cout << "Generating\n";

    // Generate new file of sorted integers
    string input = generate(N, B, M);
    ModifiedBtree tree(B,M); // Use methods to build nodes

    queue<int> leafs;
    queue<int> nodes;
    queue<int> maxKeys;

    InputStream* in = new BufferedInputStream(B);
    in->open(input.c_str());

    int size = (B/sizeof(int)); // #values. size = #elements in B. Max #keys will be 2*size-1. Min = 1/2 size.
    //cout << "Size between " << size/2 << " and " << 2*size-1 << "\n";
    int leafElementCounter = 0;
    int* keys = new int[size*2-1];
    int* values = new int[size*2];
    //cout << "Building leafs\n";
    int prev = 0;
    // Random leaf size between 1/2 size and 2*size-1
    int range = (size*2-1) - (size/2) + 1; // max - min + 1
    int nodeFanout = rand() % range + (size/2); // rand() % range + min

    // Create leafs
    while(true) {
        int read = in->readNext();
        /*if(read < prev) {
            cout << "Error " << read << " " << prev << "\n";
        }*/
        prev = read;
        keys[leafElementCounter] = read;
        values[leafElementCounter] = read;
        leafElementCounter++;
        if(leafElementCounter == nodeFanout || in->endOfStream()) {
            numberOfNodes++;
            int nodeSize = leafElementCounter;
            tree.writeNode(numberOfNodes,1,nodeSize,keys,values);
            leafs.push(numberOfNodes);
            maxKeys.push(read);
            // Clean up
            leafElementCounter = 0;
            if(in->endOfStream()) {
                break;
            }
            else {
                nodeFanout = rand() % range + (size/2); // New fanout
                keys = new int[size*2-1];
                values = new int[size*2];
            }
        }
    }

    //cout << "Recursive Build\n";
    // Create nodes one level at a time
    // alternate between using nodes and leafs queue
    bool alternate = false;
    keys = new int[size*2];
    values = new int[size*2];
    int elementCounter = 0;
    int nodeCounter = 0;
    int height = 2;
    range = (size*2) - (size/2+1) + 1; // New range, counting values, not keys
    nodeFanout = rand() % range + (size/2+1);
    while(true) {


        if(alternate) {
            // Nodes placed in nodes
            if(!nodes.empty()) {
                int node = nodes.front();
                nodes.pop();
                int max = maxKeys.front();
                maxKeys.pop();
                keys[elementCounter] = max;
                values[elementCounter] = node;
                elementCounter++;
                if(elementCounter == nodeFanout) {
                    // Write out new node
                    numberOfNodes++;
                    maxKeys.push(keys[elementCounter-1]);
                    leafs.push(numberOfNodes);
                    tree.writeNode(numberOfNodes,height,elementCounter-1,keys,values);
                    keys = new int[size*2];
                    values = new int[size*2];
                    nodeCounter++;
                    elementCounter = 0;
                    nodeFanout = rand() % range + (size/2+1);
                }
            }
            else if(elementCounter != 0){
                // Write out node
                numberOfNodes++;
                maxKeys.push(keys[elementCounter-1]);
                leafs.push(numberOfNodes);
                tree.writeNode(numberOfNodes,height,elementCounter-1,keys,values);
                keys = new int[size*2];
                values = new int[size*2];
                nodeCounter++;
                nodeFanout = rand() % range + (size/2+1);
                if(nodeCounter == 1) {
                    break;
                }
                alternate = false;
                height++;
                nodeCounter = 0;
                elementCounter = 0;
            }
            else {
                // Wrote this levels nodes already
                if(nodeCounter == 1) {
                    break;
                }
                alternate = false;
                height++;
                nodeCounter = 0;
            }

        }
        else {
            // Nodes placed in leafs
            if(!leafs.empty()) {
                int node = leafs.front();
                leafs.pop();
                int max = maxKeys.front();
                maxKeys.pop();
                keys[elementCounter] = max;
                values[elementCounter] = node;
                elementCounter++;
                if(elementCounter == nodeFanout) {
                    // Write out new node
                    numberOfNodes++;
                    maxKeys.push(keys[elementCounter-1]);
                    nodes.push(numberOfNodes);
                    tree.writeNode(numberOfNodes,height,elementCounter-1,keys,values);
                    keys = new int[size*2];
                    values = new int[size*2];
                    nodeCounter++;
                    elementCounter = 0;
                    nodeFanout = rand() % range + (size/2+1);
                }
            }
            else if(elementCounter != 0){
                // Write out node
                numberOfNodes++;
                maxKeys.push(keys[elementCounter-1]);
                nodes.push(numberOfNodes);
                tree.writeNode(numberOfNodes,height,elementCounter-1,keys,values);
                keys = new int[size*2];
                values = new int[size*2];
                nodeCounter++;
                nodeFanout = rand() % range + (size/2+1);
                if(nodeCounter == 1) {
                    break;
                }
                alternate = true;
                height++;
                nodeCounter = 0;
                elementCounter = 0;
            }
            else {
                // Wrote this levels nodes already
                if(nodeCounter == 1) {
                    break;
                }
                alternate = true;
                height++;
                nodeCounter = 0;
            }
        }
    }

    delete[] keys;
    delete[] values;

    if(FILE *file = fopen(input.c_str(), "r")) {
        fclose(file);
        remove(input.c_str());
    }

    //cout << "Done building\n";

    // Return root, last node created
    return numberOfNodes;
}

/*
 * Generates a sorted file of integers
 * in the range 1 to 2*N (max 2^31).
 */
string ModifiedBuilder::generate(int N, int B, int M) {

    srand (time(NULL));

    long tempMod = 2*N;
    int modulus;
    if(tempMod > 2147483647) {
        modulus = 2147483647;
    }
    else {
        modulus = tempMod;
    }

    long output = 0;
    long fileOutput = 0;
    int fileCounter = 0;
    queue<string> files;
    int* array = new int[M];
    // Output integers to files of size M
    //cout << "Outputting\n";
    while(true) {
        int newInt = rand() % modulus +1;
        array[fileOutput] = newInt;
        fileOutput++;
        output++;
        if(fileOutput == M || output == N) {
            // Sort
            sort(array,array+fileOutput);
            OutputStream* out = new BufferedOutputStream(B);
            fileCounter++;
            string name = "temp";
            name += to_string(fileCounter);
            files.push(name);
            out->create(name.c_str());
            for(int i = 0; i < fileOutput; i++) {
                out->write(&(array[i]));
            }
            out->close();
            delete(out);
            fileOutput = 0;
            if(output == N) {
                break;
            }
        }
    }

    delete[] array;

    //cout << "Merging\n";
    // Merge the files M/B at a time.
    int counter = 0;
    while(files.size() != 1) {

        counter++;
        //cout << counter << "\n";

        int merge = M/B;
        if(files.size() < merge) {
            merge = (int) files.size();
        }

        //cout << "Inputstreams\n";
        // Create inputstreams
        InputStream** inputStreams = new InputStream*[merge];
        for(int i = 0; i < merge; i++) {
            inputStreams[i] = new BufferedInputStream(B);
            //cout << files.front() << "\n";
            inputStreams[i]->open(files.front().c_str());
            files.pop();
        }

        //cout << "Generating heap\n";
        KeyValue** heapArray = new KeyValue*[merge+2]; //+1 for index, +1 for working space.
        // Read in first element from each stream
        for(int i = 0; i < merge; i++) {
            heapArray[i+1] = new KeyValue(inputStreams[i]->readNext(),i);
        }

        Heap* heap = new Heap;
        int heapCounter = merge;
        heap->setheap(heapArray,heapCounter);


        OutputStream* out = new BufferedOutputStream(B);
        string name = "temp";
        fileCounter++;
        name += to_string(fileCounter);
        out->create(name.c_str());
        files.push(name);

        //cout << "Actual Merge\n";
        // Run through all files
        while(heapCounter != 0) {

            KeyValue* min = heap->outheap(heapArray,heapCounter);
            out->write(&(min->key));
            if(!inputStreams[min->value]->endOfStream()) {
                min->key = inputStreams[min->value]->readNext();
                heap->inheap(heapArray,heapCounter-1,min);
            }
            else {
                delete(min);
                heapCounter--;
            }
        }

        // Clean up
        for(int i = 0; i < merge; i++) {
            inputStreams[i]->close();
            delete[] inputStreams[i];
        }
        delete[] inputStreams;
        delete[] heapArray;
        delete(heap);

        // Dont forget output (I did)
        out->close();
        delete(out);
    }

    // Delete all but the last temp file
    for(int i = 1; i < fileCounter; i++) {
        string name = "temp";
        name += to_string(i);
        if(FILE *file = fopen(name.c_str(), "r")) {
            fclose(file);
            remove(name.c_str());
        }
    }

    return files.front();
}