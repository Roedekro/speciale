//
// Created by martin on 1/10/18.
// Enables us to build a BufferedBTree in O(sort).
//

#include <queue>
#include <algorithm>
#include "BufferedBTreeBuilder.h"
#include "../Streams/OutputStream.h"
#include "../Streams/BufferedOutputStream.h"
#include "../Streams/InputStream.h"
#include "../Streams/BufferedInputStream.h"
#include "keyValueTime.h"
#include "../Btree/Heap.h"
#include "BufferedBTree.h"

using namespace std;

BufferedBTreeBuilder::BufferedBTreeBuilder() {

}

BufferedBTreeBuilder::~BufferedBTreeBuilder() {

}

/*
 * Builds a BufferedBTree of size N in O(sort).
 */
int BufferedBTreeBuilder::build(int N, int B, int M, float delta) {

    // Generate new file of sorted integers
    string input = generate(N, B, M/8);
    BufferedBTree tree(B,M,N,delta); // Use methods to build nodes

    queue<int> leafs;
    queue<int> leafSizes;
    queue<int> nodes;
    queue<int> maxKeys;

    InputStream* in = new BufferedInputStream(B); // Open file of sorted random integers.
    in->open(input.c_str());

    int size = tree.size; // #values. max fanout is 4*size-1 and min is size.
    int maxBufferSize = tree.maxBufferSize;

    int leafElementCounter = 0;
    vector<KeyValueTime>* leafElements = new vector<KeyValueTime>();
    // Random leaf size between 1/2 maxBufferSize and maxBufferSize
    int range = (maxBufferSize) - (maxBufferSize/2) + 1; // max - min + 1
    int leafSize = rand() % range + (maxBufferSize/2); // rand() % range + min

    // Create leafs
    while(true) {
        int read = in->readNext();
        leafElements->push_back(KeyValueTime(read,read,read));
        leafElementCounter++;
        if(leafElementCounter == leafSize || in->endOfStream()) {
            numberOfNodes++;
            tree.writeLeaf(numberOfNodes,leafElements);
            leafs.push(numberOfNodes);
            leafSizes.push(leafElementCounter);
            maxKeys.push(read);
            // Clean up
            leafElementCounter = 0;
            if(in->endOfStream()) {
                delete(leafElements);
                break;
            }
            else {
                leafSize = rand() % range + (maxBufferSize/2); // New leafSize
                leafElements->clear();
            }
        }
    }

    range = (4*size-1) - (size) + 1; // max - min + 1
    int nodeFanout = rand() % range + (size); // rand() % range + min
    vector<int>* keys = new vector<int>();
    keys->reserve(4*size);
    vector<int>* values = new vector<int>();
    values->reserve(4*size);
    vector<int>* leafInfo = new vector<int>();
    leafInfo->reserve(4*size);
    /*vector<KeyValueTime>* buffer = new vector<KeyValueTime>();
    buffer->reserve(maxBufferSize);*/
    int elementCounter = 0;
    int nodeCounter = 0;
    int height = 1;

    // Create first layer of nodes on top of leafs
    // Place them in the node queue
    while(true) {

        // Nodes placed in nodes
        if (!leafs.empty()) {
            int node = leafs.front();
            nodes.pop();
            int max = maxKeys.front();
            maxKeys.pop();
            (*keys)[elementCounter] = max;
            (*values)[elementCounter] = node;
            int leafSize = leafSizes.front();
            leafSizes.pop();
            (*leafInfo)[elementCounter] = leafSize;
            elementCounter++;
            if (elementCounter == nodeFanout) {
                // Write out new node
                numberOfNodes++;
                maxKeys.push((*keys)[elementCounter - 1]);
                nodes.push(numberOfNodes);
                tree.writeNode(numberOfNodes,height,elementCounter-1,0,keys,values); // Empty buffer, fix by inserting
                // Write out leaf info
                tree.writeLeafInfo(numberOfNodes,leafInfo);

                // Vectors deleted upon write
                keys = new vector<int>();
                values = new vector<int>();
                leafInfo = new vector<int>();

                nodeCounter++;
                elementCounter = 0;
                nodeFanout = rand() % range + (size + 1);
            }
        } else if (elementCounter != 0) {
            // Write out node
            numberOfNodes++;
            maxKeys.push((*keys)[elementCounter - 1]);
            nodes.push(numberOfNodes);
            tree.writeNode(numberOfNodes, height, elementCounter - 1, 0, keys, values);
            tree.writeLeafInfo(numberOfNodes,leafInfo);
            keys = new vector<int>();
            values = new vector<int>();
            nodeCounter++;
            nodeFanout = rand() % range + (size + 1);
            if (nodeCounter == 1) {
                break;
            }
            height++;
            nodeCounter = 0;
            elementCounter = 0;
        } else {
            // Wrote this levels nodes already
            if (nodeCounter == 1) {
                break;
            }
            height++;
            nodeCounter = 0;
        }
    }

    // Recursively create the rest of the tree
    bool alternate = true;
    while(true) {


        if(alternate) {
            // Nodes placed in nodes
            if(!nodes.empty()) {
                int node = nodes.front();
                nodes.pop();
                int max = maxKeys.front();
                maxKeys.pop();
                (*keys)[elementCounter] = max;
                (*values)[elementCounter] = node;
                elementCounter++;
                if(elementCounter == nodeFanout) {
                    // Write out new node
                    numberOfNodes++;
                    maxKeys.push((*keys)[elementCounter-1]);
                    leafs.push(numberOfNodes);
                    tree.writeNode(numberOfNodes,height,elementCounter-1,0,keys,values);
                    keys = new vector<int>();
                    values = new vector<int>();
                    nodeCounter++;
                    elementCounter = 0;
                    nodeFanout = rand() % range + (size+1);
                }
            }
            else if(elementCounter != 0){
                // Write out node
                numberOfNodes++;
                maxKeys.push((*keys)[elementCounter-1]);
                leafs.push(numberOfNodes);
                tree.writeNode(numberOfNodes,height,elementCounter-1,0,keys,values);
                keys = new vector<int>();
                values = new vector<int>();
                nodeCounter++;
                nodeFanout = rand() % range + (size+1);
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
                (*keys)[elementCounter] = max;
                (*values)[elementCounter] = node;
                elementCounter++;
                if(elementCounter == nodeFanout) {
                    // Write out new node
                    numberOfNodes++;
                    maxKeys.push((*keys)[elementCounter-1]);
                    nodes.push(numberOfNodes);
                    tree.writeNode(numberOfNodes,height,elementCounter-1,0,keys,values);
                    keys = new vector<int>();
                    values = new vector<int>();
                    nodeCounter++;
                    elementCounter = 0;
                    nodeFanout = rand() % range + (size+1);
                }
            }
            else if(elementCounter != 0){
                // Write out node
                numberOfNodes++;
                maxKeys.push((*keys)[elementCounter-1]);
                nodes.push(numberOfNodes);
                tree.writeNode(numberOfNodes,height,elementCounter-1,0,keys,values);
                keys = new vector<int>();
                values = new vector<int>();
                nodeCounter++;
                nodeFanout = rand() % range + (size+1);
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

    delete(keys);
    delete(values);

    if(FILE *file = fopen(input.c_str(), "r")) {
        fclose(file);
        remove(input.c_str());
    }

    return numberOfNodes; // Root

}


/*
 * Generates a sorted file of integers
 * in the range 1 to 2*N (max 2^31).
 * Ignore that it uses KeyValue, thats just a hack.
 */

std::string BufferedBTreeBuilder::generate(int N, int B, int M) {

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