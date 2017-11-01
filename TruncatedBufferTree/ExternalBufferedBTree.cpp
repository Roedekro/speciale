/*
 * Created by martin on 10/31/17.
 * Experiment to get the basics of an external
 * buffered BTree correct before implementing
 * the Truncated Buffered Tree.
 *
 * A node is laid out with its height, size, and buffersize,
 * followed by its keys and its values.
 * A node will be stored in "B<id>".
 *
 * A buffer of node id consists of integers
 * denoting KeyValueTime.
 * A buffer is stored in "Buf<id>".
 * A temp buffer may be stored in "Temp<id>".
 *
 * NOTE: Buffers MUST be deleted after flush.
 * This way new elements can just be appended.
 *
 */


#include "ExternalBufferedBTree.h"
#include "../Streams/OutputStream.h"
#include "../Streams/BufferedOutputStream.h"
#include "../Streams/InputStream.h"
#include "../Streams/BufferedInputStream.h"
#include <sstream>
#include <iostream>
#include <unistd.h>
#include <cmath>
#include <algorithm>
#include <assert.h>

using namespace std;

ExternalBufferedBTree::ExternalBufferedBTree(int B, int M) {

    streamBuffer = 4096;
    this->B = B;
    this->M = M;
    // Nodes have M/B fanout
    size = M/(sizeof(int)*2*B*4); // Max size of a node is 4x size, contains int keys and int values.
    // Internal update buffer, size O(B), consiting of KeyValueTimes.
    maxBufferSize = B/(sizeof(KeyValueTime));
    update_buffer = new KeyValueTime*[maxBufferSize];
    update_bufferSize = 0;
    // External buffers consists of KeyValueTimes
    maxExternalBufferSize = M/(sizeof(KeyValueTime));
    iocounter = 0;
    numberOfNodes = 1;
    time = 0;
    root = 1;
    rootBufferSize = 0;

    // Manually write out an empty root
    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string name = "B";
    name += to_string(1);
    os->create(name.c_str());
    int height = 1;
    os->write(&height);
    int zero = 0;
    os->write(&zero);
    os->close();
    iocounter = os->iocounter;
    delete(os);

    // Manually write out an empty root buffer
    os = new BufferedOutputStream(streamBuffer);
    name = "Buf";
    name += to_string(1);
    os->create(name.c_str());
    os->write(&zero);
    os->close();
    iocounter += os->iocounter;
    delete(os);

}

ExternalBufferedBTree::~ExternalBufferedBTree() {
    delete[] update_buffer;
}

/*
 * Updates the tree with the given KeyValueTime.
 * Time will be set according to the trees relative time.
 * A negative value denotes a delete.
 */
void ExternalBufferedBTree::update(KeyValueTime *keyValueTime) {

    time++;
    keyValueTime->time = time;
    update_buffer[update_bufferSize] = keyValueTime;
    update_bufferSize++;

    if(update_bufferSize >= maxBufferSize) {

        // Sort the buffer and remove duplicates
        update_bufferSize = sortAndRemoveDuplicatesInternalArray(update_buffer,update_bufferSize);

        // Flush to root
        if(rootBufferSize + update_bufferSize >= maxExternalBufferSize) {

            // Load in roots buffer
            KeyValueTime** rootBuffer = new KeyValueTime*[rootBufferSize];
            readBuffer(root,rootBuffer,false);
            sortInternalArray(rootBuffer,rootBufferSize);

            // Merge the two buffers into a new buffer.
            // Removes duplicate keys, and cleans up.
            //mergeInternalBuffersAndOutput(root, bufferSize, buffer, rootBufferSize, rootBuffer);

            // Load in root
            int height, nodeSize;
            int* ptr_height = &height;
            int* ptr_nodeSize = &nodeSize;
            int* keys = new int[size*4-1];
            int* values = new int[size*4];
            readNode(root,ptr_height,ptr_nodeSize,keys,values);

            // If root needs to split do so.
            // Also splits the buffer and removes the
            // need to flush.
            if(nodeSize == size*4-1) {


            }
            // Else flush roots buffer
            else {

                // Read in buffer

            }


        }
        else {
            // Append buffer to roots buffer
            appendBuffer(root,update_buffer,update_bufferSize,false);
            rootBufferSize = rootBufferSize + update_bufferSize;
        }

        // Reset internal buffer
        update_bufferSize = 0;
    }
}

/*
 * Writes out the node to file "B<id>".
 * Deletes key and value arrays.
 * Handles that leafs have size-1 values.
 */
void ExternalBufferedBTree::writeNode(int id, int height, int nodeSize, int *keys, int *values) {

    OutputStream* os = new BufferedOutputStream(streamBuffer);
    string node = "B";
    node += to_string(id);
    os->create(node.c_str());
    os->write(&height);
    os->write(&nodeSize);
    for(int k = 0; k < nodeSize; k++) {
        os->write(&keys[k]);
    }
    if(height > 1) {
        for(int k = 0; k < nodeSize+1; k++) {
            os->write(&values[k]);
        }
    }
    else {
        // Leaf
        for(int k = 0; k < nodeSize; k++) {
            os->write(&values[k]);
        }
    }

    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);
    delete[] keys;
    delete[] values;
}

/*
 * Reads the node of id into the pointers height and size, as well as fills
 * out the arrays keys and values. Handles leafs (height == 1) appropriately.
 */
void ExternalBufferedBTree::readNode(int id, int* height, int* nodeSize, int *keys, int *values) {


    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = "B";
    name += to_string(id);
    is->open(name.c_str());
    *height = is->readNext();
    *nodeSize = is->readNext();

    for(int i = 0; i < *nodeSize; i++) {
        keys[i] = is->readNext();
    }

    if(*height > 1) {
        for(int i = 0; i < *nodeSize+1; i++) {
            values[i] = is->readNext();
        }
    }
    else {
        for(int i = 0; i < *nodeSize; i++) {
            values[i] = is->readNext();
        }
    }
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);
}

/* Deprecated
int ExternalBufferedBTree::getBufferSize(int id) {
    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = "Buf";
    name += to_string(id);
    is->open(name.c_str());
    int ret = is->readNext();
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);
    return ret;
}*/

/*
 * Convention that we will never read a file over size M (in bytes).
 * Will return actual size of buffer.
 */
int ExternalBufferedBTree::readBuffer(int id, KeyValueTime **buffer, bool temp) {

    InputStream* is = new BufferedInputStream(streamBuffer);
    string name;
    if(!temp) {
        name = "Buf";
    }
    else {
        name = "Temp";
    }
    name += to_string(id);
    is->open(name.c_str());
    int newKey,newValue,newTime;
    //for(int i = 0; i < bSize; i++) {
    int i = 0;
    while(!is->endOfStream()) {
        newKey = is->readNext();
        newValue = is->readNext();
        newTime = is->readNext();
        buffer[i] = new KeyValueTime(newKey,newValue,newTime);
        i++;
    }
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);
    return i;
}

/*
 * Writes out buffer, cleans up.
 * Removes existing file, writes out new one.
 */
void ExternalBufferedBTree::writeBuffer(int id, KeyValueTime** buffer, int bSize, bool temp) {

    // Name
    string name;
    if(!temp) {
        name = "Buf";
    }
    else {
        name = "Temp";
    }
    name += to_string(id);

    // Remove existing buffer if present
    if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }

    // Create file again
    OutputStream* os = new BufferedOutputStream(streamBuffer);
    os->create(name.c_str());

    // Output buffer
    for(int i = 0; i < bSize; i++) {
        os->write(&buffer[i]->key);
        os->write(&buffer[i]->value);
        os->write(&buffer[i]->time);
    }

    // Cleanup
    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);
    for(int i = 0; i < bSize; i++) {
        delete(buffer[i]);
    }
    delete[] buffer;
}

/*
 * Appends buffer of size bSize to the buffer of node id.
 * Will clean up and delete elements after output.
 */
void ExternalBufferedBTree::appendBuffer(int id, KeyValueTime **buffer, int bSize, bool temp) {

}

/*
 * Sort an array of KeyValueTime by key and time
 */
void ExternalBufferedBTree::sortInternalArray(KeyValueTime** buffer, int bufferSize) {


    std::sort(buffer, buffer+bufferSize,[](KeyValueTime * a, KeyValueTime * b) -> bool
    {
        if(a->key == b->key) {
            return a->time < b->time;
        }
        else {
            return a->key < b->key;
        }
    } );
}

/*
 * Returns new size of the buffer after removing duplicates.
 * Will delete duplicates.
 */
int ExternalBufferedBTree::sortAndRemoveDuplicatesInternalArray(KeyValueTime **buffer, int bufferSize) {

    // Sort by key primary, time secondary
    sortInternalArray(buffer,bufferSize);
    // Remove duplicates
    int i = 1;
    int write = 0;
    KeyValueTime* kvt1 = buffer[0];
    KeyValueTime* kvt2;
    while(i < bufferSize) {

        kvt2 = buffer[i];
        if(kvt1->key == kvt2->key) {
            // Duplicate, resolve. Sorted by time as secondary.
            // Several combinations possible
            // Insert1 + delete2 = delete2
            // Insert1 + insert2 = insert2
            // delete1 + insert2 = insert2
            // delete1 + delete2 = delete2
            // Notice how second update always overwrite first
            delete(kvt1);
            kvt1 = kvt2;
            i++;
        }
        else {
            buffer[write] = kvt1;
            write++;
            kvt1 = kvt2;
            i++;
        }
    }
    buffer[write] = kvt1;
    write++;

    return write;
}

int ExternalBufferedBTree::loadSortRemoveDuplicatesOutputBuffer(int id, bool temp) {

    // Load in buffer, max size M.
    KeyValueTime** temp_buffer = new KeyValueTime*[maxExternalBufferSize];
    int temp_bufferSize = readBuffer(id,temp_buffer,temp);
    // Sort and remove duplicates, cleans up duplicates.
    int ret = sortAndRemoveDuplicatesInternalArray(temp_buffer,temp_bufferSize);
    // Output new buffer, cleans up remaining elements and buffer.
    writeBuffer(id,temp_buffer,ret,temp);
    return ret;
}

/*
 * Merge the two internal buffers while removing duplicate keys.
 * Will delete the original external buffer, and write the new merged buffer to disk.
 * Cleans up the two buffers.
 * Buffers must contain at least 2 elements;
 */
/*void ExternalBufferedBTree::mergeInternalBuffersAndOutput(int id, int bufSizeA, KeyValueTime **bufferA, int bufSizeB,
                                         KeyValueTime **bufferB) {

    assert(bufSizeA >= 2 && "mergeInternalBuffers buffer A too small");
    assert(bufSizeB >= 2 && "mergeInternalBuffers buffer B too small");

    // Pseudokode

    /*Læs ind fra A {
            Fjern duplikater
    };

    Læs ind fra B {
            Fjern duplikater
    };
    while(ikke færdig) {

        Udvælg mindste af A eller B

        Afhængig af valg, indlæs næste

        Hvis A læs ind fra A {
                Fjern duplikater
        };
        ellers
        læs ind fra B {
                Fjern duplikater
        };
    }*/


    /*int indexA = 0;
    int indexB = 0;
    KeyValueTime *a1,*a2,*b1,*b2; // Lookahead one to remove duplicates
    bool merging = true;
    bool reachedEndA = false;
    bool reachedEndB = false;
    bool localDuplicate;
    bool duplicate = false; // Any duplicate throughout the entire process

    // Find first element from A
    a1 = bufferA[indexA];
    indexA++;
    // Look ahead to remove duplicates
    localDuplicate = true;
    while(localDuplicate) {
        if(indexA < bufSizeA) {
            a2 = bufferA[indexA];
            if(a1->key == a2->key) {
                // Duplicate, resolve. Sorted by time as secondary.
                // Several combinations possible
                // Insert1 + delete2 = delete2
                // Insert1 + insert2 = insert2
                // delete1 + insert2 = insert2
                // delete1 + delete2 = delete2
                // Notice how second update always overwrite first
                a1 = a2;
                indexA++;
                duplicate = true;
            }
            else {
                localDuplicate = false;
            }
        }
        else {
            localDuplicate = false;
        }
    }
    // Element from A placed in a1

    // Find first element from B
    b1 = bufferB[indexB];
    indexB++;
    // Look ahead to remove duplicates
    localDuplicate = true;
    while(localDuplicate) {
        if(indexB < bufSizeB) {
            b2 = bufferB[indexB];
            if(b1->key == b2->key) {
                b1 = a2;
                indexB++;
                duplicate = true;
            }
            else {
                localDuplicate = false;
            }
        }
        else {
            localDuplicate = false;
        }
    }
    // Element from B placed in b1

    // Remove buffer
    string name = "Buf";
    name += to_string(id);
    remove(name.c_str());
    OutputStream* os = new BufferedOutputStream(streamBuffer);
    os->create(name.c_str());
    int newSize = bufSizeA+bufSizeB;
    os->write(&newSize);

    newSize = 0;
    // Merge until one buffer runs empty
    while(merging) {

        // Check duplicates across buffers
        if(a1->key = b1->key) {
            duplicate = true;
            // Resolve duplicate
            if(a1->time > b1->time) {
                // A won, read in new B.
                if(indexB < bufSizeB) {
                    b1 = bufferB[indexB];
                    indexB++;
                    // Look ahead to remove duplicates
                    localDuplicate = true;
                    while(localDuplicate) {
                        if(indexB < bufSizeB) {
                            b2 = bufferB[indexB];
                            if(b1->key == b2->key) {
                                b1 = a2;
                                indexB++;
                                duplicate = true;
                            }
                            else {
                                localDuplicate = false;
                            }
                        }
                        else {
                            localDuplicate = false;
                        }
                    }
                }
                else {
                    reachedEndB = true;
                    break;
                }
            }
            else {
                // B won, read in new A.
                if(indexA < bufSizeA) {
                    a1 = bufferA[indexA];
                    indexA++;
                    // Look ahead to remove duplicates
                    localDuplicate = true;
                    while(localDuplicate) {
                        if(indexA < bufSizeA) {
                            a2 = bufferA[indexA];
                            if(a1->key == a2->key) {
                                a1 = a2;
                                indexA++;
                                duplicate = true;
                            }
                            else {
                                localDuplicate = false;
                            }
                        }
                        else {
                            localDuplicate = false;
                        }
                    }
                }
                else {
                    reachedEndA;
                    break;
                }
            }
        }
        else {
            newSize++;
            if(a1->key < b1->key) {
                os->write(&a1->key);
                os->write(&a1->value);
                os->write(&a1->time);
                // Read in new KeyValueTime from A
                if(indexA < bufSizeA) {
                    a1 = bufferA[indexA];
                    indexA++;
                    // Look ahead to remove duplicates
                    localDuplicate = true;
                    while(localDuplicate) {
                        if(indexA < bufSizeA) {
                            a2 = bufferA[indexA];
                            if(a1->key == a2->key) {
                                a1 = a2;
                                indexA++;
                                duplicate = true;
                            }
                            else {
                                localDuplicate = false;
                            }
                        }
                        else {
                            localDuplicate = false;
                        }
                    }
                }
                else {
                    reachedEndA;
                    break;
                }
            }
            else {
                os->write(&b1->key);
                os->write(&b1->value);
                os->write(&b1->time);
                if(indexB < bufSizeB) {
                    b1 = bufferB[indexB];
                    indexB++;
                    // Look ahead to remove duplicates
                    localDuplicate = true;
                    while(localDuplicate) {
                        if(indexB < bufSizeB) {
                            b2 = bufferB[indexB];
                            if(b1->key == b2->key) {
                                b1 = a2;
                                indexB++;
                                duplicate = true;
                            }
                            else {
                                localDuplicate = false;
                            }
                        }
                        else {
                            localDuplicate = false;
                        }
                    }
                }
                else {
                    reachedEndB = true;
                    break;
                }
            }
        }
    }
    // Handle remaining elements from the two buffers


    // Clean up
    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);

    if(duplicate) {
        // Size of buffer doesnt match,
        // write out new size.
        OutputStream* os = new BufferedOutputStream(streamBuffer);

    }


    // Clean up
    for(int i = 0; i < bufSizeA; i++) {
        delete(bufferA[i]);
    }
    delete[] bufferA;

    for(int i = 0; i < bufSizeB; i++) {
        delete(bufferB[i]);
    }
    delete[] bufferB;
}
     */
