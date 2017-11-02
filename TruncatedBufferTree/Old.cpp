//
// Created by martin on 11/2/17.
// Used to store deprecated methods,
// and old experiments.
//


/* Kode fra split root, er nu bottom op
// If root needs to split do so.
            // Also splits the buffer and removes the
            // need to flush.
            if(nodeSize == size*4-1) {
                // Special case
                int oldRoot = root;

                // New root
                numberOfNodes++;
                currentNumberOfNodes++;
                root = numberOfNodes;
                int newRootHeight = height+1;
                int* newRootKeys = new int[1];
                int* newRootValues = new int[2];
                newRootValues[0] = oldRoot;
                rootBufferSize = 0;

                // New Child
                int newChildHeight = height;
                int newChildSize = 2*size-1;
                int newChildBufferSize = 0;
                int* ptr_newChildBufferSize = &newChildBufferSize;
                int* newChildKeys = new int[size*4-1];
                int* newChildValues = new int[size*4];

                splitChild(newRootHeight, 0, newRootKeys, newRootValues, 0,
                    ptr_nodeSize, ptr_bufferSize, keys, values,
                    ptr_newChildBufferSize, newChildKeys, newChildValues);

                // Write root, child and parent
                writeNode(oldRoot,height,nodeSize,bufferSize,tempSize,keys,values);
                writeNode(numberOfNodes,newChildHeight,newChildSize,newChildBufferSize,0,newChildKeys,newChildValues);
                writeNode(root,newRootHeight,1,0,0,newRootKeys,newRootValues);

                // Check if we need to flush children
                // TODO:
            }
            // Else flush roots buffer
            else {
                writeNodeInfo(root,height,nodeSize,0,0);
                rootBufferSize = 0;

                // OBS: CHECK VÆRDIER OVENFOR.
                flushToChildren(root,height,nodeSize,keys,values);
            }

/*
 * Splits the child into a new node. Will also split the buffer.
 * Adjusts the nodeSizes and bufferSizes appropriately.
 * Assumes that the childs buffer is sorted!
 */
/*void ExternalBufferedBTree::splitChild(int height, int nodeSize, int *keys, int *values, int childNumber,
                                       int *childSize, int *childBufferSize, int *cKeys, int *cValues,
                                       int *newBufferSize, int *newKeys, int *newValues) {

    numberOfNodes++;
    currentNumberOfNodes++;
    int newChild = numberOfNodes;

    // Fill out new childs keys
    for(int j = 0; j < size-1; j++) {
        newKeys[j] = cKeys[size+j];
    }

    // Copy values
    if(height == 2) {
        // Children are leafs
        for(int j = 0; j < size-1; j++) {
            newValues[j] = cValues[size+j]; // <---?
        }
    }
    else {
        // Children are internal nodes
        for(int j = 0; j < size; j++) {
            newValues[j] = cValues[size+j];
        }
    }

    // Move parents keys by one
    for(int j = nodeSize; j > childNumber; j--) {
        keys[j] = keys[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    keys[childNumber] = cKeys[size-1];

    // Also update size of old child.
    if(height == 2) {
        *childSize = size;
    }
    else {
        *childSize = size-1;
    }

    // Move parents values back by one
    for(int j = nodeSize+1; j > childNumber; j--) {
        values[j] = values[j-1];
    }
    // Insert pointer to new child
    values[childNumber+1] = newChild;

    // Split original childs buffer between itself and new child
    int splitNumber = keys[childNumber];
    InputStream* is = new BufferedInputStream(streamBuffer);
    string name = getBufferName(values[childNumber],1);
    is->open(name.c_str());
    OutputStream* os = new BufferedOutputStream(streamBuffer);
    name = getBufferName(numberOfNodes,1);
    os->create(name.c_str());

    int counter = 0;

    // Run through the original childs buffer until we reach the split element.
    int childKey = is->readNext();
    int childValue = is->readNext();
    int childTime = is->readNext();
    while(childKey <= splitNumber && counter < *childBufferSize) {
        childKey = is->readNext();
        childValue = is->readNext();
        childTime = is->readNext();
    }

    // If we didnt reach end of stream, and key isnt <= split
    if(!is->endOfStream() && childKey > splitNumber) {
        // Write remaining elements to new childs buffer
        os->write(&childKey);
        os->write(&childValue);
        os->write(&childTime);
        while(!is->endOfStream()) {
            childKey = is->readNext();
            childValue = is->readNext();
            childTime = is->readNext();
            os->write(&childKey);
            os->write(&childValue);
            os->write(&childTime);
        }
    }

    // Clean up
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);
    os->close();


    // Update childBufferSize and newChildBufferSize.

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
