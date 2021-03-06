//
// Created by martin on 1/15/18.
//
// Note that we use "KeyValue" elements with a forward and reverse pointer.
// That is each element in a buffer, except on the minimum sized xBox, consists
// of 4 longs.
//

#define _FILE_OFFSET_BITS 64
#define _LARGEFILE64_SOURCE

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <cmath>
#include <climits>
#include <cstring>
#include <iostream>
#include "XDict.h"

using namespace std;

/*
 * Set up an xDict containing an empty xBox consisting of an array of longs.
 */
XDict::XDict(double a) {

    alpha = a;
    elementSize = 4;
    booleanSize = 2;

    //minX = 64;
    minX = 8; // Absolute minimum, since a proper xBox needs to be at least 64.
    minX = 16; // Actually this, since we need at least one topmost xBox, not two. That requires x=16, so we cut off everything below.
    long minXalpha = (long) pow(minX,1+alpha);
    minSize = minXalpha*elementSize+3; // x, #real elements, the elements, and -1 at the end
    infoOffset = 10;

    latestSize = minSize;
    latestX = minX;

    fileSize = latestSize;
    fileName = "xDict";

    insertionCap = minXalpha/2; // The first xBox holds at most this number of real elements

    // Delete file if it already exists
    if(FILE *file = fopen(fileName.c_str(), "r")) {
        fclose(file);
        remove(fileName.c_str());
    }

    /*
     * Set up mmap
     */

    // Open original file
    fileDescriptor = open64(fileName.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    if (fileDescriptor == -1) {
        perror("Error creating original file");
        exit(EXIT_FAILURE);
    }

    // Extend file to appropriate size
    __off_t result =  lseek64(fileDescriptor, fileSize*sizeof(long), SEEK_SET);
    if (result == -1) {
        close(fileDescriptor);
        perror("Error original lseek");
        exit(EXIT_FAILURE);
    }

    // Write to end of file
    // Empty string = single byte containing '0'.
    result = write(fileDescriptor, "", 1);
    if (result != 1) {
        close(fileDescriptor);
        perror("Error original write to EOF");
        exit(EXIT_FAILURE);
    }

    // Map file
    map = (long*) mmap64(0, fileSize*sizeof(long), PROT_READ | PROT_WRITE, MAP_SHARED, fileDescriptor, 0);
    if (map == MAP_FAILED) {
        close(fileDescriptor);
        perror("Error mmap original file");
        exit(EXIT_FAILURE);
    }

    // Lay out initial xBox
    layoutXBox(0,minX);
    xBoxes = new vector<long>();
    xBoxes->push_back(0);
}

XDict::~XDict() {

    delete(xBoxes);

    // Close down mapping
    if(munmap(map, fileSize*sizeof(long)) == -1) {
        printf ("Error errno is: %s\n",strerror(errno));
        perror("Error unmapping the file");
        exit(EXIT_FAILURE);
    }
    // Close file
    ::close(fileDescriptor);

}

/***********************************************************************************************************************
 * General Methods
 **********************************************************************************************************************/

/*
 * Single element insert into the xDict.
 * This method handles the underlying BatchInsertions on xBoxes.
 */
void XDict::insert(KeyValue kvt) {

    //cout << "Inserting " << kvt.key << "\n";

    //*** First merge the element into the first xBox

    // Scan the array until we reach the end
    long index = map[1]; // This is the absolute minimum index without pointers.
    while(map[2+index*elementSize] != -1) {
        index++;
    }

    long endOfElements = index;
    // Insert -1 at new end
    map[2+(index+1)*elementSize] = -1;

    if(index == 0) {
        map[2] = kvt.key;
        map[3] = kvt.value;
    }
    else {
        // Run backwards moving elements back until we can insert the element
        while(index > 0 && kvt.key < map[2+(index-1)*elementSize]) {
            map[2+index*elementSize] = map[2+(index-1)*elementSize];
            map[2+index*elementSize+1] = map[2+(index-1)*elementSize+1];
            map[2+index*elementSize+2] = map[2+(index-1)*elementSize+2];
            map[2+index*elementSize+3] = map[2+(index-1)*elementSize+3];
            index--;
        }
        // Insert element
        map[2+index*elementSize] = kvt.key;
        map[2+index*elementSize+1] = kvt.value;
    }

    // ++
    map[1] = map[1]+1;

    long forward = 0;
    long backward = 0;

    /* We do not maintain forward and backward in topmost xBox.

    // Find forward and backward pointer
    if(index > 0 && index < endOfElements) {
        // Inherit pointers from your neighbours
        if(map[2+(index-1)*elementSize+1] > 0) {
            backward = map[2+(index-1)*elementSize+2];
        }
        else {
            backward = 2+(index-1)*elementSize;
        }
        if(map[2+(index+1)*elementSize+1] > 0) {
            forward = map[2+(index+1)*elementSize+3];
        }
        else {
            forward = 2+(index+1)*elementSize;
        }
    }
    else if(index < endOfElements) {
        // We are the first element
        if(map[2+(index+1)*elementSize+1] > 0) {
            forward = map[2+(index+1)*elementSize+3];
        }
        else {
            forward = 2+(index+1)*elementSize;
        }
    }
    else if(index > 0) {
        // We are the last element
        if(map[2+(index-1)*elementSize+1] > 0) {
            backward = map[2+(index-1)*elementSize+2];
        }
        else {
            backward = 2+(index-1)*elementSize;
        }
    }
    else {
        // First element
        backward = 0;
        forward = 0;
    } */
    map[2+index*elementSize+2] = backward;
    map[2+index*elementSize+3] = forward;

    // Print
    /*//cout << "After insertion of " << kvt.key << " first xBox contains:\n";
    index = 0;
    while(map[2+index*elementSize] != -1) {
        //cout << map[2+index*elementSize] << " ";
        index++;
    }
    //cout << "\n";*/


    // We saved minX^(1+alpha) / 2 in insertionCap
    //*** Are we at capacity?
    if(map[1] >= insertionCap) {
        //cout << "=== Check before recursive on top xBox\n";
        //recursivelyCheckxBox(0);
        recursivelyBatchInsertXBoxToXBox(0);
    }
}

void XDict::addXBox() {

    // Expand mapping
    long x = pow(latestX,1+alpha);
    /*if(x < 64) {
        x = 64; // Smallest allowable
    }*/
    long pointer = fileSize;
    long newSize = recursivelyCalculateSize(x);
    extendMapTo(fileSize+newSize);

    latestX = x;

    // Layout xBox
    long endBox = layoutXBox(pointer,x);

    //cout << "New xBox ending before " << endBox << "\n";

    // Add to vector
    xBoxes->push_back(pointer);

}

/*
 * Query down through the xDict
 */
long XDict::query(long element) {

    long ret = -1;
    long xBoxPointer = 0;

    // Naive search of the first xBox, which is just an array.
    // Notice the first xBox is of minimum size, and a special Search is made.
    //ret = search(xBoxPointer,2,element);

    // New custom search of topmost xBox, since we dont maintain forward and backward
    long index = 0;
    ret = 0;
    while(map[2+index*elementSize] != -1 && map[2+index*elementSize] < element) {
        if(map[2+index*elementSize+1] < 0) {
            ret = map[2+index*elementSize+2];
        }
        index++;
    }

    if(map[2+index*elementSize] == element) {

        if(map[2+index*elementSize+1] > 0) {
            return map[2+index*elementSize+1];
        }
        else {
            // More elements with the same key?
            long tInd = index;
            while(map[2+tInd*elementSize] != -1 && map[2+tInd*elementSize] == element) {
                if(map[2+tInd*elementSize+1] > 0) {
                    return map[2+tInd*elementSize+1];
                }
                tInd++;
            }
        }

    }

    ////cout << "Search on first xBox = " << ret << " " << map[ret] << " " << map[ret+1]<< "\n";

    if(map[ret] == element && map[ret+1] > 0) {
        return map[ret+1]; // If only it was always this easy!
    }

    if(ret == 0) {
        ret = xBoxes->at(1) + infoOffset;
    }

    // We now have a pointer into the first real xBox.
    // Recursively use this pointer to search down the xDict.
    for(int i = 1; i < xBoxes->size(); i++) {
        xBoxPointer = xBoxes->at(i);
        ret = search(xBoxPointer,ret,element);
        if(ret == -1) {
            return -1; // Was an error, i.e. element not present and we reached the end.
        }
        if(map[ret] == element && map[ret+1] > 0) {
            return map[ret+1];
        }
        if(ret == 0 && i != xBoxes->size()-1) {
            ret = xBoxes->at(i+1) + infoOffset;
        }
    }

    // Old, now always holds true.
    if(ret != -1) {
        if(map[ret] == element && map[ret+1] > 0) {
            return map[ret+1];
        }
    }
    return -1;
}



/***********************************************************************************************************************
 * Standard Methods, from the paper
 **********************************************************************************************************************/

/*
 * BatchInserts elements marked by pointers.
 * Important that numberOfRealElements is set correct!
 * Notice that pointerStart includes the last element, but pointerEnd is AFTER the last element.
 */
void XDict::batchInsert(long xBoxPointer, long pointerStart, long pointerEnd, long numberOfRealElements) {

    /*cout << "========== BatchInsert on xBox located at " << xBoxPointer << " " << pointerStart << " " << pointerEnd << " " << numberOfRealElements << " " << map[xBoxPointer+1] << " " << map[xBoxPointer]<< "\n";
    cout << "First element is " << map[pointerStart] << " " << map[pointerStart+1] << "\n";*/

    /*cout << "Elements to be inserted: ";
    long tempPointerIndex = pointerStart;
    while(tempPointerIndex != pointerEnd) {
        cout << map[tempPointerIndex] << " ";
        cout << map[tempPointerIndex+1] << " ";
        cout << map[tempPointerIndex+2] << " ";
        cout << map[tempPointerIndex+3] << " | ";
        tempPointerIndex = tempPointerIndex + 4;
    }
    cout << "\n";*/

        // *** If this is a simple subbox we merge while preserving all pointers in subbox, then return
    if(map[xBoxPointer] <= minX) {

        /*long tInd = 0;
        cout << "Elements already present: ";
        while(map[xBoxPointer+2+tInd*elementSize] != -1) {
            cout << map[xBoxPointer+2+tInd*elementSize] << " ";
            cout << map[xBoxPointer+2+tInd*elementSize+1] << " ";
            cout << map[xBoxPointer+2+tInd*elementSize+2] << " ";
            cout << map[xBoxPointer+2+tInd*elementSize+3] << " | ";
            tInd++;
        }
        cout << "\n";*/

        //cout << "Simple xBox\n";

        map[xBoxPointer+1] = map[xBoxPointer+1] + numberOfRealElements;

        long index = 0;
        long pointerToInputBuffer = xBoxPointer+2;
        while(map[pointerToInputBuffer+index*elementSize] != -1) {
            index++;
        }

        map[pointerToInputBuffer+(index+numberOfRealElements)*elementSize] = -1; // New end of original elements.
        // Move elements back
        for(long i = index-1; i > -1; i--) {
            map[pointerToInputBuffer+(numberOfRealElements+i)*elementSize] = map[pointerToInputBuffer+i*elementSize];
            map[pointerToInputBuffer+(numberOfRealElements+i)*elementSize+1] = map[pointerToInputBuffer+i*elementSize+1];
            map[pointerToInputBuffer+(numberOfRealElements+i)*elementSize+2] = map[pointerToInputBuffer+i*elementSize+2];
            map[pointerToInputBuffer+(numberOfRealElements+i)*elementSize+3] = map[pointerToInputBuffer+i*elementSize+3];
        }

        long mergeArray[4];
        long subboxPointer = 0; // Used to preserve the lookahead pointers. Reused from below.
        long boolPointer = 0;

        long pointerToStartOfOriginalElements = pointerToInputBuffer + numberOfRealElements*elementSize;

        // Populate merge array
        mergeArray[0] = map[pointerToStartOfOriginalElements];
        mergeArray[1] = map[pointerToStartOfOriginalElements+1];
        subboxPointer = map[pointerToStartOfOriginalElements+2];
        boolPointer = map[pointerToStartOfOriginalElements+3];

        mergeArray[2] = map[pointerStart];
        mergeArray[3] = map[pointerStart+1];

        long writeIndex = 0;
        long insertIndex = 0;
        long originalIndex = 0;

        long key, value;
        long smallestIndex;

        long currentReversePointer = 0;

        /*long numberOfRealElementsAdded = 0;
        long tInd = 0;
        long actualRealElementsToBeInserted = 0;
        while(map[pointerStart+tInd*elementSize] != -1 && pointerStart+tInd*elementSize < pointerEnd) {
            if(map[pointerStart+tInd*elementSize+1] > 0) {
                actualRealElementsToBeInserted++;
            }
            tInd++;
        }
        if(actualRealElementsToBeInserted != numberOfRealElements) {
            tInd = 0;
            while(map[pointerStart+tInd*elementSize] != -1 && pointerStart+tInd*elementSize < pointerEnd) {
                cout << map[pointerStart+tInd*elementSize] << " ";
                cout << map[pointerStart+tInd*elementSize+1] << " ";
                cout << map[pointerStart+tInd*elementSize+2] << " ";
                cout << map[pointerStart+tInd*elementSize+3] << " | ";
                tInd++;
            }
            cout << "\n";
        }*/

        // Perform the merge
        while(true) {

            // First check that none are -1.
            if(mergeArray[0] == -1 && mergeArray[2] == -1) {
                break; // Done
            }

            key = LONG_MAX;
            smallestIndex = 0;
            for(int i = 0; i < 2; i++) {
                if(mergeArray[i*2] != -1 && mergeArray[i*2] < key) {
                    smallestIndex = i;
                    key = mergeArray[i*2];
                    value = mergeArray[i*2+1];
                }
            }

            if(smallestIndex == 0) {

                // Not a lookahead pointer
                map[pointerToInputBuffer+writeIndex*elementSize] = key;
                map[pointerToInputBuffer+writeIndex*elementSize+1] = value;
                if(value > 0) {
                    map[pointerToInputBuffer+writeIndex*elementSize+2] = currentReversePointer;
                    map[pointerToInputBuffer+writeIndex*elementSize+3] = 0;
                }
                else {
                    map[pointerToInputBuffer+writeIndex*elementSize+2] = subboxPointer;
                    map[pointerToInputBuffer+writeIndex*elementSize+3] = boolPointer;
                    currentReversePointer = pointerToInputBuffer+writeIndex*elementSize;
                }
                writeIndex++;

                // Reload!
                originalIndex++;
                mergeArray[0] = map[pointerToStartOfOriginalElements+originalIndex*elementSize];
                mergeArray[1] = map[pointerToStartOfOriginalElements+originalIndex*elementSize+1];
                subboxPointer = map[pointerToStartOfOriginalElements+originalIndex*elementSize+2];
                boolPointer = map[pointerToStartOfOriginalElements+originalIndex*elementSize+3];
            }
            else {
                // Check if we write out this element
                if(value > 0) {
                    // Not a  pointer
                    map[pointerToInputBuffer+writeIndex*elementSize] = key;
                    map[pointerToInputBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToInputBuffer+writeIndex*elementSize+2] = currentReversePointer;
                    map[pointerToInputBuffer+writeIndex*elementSize+3] = 0;
                    writeIndex++;
                    //numberOfRealElementsAdded++;
                }

                // Reload!
                insertIndex++;
                if(pointerStart+insertIndex*elementSize >= pointerEnd) {
                    mergeArray[2] = -1;
                    mergeArray[3] = -1;
                }
                else {
                    mergeArray[2] = map[pointerStart+insertIndex*elementSize];
                    mergeArray[3] = map[pointerStart+insertIndex*elementSize+1];
                }
            }
        }

        // Insert -1 at the end of input.
        //map[pointerStart+writeIndex*elementSize] = -1;

        long currentForwardPointer = 0;
        for(long i = writeIndex-1; i > -1; i--) {
            key = map[pointerToInputBuffer+i*elementSize];
            value = map[pointerToInputBuffer+i*elementSize+1];
            if(value > 0) {
                map[pointerToInputBuffer+i*elementSize+3] = currentForwardPointer;
            }
            else {
                currentForwardPointer = pointerToInputBuffer+i*elementSize;
            }
        }

        /*cout << "Elements in " << xBoxPointer << " after merge into input:\n";
        index = 0;
        while(map[pointerToInputBuffer+index*elementSize] != -1) {
            cout << map[pointerToInputBuffer+index*elementSize] << " ";
            cout << map[pointerToInputBuffer+index*elementSize+1] << " ";
            cout << map[pointerToInputBuffer+index*elementSize+2] << " ";
            cout << map[pointerToInputBuffer+index*elementSize+3] << " | ";
            index++;
        }
        cout << "\n";*/

        /*if(map[0] != 8) {
            cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! xBox 0 was overwritten after batch insert on simple xBox\n";
        }*/

        return;
    }

    /*long tempPointerOutput = map[xBoxPointer+9];
    long tInd = 0;
    while(map[tempPointerOutput+tInd*elementSize] != -1) {
        if(map[tempPointerOutput+tInd*elementSize+1] > 0 && map[tempPointerOutput+tInd*elementSize+2] != 0
           && map[tempPointerOutput+tInd*elementSize+2] < tempPointerOutput) {
            long reversePointer = map[tempPointerOutput+tInd*elementSize+2];
            cout << "*** 1Error, pointing outside output\n";
        }
        tInd++;
    }*/

    /*if(map[map[xBoxPointer+6]+1] == -1) {
        cout << "*** Error before any action taken\n";
    }

    if(fileSize > 1967490 && map[1967490] == -1) {
        cout << "*** 21 " << xBoxPointer << "\n";
    }*/


    // *** Update number of real elements in this xBox with the number of inserted elements
    map[xBoxPointer+1] = map[xBoxPointer+1] + numberOfRealElements;

    // *** Merge elements into input buffer while removing lookahead pointers.

    // Find the subbox with the smallest element while we scan, for use later.
    /*bool subboxSet = false;
    long pointerToCurrentSubbox = 0;
    long pointerDirectlyToBooleanForCurrentSubbox = 0;*/
    long upperLevelSubboxCounter = 0;

    // First scan the input buffer to count the elements.
    long index = 0;
    long pointerToInputBuffer = xBoxPointer+infoOffset;
    while(map[pointerToInputBuffer+index*elementSize] != -1) {
        if(map[pointerToInputBuffer+index*elementSize+1] == -1) {
            upperLevelSubboxCounter++;
            /*if(!subboxSet) {
                pointerToCurrentSubbox = map[pointerToInputBuffer+index*elementSize+2];
                pointerDirectlyToBooleanForCurrentSubbox = map[pointerToInputBuffer+index*elementSize+3];
                subboxSet = true;
            }*/
        }
        index++;
    }
    map[pointerToInputBuffer+(index+numberOfRealElements)*elementSize] = -1; // New end of original elements.
    // Move elements back
    for(long i = index-1; i > -1; i--) {
        map[pointerToInputBuffer+(numberOfRealElements+i)*elementSize] = map[pointerToInputBuffer+i*elementSize];
        map[pointerToInputBuffer+(numberOfRealElements+i)*elementSize+1] = map[pointerToInputBuffer+i*elementSize+1];
        map[pointerToInputBuffer+(numberOfRealElements+i)*elementSize+2] = map[pointerToInputBuffer+i*elementSize+2];
        map[pointerToInputBuffer+(numberOfRealElements+i)*elementSize+3] = map[pointerToInputBuffer+i*elementSize+3];
    }

    long mergeArray[4];
    long subboxPointer = 0; // Used to preserve the subbox pointers as we merge. We throw away lookahead pointers.
    long boolPointer = 0;

    long pointerToStartOfOriginalElements = pointerToInputBuffer + numberOfRealElements*elementSize;
    // Populate merge array
    mergeArray[0] = map[pointerToStartOfOriginalElements];
    mergeArray[1] = map[pointerToStartOfOriginalElements+1];
    subboxPointer = map[pointerToStartOfOriginalElements+2];
    boolPointer = map[pointerToStartOfOriginalElements+3];

    mergeArray[2] = map[pointerStart];
    mergeArray[3] = map[pointerStart+1];

    long writeIndex = 0;
    long insertIndex = 0;
    long originalIndex = 0;
    long insertCounter = 0; // TODO: Do we really need to count these? Removed it for now.

    long key, value;
    long smallestIndex;

    // Perform the merge
    while(true) {

        // First check that none are -1.
        if(mergeArray[0] == -1 && mergeArray[2] == -1) {
            break; // Done
        }

        key = LONG_MAX;
        smallestIndex = 0;
        for(int i = 0; i < 2; i++) {
            if(mergeArray[i*2] != -1 && mergeArray[i*2] < key) {
                smallestIndex = i;
                key = mergeArray[i*2];
                value = mergeArray[i*2+1];
            }
        }

        if(smallestIndex == 0) {
            // Check if we write out this element
            if(value > -2) {
                // Not a lookahead pointer
                map[pointerToInputBuffer+writeIndex*elementSize] = key;
                map[pointerToInputBuffer+writeIndex*elementSize+1] = value;
                map[pointerToInputBuffer+writeIndex*elementSize+2] = subboxPointer;
                map[pointerToInputBuffer+writeIndex*elementSize+3] = boolPointer;
                writeIndex++;
            }

            // Reload!
            originalIndex++;
            mergeArray[0] = map[pointerToStartOfOriginalElements+originalIndex*elementSize];
            mergeArray[1] = map[pointerToStartOfOriginalElements+originalIndex*elementSize+1];
            subboxPointer = map[pointerToStartOfOriginalElements+originalIndex*elementSize+2];
            boolPointer = map[pointerToStartOfOriginalElements+originalIndex*elementSize+3];
        }
        else {
            // Check if we write out this element
            if(value > 0) {
                // Not a  pointer
                map[pointerToInputBuffer+writeIndex*elementSize] = key;
                map[pointerToInputBuffer+writeIndex*elementSize+1] = value;
                map[pointerToInputBuffer+writeIndex*elementSize+2] = 0;
                map[pointerToInputBuffer+writeIndex*elementSize+3] = 0;
                writeIndex++;
                insertCounter++;
            }

            // Reload!
            insertIndex++;
            if(pointerStart+insertIndex*elementSize >= pointerEnd) {
                mergeArray[2] = -1;
                mergeArray[3] = -1;
            }
            else {
                mergeArray[2] = map[pointerStart+insertIndex*elementSize];
                mergeArray[3] = map[pointerStart+insertIndex*elementSize+1];
            }

            /*if(insertCounter < numberOfRealElements) {
                mergeArray[2] = map[pointerStart+insertIndex*elementSize];
                mergeArray[3] = map[pointerStart+insertIndex*elementSize+1];
            }
            else {
                // We already wrote out all the real elements to be inserted.
                mergeArray[-1] = map[pointerStart+insertIndex*elementSize];
            }*/
        }
    }

    // Insert -1 at the end of input.
    //map[pointerStart+writeIndex*elementSize] = -1;
    map[pointerToInputBuffer+writeIndex*elementSize] = -1;

    /*cout << "Elements in " << xBoxPointer << " after merge into input:\n";
    index = 0;
    while(map[pointerToInputBuffer+index*elementSize] != -1) {
        cout << map[pointerToInputBuffer+index*elementSize] << " ";
        cout << map[pointerToInputBuffer+index*elementSize+1] << " ";
        cout << map[pointerToInputBuffer+index*elementSize+2] << " ";
        cout << map[pointerToInputBuffer+index*elementSize+3] << " | ";
        index++;
    }
    cout << "\n";*/

    /*tempPointerOutput = map[xBoxPointer+9];
    tInd = 0;
    while(map[tempPointerOutput+tInd*elementSize] != -1) {
        if(map[tempPointerOutput+tInd*elementSize+1] > 0 && map[tempPointerOutput+tInd*elementSize+2] != 0
           && map[tempPointerOutput+tInd*elementSize+2] < tempPointerOutput) {
            long reversePointer = map[tempPointerOutput+tInd*elementSize+2];
            cout << "*** 1Error, pointing outside output\n";
        }
        tInd++;
    }*/

    /*if(map[map[xBoxPointer+6]+1] == -1) {
        cout << "*** Error after merge to input\n";
    }

    if(fileSize > 1967490 && map[1967490] == -1) {
        cout << "*** 22 " << xBoxPointer << "\n";
    }*/

    // *** Insert elements into upper level subboxes, updating subbox pointers!
    // +
    // *** Handle splits
    index = 0; long startIndex = 0; writeIndex = 0;
    long start = pointerToInputBuffer; long end = 0;
    long counter = 0;
    long sizeOfBatch = (map[xBoxPointer+2] / 2); // y/2
    long maxRealElementsInSubbox = ((long) pow(map[xBoxPointer+2],1+alpha) / 2); // Size of subbox output / 2.
    long maxNumberOfUpperLevelSubboxes = map[xBoxPointer+4];
    long currentSubboxIndex = 0;
    long pointerUpperBool = map[xBoxPointer+6];
    long nextSubboxVal = map[pointerUpperBool+(currentSubboxIndex+1)*booleanSize+1]; // Values below this val is to
                                                                                     // be inserted into current subbox.
    if(nextSubboxVal == 0) {
        nextSubboxVal = LONG_MAX;
    }
    bool aborted = false;
    //long upperLevelSubboxCounter = 0;

    // Find the first subbox. If there are none, create one.
    long pointerToCurrentSubbox = map[pointerUpperBool+currentSubboxIndex*booleanSize];
    //cout << pointerToCurrentSubbox << "\n";
    if(pointerToCurrentSubbox == 0) {
        //cout << "=== No subbox existed, creating new one\n";
        long location = pointerUpperBool + maxNumberOfUpperLevelSubboxes*booleanSize;
        long y = map[xBoxPointer+2];
        layoutXBox(location,y);
        map[pointerUpperBool] = location;
        pointerToCurrentSubbox = location;
        upperLevelSubboxCounter++;
        // Next subbox val already set to max
    }

    //cout << "Inserting into subboxes in batches of " << sizeOfBatch << " maxSubboxSize " << maxRealElementsInSubbox << "\n";
    //cout << "Maximum number of subboxes is " << maxNumberOfUpperLevelSubboxes << "\n";

    while(map[pointerToInputBuffer+index*elementSize] != -1) {

        key = map[pointerToInputBuffer+index*elementSize];
        value = map[pointerToInputBuffer+index*elementSize+1];

        if(value > 0) {
            counter++;
        }
        /*else {
            // Check if we need to update current subbox
            if(map[pointerToInputBuffer+index*elementSize+1] == -1) {
                pointerToCurrentSubbox = map[pointerToInputBuffer+index*elementSize+2];
                pointerDirectlyToBooleanForCurrentSubbox = map[pointerToInputBuffer+index*elementSize+3];
                if(firstSubboxEncountered) {
                    // This batch wasnt big enough to get inserted, reset counter and start
                    end = pointerToInputBuffer+(index+1)*elementSize;
                    start = end;
                    counter = 0;
                }
                else {
                    firstSubboxEncountered = true;
                }
            }
        }*/

        // Check if we are within range
        if(key >= nextSubboxVal) {
            //cout << "Switching subbox from " << pointerToCurrentSubbox << " " << nextSubboxVal << " " << index << " " << key << "\n";
            // Move this aborted batch back to start of array.
            /*for(long i = 0; i < index-startIndex; i++) {
                map[pointerToInputBuffer+writeIndex*elementSize] = map[pointerToInputBuffer+(startIndex+i)*elementSize];
                map[pointerToInputBuffer+writeIndex*elementSize+1] = map[pointerToInputBuffer+(startIndex+i)*elementSize+1];
                map[pointerToInputBuffer+writeIndex*elementSize+2] = map[pointerToInputBuffer+(startIndex+i)*elementSize+1];
                map[pointerToInputBuffer+writeIndex*elementSize+3] = map[pointerToInputBuffer+(startIndex+i)*elementSize+1];
                writeIndex++;
            }*/

            /*for(int i = 0; i < maxNumberOfUpperLevelSubboxes; i++) {
                //cout << map[pointerUpperBool+i*booleanSize] << " ";
                //cout << map[pointerUpperBool+i*booleanSize+1] << " | ";
            }
            //cout << "\n";*/

            // The range ran out, switch subbox
            currentSubboxIndex++;
            pointerToCurrentSubbox = map[pointerUpperBool+currentSubboxIndex*booleanSize];
            nextSubboxVal = map[pointerUpperBool+(currentSubboxIndex+1)*booleanSize+1];
            if(nextSubboxVal == 0 || currentSubboxIndex+1 == maxNumberOfUpperLevelSubboxes) {
                nextSubboxVal = LONG_MAX;
            }
            counter = 0;
            startIndex = index;
            start = pointerToInputBuffer+startIndex*elementSize;
            index--; // Recount this element, could jump several subboxes!
        }
        else if(counter >= sizeOfBatch) {
            //cout << "Reached batch size " << index << "\n";
            //cout << upperLevelSubboxCounter << " " << maxNumberOfUpperLevelSubboxes << "\n";
            //cout << map[pointerToInputBuffer+startIndex*elementSize] << " " << map[pointerToInputBuffer+index*elementSize] << "\n";
            end = pointerToInputBuffer+(index+1)*elementSize;
            // Check if we need to split
            if(map[pointerToCurrentSubbox+1] + counter > maxRealElementsInSubbox) {
                // Do we have room to split?
                if(upperLevelSubboxCounter < maxNumberOfUpperLevelSubboxes) {

                    flush(pointerToCurrentSubbox,false);

                    //cout << "Split\n";
                    splitSubbox(pointerToCurrentSubbox,map[xBoxPointer+3],currentSubboxIndex,maxNumberOfUpperLevelSubboxes,pointerUpperBool);

                    upperLevelSubboxCounter++;
                    // Reset and scan this range again
                    index = startIndex-1; // Rescan this entire batch
                    counter = 0;

                    nextSubboxVal = map[pointerUpperBool+(currentSubboxIndex+1)*booleanSize+1];
                    if(nextSubboxVal == 0 || currentSubboxIndex+1 == maxNumberOfUpperLevelSubboxes) {
                        nextSubboxVal = LONG_MAX;
                    }
                }
                else {
                    // Break and flush upper level. Then merge input + upper level to middle.
                    //cout << "Aborted insertion into subboxes\n";
                    aborted = true;
                    break;
                }

            }
            else {
                // Batch insert into subbox
                batchInsert(pointerToCurrentSubbox,start,end,counter);

                // Mark elements in the batch as zeroed out.
                for(long i = 0; i < index-startIndex+1; i++) {
                    map[pointerToInputBuffer+(startIndex+i)*elementSize] = 0;
                    map[pointerToInputBuffer+(startIndex+i)*elementSize+1] = 0;
                    map[pointerToInputBuffer+(startIndex+i)*elementSize+2] = 0;
                    map[pointerToInputBuffer+(startIndex+i)*elementSize+3] = 0;
                }
                startIndex = index+1;
                start = end;
                counter = 0;
            }
        }
        index++;
    }

    /*tempPointerOutput = map[xBoxPointer+9];
    tInd = 0;
    while(map[tempPointerOutput+tInd*elementSize] != -1) {
        if(map[tempPointerOutput+tInd*elementSize+1] > 0 && map[tempPointerOutput+tInd*elementSize+2] != 0
           && map[tempPointerOutput+tInd*elementSize+2] < tempPointerOutput) {
            long reversePointer = map[tempPointerOutput+tInd*elementSize+2];
            cout << "*** 1Error, pointing outside output\n";
        }
        tInd++;
    }*/

    //cout << "Finished inserting into upper subboxes of xBox " << xBoxPointer << " at index " << index << "\n";
    ////cout << map[pointerToInputBuffer+index*elementSize] << "\n";

    // *** If we finish inserting into the upper level without running out of subboxes we now
    // *** sample from the input buffer of the upper layer, restoring pointers, and return.

    /*if(map[map[xBoxPointer+6]+1] == -1) {
        cout << "Error after insertion into upper level\n";
    }
    if(fileSize > 1967490 && map[1967490] == -1) {
        cout << "*** 23 " << xBoxPointer << "\n";
    }*/

    if(!aborted) {

        // * First scan upper level to determine how many pointers to make room for.
        // * ^ NO, just estimate it.
        long sampleEveryNth = 16;
        long y = map[xBoxPointer+2];
        long maxNumberOfPointersToSample;
        if(y <= minX) {
            long numerator = (long) pow(y,1+alpha); // Actual size of array
            long ratio = numerator/y; // ratio
            sampleEveryNth = 16 * ratio;
            maxNumberOfPointersToSample = upperLevelSubboxCounter * numerator / sampleEveryNth; // Worst case
        }
        else {
            maxNumberOfPointersToSample = upperLevelSubboxCounter * y / sampleEveryNth; // Worst case
        }

        // At least one subbox pointer from each subbox.
        if(maxNumberOfPointersToSample < upperLevelSubboxCounter) {
            maxNumberOfPointersToSample = upperLevelSubboxCounter;
        }

        // * Then rescan input array and remove zeroed out elements that got inserted into suboxxes.
        // * Also remove all pointers.
        //cout << "Removing zero elements from input of " << xBoxPointer << ": ";
        writeIndex = 0;
        index = 0;
        while(map[pointerToInputBuffer+index*elementSize] != -1) {

            //cout << map[pointerToInputBuffer+index*elementSize] << " ";
            //cout << map[pointerToInputBuffer+index*elementSize+1] << " ";
            //cout << map[pointerToInputBuffer+index*elementSize+2] << " ";
            //cout << map[pointerToInputBuffer+index*elementSize+3] << " | ";

            key = map[pointerToInputBuffer+index*elementSize];
            value = map[pointerToInputBuffer+index*elementSize+1];
            if(key != 0 && value > 0) {
                map[pointerToInputBuffer+writeIndex*elementSize] = map[pointerToInputBuffer+index*elementSize];
                map[pointerToInputBuffer+writeIndex*elementSize+1] = map[pointerToInputBuffer+index*elementSize+1];
                map[pointerToInputBuffer+writeIndex*elementSize+2] = 0;
                map[pointerToInputBuffer+writeIndex*elementSize+3] = 0;
                writeIndex++;
            }

            index++;
        }
        //cout << "\n";

        long remainingElements = writeIndex; // TODO was index, but should be writeIndex surely!

        // * Place this array so we have room for pointers from subboxes.

        // TODO this doesnt look right! - Fixed, but keep this place in mind if there are bugs.
        for(long i = remainingElements-1; i > -1; i--) {
            map[pointerToInputBuffer+(i+maxNumberOfPointersToSample)*elementSize] = map[pointerToInputBuffer+i*elementSize];
            map[pointerToInputBuffer+(i+maxNumberOfPointersToSample)*elementSize+1] = map[pointerToInputBuffer+i*elementSize+1];
            map[pointerToInputBuffer+(i+maxNumberOfPointersToSample)*elementSize+2] = map[pointerToInputBuffer+i*elementSize+2];
            map[pointerToInputBuffer+(i+maxNumberOfPointersToSample)*elementSize+3] = map[pointerToInputBuffer+i*elementSize+3];
        }

        map[pointerToInputBuffer+(remainingElements+maxNumberOfPointersToSample)*elementSize] = -1;

        long pointerToRemainingElements = pointerToInputBuffer+maxNumberOfPointersToSample*elementSize;

        // * Now merge the pointers and elements in the input buffer.
        mergeArray[0] = map[pointerToRemainingElements];
        mergeArray[1] = map[pointerToRemainingElements+1];

        mergeArray[2] = map[pointerUpperBool+1]; // TODO WTF!? Ah, its the minimum element in the subbox.
        mergeArray[3] = -1; // Subbox

        pointerToCurrentSubbox = map[pointerUpperBool];
        currentSubboxIndex = 0;
        long currentReversePointer = 0; // Use to build up reverse pointers
        long tempPointer = 0;

        long pointerToInputBufferOfSubbox;
        if(y <= minX) {
            pointerToInputBufferOfSubbox = pointerToCurrentSubbox+2;
        }
        else {
            pointerToInputBufferOfSubbox = pointerToCurrentSubbox+infoOffset;
        }

        counter = 0;
        writeIndex = 0;
        originalIndex = 0;
        insertIndex = 0;

        while(true) {

            // First check that none are -1.
            if(mergeArray[0] == -1 && mergeArray[2] == -1) {
                break; // Done
            }

            key = LONG_MAX;
            smallestIndex = 0;
            for(int i = 0; i < 2; i++) {
                if(mergeArray[i*2] != -1 && mergeArray[i*2] < key) {
                    smallestIndex = i;
                    key = mergeArray[i*2];
                    value = mergeArray[i*2+1];
                }
            }

            if(smallestIndex == 0) {
                // Write
                map[pointerToInputBuffer+writeIndex*elementSize] = key;
                map[pointerToInputBuffer+writeIndex*elementSize+1] = value;
                map[pointerToInputBuffer+writeIndex*elementSize+2] = currentReversePointer;
                map[pointerToInputBuffer+writeIndex*elementSize+3] = 0;
                writeIndex++;

                originalIndex++;
                mergeArray[0] = map[pointerToRemainingElements+originalIndex*elementSize];
                mergeArray[1] = map[pointerToRemainingElements+originalIndex*elementSize+1];
            }
            else {
                // Write
                if(value == -1) {
                    // Subbox pointer
                    map[pointerToInputBuffer+writeIndex*elementSize] = key;
                    map[pointerToInputBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToInputBuffer+writeIndex*elementSize+2] = pointerToCurrentSubbox;
                    map[pointerToInputBuffer+writeIndex*elementSize+3] = pointerUpperBool+currentSubboxIndex*booleanSize;
                    currentReversePointer = pointerToInputBuffer+writeIndex*elementSize;
                    writeIndex++;
                }
                else {
                    // Lookahead
                    map[pointerToInputBuffer+writeIndex*elementSize] = key;
                    map[pointerToInputBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToInputBuffer+writeIndex*elementSize+2] = tempPointer;
                    map[pointerToInputBuffer+writeIndex*elementSize+3] = 0;
                    currentReversePointer = pointerToInputBuffer+writeIndex*elementSize;
                    writeIndex++;
                }

                // Try and sample from same subbox
                insertIndex++;
                counter = 0;
                bool foundSample = false;
                while(map[pointerToInputBufferOfSubbox+insertIndex*elementSize] != -1) {
                    counter++;
                    if(counter == sampleEveryNth) {
                        mergeArray[2] = map[pointerToInputBufferOfSubbox+insertIndex*elementSize];
                        mergeArray[3] = -2; // Lookahead
                        tempPointer = pointerToInputBufferOfSubbox+insertIndex*elementSize;
                        foundSample = true;
                        break;
                    }
                    insertIndex++;
                }
                // If not possible switch subbox
                if(!foundSample) {
                    currentSubboxIndex++;
                    if(currentSubboxIndex < upperLevelSubboxCounter) {
                        pointerToCurrentSubbox = map[pointerUpperBool+currentSubboxIndex*booleanSize];
                        if(y <= minX) {
                            pointerToInputBufferOfSubbox = pointerToCurrentSubbox+2;
                        }
                        else {
                            pointerToInputBufferOfSubbox = pointerToCurrentSubbox+infoOffset;
                        }
                        mergeArray[2] = map[pointerUpperBool+currentSubboxIndex*booleanSize+1];
                        mergeArray[3] = -1; // Subbox
                        insertIndex = 0;
                    }
                        // If no more subboxes are available insert -1
                    else {
                        mergeArray[2] = -1;
                        mergeArray[3] = -1;
                    }
                }
            }
        }
        map[pointerToInputBuffer+writeIndex*elementSize] = -1; // TODO problem? Dont think so.

        // * Restore reverse and forward pointers
        long currentForwardPointer = 0;
        for(long i = writeIndex-1; i > -1; i--) {
            key = map[pointerToInputBuffer+i*elementSize];
            value = map[pointerToInputBuffer+i*elementSize+1];
            if(value > 0) {
                map[pointerToInputBuffer+i*elementSize+3] = currentForwardPointer;
            }
            else {
                currentForwardPointer = pointerToInputBuffer+i*elementSize;
            }
        }

        /*if(map[6] == -1) {
            cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! -1 in input after insertion into upper level of " << xBoxPointer << "\n";
        }*/

        if(map[map[xBoxPointer+6]+1] == -1) {
            cout << "Error after succesfull insertion into upper level\n";
        }
        if(fileSize > 1967490 && map[1967490] == -1) {
            cout << "*** 24 " << xBoxPointer << "\n";
        }

        return;
    }

    /*tempPointerOutput = map[xBoxPointer+9];
    tInd = 0;
    while(map[tempPointerOutput+tInd*elementSize] != -1) {
        if(map[tempPointerOutput+tInd*elementSize+1] > 0 && map[tempPointerOutput+tInd*elementSize+2] != 0
           && map[tempPointerOutput+tInd*elementSize+2] < tempPointerOutput) {
            long reversePointer = map[tempPointerOutput+tInd*elementSize+2];
            cout << "*** 1Error, pointing outside output\n";
        }
        tInd++;
    }*/

    // *** Else we ran out of subboxes. Flush upper level, then merge input and upper level into middle,
    // *** while removing pointers.

    // Flush upper level
    for(long i = 0; i < maxNumberOfUpperLevelSubboxes; i++) {
        if(map[pointerUpperBool+i*booleanSize] != 0) {
            flush(map[pointerUpperBool+i*booleanSize],true);
        }
    }

    long x = map[xBoxPointer];
    long y = map[xBoxPointer+2];

    // Move middle buffer back to accomodate merge
    long pointerToMiddleBuffer = map[xBoxPointer+7];

    /* Dont bother counting, worst case we must move real elements in middle + subbox
     * pointers to the very back of the middle buffer.
    long maxElementsInUpper = maxNumberOfUpperLevelSubboxes * pow(y,1+alpha) / 2;
    long elementsToMove = maxElementsInUpper+x; // + input*/

    // Count the elements to move
    long counterRealElementsAndSubboxPointersInMiddle = 0;
    index = 0;
    while(map[pointerToMiddleBuffer+index*elementSize] != -1) {
        if(map[pointerToMiddleBuffer+index*elementSize+1] > -2) {
            counterRealElementsAndSubboxPointersInMiddle++;
        }
        index++;
    }

    long pointerToLowerBool = map[xBoxPointer+8];
    long endOfMiddle = pointerToLowerBool-1; // -1 for the -1 located at the end.
    long pointerToOriginalElementsInMiddle = endOfMiddle - (counterRealElementsAndSubboxPointersInMiddle*4);

    // Place original elements at end of middle. Run backwards so we dont overwrite any elements.
    writeIndex = 0;
    index--;
    while(index > -1) {
        if(map[pointerToMiddleBuffer+index*elementSize+1] > -2) {
            writeIndex++;
            map[endOfMiddle-writeIndex*elementSize] = map[pointerToMiddleBuffer+index*elementSize];
            map[endOfMiddle-writeIndex*elementSize+1] = map[pointerToMiddleBuffer+index*elementSize+1];
            map[endOfMiddle-writeIndex*elementSize+2] = map[pointerToMiddleBuffer+index*elementSize+2];
            map[endOfMiddle-writeIndex*elementSize+3] = map[pointerToMiddleBuffer+index*elementSize+3];
        }
        index--;
    }

    // Perform 3 way merge into middle
    long threeWayMerge[6];
    currentSubboxIndex = 0;

    threeWayMerge[0] = map[pointerToOriginalElementsInMiddle];
    threeWayMerge[1] = map[pointerToOriginalElementsInMiddle+1];
    subboxPointer = map[pointerToOriginalElementsInMiddle+2];
    long pointerSubBool = map[pointerToOriginalElementsInMiddle+3];

    threeWayMerge[2] = map[pointerToInputBuffer];
    threeWayMerge[3] = map[pointerToInputBuffer+1];

    pointerToCurrentSubbox = map[pointerUpperBool];
    long pointerToCurrentSubboxOutput = 0;
    if(pointerToCurrentSubbox != 0) {
        if(y <= minX) {
            pointerToCurrentSubboxOutput = pointerToCurrentSubbox+2;
        }
        else {
            pointerToCurrentSubboxOutput = map[pointerToCurrentSubbox+9];
        }
        threeWayMerge[4] = map[pointerToCurrentSubboxOutput];
        threeWayMerge[5] = map[pointerToCurrentSubboxOutput+1];
    }
    else {
        threeWayMerge[4] = -1;
        threeWayMerge[5] = -1;
    }

    writeIndex = 0;
    originalIndex = 0;
    long inputIndex = 0;
    long subboxIndex = 0;

    long lowerLevelSubboxCounter = 0;

    while(true) {

        // First check that none are -1.
        if(threeWayMerge[0] == -1 && threeWayMerge[2] == -1 && threeWayMerge[4] == -1) {
            break; // Done
        }

        key = LONG_MAX;
        smallestIndex = 0;
        for(int i = 0; i < 3; i++) {
            if(threeWayMerge[i*2] != -1 && threeWayMerge[i*2] < key) {
                smallestIndex = i;
                key = threeWayMerge[i*2];
                value = threeWayMerge[i*2+1];
            }
        }

        if(smallestIndex == 0) {
            //cout << "Placed " << key << " " << value << " in middle, originally from middle\n";
            map[pointerToMiddleBuffer+writeIndex*elementSize] = key;
            map[pointerToMiddleBuffer+writeIndex*elementSize+1] = value;
            if(value == -1) {
                map[pointerToMiddleBuffer+writeIndex*elementSize+2] = subboxPointer;
                map[pointerToMiddleBuffer+writeIndex*elementSize+3] = pointerSubBool;
                lowerLevelSubboxCounter++;
            }
            else {
                map[pointerToMiddleBuffer+writeIndex*elementSize+2] = 0;
                map[pointerToMiddleBuffer+writeIndex*elementSize+3] = 0;
            }
            writeIndex++;
            originalIndex++;
            threeWayMerge[0] = map[pointerToOriginalElementsInMiddle+originalIndex*elementSize];
            threeWayMerge[1] = map[pointerToOriginalElementsInMiddle+originalIndex*elementSize+1];
            subboxPointer = map[pointerToOriginalElementsInMiddle+originalIndex*elementSize+2];
            pointerSubBool = map[pointerToOriginalElementsInMiddle+originalIndex*elementSize+3];
        }
        else if(smallestIndex == 1) {

            // Remember we might have zeroed out a lot keys in input
            if(key != 0 && value > 0) {
                //cout << "Placed " << key << " " << value << " in middle, originally from input\n";
                map[pointerToMiddleBuffer+writeIndex*elementSize] = key;
                map[pointerToMiddleBuffer+writeIndex*elementSize+1] = value;
                map[pointerToMiddleBuffer+writeIndex*elementSize+2] = 0;
                map[pointerToMiddleBuffer+writeIndex*elementSize+3] = 0;
                writeIndex++;
            }

            inputIndex++;
            threeWayMerge[2] = map[pointerToInputBuffer+inputIndex*elementSize];
            threeWayMerge[3] = map[pointerToInputBuffer+inputIndex*elementSize+1];
        }
        else {

            if(value > 0) {
                //cout << "Placed " << key << " " << value << " in middle, originally from subbox\n";
                map[pointerToMiddleBuffer+writeIndex*elementSize] = key;
                map[pointerToMiddleBuffer+writeIndex*elementSize+1] = value;
                map[pointerToMiddleBuffer+writeIndex*elementSize+2] = 0;
                map[pointerToMiddleBuffer+writeIndex*elementSize+3] = 0;
                writeIndex++;
            }

            subboxIndex++;
            threeWayMerge[4] = map[pointerToCurrentSubboxOutput+subboxIndex*elementSize];
            threeWayMerge[5] = map[pointerToCurrentSubboxOutput+subboxIndex*elementSize+1];

            if(threeWayMerge[4] == -1) {
                // Try and load in new subbox
                currentSubboxIndex++;
                if(currentSubboxIndex < maxNumberOfUpperLevelSubboxes) {
                    pointerToCurrentSubbox = map[pointerUpperBool+currentSubboxIndex*booleanSize];
                    if(y <= minX) {
                        pointerToCurrentSubboxOutput = pointerToCurrentSubbox+2;
                    }
                    else {
                        pointerToCurrentSubboxOutput = map[pointerToCurrentSubbox+9];
                    }
                    subboxIndex = 0;
                    threeWayMerge[4] = map[pointerToCurrentSubboxOutput+subboxIndex*elementSize];
                    threeWayMerge[5] = map[pointerToCurrentSubboxOutput+subboxIndex*elementSize+1];
                }
            }
        }
    }

    // Mark new end of middle buffer
    map[pointerToMiddleBuffer+writeIndex*elementSize] = -1;

    // Mark input buffer empty
    map[pointerToInputBuffer] = -1;

    // Delete upper subboxes
    for(long i = 0; i < maxNumberOfUpperLevelSubboxes*booleanSize; i++) {
        map[pointerUpperBool+i] = 0;
    }

    aborted = false;

    /*tempPointerOutput = map[xBoxPointer+9];
    tInd = 0;
    while(map[tempPointerOutput+tInd*elementSize] != -1) {
        if(map[tempPointerOutput+tInd*elementSize+1] > 0 && map[tempPointerOutput+tInd*elementSize+2] != 0
           && map[tempPointerOutput+tInd*elementSize+2] < tempPointerOutput) {
            long reversePointer = map[tempPointerOutput+tInd*elementSize+2];
            cout << "*** 1Error, pointing outside output\n";
        }
        tInd++;
    }*/

    /*//cout << "Middle buffer now contains:\n";
    index = 0;
    while(map[pointerToMiddleBuffer+index*elementSize] != -1) {
        //cout << map[pointerToMiddleBuffer+index*elementSize] << " ";
        //cout << map[pointerToMiddleBuffer+index*elementSize+1] << " ";
        //cout << map[pointerToMiddleBuffer+index*elementSize+2] << " ";
        //cout << map[pointerToMiddleBuffer+index*elementSize+3] << " | ";
        index++;
    }
    //cout << "\n";*/

    // *** Insert elements into lower level subboxes, updating subbox pointers!
    // +
    // *** Handle splits

    long maxNumberOfLowerLevelSubboxes = map[xBoxPointer+5];

    //cout << "=== Inserting from middle into subboxes in batches of " << sizeOfBatch << " maxSubboxSize " << maxRealElementsInSubbox << "\n";
    //cout << "Maximum number of subboxes is " << maxNumberOfLowerLevelSubboxes << "\n";

    index = 0;
    counter = 0;
    start = pointerToMiddleBuffer;
    end = pointerToMiddleBuffer;
    startIndex = 0;
    currentSubboxIndex = 0;

    nextSubboxVal = map[pointerToLowerBool+(currentSubboxIndex+1)*booleanSize+1];
    if(nextSubboxVal == 0) {
        nextSubboxVal = LONG_MAX;
    }

    // Find the first subbox. If there are none, create one.
    pointerToCurrentSubbox = map[pointerToLowerBool+currentSubboxIndex*booleanSize];
    if(pointerToCurrentSubbox == 0) {
        //cout << "=== No subbox existed, creating new one\n";
        long location = pointerToLowerBool + maxNumberOfLowerLevelSubboxes*booleanSize;
        layoutXBox(location,y);
        map[pointerToLowerBool] = location;
        pointerToCurrentSubbox = location;
        lowerLevelSubboxCounter++;
        // Next subbox val already set to max
    }

    while(map[pointerToMiddleBuffer+index*elementSize] != -1) {

        key = map[pointerToMiddleBuffer+index*elementSize];
        value = map[pointerToMiddleBuffer+index*elementSize+1];

        if(value > 0) {
            counter++;
        }

        // Check if we are within range
        if(key >= nextSubboxVal) {
            //cout << "Switching subbox from " << pointerToCurrentSubbox << " " << nextSubboxVal << " " << index << " " << key << "\n";

            /*for(int i = 0; i < maxNumberOfLowerLevelSubboxes; i++) {
                //cout << map[pointerToLowerBool+i*booleanSize] << " ";
                //cout << map[pointerToLowerBool+i*booleanSize+1] << " | ";
            }
            //cout << "\n";*/

            // The range ran out, switch subbox
            currentSubboxIndex++;
            pointerToCurrentSubbox = map[pointerToLowerBool+currentSubboxIndex*booleanSize];
            nextSubboxVal = map[pointerToLowerBool+(currentSubboxIndex+1)*booleanSize+1];
            if(nextSubboxVal == 0 || currentSubboxIndex+1 == maxNumberOfLowerLevelSubboxes) {
                nextSubboxVal = LONG_MAX;
            }
            counter = 0;
            startIndex = index;
            start = pointerToMiddleBuffer+startIndex*elementSize;
            index--; // Recount this element, could jump several subboxes!
        }
        else if(counter >= sizeOfBatch) {
            //cout << "Reached batch size " << index << "\n";
            //cout << lowerLevelSubboxCounter << " " << maxNumberOfLowerLevelSubboxes << "\n";
            end = pointerToMiddleBuffer+(index+1)*elementSize;
            // Check if we need to split
            if(map[pointerToCurrentSubbox+1] + counter > maxRealElementsInSubbox) {
                // Do we have room to split?
                if(lowerLevelSubboxCounter < maxNumberOfLowerLevelSubboxes) {

                    /*cout << "999 Checking before flush before split\n";
                    recursivelyCheckxBox(pointerToCurrentSubbox);*/
                    flush(pointerToCurrentSubbox,false);
                    /*cout << "888 Checking after flush before split\n";
                    recursivelyCheckxBox(pointerToCurrentSubbox);*/

                    splitSubbox(pointerToCurrentSubbox,map[xBoxPointer+3],currentSubboxIndex,maxNumberOfLowerLevelSubboxes,pointerToLowerBool);

                    lowerLevelSubboxCounter++;
                    // Reset and scan this range again
                    index = startIndex-1; // Rescan this entire batch
                    counter = 0;

                    nextSubboxVal = map[pointerToLowerBool+(currentSubboxIndex+1)*booleanSize+1];
                    if(nextSubboxVal == 0 || currentSubboxIndex+1 == maxNumberOfLowerLevelSubboxes) {
                        nextSubboxVal = LONG_MAX;
                    }
                }
                else {
                    // Break and flush upper level. Then merge input + upper level to middle.
                    //cout << "Aborted insertion into subboxes\n";
                    aborted = true;
                    break;
                }

            }
            else {
                // Batch insert into subbox
                batchInsert(pointerToCurrentSubbox,start,end,counter);

                // Mark elements in the batch as zeroed out.
                /*cout << "Zeroing beginning at element " << map[pointerToMiddleBuffer+(startIndex)*elementSize]
                     << " and last element " << map[pointerToMiddleBuffer+(startIndex+index-startIndex+1)*elementSize] << "\n";*/
                for(long i = 0; i < index-startIndex+1; i++) {
                    map[pointerToMiddleBuffer+(startIndex+i)*elementSize] = 0;
                    map[pointerToMiddleBuffer+(startIndex+i)*elementSize+1] = 0;
                    map[pointerToMiddleBuffer+(startIndex+i)*elementSize+2] = 0;
                    map[pointerToMiddleBuffer+(startIndex+i)*elementSize+3] = 0;
                }
                startIndex = index+1;
                start = end;
                counter = 0;
            }
        }
        index++;
    }

    /*if(map[map[xBoxPointer+6]+1] == -1) {
        cout << "Error after merge down into lower level\n";
    }
    if(fileSize > 1967490 && map[1967490] == -1) {
        cout << "*** 241 " << xBoxPointer << "\n";
    }*/

    // *** If we finish inserting into the lower level without running out of subboxes we now
    // *** sample from the input buffer of the lower layer, restoring pointers.
    // *** Notice that the upper level must be empty, sample up from the middle to the input,
    // *** creating the necessary subboxes. Then return.
    if(!aborted) {

        /*if(map[0] != 8) {
            cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! xBox 0 was overwritten before clean up of lower level " << xBoxPointer << "\n";
        }*/

        ////cout << "======================================================================================================\n";

        // * Estimate pointers in lower level.
        long sampleEveryNth = 16;
        long y = map[xBoxPointer+2];
        long maxNumberOfPointersToSample;
        if(y <= minX) {
            long numerator = (long) pow(y,1+alpha); // Actual size of array
            long ratio = numerator/y; // ratio
            sampleEveryNth = 16 * ratio;
            maxNumberOfPointersToSample = lowerLevelSubboxCounter * numerator / sampleEveryNth; // Worst case
        }
        else {
            maxNumberOfPointersToSample = lowerLevelSubboxCounter * y / sampleEveryNth; // Worst case
        }

        // At least one subbox pointer from each subbox.
        if(maxNumberOfPointersToSample < lowerLevelSubboxCounter) {
            maxNumberOfPointersToSample = lowerLevelSubboxCounter;
        }

        // * Then rescan middle array and remove zeroed out elements that got inserted into suboxxes.
        // * Also remove all pointers.
        //cout << "Removing zero elements from midde: ";
        writeIndex = 0;
        index = 0;
        while(map[pointerToMiddleBuffer+index*elementSize] != -1) {

            //cout << map[pointerToMiddleBuffer+index*elementSize] << " ";
            //cout << map[pointerToMiddleBuffer+index*elementSize+1] << " ";
            //cout << map[pointerToMiddleBuffer+index*elementSize+2] << " ";
            //cout << map[pointerToMiddleBuffer+index*elementSize+3] << " | ";

            key = map[pointerToMiddleBuffer+index*elementSize];
            value = map[pointerToMiddleBuffer+index*elementSize+1];
            if(key != 0 && value > 0) {
                map[pointerToMiddleBuffer+writeIndex*elementSize] = key;
                map[pointerToMiddleBuffer+writeIndex*elementSize+1] = value;
                map[pointerToMiddleBuffer+writeIndex*elementSize+2] = 0;
                map[pointerToMiddleBuffer+writeIndex*elementSize+3] = 0;
                writeIndex++;
            }
            index++;
        }
        //cout << "\n";

        /*if(fileSize > 1967490 && map[1967490] == -1) {
            cout << "*** 2421 " << xBoxPointer << "\n";
        }*/

        long remainingElements = writeIndex; // TODO was index, fix above as well

        /*cout << "Remaining elements " << index << " " << writeIndex << " " << maxNumberOfPointersToSample << " "
             << lowerLevelSubboxCounter << "\n";*/

        // * Place this array so we have room for pointers from subboxes.

        // TODO this doesnt look right! - Fixed, but obv. place to check if bugs. Also see line ~755

        //cout << "Moving elements in middle buffer:\n";
        for(long i = remainingElements-1; i > -1; i--) {
            //cout << map[pointerToMiddleBuffer+i*elementSize] << " ";
            map[pointerToMiddleBuffer+(i+maxNumberOfPointersToSample)*elementSize] = map[pointerToMiddleBuffer+i*elementSize];
            map[pointerToMiddleBuffer+(i+maxNumberOfPointersToSample)*elementSize+1] = map[pointerToMiddleBuffer+i*elementSize+1];
            map[pointerToMiddleBuffer+(i+maxNumberOfPointersToSample)*elementSize+2] = map[pointerToMiddleBuffer+i*elementSize+2];
            map[pointerToMiddleBuffer+(i+maxNumberOfPointersToSample)*elementSize+3] = map[pointerToMiddleBuffer+i*elementSize+3];
        }
        //cout << "\n";

        map[pointerToMiddleBuffer+(remainingElements+maxNumberOfPointersToSample)*elementSize] = -1;

        /*if(fileSize > 1967490 && map[1967490] == -1) {
            cout << "*** 2422 " << xBoxPointer << "\n";
        }*/

        long pointerToRemainingElements = pointerToMiddleBuffer + maxNumberOfPointersToSample*elementSize;

        // * Now merge the pointers and elements in the middle buffer.
        mergeArray[0] = map[pointerToRemainingElements];
        mergeArray[1] = map[pointerToRemainingElements+1];

        mergeArray[2] = map[pointerToLowerBool+1];
        mergeArray[3] = -1; // Subbox

        pointerToCurrentSubbox = map[pointerToLowerBool];
        currentSubboxIndex = 0;
        long currentReversePointer = 0; // Use to build up reverse pointers
        long tempPointer = 0;

        long pointerToInputBufferOfSubbox;
        if(y <= minX) {
            pointerToInputBufferOfSubbox = pointerToCurrentSubbox+2;
        }
        else {
            pointerToInputBufferOfSubbox = pointerToCurrentSubbox+infoOffset;
        }

        counter = 0;
        writeIndex = 0;
        originalIndex = 0;
        insertIndex = 0;

        while(true) {

            // First check that none are -1.
            if(mergeArray[0] == -1 && mergeArray[2] == -1) {
                break; // Done
            }

            key = LONG_MAX;
            smallestIndex = 0;
            for(int i = 0; i < 2; i++) {
                if(mergeArray[i*2] != -1 && mergeArray[i*2] < key) {
                    smallestIndex = i;
                    key = mergeArray[i*2];
                    value = mergeArray[i*2+1];
                }
            }

            if(smallestIndex == 0) {
                // Write
                map[pointerToMiddleBuffer+writeIndex*elementSize] = key;
                map[pointerToMiddleBuffer+writeIndex*elementSize+1] = value;
                map[pointerToMiddleBuffer+writeIndex*elementSize+2] = currentReversePointer;
                map[pointerToMiddleBuffer+writeIndex*elementSize+3] = 0;
                writeIndex++;

                originalIndex++;
                mergeArray[0] = map[pointerToRemainingElements+originalIndex*elementSize];
                mergeArray[1] = map[pointerToRemainingElements+originalIndex*elementSize+1];
            }
            else {
                // Write
                if(value == -1) {
                    // Subbox pointer
                    map[pointerToMiddleBuffer+writeIndex*elementSize] = key;
                    map[pointerToMiddleBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToMiddleBuffer+writeIndex*elementSize+2] = pointerToCurrentSubbox;
                    map[pointerToMiddleBuffer+writeIndex*elementSize+3] = pointerUpperBool+currentSubboxIndex*booleanSize;
                    currentReversePointer = pointerToMiddleBuffer+writeIndex*elementSize;
                    writeIndex++;
                }
                else {
                    // Lookahead
                    map[pointerToMiddleBuffer+writeIndex*elementSize] = key;
                    map[pointerToMiddleBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToMiddleBuffer+writeIndex*elementSize+2] = tempPointer;
                    map[pointerToMiddleBuffer+writeIndex*elementSize+3] = 0;
                    currentReversePointer = pointerToMiddleBuffer+writeIndex*elementSize;
                    writeIndex++;
                }

                // Try and sample from same subbox
                insertIndex++;
                counter = 0;
                bool foundSample = false;
                while(map[pointerToInputBufferOfSubbox+insertIndex*elementSize] != -1) {
                    counter++;
                    if(counter == sampleEveryNth) {
                        mergeArray[2] = map[pointerToInputBufferOfSubbox+insertIndex*elementSize];
                        mergeArray[3] = -2; // Lookahead
                        tempPointer = pointerToInputBufferOfSubbox+insertIndex*elementSize;
                        foundSample = true;
                        break;
                    }
                    insertIndex++;
                }
                // If not possible switch subbox
                if(!foundSample) {
                    currentSubboxIndex++;
                    if(currentSubboxIndex < lowerLevelSubboxCounter) {
                        pointerToCurrentSubbox = map[pointerToLowerBool+currentSubboxIndex*booleanSize];
                        if(y <= minX) {
                            pointerToInputBufferOfSubbox = pointerToCurrentSubbox+2;
                        }
                        else {
                            pointerToInputBufferOfSubbox = pointerToCurrentSubbox+infoOffset;
                        }
                        mergeArray[2] = map[pointerToLowerBool+currentSubboxIndex*booleanSize+1];
                        mergeArray[3] = -1; // Subbox
                        insertIndex = 0;
                    }
                        // If no more subboxes are available insert -1
                    else {
                        mergeArray[2] = -1;
                        mergeArray[3] = -1;
                    }
                }
            }
        }
        map[pointerToMiddleBuffer+writeIndex*elementSize] = -1;

        /*if(fileSize > 1967490 && map[1967490] == -1) {
            cout << "*** 2423 " << xBoxPointer << "\n";
        }*/


        // * Restore reverse and forward pointers
        long currentForwardPointer = 0;
        for(long i = writeIndex-1; i > -1; i--) {
            key = map[pointerToMiddleBuffer+i*elementSize];
            value = map[pointerToMiddleBuffer+i*elementSize+1];
            if(value > 0) {
                map[pointerToMiddleBuffer+i*elementSize+3] = currentForwardPointer;
            }
            else {
                currentForwardPointer = pointerToMiddleBuffer+i*elementSize;
            }
        }

        // * Now we must sample up from the middle, to the upper level, and from there to the input.

        // Calculate how many elements to sample
        long sampleEveryNthElement = 0;
        if(y > minX) {
            sampleEveryNthElement = 16; // Subbox is a normal subbox
        }
        else {
            // Special case, subbox is an array much larger than a normal input array.
            // Therefore we sample less than every 16th element!
            // Multiply 16 with the increased ratio
            long numerator = (long) pow(y,1+alpha); // Actual size of array
            long ratio = numerator/y; // ratio
            sampleEveryNthElement = 16 * ratio;
        }

        // Calculate size of subbox output
        /*long power = (long) pow(x,0.5 + alpha/2); // Size of output buffer of subbox
        if(y <= minX) {
            power = pow(y,1 + alpha); // Single array
        }*/
        long power = (long) pow(y,1 + alpha);

        // Create the appropriate amount of upper level subboxes
        long numberOfPointersToCreate = writeIndex/16;
        /*if(writeIndex % 16 != 0) {
            numberOfPointersToCreate++;
        }*/
        numberOfPointersToCreate++;

        if(numberOfPointersToCreate < 1) {
            numberOfPointersToCreate = 1;
        }
        // Power contains output buffer size of subbox
        long upperLevelSubboxesToCreate = numberOfPointersToCreate/(power/2); // Never more than half of max
        if(numberOfPointersToCreate % (power/2) != 0) {
            upperLevelSubboxesToCreate++; // Gotta create them all, divide evenly
        }
        if(upperLevelSubboxesToCreate < 1) {
            upperLevelSubboxesToCreate = 1; // Minimum
        }

        long latestLocation = pointerUpperBool+maxNumberOfUpperLevelSubboxes*booleanSize; // First subbox

        ////cout << "Going to create " << upperLevelSubboxesToCreate << " new upper level subboxes of y = " << y << "\n";
        for(int i = 0; i < upperLevelSubboxesToCreate; i++) {
            ////cout << "Subbox will be located at " << latestLocation << "\n";
            map[pointerUpperBool+i*booleanSize] = latestLocation;
            latestLocation = layoutXBox(latestLocation,y);
            ////cout << latestLocation << "\n";
        }

        // Sample up into the upper level subboxes
        currentSubboxIndex = 0;
        long currentSubboxOutputBuffer;

        long pointersToEachSubbox = numberOfPointersToCreate/upperLevelSubboxesToCreate;
        if(numberOfPointersToCreate % upperLevelSubboxesToCreate != 0) {
            pointersToEachSubbox++;
        }
        counter = 0;
        long sixteenth = 15; // So we make the first element a pointer
        index = 0;

        if(y > minX) {
            // Subboxes are "proper" subboxes, i.e. not arrays.
            currentSubboxOutputBuffer = map[map[pointerUpperBool+currentSubboxIndex*booleanSize]+9];
        }
        else {
            // Subboxes are just arrays
            currentSubboxOutputBuffer = map[pointerUpperBool+currentSubboxIndex*booleanSize] + 2;
        }

        /*if(map[0] != 8) {
            cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! xBox 0 was overwritten before sampling into middle " << xBoxPointer << "\n";
        }*/

        /*long tempIndex = -1;
        //cout << "Middle consists of: ";
        do {
            tempIndex++;
            //cout << map[pointerToMiddleBuffer+tempIndex*elementSize] << " ";
        }
        while(map[pointerToMiddleBuffer+tempIndex*elementSize] != -1);
        //cout << "\n";*/

        //cout << "Pointers to each subbox = " << pointersToEachSubbox << "\n";

        /*cout << pointerToCurrentSubboxOutput << " " << pointersToEachSubbox << " " << numberOfPointersToCreate << " " << writeIndex << "\n";

        for(long i = 0; i < maxNumberOfUpperLevelSubboxes; i++) {
            cout << map[pointerUpperBool+i*booleanSize] << " ";
            cout << map[pointerUpperBool+i*booleanSize+1] << " | ";
        }
        cout << "\n";*/

        // Run through middle creating pointers
        while(map[pointerToMiddleBuffer+index*elementSize] != -1) {
            sixteenth++;
            if(sixteenth == 16) {
                // Create pointer
                if(counter < pointersToEachSubbox) {
                    // Create pointer in current subbox
                    ////cout << "Writing " << map[pointerToMiddle+index*4] << " to " << map[pointerToUpperBoolean+currentSubboxIndex] << "\n";
                    map[currentSubboxOutputBuffer+counter*elementSize] = map[pointerToMiddleBuffer+index*elementSize];
                    map[currentSubboxOutputBuffer+counter*elementSize+1] = -2; // Forward pointer
                    map[currentSubboxOutputBuffer+counter*elementSize+2] = pointerToMiddleBuffer+index*elementSize; // Element in output buffer
                    map[currentSubboxOutputBuffer+counter*elementSize+3] = 0; // Empty
                }
                else {
                    // Switch subbox
                    counter = 0;



                    // Insert -1 at end of current subbox's buffer
                    map[currentSubboxOutputBuffer+pointersToEachSubbox*elementSize] = -1;

                    // Find next subbox
                    currentSubboxIndex++;
                    if(y > minX) {
                        // Subboxes are "proper" subboxes, i.e. not arrays.
                        currentSubboxOutputBuffer = map[map[pointerUpperBool+currentSubboxIndex*booleanSize]+9];
                    }
                    else {
                        // Subboxes are just arrays
                        currentSubboxOutputBuffer = map[pointerUpperBool+currentSubboxIndex*booleanSize] + 2;
                    }

                    //cout << "Switched subbox " << currentSubboxOutputBuffer << " " << index << "\n";

                    // Insert this pointer
                    map[currentSubboxOutputBuffer+counter*elementSize] = map[pointerToMiddleBuffer+index*elementSize];
                    map[currentSubboxOutputBuffer+counter*elementSize+1] = -2; // Forward pointer
                    map[currentSubboxOutputBuffer+counter*elementSize+2] = pointerToMiddleBuffer+index*elementSize; // Element in output buffer
                    map[currentSubboxOutputBuffer+counter*elementSize+3] = 0; // Empty
                }
                sixteenth = 0;
                counter++;
            }
            index++;
        }

        /*if(map[currentSubboxOutputBuffer] == -1) {
            cout << "*** 1Last subbox contains -1 in output\n";
        }

        if(currentSubboxIndex+1 < upperLevelSubboxesToCreate) {
            cout << "*** Didnt push pointer to all created upper subboxes in " << xBoxPointer << "\n";
        }

        if(fileSize > 1967490 && map[1967490] == -1) {
            cout << "*** 2424 " << xBoxPointer << "\n";
        }*/

        /*for(long i = 0; i < maxNumberOfUpperLevelSubboxes; i++) {
            cout << map[pointerUpperBool+i*booleanSize] << " ";
            cout << map[pointerUpperBool+i*booleanSize+1] << " | ";
        }
        cout << "\n";

        if(map[0] != 8) {
            cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! xBox 0 was overwritten after creation of pointers from middle " << xBoxPointer << "\n";
        }*/

        // Insert -1 at the end of the last subbox's buffer
        map[currentSubboxOutputBuffer+counter*elementSize] = -1;

        /*if(map[currentSubboxOutputBuffer] == -1) {
            cout << "*** 2Last subbox contains -1 in output\n";
        }

        if(fileSize > 1967490 && map[1967490] == -1) {
            cout << "*** 2425 " << xBoxPointer << "\n";
        }*/

        /*for(int i = 0; i < upperLevelSubboxesToCreate; i++) {
            long tempPointer = map[pointerUpperBool+i*booleanSize];
            if(map[tempPointer] <= minX) {
                if(map[tempPointer+2] == -1) {
                    cout << "*** About to call sample up on empty simple subbox from " << xBoxPointer << "\n";
                }
            }
            else {
                if(map[map[tempPointer+9]] == -1) {
                    cout << "*** About to call sample up on empty subbox from " << xBoxPointer << "\n";
                    cout << i << " " << upperLevelSubboxesToCreate << " " << maxNumberOfUpperLevelSubboxes << "\n";
                    cout << map[tempPointer+9] << " " << currentSubboxOutputBuffer << " " << currentSubboxIndex << "\n";
                }
            }
        }*/


        // Recursively call SampleUp on the upper level subboxes
        for(int i = 0; i < upperLevelSubboxesToCreate; i++) {
            sampleUpAfterFlush(map[pointerUpperBool+i*booleanSize]);
        }

        /*if(fileSize > 1967490 && map[1967490] == -1) {
            cout << "*** 2426 " << xBoxPointer << "\n";
            /*for(int i = 0; i < maxNumberOfUpperLevelSubboxes; i++) {
                cout << map[pointerUpperBool+i*booleanSize] << " ";
                cout << map[pointerUpperBool+i*booleanSize+1] << " | ";
            }
            cout << "\n";*/
            /*recursivelyPrintXBox(1967491);
            cout << std::flush;

        }*/

        /*if(map[0] != 8) {
            cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! xBox 0 was overwritten before sampling into upper level of " << xBoxPointer << "\n";
        }*/

        // Sample from the upper level subboxes into the input buffer
        //cout << "Sampling every " << sampleEveryNthElement << "th element in upper level to input\n";

        writeIndex = -1; // So we can count up every time
        long pointerToInput = xBoxPointer+infoOffset;
        // Assume the subboxes where place in appropriate sequence (they are)
        for(int i = 0; i < upperLevelSubboxesToCreate; i++) {
            long pointerToSubbox = map[pointerUpperBool+i*booleanSize];
            long pointerToSubboxInput = 0;
            long smallestKeyInSubbox = 0;
            if(y > minX) {
                pointerToSubboxInput = pointerToSubbox + infoOffset;
                smallestKeyInSubbox = map[map[pointerToSubbox+9]];
            }
            else {
                pointerToSubboxInput = pointerToSubbox+2;
                smallestKeyInSubbox = map[pointerToSubboxInput];
            }
            // Update
            map[pointerUpperBool+i*booleanSize+1] = smallestKeyInSubbox;
            // Create subbox pointer
            writeIndex++;
            map[pointerToInput+writeIndex*elementSize] = smallestKeyInSubbox; // Key
            map[pointerToInput+writeIndex*elementSize+1] = -1; // Subbox pointer
            map[pointerToInput+writeIndex*elementSize+2] = pointerToSubbox;
            map[pointerToInput+writeIndex*elementSize+3] = pointerUpperBool+i*booleanSize;

            // Read elements
            long subboxIndex = 1;
            long counter = 0;
            while(map[pointerToSubboxInput+subboxIndex*elementSize] != -1) {
                counter++;
                if(counter == sampleEveryNthElement) {
                    // Sample this element
                    writeIndex++;
                    map[pointerToInput+writeIndex*elementSize] = map[pointerToSubboxInput+subboxIndex*elementSize]; // Key
                    map[pointerToInput+writeIndex*elementSize+1] = -2; // Forward pointer
                    map[pointerToInput+writeIndex*elementSize+2] = pointerToSubboxInput+subboxIndex*elementSize; // Point directly to this element
                    map[pointerToInput+writeIndex*elementSize+3] = 0;
                    counter = 0;
                }
                subboxIndex++;
            }
            //cout << "Counter=" << counter << "\n";
        }
        // Insert -1 at end of array
        writeIndex++;
        map[pointerToInput+writeIndex*elementSize] = -1;

        //cout << "Created " << writeIndex << " pointers in input buffer\n";

        /*if(map[6] == -1) {
            cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! -1 in input after insertion into lower level of " << xBoxPointer << "\n";
        }*/

        /*if(map[map[xBoxPointer+6]+1] == -1) {
            cout << "Error after succesfull insertion into lower level\n";
        }
        if(fileSize > 1967490 && map[1967490] == -1) {
            cout << "*** 242 " << xBoxPointer << "\n";
        }*/

        return;
    }

    /*tempPointerOutput = map[xBoxPointer+9];
    tInd = 0;
    while(map[tempPointerOutput+tInd*elementSize] != -1) {
        if(map[tempPointerOutput+tInd*elementSize+1] > 0 && map[tempPointerOutput+tInd*elementSize+2] != 0
           && map[tempPointerOutput+tInd*elementSize+2] < tempPointerOutput) {
            long reversePointer = map[tempPointerOutput+tInd*elementSize+2];
            cout << "*** 1Error, pointing outside output\n";
        }
        tInd++;
    }*/

    /*if(fileSize > 1967490 && map[1967490] == -1) {
        cout << "*** 25\n";
    }*/

    // *** Else we ran out of subboxes. Flush lower level, then merge middle and lower level into output,
    // *** while removing pointers.

    // Clean up the middle and call flush on this subbox.
    // Flush will flush our lower level and place everything in the output buffer.

    // Remove zeroed out elements from middle
    writeIndex = 0;
    index = 0;
    while(map[pointerToMiddleBuffer+index*elementSize] != -1) {

        key = map[pointerToMiddleBuffer+index*elementSize];
        value = map[pointerToMiddleBuffer+index*elementSize+1];
        if(key != 0 && value > 0) {
            map[pointerToMiddleBuffer+writeIndex*elementSize] = map[pointerToMiddleBuffer+index*elementSize];
            map[pointerToMiddleBuffer+writeIndex*elementSize+1] = map[pointerToMiddleBuffer+index*elementSize+1];
            map[pointerToMiddleBuffer+writeIndex*elementSize+2] = 0;
            map[pointerToMiddleBuffer+writeIndex*elementSize+3] = 0;
            writeIndex++;
        }
        index++;
    }
    map[pointerToMiddleBuffer+writeIndex*elementSize] = -1; // THE END!

    //cout << "==================================================================================================================================\n";

    /*if(map[6] == -1) {
        cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! -1 in input before flush in complete " << xBoxPointer << "\n";
        for(int i = 0; i < 20; i++) {
            cout << map[i] << " ";
        }
        cout << "\n";
    }*/

    /*tempPointerOutput = map[xBoxPointer+9];
    tInd = 0;
    while(map[tempPointerOutput+tInd*elementSize] != -1) {
        if(map[tempPointerOutput+tInd*elementSize+1] > 0 && map[tempPointerOutput+tInd*elementSize+2] != 0
           && map[tempPointerOutput+tInd*elementSize+2] < tempPointerOutput) {
            long reversePointer = map[tempPointerOutput+tInd*elementSize+2];
            cout << "*** 1Error, pointing outside output\n";
        }
        tInd++;
    }*/

    /*cout << "=== Checking xBox " << xBoxPointer << " before final flush\n";
    recursivelyCheckxBox(xBoxPointer);*/

    flush(xBoxPointer,false); // False = preserves pointers.

    /*tInd = 0;
    while(map[tempPointerOutput+tInd*elementSize] != -1) {
        if(map[tempPointerOutput+tInd*elementSize+1] > 0 && map[tempPointerOutput+tInd*elementSize+2] != 0
           && map[tempPointerOutput+tInd*elementSize+2] < tempPointerOutput) {
            cout << "*** 2Error, pointing outside output\n";
        }
        tInd++;
    }*/

    /*if(fileSize > 1967490 && map[1967490] == -1) {
        cout << "*** 26\n";
    }*/

    /*cout << "=== Checking xBox " << xBoxPointer << " AFTER final flush\n";
    recursivelyCheckxBox(xBoxPointer);*/

    //cout << "After flush " << xBoxPointer << " now contains in its output buffer: \n";
    /*index = 0;
    long pointerToOutputBuffer = map[xBoxPointer+9];
    while(map[pointerToOutputBuffer+index*elementSize] != -1) {
        //cout << map[pointerToOutputBuffer+index*elementSize] << " ";
        //cout << map[pointerToOutputBuffer+index*elementSize+1] << " ";
        //cout << map[pointerToOutputBuffer+index*elementSize+2] << " ";
        //cout << map[pointerToOutputBuffer+index*elementSize+3] << " | ";
        index++;
    }
    //cout << "\n";*/

    /*if(map[6] == -1) {
        cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! -1 in input before sampleUp in complete " << xBoxPointer << "\n";
        for(int i = 0; i < 20; i++) {
            cout << map[i] << " ";
        }
        cout << "\n";
    }*/

    // Sample up the entire structure.
    sampleUpAfterFlush(xBoxPointer); // Our state is that of a flushed xBox.

    /*tInd = 0;
    while(map[tempPointerOutput+tInd*elementSize] != -1) {
        if(map[tempPointerOutput+tInd*elementSize+1] > 0 && map[tempPointerOutput+tInd*elementSize+2] != 0
           && map[tempPointerOutput+tInd*elementSize+2] < tempPointerOutput) {
            cout << "*** 3Error, pointing outside output\n";
        }
        tInd++;
    }*/

    /*if(fileSize > 1967490 && map[1967490] == -1) {
        cout << "*** 27\n";
    }*/

    /*if(map[6] == -1) {
        cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! -1 in input after complete " << xBoxPointer << "\n";
        for(int i = 0; i < 20; i++) {
            cout << map[i] << " ";
        }
        cout << "\n";
    }*/

    /*if(map[map[xBoxPointer+6]+1] == -1) {
        cout << "Error after DONE\n";
    }*/
}

// TODO lots of todos to highlight this section
// TODO
// TODO
// TODO
// TODO
// TODO
// TODO
// TODO



/*
 * Flush the xBox pointed at.
 * Recursive indicates wether we should bother restoring pointers,
 * since a recursive call discards them.
 */
void XDict::flush(long pointer, bool recursive) {

    //cout << "FLUSH on xBox at position " << pointer << "\n";

    if(map[pointer] <= minX) {
        /*//cout << "It is a simple xBox and contains: ";
        long index = 0;
        while(map[pointer+2+index*elementSize] != -1) {
            //cout << map[pointer+2+index*elementSize] << " ";
            //cout << map[pointer+2+index*elementSize+1] << " ";
            //cout << map[pointer+2+index*elementSize+2] << " ";
            //cout << map[pointer+2+index*elementSize+3] << " | ";
            index++;
        }
        //cout << "\n";*/
        return; // This xBox is just an array, and is thus by default flushed.
    }

    // Retrieve info
    long x = map[pointer];
    long numberOfRealElements = map[pointer+1];
    long y = map[pointer+2];
    long sizeOfSubboxes = map[pointer+3];
    long numberOfUpperLevelSubboxes = map[pointer+4];
    long numberOfLowerLevelSubboxes = map[pointer+5];
    long pointerToUpperBoolean = map[pointer+6];
    long pointerToMiddleBuffer = map[pointer+7];
    long pointerToLowerBoolean = map[pointer+8];
    long pointerToOutputBuffer = map[pointer+9];

    long pointerToInputBuffer = pointer+infoOffset;

    long upperSubboxCounter = 0;
    long lowerSubboxCounter = 0;

    // Flush all subboxes
    for(int i = 0; i < numberOfUpperLevelSubboxes; i++) {
        if(map[pointerToUpperBoolean+i*booleanSize] != 0) {
            flush(map[pointerToUpperBoolean+i*booleanSize],true);
        }
        else {
            break;
        }
    }

    for(int i = 0; i < numberOfLowerLevelSubboxes; i++) {
        if(map[pointerToLowerBoolean+i*booleanSize] != 0) {
            flush(map[pointerToLowerBoolean+i*booleanSize],true);
        }
        else {
            break;
        }
    }

    // Elements are now placed in potentially five places.
    // The three main buffers and the output buffers of all subboxes.
    // The output buffers of each level of subboxes can be concidered an array.
    // Perform a five way merge to place all elements into the output buffer
    if(y > minX) {
        // Merge with normal subboxes

        /* Basic outline of the merge
         *
         * 1) Scan the output buffer to determine if we are already flushed.
         * While scanning the buffer count the number of real elements.
         * If the real elements in the output buffer matches the number of real
         * elements in the xBox then we are already flushed. Return.
         * It costs O(x^(1+alpha) / B), which is our upper bound.
         *
         * 2) Move the elements in the output buffer back to make room for the
         * remaining real elements. This is done so we can merge directly into
         * the output buffer. It costs O(x^(1+alpha) / B), which is our upper bound.
         * We need the index from step 1 to perform step 2, otherwhise they could
         * have been combined.
         *
         * 3) Perform the five way merge, writing into the output buffer. This costs
         * O(x^(1+alpha) / B), which is our upper bound.
         */

        long writeIndex = 0;
        long numberOfRealElementsInOutputBuffer = 0;

        // 1) Scan output buffer to find proper writeIndex
        while(map[pointerToOutputBuffer+writeIndex*elementSize] != -1) {
            if(map[pointerToOutputBuffer+writeIndex*elementSize+1] > 0) {
                // Real element
                numberOfRealElementsInOutputBuffer++;
            }
            writeIndex++;
        }

        if(numberOfRealElements == numberOfRealElementsInOutputBuffer) {
            map[pointerToInputBuffer] = -1;
            for(long i = 0; i < numberOfUpperLevelSubboxes; i++) {
                map[pointerToUpperBoolean+i*booleanSize] = 0;
                map[pointerToUpperBoolean+i*booleanSize+1] = 0;
            }
            map[pointerToMiddleBuffer] = -1;
            for(long i = 0; i < numberOfLowerLevelSubboxes; i++) {
                map[pointerToLowerBoolean+i*booleanSize] = 0;
                map[pointerToLowerBoolean+i*booleanSize+1] = 0;
            }
            return; // We are already flushed
        }

        // 2) Move the elements in the output buffer back to make room for the remaining real elements.
        long deltaRealElements = numberOfRealElements-numberOfRealElementsInOutputBuffer;
        long elementsToMove = writeIndex; // Includes forward pointers!
        long moveByThisMuch = elementSize * deltaRealElements;

        if(map[pointerToOutputBuffer] != -1) {
            map[pointerToOutputBuffer + moveByThisMuch + elementsToMove*elementSize] = -1;
            for(long i = elementsToMove-1; i > -1; i--) {
                map[pointerToOutputBuffer + moveByThisMuch + i*elementSize+3] = map[pointerToOutputBuffer + i*elementSize+3];
                map[pointerToOutputBuffer + moveByThisMuch + i*elementSize+2] = map[pointerToOutputBuffer + i*elementSize+2];
                map[pointerToOutputBuffer + moveByThisMuch + i*elementSize+1] = map[pointerToOutputBuffer + i*elementSize+1];
                map[pointerToOutputBuffer + moveByThisMuch + i*elementSize] = map[pointerToOutputBuffer + i*elementSize];
            }
        }



        // 3) Perform the five way merge
        // Obs new notations below.

        // Find upper level subbox with smallest element in output buffer
        /*long upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
        long upperSubbox,upperMax,upperBuffer;
        if(upperPointer != -1) {
            upperSubbox = map[upperPointer];
            upperMax = map[upperSubbox+1]; // Real elements in subbox
            upperBuffer = map[upperSubbox+9]; // Its output buffer
        }*/
        long upperSubbox,upperMax,upperBuffer;
        upperSubbox = map[pointerToUpperBoolean+upperSubboxCounter*booleanSize];
        if(upperSubbox != 0) {
            //upperBuffer = upperSubbox+2; // Its output buffer
            upperBuffer = map[upperSubbox+9];
            ////cout << "Upper buffer placed at " << upperBuffer << " " << upperSubbox << "\n";
        }


        /*long lowerPointer = findNextSubboxForMerge(pointerToLowerBoolean,numberOfLowerLevelSubboxes);
        // Its now -1 if none was found, but proceed as if it found one.
        long lowerSubbox,lowerMax,lowerBuffer;
        if(lowerPointer != -1) {
            lowerSubbox = map[lowerPointer];
            lowerMax = map[lowerSubbox+1]; // Real elements in subbox
            lowerBuffer = map[lowerSubbox+9]; // Its output buffer
        }*/

        long lowerSubbox,lowerMax,lowerBuffer;
        lowerSubbox = map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize];
        if(lowerSubbox != 0) {
            //lowerBuffer = lowerSubbox+2; // Its output buffer
            lowerBuffer = map[lowerSubbox+9];
        }

        long upperIndex = 0;
        long lowerIndex = 0;
        long inputIndex = 0;
        long middleIndex = 0;
        long outputIndex = 0;

        long tempPointerOutput = pointerToOutputBuffer + moveByThisMuch; // Points to where we moved the elements
        if(map[pointerToOutputBuffer] == -1) {
            tempPointerOutput = pointerToOutputBuffer;
        }

        /*cout << "Flush moved " << map[pointerToOutputBuffer] << " " << map[pointerToOutputBuffer+1] << " " << map[tempPointerOutput] << " " << map[tempPointerOutput+1] <<"\n";
        cout << pointerToOutputBuffer << " " << tempPointerOutput << "\n";*/

        writeIndex = 0; // Reuse to indicate our position in the final output

        long mergeArray[10]; // Will use this array to merge the five buffers consisting of KeyValues
        // The order of the array is input, upper, middle, lower, output.
        // -1 indicates an empty position / we ran out of elements.

        /*
         * Populate the merge array in five steps
         */

        // 1) Input
        if(map[pointerToInputBuffer] == -1) {
            // Input is empty
            mergeArray[0] = -1;
            mergeArray[1] = -1;
        }
        else {
            // Find first real element in input buffer.
            // Concider that there might only be pointers in the input buffer!
            while(map[pointerToInputBuffer+inputIndex*elementSize+1] < 0 &&
                  map[pointerToInputBuffer+inputIndex*elementSize] != -1) {
                inputIndex++;
            }
            // No real elements (ie we reached the end)?
            if(map[pointerToInputBuffer+inputIndex*elementSize] == -1) {
                // Input is empty
                mergeArray[0] = -1;
                mergeArray[1] = -1;
            }
            else {
                mergeArray[0] = map[pointerToInputBuffer+inputIndex*elementSize]; // Key
                mergeArray[1] = map[pointerToInputBuffer+inputIndex*elementSize+1]; // Value
            }
        }

        // 2) Upper level subboxes
        if(upperSubbox == 0) {
            // We found no upper level subbox
            mergeArray[2] = -1;
            mergeArray[3] = -1;
        }
        else {
            mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
            mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
            /* OLD WAY
            // Find first element that isnt a pointer
            while(map[upperBuffer+upperIndex*elementSize+1] < 0 &&
                  map[upperBuffer+upperIndex*elementSize] != -1) {
                upperIndex++;
            }
            if(map[upperBuffer+upperIndex*elementSize] == -1) {
                mergeArray[2] = -1;
                mergeArray[3] = -1;
            }
            else {
                mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
                mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
            }
             */
        }

        // 3) Middle
        if(map[pointerToMiddleBuffer] == -1) {
            // Input is empty
            mergeArray[4] = -1;
            mergeArray[5] = -1;
        }
        else {
            // Find first real element in buffer.
            // Concider that there might only be pointers in the buffer!
            while(map[pointerToMiddleBuffer+middleIndex*elementSize+1] < 0 &&
                  map[pointerToMiddleBuffer+middleIndex*elementSize] != -1) {
                middleIndex++;
            }
            // No real elements (ie we reached the end)?
            if(map[pointerToMiddleBuffer+middleIndex*elementSize] == -1) {
                // Input is empty
                mergeArray[4] = -1;
                mergeArray[5] = -1;
            }
            else {
                mergeArray[4] = map[pointerToMiddleBuffer+middleIndex*elementSize]; // Key
                mergeArray[5] = map[pointerToMiddleBuffer+middleIndex*elementSize+1]; // Value
            }
        }

        // 4) Lower level subboxes
        if(lowerSubbox == 0) {
            // We found no lower level subbox
            mergeArray[6] = -1;
            mergeArray[7] = -1;
        }
        else {
            mergeArray[6] = map[lowerBuffer+lowerIndex*elementSize];
            mergeArray[7] = map[lowerBuffer+lowerIndex*elementSize+1];
            /* Old way
            // Find first element that isnt a pointer
            while(map[lowerBuffer+lowerIndex*elementSize+1] < 0 &&
                  map[lowerBuffer+lowerIndex*elementSize] != -1) {
                lowerIndex++;
            }
            if(map[lowerBuffer+lowerIndex*elementSize] == -1) {
                mergeArray[6] = -1;
                mergeArray[7] = -1;
            }
            else {
                mergeArray[6] = map[lowerBuffer+lowerIndex*elementSize];
                mergeArray[7] = map[lowerBuffer+lowerIndex*elementSize+1];
            }*/
        }

        // 5) Output (which we moved!)
        long temp1 = 0; // To handle pointers in original output buffer
        long temp2 = 0;
        if(map[tempPointerOutput] == -1) {
            // Input is empty
            mergeArray[8] = -1;
            mergeArray[9] = -1;
        }
        else {
            //cout << "Loading in element from old output\n";
            mergeArray[8] = map[tempPointerOutput+outputIndex*elementSize]; // Key
            mergeArray[9] = map[tempPointerOutput+outputIndex*elementSize+1]; // Value
            temp1 = map[tempPointerOutput+outputIndex*elementSize+2];
            temp2 = map[tempPointerOutput+outputIndex*elementSize+3];
        }

        // Now that the initial mergeArray is populated we continuously write the smallest (>0) element to
        // the output buffer and read in a new element from the relevant buffer. Once a buffer runs
        // out we mark the entry in the array with -1. We know the number of elements to output since
        // we only output the real elements + the pointers in the original output buffer, which

        long latestReversePointer = 0; // Build up reverse pointers as we output

        long totalElementsToOutput = elementsToMove + deltaRealElements;
        long smallest;
        long smallestIndex;
        long key, value;

        /*for(int i = 0; i < 10; i++) {
            cout << mergeArray[i] << " ";
        }
        cout << "\n";*/

        // New merge, where we handle subboxes containing only pointers.
        while(true) {

            // Break?
            if(mergeArray[0] == -1 && mergeArray[2] == -1 && mergeArray[4] == -1
               && mergeArray[6] == -1 && mergeArray[8] == -1) {
                break;
            }

            // Select smallest element by scanning mergeArray
            smallest = LONG_MAX;
            smallestIndex = 0;
            for(int i = 0; i < 5; i++) {
                if(mergeArray[i*2] != -1 && mergeArray[i*2] < smallest) {
                    smallestIndex = i;
                    smallest = mergeArray[i*2];
                }
            }

            key = mergeArray[smallestIndex*2];
            value = mergeArray[smallestIndex*2+1];

            /*for(int i = 0; i < 10; i++) {
                cout << mergeArray[i] << " ";
            }
            cout << "\n";*/

            if(smallestIndex == 0) {
                // Input

                // Do we write this out?
                if(value > 0) {
                    map[pointerToOutputBuffer+writeIndex*elementSize] = key;
                    map[pointerToOutputBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToOutputBuffer+writeIndex*elementSize+2] = latestReversePointer;
                    map[pointerToOutputBuffer+writeIndex*elementSize+3] = 0; // No forward pointer
                    writeIndex++;
                }

                // Read in new element
                inputIndex++;
                mergeArray[0] = map[pointerToInputBuffer+inputIndex*elementSize]; // Key
                mergeArray[1] = map[pointerToInputBuffer+inputIndex*elementSize+1]; // Value

            }
            else if(smallestIndex == 1) {
                // Upper

                // Do we write this out?
                if(value > 0) {
                    map[pointerToOutputBuffer+writeIndex*elementSize] = key;
                    map[pointerToOutputBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToOutputBuffer+writeIndex*elementSize+2] = latestReversePointer;
                    map[pointerToOutputBuffer+writeIndex*elementSize+3] = 0; // No forward pointer
                    writeIndex++;
                }

                // Try to read in another element from the upper level subbox.
                // Search for new subbox if we run out.
                // Mark subbox empty when we search for new one.
                // Discard any pointers.
                upperIndex++;

                if(map[upperBuffer+upperIndex*elementSize] != -1) {
                    // Found element in this subbox
                    mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
                    mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
                }
                else {
                    // Else load in new subbox

                    // First mark the old subbox as deleted
                    map[pointerToUpperBoolean+upperSubboxCounter*booleanSize] = 0; // upperPointed pointed into our array of upper level subboxes.
                    map[pointerToUpperBoolean+upperSubboxCounter*booleanSize+1] = 0;
                    //upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
                    upperSubboxCounter++;
                    upperSubbox = map[pointerToUpperBoolean+upperSubboxCounter*booleanSize];
                    // Its now -1 if none was found, but proceed as if it found one.

                    // Did we find a subbox?
                    if(upperSubbox == 0 || upperSubboxCounter >= numberOfUpperLevelSubboxes) {
                        mergeArray[2] = -1;
                        mergeArray[3] = -1;
                    }
                    else {
                        upperBuffer = map[upperSubbox+9]; // Its output buffer
                        upperIndex = 0;
                        mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
                        mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
                    }
                }
            }
            else if(smallestIndex == 2) {
                // Middle

                // Do we write this out?
                if(value > 0) {
                    map[pointerToOutputBuffer+writeIndex*elementSize] = key;
                    map[pointerToOutputBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToOutputBuffer+writeIndex*elementSize+2] = latestReversePointer;
                    map[pointerToOutputBuffer+writeIndex*elementSize+3] = 0; // No forward pointer
                    writeIndex++;
                }

                // Read in new element
                middleIndex++;
                mergeArray[4] = map[pointerToMiddleBuffer+middleIndex*elementSize]; // Key
                mergeArray[5] = map[pointerToMiddleBuffer+middleIndex*elementSize+1]; // Value

            }
            else if(smallestIndex == 3) {
                // Lower

                // Do we write this out?
                if(value > 0) {
                    map[pointerToOutputBuffer+writeIndex*elementSize] = key;
                    map[pointerToOutputBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToOutputBuffer+writeIndex*elementSize+2] = latestReversePointer;
                    map[pointerToOutputBuffer+writeIndex*elementSize+3] = 0; // No forward pointer
                    writeIndex++;
                }

                lowerIndex++;
                if(map[lowerBuffer+lowerIndex*elementSize] != -1) {
                    // Found element in this subbox
                    mergeArray[6] = map[lowerBuffer+lowerIndex*elementSize];
                    mergeArray[7] = map[lowerBuffer+lowerIndex*elementSize+1];
                }
                else {
                    // Else load in new subbox

                    // First mark the old subbox as deleted
                    map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize] = 0; // upperPointed pointed into our array of upper level subboxes.
                    map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize+1] = 0;
                    //upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
                    lowerSubboxCounter++;
                    lowerSubbox = map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize];

                    // Did we find a subbox?
                    if(lowerSubbox == 0 || lowerSubboxCounter >= numberOfLowerLevelSubboxes) {
                        mergeArray[6] = -1;
                        mergeArray[7] = -1;
                    }
                    else {
                        lowerBuffer = map[lowerSubbox+9]; // Its output buffer
                        lowerIndex = 0;
                        mergeArray[6] = map[lowerBuffer+lowerIndex*elementSize];
                        mergeArray[7] = map[lowerBuffer+lowerIndex*elementSize+1];
                    }
                }

            }
            else if(smallestIndex == 4) {
                // Output
                map[pointerToOutputBuffer+writeIndex*elementSize] = key;
                map[pointerToOutputBuffer+writeIndex*elementSize+1] = value;
                if(value > 0) {
                    map[pointerToOutputBuffer+writeIndex*elementSize+2] = latestReversePointer;
                    map[pointerToOutputBuffer+writeIndex*elementSize+3] = 0; // No forward pointer
                }
                else {
                    map[pointerToOutputBuffer+writeIndex*elementSize+2] = temp1;
                    map[pointerToOutputBuffer+writeIndex*elementSize+3] = temp2;
                    latestReversePointer = pointerToOutputBuffer+writeIndex*elementSize; // Point to this
                }
                writeIndex++;

                outputIndex++;
                mergeArray[8] = map[tempPointerOutput+outputIndex*elementSize]; // Key
                mergeArray[9] = map[tempPointerOutput+outputIndex*elementSize+1]; // Value
                temp1 = map[tempPointerOutput+outputIndex*elementSize+2];
                temp2 = map[tempPointerOutput+outputIndex*elementSize+3];
            }
        }



        /* OLD MERGE
        for(writeIndex = 0; writeIndex < totalElementsToOutput; writeIndex++) {

            // Select smallest element by scanning mergeArray
            smallest = LONG_MAX;
            smallestIndex = 0;
            for(int i = 0; i < 5; i++) {
                if(mergeArray[i*2] != -1 && mergeArray[i*2] < smallest) {
                    smallestIndex = i;
                    smallest = mergeArray[i*2];
                }
            }

            // Output
            ////cout << mergeArray[0] << " " << mergeArray[2] << " " << mergeArray[4] << " " << mergeArray[6] << " " << mergeArray[8] << "\n";
            map[pointerToOutputBuffer+writeIndex*elementSize] = mergeArray[smallestIndex*2];
            map[pointerToOutputBuffer+writeIndex*elementSize+1] = mergeArray[smallestIndex*2+1];
            if(mergeArray[smallestIndex*2+1] > 0) {
                // This is a real element
                map[pointerToOutputBuffer+writeIndex*elementSize+2] = latestReversePointer;
                map[pointerToOutputBuffer+writeIndex*elementSize+3] = 0; // No forward pointer
            }
            else {
                // This is a pointer
                map[pointerToOutputBuffer+writeIndex*elementSize+2] = temp1;
                map[pointerToOutputBuffer+writeIndex*elementSize+3] = temp2;
                latestReversePointer = pointerToOutputBuffer+writeIndex*elementSize; // Point to this
            }

            // Read in new longs to replace the ones we output
            if(smallestIndex == 0) {
                // Try to read in another element from the input buffer
                // Discard any pointers
                inputIndex++;
                while(map[pointerToInputBuffer+inputIndex*elementSize+1] < 0 &&
                      map[pointerToInputBuffer+inputIndex*elementSize] != -1) {
                    inputIndex++;
                }
                // No real elements (ie we reached the end)?
                if(map[pointerToInputBuffer+inputIndex*elementSize] == -1) {
                    // Input is empty
                    mergeArray[0] = -1;
                    mergeArray[1] = -1;
                }
                else {
                    mergeArray[0] = map[pointerToInputBuffer+inputIndex*elementSize]; // Key
                    mergeArray[1] = map[pointerToInputBuffer+inputIndex*elementSize+1]; // Value
                }


            }
            else if(smallestIndex == 1) {
                // Try to read in another element from the upper level subbox.
                // Search for new subbox if we run out.
                // Mark subbox empty when we search for new one.
                // Discard any pointers.
                upperIndex++;

                // Search for next real element
                while(map[upperBuffer+upperIndex*elementSize+1] < 0 &&
                      map[upperBuffer+upperIndex*elementSize] != -1) {
                    upperIndex++;
                }
                if(map[upperBuffer+upperIndex*elementSize] != -1) {
                    // Found element in this subbox
                    mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
                    mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
                }
                else {
                    // Else load in new subbox

                    // First mark the old subbox as deleted
                    map[pointerToUpperBoolean+upperSubboxCounter*booleanSize] = 0; // upperPointed pointed into our array of upper level subboxes.
                    map[pointerToUpperBoolean+upperSubboxCounter*booleanSize+1] = 0;
                    //upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
                    upperSubboxCounter++;
                    upperSubbox = map[pointerToUpperBoolean+upperSubboxCounter*booleanSize];
                    // Its now -1 if none was found, but proceed as if it found one.

                    // Did we find a subbox?
                    if(upperSubbox == 0 || upperSubboxCounter >= numberOfUpperLevelSubboxes) {
                        mergeArray[2] = -1;
                        mergeArray[3] = -1;
                    }
                    else {
                        upperBuffer = map[upperSubbox+9]; // Its output buffer
                        upperIndex = 0;
                        // Find first real element in the subbox
                        while(map[upperBuffer+upperIndex*elementSize+1] < 0 &&
                              map[upperBuffer+upperIndex*elementSize] != -1) {
                            upperIndex++;
                        }
                        if(map[upperBuffer+upperIndex*elementSize] == -1) {
                            mergeArray[2] = -1;
                            mergeArray[3] = -1;
                        }
                        else {
                            mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
                            mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
                        }
                    }
                }
            }
            else if(smallestIndex == 2) {
                // Try to read in another element from the middle buffer
                // Discard any pointers
                middleIndex++;
                while(map[pointerToMiddleBuffer+middleIndex*elementSize+1] < 0 &&
                      map[pointerToMiddleBuffer+middleIndex*elementSize] != -1) {
                    middleIndex++;
                }
                // No real elements (ie we reached the end)?
                if(map[pointerToMiddleBuffer+middleIndex*elementSize] == -1) {
                    // Input is empty
                    mergeArray[4] = -1;
                    mergeArray[5] = -1;
                }
                else {
                    mergeArray[4] = map[pointerToMiddleBuffer+middleIndex*elementSize]; // Key
                    mergeArray[5] = map[pointerToMiddleBuffer+middleIndex*elementSize+1]; // Value
                }

            }
            else if(smallestIndex == 3) {
                // Try to read in another element from the lower level subbox.
                // Search for new subbox if we run out.
                // Mark subbox empty when we search for new one
                // Discard any pointers
                lowerIndex++;

                // Search for next real element
                while(map[lowerBuffer+lowerIndex*elementSize+1] < 0 &&
                      map[lowerBuffer+lowerIndex*elementSize] != -1) {
                    lowerIndex++;
                }
                if(map[lowerBuffer+lowerIndex*elementSize] != -1) {
                    mergeArray[6] = map[lowerBuffer+lowerIndex*elementSize];
                    mergeArray[7] = map[lowerBuffer+lowerIndex*elementSize+1];
                }
                else {
                    // Need to load in new subbox
                    map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize] = 0; // Mark old subbox as deleted.
                    map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize+1] = 0;
                    //lowerPointer = findNextSubboxForMerge(pointerToLowerBoolean,numberOfLowerLevelSubboxes);
                    lowerSubboxCounter++;
                    lowerSubbox = map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize];
                    // Its now -1 if none was found, but proceed as if it found one.

                    // Did we find a subbox?
                    if(lowerSubbox == 0 || lowerSubboxCounter >= numberOfLowerLevelSubboxes) {
                        mergeArray[6] = -1;
                        mergeArray[7] = -1;
                    }
                    else {
                        lowerBuffer = map[lowerSubbox+9]; // Its output buffer
                        lowerIndex = 0;
                        // Find first real element
                        while(map[lowerBuffer+lowerIndex*elementSize+1] < 0 &&
                              map[lowerBuffer+lowerIndex*elementSize] != -1) {
                            lowerIndex++;
                        }
                        if(map[lowerBuffer+lowerIndex*elementSize] == -1) {
                            mergeArray[6] = -1;
                            mergeArray[7] = -1;
                        }
                        else {
                            mergeArray[6] = map[lowerBuffer+lowerIndex*elementSize];
                            mergeArray[7] = map[lowerBuffer+lowerIndex*elementSize+1];
                        }
                    }
                }
            }
            else if(smallestIndex == 4) {
                // Try to read in another element from the output buffer
                // KEEP POINTERS!
                outputIndex++;
                if(map[tempPointerOutput+outputIndex*elementSize] == -1) {
                    // Input is empty
                    mergeArray[8] = -1;
                    mergeArray[9] = -1;
                }
                else {
                    mergeArray[8] = map[tempPointerOutput+outputIndex*elementSize]; // Key
                    mergeArray[9] = map[tempPointerOutput+outputIndex*elementSize+1]; // Value
                    temp1 = map[tempPointerOutput+outputIndex*elementSize+2];
                    temp2 = map[tempPointerOutput+outputIndex*elementSize+3];
                }
            }
        }

        OLD MERGE */

        map[pointerToInputBuffer] = -1;
        map[pointerToMiddleBuffer] = -1;
        //map[pointerToOutputBuffer+totalElementsToOutput*elementSize] = -1;
        map[pointerToOutputBuffer+writeIndex*elementSize] = -1;

        // Merge done

        // In case we finished with an element from one of the subbox layers
        /*if(upperPointer != -1) {
            map[upperPointer] = 0;
        }
        if(lowerPointer != -1) {
            map[lowerPointer] = 0;
        }*/
        if(upperSubboxCounter < numberOfUpperLevelSubboxes) {
            map[pointerToUpperBoolean+upperSubboxCounter*booleanSize] = 0; // upperPointed pointed into our array of upper level subboxes.
            map[pointerToUpperBoolean+upperSubboxCounter*booleanSize+1] = 0;
        }
        if(lowerSubboxCounter < numberOfLowerLevelSubboxes) {
            map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize] = 0; // Mark old subbox as deleted.
            map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize+1] = 0;
        }

        // Now build up forward pointers with a linear scan of the output
        // but only if this isnt a recursive call, since a recursive call
        // simply discards the pointers.
        if(!recursive) {
            long latestForward = 0;
            for(long i = totalElementsToOutput-1; i > -1; i--) {
                if(map[pointerToOutputBuffer+i*elementSize+1] > 0) {
                    // Real element, update forward pointer
                    map[pointerToOutputBuffer+i*elementSize+3] = latestForward;
                }
                else {
                    latestForward = pointerToOutputBuffer+i*elementSize;
                }
            }
        }
    }
    else {
        // Special case, subboxes are merely arrays. Therefore we do not index by elementSize!

        /* Basic outline of the merge
         *
         * 1) Scan the output buffer to determine if we are already flushed.
         * While scanning the buffer count the number of real elements.
         * If the real elements in the output buffer matches the number of real
         * elements in the xBox then we are already flushed. Return.
         * It costs O(x^(1+alpha) / B), which is our upper bound.
         *
         * 2) Move the elements in the output buffer back to make room for the
         * remaining real elements. This is done so we can merge directly into
         * the output buffer. It costs O(x^(1+alpha) / B), which is our upper bound.
         * We need the index from step 1 to perform step 2, otherwhise they could
         * have been combined.
         *
         * 3) Perform the five way merge, writing into the output buffer. This costs
         * O(x^(1+alpha) / B), which is our upper bound.
         */

        long writeIndex = 0;
        long numberOfRealElementsInOutputBuffer = 0;

        // 1) Scan output buffer to find proper writeIndex
        while(map[pointerToOutputBuffer+writeIndex*elementSize] != -1) {
            if(map[pointerToOutputBuffer+writeIndex*elementSize+1] > 0) {
                // Real element
                numberOfRealElementsInOutputBuffer++;
            }
            writeIndex++;
        }

        if(numberOfRealElements == numberOfRealElementsInOutputBuffer) {
            map[pointerToInputBuffer] = -1;
            for(long i = 0; i < numberOfUpperLevelSubboxes; i++) {
                map[pointerToUpperBoolean+i*booleanSize] = 0;
                map[pointerToUpperBoolean+i*booleanSize+1] = 0;
            }
            map[pointerToMiddleBuffer] = -1;
            for(long i = 0; i < numberOfLowerLevelSubboxes; i++) {
                map[pointerToLowerBoolean+i*booleanSize] = 0;
                map[pointerToLowerBoolean+i*booleanSize+1] = 0;
            }
            return; // We are already flushed
        }

        // 2) Move the elements in the output buffer back to make room for the remaining real elements.
        long deltaRealElements = numberOfRealElements-numberOfRealElementsInOutputBuffer;
        long elementsToMove = writeIndex; // Includes forward pointers!
        long moveByThisMuch = elementSize * deltaRealElements;

        /*//cout << "Move " << numberOfRealElements << " " << numberOfRealElementsInOutputBuffer << " "
             << deltaRealElements << " " << elementsToMove << " " << moveByThisMuch << "\n";*/

        map[pointerToOutputBuffer + moveByThisMuch + elementsToMove*elementSize] = -1;
        for(long i = elementsToMove-1; i > -1; i--) {
            map[pointerToOutputBuffer + moveByThisMuch + i*elementSize+3] = map[pointerToOutputBuffer + i*elementSize+3];
            map[pointerToOutputBuffer + moveByThisMuch + i*elementSize+2] = map[pointerToOutputBuffer + i*elementSize+2];
            map[pointerToOutputBuffer + moveByThisMuch + i*elementSize+1] = map[pointerToOutputBuffer + i*elementSize+1];
            map[pointerToOutputBuffer + moveByThisMuch + i*elementSize] = map[pointerToOutputBuffer + i*elementSize];
        }

        // 3) Perform the five way merge
        // Obs new notations below.

        // Find upper level subbox with smallest element in output buffer
        /*long upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
        ////cout << "Upper Pointer = " << upperPointer << "\n";
        long upperSubbox,upperMax,upperBuffer;
        if(upperPointer != -1) {
            upperSubbox = map[upperPointer];
            upperMax = map[upperSubbox+1]; // Real elements in subbox
            upperBuffer = upperSubbox+2; // Its output buffer
        }*/


        long upperSubbox,upperMax,upperBuffer;
        upperSubbox = map[pointerToUpperBoolean+upperSubboxCounter*booleanSize];
        if(upperSubbox != 0) {
            upperBuffer = upperSubbox+2; // Its output buffer
            ////cout << "Upper buffer placed at " << upperBuffer << " " << upperSubbox << "\n";
        }

        /*long lowerPointer = findNextSubboxForMerge(pointerToLowerBoolean,numberOfLowerLevelSubboxes);
        // Its now -1 if none was found, but proceed as if it found one.
        long lowerSubbox,lowerMax,lowerBuffer;
        if(lowerPointer != -1) {
            lowerSubbox = map[lowerPointer];
            lowerMax = map[lowerSubbox+1]; // Real elements in subbox
            lowerBuffer = lowerSubbox+2; // Its output buffer
        }*/

        long lowerSubbox,lowerMax,lowerBuffer;
        lowerSubbox = map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize];
        if(lowerSubbox != 0) {
            lowerBuffer = lowerSubbox+2; // Its output buffer
        }

        long upperIndex = 0;
        long lowerIndex = 0;
        long inputIndex = 0;
        long middleIndex = 0;
        long outputIndex = 0;

        long tempPointerOutput = pointerToOutputBuffer + moveByThisMuch; // Points to where we moved the elements

        writeIndex = 0; // Reuse to indicate our position in the final output

        long mergeArray[10]; // Will use this array to merge the five buffers consisting of KeyValues
        // The order of the array is input, upper, middle, lower, output.
        // -1 indicates an empty position / we ran out of elements.

        /*
         * Populate the merge array in five steps
         */

        // 1) Input
        if(map[pointerToInputBuffer] == -1) {
           // Input is empty
           mergeArray[0] = -1;
           mergeArray[1] = -1;
        }
        else {
            // Find first real element in input buffer.
            // Concider that there might only be pointers in the input buffer!
            while(map[pointerToInputBuffer+inputIndex*elementSize+1] < 0 &&
                    map[pointerToInputBuffer+inputIndex*elementSize] != -1) {
                inputIndex++;
            }
            // No real elements (ie we reached the end)?
            if(map[pointerToInputBuffer+inputIndex*elementSize] == -1) {
                // Input is empty
                mergeArray[0] = -1;
                mergeArray[1] = -1;
            }
            else {
                mergeArray[0] = map[pointerToInputBuffer+inputIndex*elementSize]; // Key
                mergeArray[1] = map[pointerToInputBuffer+inputIndex*elementSize+1]; // Value
            }
        }

        // 2) Upper level subboxes
        if(upperSubbox == 0) {
            // We found no upper level subbox
            mergeArray[2] = -1;
            mergeArray[3] = -1;
        }
        else {
            mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
            mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
            /* OLD
            // Find first element that isnt a pointer
            while(map[upperBuffer+upperIndex*elementSize+1] < 0 &&
                  map[upperBuffer+upperIndex*elementSize] != -1) {
                upperIndex++;
            }
            if(map[upperBuffer+upperIndex*elementSize] == -1) {
                mergeArray[2] = -1;
                mergeArray[3] = -1;
            }
            else {
                mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
                mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
                ////cout << "Key upper = " << mergeArray[2] << "\n";
                ////cout << "Value upper = " << mergeArray[3] << "\n";
            }*/
        }

        // 3) Middle
        if(map[pointerToMiddleBuffer] == -1) {
            // Input is empty
            mergeArray[4] = -1;
            mergeArray[5] = -1;
        }
        else {
            // Find first real element in buffer.
            // Concider that there might only be pointers in the buffer!
            while(map[pointerToMiddleBuffer+middleIndex*elementSize+1] < 0 &&
                  map[pointerToMiddleBuffer+middleIndex*elementSize] != -1) {
                middleIndex++;
            }
            // No real elements (ie we reached the end)?
            if(map[pointerToMiddleBuffer+middleIndex*elementSize] == -1) {
                // Input is empty
                mergeArray[4] = -1;
                mergeArray[5] = -1;
            }
            else {
                mergeArray[4] = map[pointerToMiddleBuffer+middleIndex*elementSize]; // Key
                mergeArray[5] = map[pointerToMiddleBuffer+middleIndex*elementSize+1]; // Value
            }
        }

        // 4) Lower level subboxes
        if(lowerSubbox == 0) {
            // We found no lower level subbox
            //cout << "Found no lower level subbox\n";
            mergeArray[6] = -1;
            mergeArray[7] = -1;
        }
        else {
            mergeArray[6] = map[lowerBuffer+lowerIndex*elementSize];
            mergeArray[7] = map[lowerBuffer+lowerIndex*elementSize+1];
            /* OLD
            //cout << "Fiding first element in initial lower level subbox\n";
            // Find first element that isnt a pointer
            while(map[lowerBuffer+lowerIndex*elementSize+1] < 0 &&
                  map[lowerBuffer+lowerIndex*elementSize] != -1) {
                lowerIndex++;
                //cout << map[lowerBuffer+lowerIndex*elementSize] << " " << map[lowerBuffer+lowerIndex*elementSize+1] << " | ";
            }
            //cout << "\n";
            if(map[lowerBuffer+lowerIndex*elementSize] == -1) {
                mergeArray[6] = -1;
                mergeArray[7] = -1;
            }
            else {
                mergeArray[6] = map[lowerBuffer+lowerIndex*elementSize];
                mergeArray[7] = map[lowerBuffer+lowerIndex*elementSize+1];
            }*/
        }

        // 5) Output (which we moved!)
        long temp1 = 0; // To handle pointers in original output buffer
        long temp2 = 0;
        if(map[tempPointerOutput] == -1) {
            // Input is empty
            mergeArray[8] = -1;
            mergeArray[9] = -1;
        }
        else {
            mergeArray[8] = map[tempPointerOutput+outputIndex*elementSize]; // Key
            mergeArray[9] = map[tempPointerOutput+outputIndex*elementSize+1]; // Value
            temp1 = map[tempPointerOutput+outputIndex*elementSize+2];
            temp2 = map[tempPointerOutput+outputIndex*elementSize+3];
        }

        // Now that the initial mergeArray is populated we continuously write the smallest (>0) element to
        // the output buffer and read in a new element from the relevant buffer. Once a buffer runs
        // out we mark the entry in the array with -1. We know the number of elements to output since
        // we only output the real elements + the pointers in the original output buffer, which

        long latestReversePointer = 0; // Build up reverse pointers as we output

        long totalElementsToOutput = elementsToMove + deltaRealElements;
        long smallest;
        long smallestIndex;

        ////cout << "Going to output " << totalElementsToOutput << " elements\n";
        //cout << "Flushing into output of " << pointer << "\n";
        /*//cout << mergeArray[0] << " " << mergeArray[2] << " " << mergeArray[4] << " " << mergeArray[6] << " "
             << mergeArray[8] << "\n";*/

        long key, value;

        // New merge, where we handle subboxes containing only pointers.
        while(true) {

            // Break?
            if(mergeArray[0] == -1 && mergeArray[2] == -1 && mergeArray[4] == -1
               && mergeArray[6] == -1 && mergeArray[8] == -1) {
                break;
            }

            // Select smallest element by scanning mergeArray
            smallest = LONG_MAX;
            smallestIndex = 0;
            for(int i = 0; i < 5; i++) {
                if(mergeArray[i*2] != -1 && mergeArray[i*2] < smallest) {
                    smallestIndex = i;
                    smallest = mergeArray[i*2];
                }
            }

            /*cout << mergeArray[0] << " " << mergeArray[2] << " " << mergeArray[4] << " " << mergeArray[6] << " "
                 << mergeArray[8] << "\n";*/

            key = mergeArray[smallestIndex*2];
            value = mergeArray[smallestIndex*2+1];

            if(smallestIndex == 0) {
                // Input

                // Do we write this out?
                if(value > 0) {
                    map[pointerToOutputBuffer+writeIndex*elementSize] = key;
                    map[pointerToOutputBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToOutputBuffer+writeIndex*elementSize+2] = latestReversePointer;
                    map[pointerToOutputBuffer+writeIndex*elementSize+3] = 0; // No forward pointer
                    writeIndex++;
                }

                // Read in new element
                inputIndex++;
                mergeArray[0] = map[pointerToInputBuffer+inputIndex*elementSize]; // Key
                mergeArray[1] = map[pointerToInputBuffer+inputIndex*elementSize+1]; // Value

            }
            else if(smallestIndex == 1) {
                // Upper

                // Do we write this out?
                if(value > 0) {
                    map[pointerToOutputBuffer+writeIndex*elementSize] = key;
                    map[pointerToOutputBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToOutputBuffer+writeIndex*elementSize+2] = latestReversePointer;
                    map[pointerToOutputBuffer+writeIndex*elementSize+3] = 0; // No forward pointer
                    writeIndex++;
                }

                // Try to read in another element from the upper level subbox.
                // Search for new subbox if we run out.
                // Mark subbox empty when we search for new one.
                // Discard any pointers.
                upperIndex++;
                if(map[upperBuffer+upperIndex*elementSize] != -1) {
                    // Found element in this subbox
                    mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
                    mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
                }
                else {
                    // Else load in new subbox

                    ////cout << "Switching upper level subbox\n";

                    // First mark the old subbox as deleted
                    map[pointerToUpperBoolean+upperSubboxCounter*booleanSize] = 0; // upperPointed pointed into our array of upper level subboxes.
                    map[pointerToUpperBoolean+upperSubboxCounter*booleanSize+1] = 0;
                    //upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
                    upperSubboxCounter++;
                    upperSubbox = map[pointerToUpperBoolean+upperSubboxCounter*booleanSize];
                    // Its now -1 if none was found, but proceed as if it found one.

                    ////cout << "New subbox " << upperSubbox << "\n";

                    // Did we find a subbox?
                    if(upperSubbox == 0 || upperSubboxCounter >= numberOfUpperLevelSubboxes) {
                        mergeArray[2] = -1;
                        mergeArray[3] = -1;
                    }
                    else {
                        upperBuffer = upperSubbox+2; // Its output buffer
                        upperIndex = 0;
                        mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
                        mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
                    }
                }
            }
            else if(smallestIndex == 2) {
                // Middle

                // Do we write this out?
                if(value > 0) {
                    map[pointerToOutputBuffer+writeIndex*elementSize] = key;
                    map[pointerToOutputBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToOutputBuffer+writeIndex*elementSize+2] = latestReversePointer;
                    map[pointerToOutputBuffer+writeIndex*elementSize+3] = 0; // No forward pointer
                    writeIndex++;
                }

                // Read in new element
                middleIndex++;
                mergeArray[4] = map[pointerToMiddleBuffer+middleIndex*elementSize]; // Key
                mergeArray[5] = map[pointerToMiddleBuffer+middleIndex*elementSize+1]; // Value

            }
            else if(smallestIndex == 3) {
                // Lower

                // Do we write this out?
                if(value > 0) {
                    map[pointerToOutputBuffer+writeIndex*elementSize] = key;
                    map[pointerToOutputBuffer+writeIndex*elementSize+1] = value;
                    map[pointerToOutputBuffer+writeIndex*elementSize+2] = latestReversePointer;
                    map[pointerToOutputBuffer+writeIndex*elementSize+3] = 0; // No forward pointer
                    writeIndex++;
                }

                lowerIndex++;
                if(map[lowerBuffer+lowerIndex*elementSize] != -1) {
                    // Found element in this subbox
                    mergeArray[6] = map[lowerBuffer+lowerIndex*elementSize];
                    mergeArray[7] = map[lowerBuffer+lowerIndex*elementSize+1];
                }
                else {
                    // Else load in new subbox

                    // First mark the old subbox as deleted
                    map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize] = 0; // upperPointed pointed into our array of upper level subboxes.
                    map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize+1] = 0;
                    //upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
                    lowerSubboxCounter++;
                    lowerSubbox = map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize];

                    // Did we find a subbox?
                    if(lowerSubbox == 0 || lowerSubboxCounter >= numberOfLowerLevelSubboxes) {
                        mergeArray[6] = -1;
                        mergeArray[7] = -1;
                    }
                    else {
                        lowerBuffer = lowerSubbox+2; // Its output buffer
                        lowerIndex = 0;
                        mergeArray[6] = map[lowerBuffer+lowerIndex*elementSize];
                        mergeArray[7] = map[lowerBuffer+lowerIndex*elementSize+1];
                    }
                }

            }
            else if(smallestIndex == 4) {
                // Output
                map[pointerToOutputBuffer+writeIndex*elementSize] = key;
                map[pointerToOutputBuffer+writeIndex*elementSize+1] = value;
                if(value > 0) {
                    map[pointerToOutputBuffer+writeIndex*elementSize+2] = latestReversePointer;
                    map[pointerToOutputBuffer+writeIndex*elementSize+3] = 0; // No forward pointer
                }
                else {
                    map[pointerToOutputBuffer+writeIndex*elementSize+2] = temp1;
                    map[pointerToOutputBuffer+writeIndex*elementSize+3] = temp2;
                    latestReversePointer = pointerToOutputBuffer+writeIndex*elementSize; // Point to this
                }
                writeIndex++;

                outputIndex++;
                mergeArray[8] = map[tempPointerOutput+outputIndex*elementSize]; // Key
                mergeArray[9] = map[tempPointerOutput+outputIndex*elementSize+1]; // Value
                temp1 = map[tempPointerOutput+outputIndex*elementSize+2];
                temp2 = map[tempPointerOutput+outputIndex*elementSize+3];
            }
        }



        /* OLD MERGE
        for(writeIndex = 0; writeIndex < totalElementsToOutput; writeIndex++) {

            // Select smallest element by scanning mergeArray
            smallest = LONG_MAX;
            smallestIndex = 0;
            for(int i = 0; i < 5; i++) {
                ////cout << mergeArray[i*2] << " ";
                if(mergeArray[i*2] != -1 && mergeArray[i*2] < smallest) {
                    smallestIndex = i;
                    smallest = mergeArray[i*2];
                }
            }
            ////cout << "\n";

            // Output
            map[pointerToOutputBuffer+writeIndex*elementSize] = mergeArray[smallestIndex*2];
            //cout << "Wrote out " << mergeArray[smallestIndex*2] << " from " << smallestIndex << "\n";
            map[pointerToOutputBuffer+writeIndex*elementSize+1] = mergeArray[smallestIndex*2+1];
            if(mergeArray[smallestIndex*2+1] > 0) {
                // This is a real element
                map[pointerToOutputBuffer+writeIndex*elementSize+2] = latestReversePointer;
                map[pointerToOutputBuffer+writeIndex*elementSize+3] = 0; // No forward pointer
            }
            else {
                // This is a pointer
                map[pointerToOutputBuffer+writeIndex*elementSize+2] = temp1;
                map[pointerToOutputBuffer+writeIndex*elementSize+3] = temp2;
                latestReversePointer = pointerToOutputBuffer+writeIndex*elementSize; // Point to this
            }

            // Read in new longs to replace the ones we output
            if(smallestIndex == 0) {
                // Try to read in another element from the input buffer
                // Discard any pointers
                inputIndex++;
                while(map[pointerToInputBuffer+inputIndex*elementSize+1] < 0 &&
                      map[pointerToInputBuffer+inputIndex*elementSize] != -1) {
                    inputIndex++;
                }
                // No real elements (ie we reached the end)?
                if(map[pointerToInputBuffer+inputIndex*elementSize] == -1) {
                    // Input is empty
                    mergeArray[0] = -1;
                    mergeArray[1] = -1;
                    ////cout << "Aborted index 0. " << inputIndex << " " << map[pointerToInputBuffer+inputIndex*elementSize] << "\n";
                }
                else {
                    mergeArray[0] = map[pointerToInputBuffer+inputIndex*elementSize]; // Key
                    mergeArray[1] = map[pointerToInputBuffer+inputIndex*elementSize+1]; // Value
                }


            }
            else if(smallestIndex == 1) {
                // Try to read in another element from the upper level subbox.
                // Search for new subbox if we run out.
                // Mark subbox empty when we search for new one.
                // Discard any pointers.
                upperIndex++;

                // Search for next real element
                while(map[upperBuffer+upperIndex*elementSize+1] < 0 &&
                      map[upperBuffer+upperIndex*elementSize] != -1) {
                    upperIndex++;
                }
                if(map[upperBuffer+upperIndex*elementSize] != -1) {
                    // Found element in this subbox
                    ////cout << "Placing subbox key " << map[upperBuffer+upperIndex*2] << " from index " << upperIndex << "\n";
                    mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
                    mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
                }
                else {
                    // Else load in new subbox

                    // First mark the old subbox as deleted
                    map[pointerToUpperBoolean+upperSubboxCounter*booleanSize] = 0; // upperPointed pointed into our array of upper level subboxes.
                    map[pointerToUpperBoolean+upperSubboxCounter*booleanSize+1] = 0;
                    /*upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
                    //cout << "=== " << upperPointer << "\n";*/
                    /*upperSubboxCounter++;
                    upperSubbox = map[pointerToUpperBoolean+upperSubboxCounter*booleanSize];

                    // Did we find a subbox?
                    if(upperSubbox == 0 || upperSubboxCounter >= numberOfUpperLevelSubboxes) {
                        mergeArray[2] = -1;
                        mergeArray[3] = -1;
                    }
                    else {
                        upperBuffer = upperSubbox+2; // Its output buffer
                        upperIndex = 0;

                        // Find first real element in the subbox
                        while(map[upperBuffer+upperIndex*elementSize+1] < 0 &&
                              map[upperBuffer+upperIndex*elementSize] != -1) {
                            upperIndex++;
                        }
                        if(map[upperBuffer+upperIndex*elementSize] == -1) {
                            mergeArray[2] = -1;
                            mergeArray[3] = -1;
                        }
                        else {
                            mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
                            mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
                        }
                    }
                }
            }
            else if(smallestIndex == 2) {
                // Try to read in another element from the middle buffer
                // Discard any pointers
                middleIndex++;
                while(map[pointerToMiddleBuffer+middleIndex*elementSize+1] < 0 &&
                      map[pointerToMiddleBuffer+middleIndex*elementSize] != -1) {
                    middleIndex++;
                }
                // No real elements (ie we reached the end)?
                if(map[pointerToMiddleBuffer+middleIndex*elementSize] == -1) {
                    // Input is empty
                    mergeArray[4] = -1;
                    mergeArray[5] = -1;
                }
                else {
                    mergeArray[4] = map[pointerToMiddleBuffer+middleIndex*elementSize]; // Key
                    mergeArray[5] = map[pointerToMiddleBuffer+middleIndex*elementSize+1]; // Value
                }

            }
            else if(smallestIndex == 3) {
                // Try to read in another element from the lower level subbox.
                // Search for new subbox if we run out.
                // Mark subbox empty when we search for new one
                // Discard any pointers
                lowerIndex++;

                // Search for next real element
                while(map[lowerBuffer+lowerIndex*elementSize+1] < 0 &&
                      map[lowerBuffer+lowerIndex*elementSize] != -1) {
                    lowerIndex++;
                }
                if(map[lowerBuffer+lowerIndex*elementSize] != -1) {
                    mergeArray[6] = map[lowerBuffer+lowerIndex*elementSize];
                    mergeArray[7] = map[lowerBuffer+lowerIndex*elementSize+1];
                }
                else {
                    // Need to load in new subbox
                    //cout << "Loading in new lower level subbox\n";
                    map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize] = 0; // Mark old subbox as deleted.
                    map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize+1] = 0;
                    //lowerPointer = findNextSubboxForMerge(pointerToLowerBoolean,numberOfLowerLevelSubboxes);
                    lowerSubboxCounter++;
                    lowerSubbox = map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize];

                    // Did we find a subbox?
                    if(lowerSubbox == 0 || lowerSubboxCounter >= numberOfLowerLevelSubboxes) {
                        mergeArray[6] = -1;
                        mergeArray[7] = -1;
                    }
                    else {
                        lowerBuffer = lowerSubbox+2; // Its output buffer
                        lowerIndex = 0;

                        // Find first real element
                        while(map[lowerBuffer+lowerIndex*elementSize+1] < 0 &&
                              map[lowerBuffer+lowerIndex*elementSize] != -1) {
                            lowerIndex++;
                        }
                        if(map[lowerBuffer+lowerIndex*elementSize] == -1) {
                            mergeArray[6] = -1;
                            mergeArray[7] = -1;
                        }
                        else {
                            mergeArray[6] = map[lowerBuffer+lowerIndex*elementSize];
                            mergeArray[7] = map[lowerBuffer+lowerIndex*elementSize+1];
                        }
                    }
                }
            }
            else if(smallestIndex == 4) {
                // Try to read in another element from the output buffer
                // KEEP POINTERS!
                outputIndex++;
                if(map[tempPointerOutput+outputIndex*elementSize] == -1) {
                    // Input is empty
                    mergeArray[8] = -1;
                    mergeArray[9] = -1;
                }
                else {
                    mergeArray[8] = map[tempPointerOutput+outputIndex*elementSize]; // Key
                    mergeArray[9] = map[tempPointerOutput+outputIndex*elementSize+1]; // Value
                    temp1 = map[tempPointerOutput+outputIndex*elementSize+2];
                    temp2 = map[tempPointerOutput+outputIndex*elementSize+3];
                }
            }
        }
        OLD MERGE*/

        map[pointerToInputBuffer] = -1;
        map[pointerToMiddleBuffer] = -1;
        //map[pointerToOutputBuffer+totalElementsToOutput*elementSize] = -1;
        map[pointerToOutputBuffer+writeIndex*elementSize] = -1;

        // Merge done

        // In case we finished with an element from one of the subbox layers
        /*if(upperPointer != -1) {
            map[upperPointer] = 0;
        }
        if(lowerPointer != -1) {
            map[lowerPointer] = 0;
        }*/
        if(upperSubboxCounter < numberOfUpperLevelSubboxes) {
            map[pointerToUpperBoolean+upperSubboxCounter*booleanSize] = 0; // upperPointed pointed into our array of upper level subboxes.
            map[pointerToUpperBoolean+upperSubboxCounter*booleanSize+1] = 0;
        }
        if(lowerSubboxCounter < numberOfLowerLevelSubboxes) {
            map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize] = 0; // Mark old subbox as deleted.
            map[pointerToLowerBoolean+lowerSubboxCounter*booleanSize+1] = 0;
        }

        // Now build up forward pointers with a linear scan of the output
        // but only if this isnt a recursive call, since a recursive call
        // simply discards the pointers.
        if(!recursive) {
            long latestForward = 0;
            for(long i = totalElementsToOutput-1; i > -1; i--) {
                if(map[pointerToOutputBuffer+i*elementSize+1] > 0) {
                    // Real element, update forward pointer
                    map[pointerToOutputBuffer+i*elementSize+3] = latestForward;
                }
                else {
                    latestForward = pointerToOutputBuffer+i*elementSize;
                }
            }
        }
    }
}

/*
 * THIS PARTICULAR SAMPLE UP IS ONLY TO BE CALLED AFTER FLUSH.
 * For other Sample Up uses in the article use the other relevant methods.
 * Assumes all elements in an xBox is located in the output buffer, and
 * that the xBox is otherwise empty with no subboxes allocated.
 * We create up to half the subboxes and fill each up with forward pointers equivalent to
 * half the elements it can contain in its output buffer.
 * We then recursively call SampleUp on these subboxes.
 * We sample into the middle buffer.
 * We perform the same procedure for the upper level.
 * And finally we sample up into the input buffer.
 */
void XDict::sampleUpAfterFlush(long pointerToXBox) {

    //cout << "=== SampleUp on xBox " << pointerToXBox << "\n";

    if(map[pointerToXBox] <= minX) {

        /*if(map[pointerToXBox+2] == -1) {
            cout << "*** Called sample up on empty simple xbox " << pointerToXBox << "\n";
        }*/

        //cout << "Elements in subbox located at " << pointerToXBox << ": ";
        /*long index = 0;
        while(map[pointerToXBox+2+index*elementSize] != -1) {
            //cout << map[pointerToXBox+2+index*elementSize] << " ";
            if(index > minX*minX) {
                //cout << "\n";
                //cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ERROR IN SUBBOX\n";
            }
            index++;
        }
        //cout << "\n";*/

        return; // The surrounding xBox gave us pointers and will sample from us.
    }

    /*if(map[map[pointerToXBox+9]] == -1) {
        cout << "*** Called sample up on empty xbox " << pointerToXBox << "\n";
    }*/

    long x = map[pointerToXBox];
    long y = map[pointerToXBox+2];

    /*if(map[pointerToXBox+7] < pointerToXBox) {
        cout << "Error, middle pointer points outside xBox for " << pointerToXBox << "\n";
    }*/

    // Count the number of real elements + forward pointers from the xBox below.
    long elementsInOutput = 0;
    long pointerToOutput = map[pointerToXBox+9];
    while(map[pointerToOutput+elementsInOutput*elementSize] != -1) {
        elementsInOutput++;
    }

    // Create the appropriate amount of lower level subboxes
    long numberOfPointersToCreate = elementsInOutput/16;
    /*if(numberOfPointersToCreate % 16 != 0) {
        numberOfPointersToCreate++;
    }*/
    numberOfPointersToCreate++;

    if(numberOfPointersToCreate < 1) {
        numberOfPointersToCreate = 1;
    }

    /*long power = (long) pow(x,0.5 + alpha/2); // Size of output buffer of subbox
    if(y <= minX) {
        power = pow(y,1 + alpha); // Single array
    }*/
    long power = (long) pow(y,1 + alpha);

    //cout << elementsInOutput << " " << numberOfPointersToCreate << " " << power <<  "\n";

    long lowerLevelSubboxesToCreate = numberOfPointersToCreate/(power/2); // Never more than half of max
    if(numberOfPointersToCreate % (power/2) != 0) {
        lowerLevelSubboxesToCreate++; // Gotta create them all, divide evenly
    }
    if(lowerLevelSubboxesToCreate < 1) {
        lowerLevelSubboxesToCreate = 1; // Minimum
    }

    long maxNumberOfLowerLevel = map[pointerToXBox+5];
    long pointerToLowerBoolean = map[pointerToXBox+8];
    long latestLocation = pointerToLowerBoolean+maxNumberOfLowerLevel*booleanSize; // First subbox

    if(lowerLevelSubboxesToCreate > maxNumberOfLowerLevel) {
        lowerLevelSubboxesToCreate = maxNumberOfLowerLevel;
    }

    //cout << "Going to create " << lowerLevelSubboxesToCreate << " new lower level subboxes of y = " << y << "\n";
    for(int i = 0; i < lowerLevelSubboxesToCreate; i++) {
        ////cout << "Subbox will be located at " << latestLocation << "\n";
        map[pointerToLowerBoolean+i*booleanSize] = latestLocation;
        latestLocation = layoutXBox(latestLocation,y);
        ////cout << latestLocation << "\n";
    }

    // Create forward pointers in the output buffers of the subboxes
    long currentSubboxIndex = 0;
    long currentSubboxOutputBuffer;

    long pointersToEachSubbox = numberOfPointersToCreate/lowerLevelSubboxesToCreate;
    if(numberOfPointersToCreate % lowerLevelSubboxesToCreate != 0) {
        pointersToEachSubbox++;
    }
    long counter = 0;
    long sixteenth = 15; // So we make the first element a pointer
    long index = 0;

    if(y > minX) {
        // Subboxes are "proper" subboxes, i.e. not arrays.
        currentSubboxOutputBuffer = map[map[pointerToLowerBoolean+currentSubboxIndex*booleanSize]+9];
    }
    else {
        // Subboxes are just arrays
        currentSubboxOutputBuffer = map[pointerToLowerBoolean+currentSubboxIndex*booleanSize] + 2;
    }

    /*if(map[6] == -1) {
        cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! -1 in input before sampleUp to lower\n";
        for(int i = 0; i < 20; i++) {
            cout << map[i] << " ";
        }
        cout << "\n";
    }*/

    // Run through output creating pointers
    while(map[pointerToOutput+index*elementSize] != -1) {
        sixteenth++;
        if(sixteenth == 16) {
            // Create pointer
            if(counter < pointersToEachSubbox) {
                // Create pointer in current subbox
                map[currentSubboxOutputBuffer+counter*elementSize] = map[pointerToOutput+index*elementSize];
                map[currentSubboxOutputBuffer+counter*elementSize+1] = -2; // Forward pointer
                map[currentSubboxOutputBuffer+counter*elementSize+2] = pointerToOutput+index*elementSize; // Element in output buffer
                map[currentSubboxOutputBuffer+counter*elementSize+3] = 0; // Empty
            }
            else {
                // Switch subbox
                counter = 0;

                // Insert -1 at end of current subbox's buffer
                map[currentSubboxOutputBuffer+pointersToEachSubbox*elementSize] = -1;

                // Find next subbox
                currentSubboxIndex++;
                if(y > minX) {
                    // Subboxes are "proper" subboxes, i.e. not arrays.
                    currentSubboxOutputBuffer = map[map[pointerToLowerBoolean+currentSubboxIndex*booleanSize]+9];
                }
                else {
                    // Subboxes are just arrays
                    currentSubboxOutputBuffer = map[pointerToLowerBoolean+currentSubboxIndex*booleanSize] + 2;
                }

                //cout << "Switched subbox " << currentSubboxOutputBuffer << " " << index << "\n";

                // Insert this pointer
                map[currentSubboxOutputBuffer+counter*elementSize] = map[pointerToOutput+index*elementSize];
                map[currentSubboxOutputBuffer+counter*elementSize+1] = -2; // Forward pointer
                map[currentSubboxOutputBuffer+counter*elementSize+2] = pointerToOutput+index*elementSize; // Element in output buffer
                map[currentSubboxOutputBuffer+counter*elementSize+3] = 0; // Empty
            }
            sixteenth = 0;
            counter++;
        }
        index++;
    }

    // Insert -1 at the end of the last subbox's buffer
    map[currentSubboxOutputBuffer+counter*elementSize] = -1;

    /*if(currentSubboxIndex+1 < lowerLevelSubboxesToCreate) {
        cout << "*** Sample1 didnt sample to all created lower subboxes in " << pointerToXBox << "\n";
    }*/

    /*if(map[6] == -1) {
        cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! -1 in input after sampleUp to lower\n";
        cout << numberOfPointersToCreate << " " << maxNumberOfLowerLevel << " " << lowerLevelSubboxesToCreate
             << " " << currentSubboxIndex << " " << pointersToEachSubbox << " " << index
             << " " << elementsInOutput << "\n";

        for(int i = 0; i < 20; i++) {
            cout << map[i] << " ";
        }
        cout << "\n";
    }*/

    // Recursively call sample up on the subboxes in the lower level
    for(int i = 0; i < lowerLevelSubboxesToCreate; i++) {
        sampleUpAfterFlush(map[pointerToLowerBoolean+i*booleanSize]);
    }

    // Sample into the middle buffer while counting elements
    long sampleEveryNthElement = 0;
    if(y > minX) {
        sampleEveryNthElement = 16; // Subbox is a normal subbox
    }
    else {
        // Special case, subbox is an array much larger than a normal input array.
        // Therefore we sample less than every 16th element!
        // Multiply 16 with the increased ratio
        long numerator = (long) pow(y,1+alpha); // Actual size of array
        long ratio = numerator/y; // ratio
        sampleEveryNthElement = 16 * ratio;
    }

    //cout << "Sampling every " << sampleEveryNthElement << "th element in lower level to middle\n";

    long writeIndex = -1; // So we can count up every time
    long pointerToMiddle = map[pointerToXBox+7];
    // Assume the subboxes where place in appropriate sequence (they are)
    for(int i = 0; i < lowerLevelSubboxesToCreate; i++) {
        long pointerToSubbox = map[pointerToLowerBoolean+i*booleanSize];
        long pointerToSubboxInput = 0;
        long smallestKeyInSubbox = 0;
        if(y > minX) {
            pointerToSubboxInput = pointerToSubbox + infoOffset;
            smallestKeyInSubbox = map[map[pointerToSubbox+9]];
        }
        else {
            pointerToSubboxInput = pointerToSubbox+2;
            smallestKeyInSubbox = map[pointerToSubboxInput];
            /*if(smallestKeyInSubbox == -1) {
                cout << "*** Error, smallest key -1 in subbox " << pointerToSubbox << " parent " << pointerToXBox << "\n";
                recursivelyPrintXBox(pointerToSubbox);
            }*/
        }
        // Update
        map[pointerToLowerBoolean+i*booleanSize+1] = smallestKeyInSubbox;
        // Create subbox pointer
        writeIndex++;
        map[pointerToMiddle+writeIndex*elementSize] = smallestKeyInSubbox; // Key
        map[pointerToMiddle+writeIndex*elementSize+1] = -1; // Subbox pointer
        map[pointerToMiddle+writeIndex*elementSize+2] = pointerToSubbox;
        map[pointerToMiddle+writeIndex*elementSize+3] = pointerToLowerBoolean+i*booleanSize;

        // Read elements
        long subboxIndex = 1;
        long counter = 0;
        while(map[pointerToSubboxInput+subboxIndex*elementSize] != -1) {
            counter++;
            if(counter == sampleEveryNthElement) {
                // Sample this element
                writeIndex++;
                map[pointerToMiddle+writeIndex*elementSize] = map[pointerToSubboxInput+subboxIndex*elementSize]; // Key
                map[pointerToMiddle+writeIndex*elementSize+1] = -2; // Forward pointer
                map[pointerToMiddle+writeIndex*elementSize+2] = pointerToSubboxInput+subboxIndex*elementSize; // Point directly to this element
                map[pointerToMiddle+writeIndex*elementSize+3] = 0;
                counter = 0;
            }
            subboxIndex++;
        }
        //cout << "Counter=" << counter << "\n";
    }
    // Insert -1 at end of array
    writeIndex++;
    map[pointerToMiddle+writeIndex*elementSize] = -1;

    //cout << "Created " << writeIndex << " pointers in middle buffer\n";


    // Create the appropriate amount of upper level subboxes
    numberOfPointersToCreate = writeIndex/16;
    /*if(numberOfPointersToCreate % 16 != 0) {
        numberOfPointersToCreate++;
    }*/
    numberOfPointersToCreate++;
    /*if(numberOfPointersToCreate < 1) {
        numberOfPointersToCreate = 1;
    }*/
    // Power contains output buffer size of subbox
    long upperLevelSubboxesToCreate = numberOfPointersToCreate/(power/2); // Never more than half of max
    if(numberOfPointersToCreate % (power/2) != 0) {
        upperLevelSubboxesToCreate++; // Gotta create them all, divide evenly
    }
    if(upperLevelSubboxesToCreate < 1) {
        upperLevelSubboxesToCreate = 1; // Minimum
    }

    long maxNumberOfUpperLevel = map[pointerToXBox+4];
    long pointerToUpperBoolean = map[pointerToXBox+6];
    latestLocation = pointerToUpperBoolean+maxNumberOfUpperLevel*booleanSize; // First subbox

    if(upperLevelSubboxesToCreate > maxNumberOfUpperLevel) {
        upperLevelSubboxesToCreate = maxNumberOfUpperLevel;
    }

    //cout << "Going to create " << upperLevelSubboxesToCreate << " new upper level subboxes of y = " << y << "\n";
    for(int i = 0; i < upperLevelSubboxesToCreate; i++) {
        //cout << "Subbox will be located at " << latestLocation << "\n";
        map[pointerToUpperBoolean+i*booleanSize] = latestLocation;
        latestLocation = layoutXBox(latestLocation,y);
        ////cout << latestLocation << "\n";
    }

    // Sample up into the upper level subboxes
    currentSubboxIndex = 0;
    currentSubboxOutputBuffer;

    pointersToEachSubbox = numberOfPointersToCreate/upperLevelSubboxesToCreate;
    if(numberOfPointersToCreate % upperLevelSubboxesToCreate != 0) {
        pointersToEachSubbox++;
    }
    counter = 0;
    sixteenth = 15; // So we make the first element a pointer
    index = 0;

    if(y > minX) {
        // Subboxes are "proper" subboxes, i.e. not arrays.
        currentSubboxOutputBuffer = map[map[pointerToUpperBoolean+currentSubboxIndex*booleanSize]+9];
    }
    else {
        // Subboxes are just arrays
        currentSubboxOutputBuffer = map[pointerToUpperBoolean+currentSubboxIndex*booleanSize] + 2;
    }

    /*long tempIndex = -1;
    //cout << "Middle consists of: ";
    do {
        tempIndex++;
        //cout << map[pointerToMiddle+tempIndex*elementSize] << " ";
    }
    while(map[pointerToMiddle+tempIndex*elementSize] != -1);
    //cout << "\n";*/

    //cout << "Pointers to each subbox = " << pointersToEachSubbox << "\n";
    /*if(fileSize > 1967490 && map[1967490] == -1) {
        cout << "*** 64 " << pointerToXBox << "\n";
    }*/

    // Run through middle creating pointers
    while(map[pointerToMiddle+index*elementSize] != -1) {
        sixteenth++;
        if(sixteenth == 16) {
            // Create pointer
            if(counter < pointersToEachSubbox) {
                // Create pointer in current subbox
                ////cout << "Writing " << map[pointerToMiddle+index*4] << " to " << map[pointerToUpperBoolean+currentSubboxIndex] << "\n";
                /*if(map[pointerToUpperBoolean+currentSubboxIndex*booleanSize] == 1967491) {
                    cout << "Inserting " << map[pointerToMiddle+index*4] << " into 1967491\n";
                }*/

                map[currentSubboxOutputBuffer+counter*elementSize] = map[pointerToMiddle+index*elementSize];
                map[currentSubboxOutputBuffer+counter*elementSize+1] = -2; // Forward pointer
                map[currentSubboxOutputBuffer+counter*elementSize+2] = pointerToMiddle+index*elementSize; // Element in output buffer
                map[currentSubboxOutputBuffer+counter*elementSize+3] = 0; // Empty
            }
            else {
                // Switch subbox
                counter = 0;

                // Insert -1 at end of current subbox's buffer
                map[currentSubboxOutputBuffer+pointersToEachSubbox*elementSize] = -1;

                // Find next subbox
                currentSubboxIndex++;
                if(y > minX) {
                    // Subboxes are "proper" subboxes, i.e. not arrays.
                    currentSubboxOutputBuffer = map[map[pointerToUpperBoolean+currentSubboxIndex*booleanSize]+9];
                }
                else {
                    // Subboxes are just arrays
                    currentSubboxOutputBuffer = map[pointerToUpperBoolean+currentSubboxIndex*booleanSize] + 2;
                }

                /*if(map[pointerToUpperBoolean+currentSubboxIndex*booleanSize] == 1967491) {
                    cout << "Inserting " << map[pointerToMiddle+index*4] << " into 1967491\n";
                }*/

                // Insert this pointer
                map[currentSubboxOutputBuffer+counter*elementSize] = map[pointerToMiddle+index*elementSize];
                map[currentSubboxOutputBuffer+counter*elementSize+1] = -2; // Forward pointer
                map[currentSubboxOutputBuffer+counter*elementSize+2] = pointerToMiddle+index*elementSize; // Element in output buffer
                map[currentSubboxOutputBuffer+counter*elementSize+3] = 0; // Empty
            }
            sixteenth = 0;
            counter++;
        }
        index++;
    }

    // Insert -1 at the end of the last subbox's buffer
    map[currentSubboxOutputBuffer+counter*elementSize] = -1;

    /*if(currentSubboxIndex+1 < upperLevelSubboxesToCreate) {
        cout << "*** Sample2 Didnt sample pointers to all created upper subboxes in " << pointerToXBox << "\n";
    }

    if(fileSize > 1967490 && map[1967490] == -1) {
        cout << "*** 65 " << pointerToXBox << "\n";
    }*/


    // Recursively call SampleUp on the upper level subboxes
    for(int i = 0; i < upperLevelSubboxesToCreate; i++) {
        sampleUpAfterFlush(map[pointerToUpperBoolean+i*booleanSize]);
    }

    /*if(fileSize > 1967490 && map[1967490] == -1) {
        cout << "*** 66 " << pointerToXBox << "\n";
    }*/

    // Sample from the upper level subboxes into the input buffer
    //cout << "Sampling every " << sampleEveryNthElement << "th element in upper level to input\n";

    writeIndex = -1; // So we can count up every time
    long pointerToInput = pointerToXBox+infoOffset;
    // Assume the subboxes where place in appropriate sequence (they are)
    for(int i = 0; i < upperLevelSubboxesToCreate; i++) {
        long pointerToSubbox = map[pointerToUpperBoolean+i*booleanSize];
        long pointerToSubboxInput = 0;
        long smallestKeyInSubbox = 0;
        if(y > minX) {
            pointerToSubboxInput = pointerToSubbox + infoOffset;
            smallestKeyInSubbox = map[map[pointerToSubbox+9]];
        }
        else {
            pointerToSubboxInput = pointerToSubbox+2;
            smallestKeyInSubbox = map[pointerToSubboxInput];
        }
        // Update
        map[pointerToUpperBoolean+i*booleanSize+1] = smallestKeyInSubbox;
        // Create subbox pointer
        writeIndex++;
        map[pointerToInput+writeIndex*elementSize] = smallestKeyInSubbox; // Key
        map[pointerToInput+writeIndex*elementSize+1] = -1; // Subbox pointer
        map[pointerToInput+writeIndex*elementSize+2] = pointerToSubbox;
        map[pointerToInput+writeIndex*elementSize+3] = pointerToUpperBoolean+i*booleanSize;

        // Read elements
        long subboxIndex = 1;
        long counter = 0;
        while(map[pointerToSubboxInput+subboxIndex*elementSize] != -1) {
            counter++;
            if(counter == sampleEveryNthElement) {
                // Sample this element
                writeIndex++;
                map[pointerToInput+writeIndex*elementSize] = map[pointerToSubboxInput+subboxIndex*elementSize]; // Key
                map[pointerToInput+writeIndex*elementSize+1] = -2; // Forward pointer
                map[pointerToInput+writeIndex*elementSize+2] = pointerToSubboxInput+subboxIndex*elementSize; // Point directly to this element
                map[pointerToInput+writeIndex*elementSize+3] = 0;
                counter = 0;
            }
            subboxIndex++;
        }
        //cout << "Counter=" << counter << "\n";
    }
    // Insert -1 at end of array
    writeIndex++;
    map[pointerToInput+writeIndex*elementSize] = -1;

    /*if(fileSize > 1967490 && map[1967490] == -1) {
        cout << "*** 67 " << pointerToXBox << "\n";
    }*/

    //cout << "Created " << writeIndex << " pointers in input buffer\n";

    /*tempIndex = -1;
    //cout << "Input consists of: ";
    do {
        tempIndex++;
        //cout << map[pointerToInput+tempIndex*elementSize] << " ";
    }
    while(map[pointerToInput+tempIndex*elementSize] != -1);
    //cout << "\n";*/
}

/*
 * Queries an xBox at the given location, recursively.
 * ForwardPointer points into the given xBox's input buffer.
 * Will return a pointer. This is either a pointer to the element,
 * or a pointer into the next xBox. This differs from the paper where
 * we return a pointer into this xBox's output buffer.
 */
long XDict::search(long xBoxPointer, long forwardPointer, long element) {

    //cout << "Search on xBox " << xBoxPointer << " with forward pointer " << forwardPointer << " for element " << element << "\n";

    if(map[xBoxPointer] <= minX) {
        // Special case, scan the array at forward pointer
        long index = 0;
        //cout << "Listing the elements we search: ";
        while(map[forwardPointer+index*elementSize] != -1 && map[forwardPointer+index*elementSize] < element) {
            /*cout << map[forwardPointer+index*elementSize] << " " << map[forwardPointer+index*elementSize+1] << " "
                    << map[forwardPointer+index*elementSize+2] << " " << map[forwardPointer+index*elementSize+3] << " | ";*/
            index++;
        }
        /*cout << map[forwardPointer+index*elementSize] << " " << map[forwardPointer+index*elementSize+1] << " "
             << map[forwardPointer+index*elementSize+2] << " " << map[forwardPointer+index*elementSize+3] << " | ";
        cout << "\n";*/

        /*if(element == 361) {
            long tInd = 0;
            cout << "Listing the elements we search in " << xBoxPointer << ": ";
            while(map[forwardPointer+tInd*elementSize] != -1 && map[forwardPointer+tInd*elementSize] < element) {
                cout << map[forwardPointer+tInd*elementSize] << " " << map[forwardPointer+tInd*elementSize+1] << " "
                        << map[forwardPointer+tInd*elementSize+2] << " " << map[forwardPointer+tInd*elementSize+3] << " | ";
                tInd++;
            }
            cout << map[forwardPointer+tInd*elementSize] << " " << map[forwardPointer+tInd*elementSize+1] << " "
                 << map[forwardPointer+tInd*elementSize+2] << " " << map[forwardPointer+tInd*elementSize+3] << " | ";
            cout << "\n";
        }*/

        // Couple of cases now
        // 1) We reached the end of the array
        // 2) We found an element with the same key
        // 2a) Its the real deal
        // 2b) Its a pointer we can follow
        // 3) We found an element with a larger key
        // 3a) We found a real element with a larger key
        // 3b) We found a pointer, subbox or forward, with a larger key

        // Case 1) We reached the end of the array
        if(map[forwardPointer+index*elementSize] == -1) {
            ////cout << "Case 1\n";
            index--;
            if(map[forwardPointer+index*elementSize+1] < 0) {
                // Return this pointer
                //return map[forwardPointer+index*elementSize+2];
                // This is either a forward pointer or a subbox pointer
                long value = map[forwardPointer+index*elementSize+1];
                long pointer = map[forwardPointer+index*elementSize+2];
                if(value == -2) {
                    // Forward
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    return pointer;
                }
                else {
                    // Subbox, calculate start of input buffer
                    long y = map[pointer];
                    long ret = 0;
                    if(y <= minX) {
                        // Start of input of a simple xBox
                        ret = pointer+2;
                    }
                    else {
                        // Location + offset
                        ret = pointer + infoOffset;
                    }
                    return ret;
                }
            }
            else {
                // Return the pointer of the reverse pointer
                // return map[map[forwardPointer+index*elementSize+2]+2];

                // This is either a forward pointer or a subbox pointer
                long reversePointer = map[forwardPointer+index*elementSize+2];
                if(reversePointer == 0) {
                    return 0; // Convention
                }
                long value = map[reversePointer+1];
                long pointer = map[reversePointer+2];
                if(value == -2) {
                    // Forward
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    return pointer;
                }
                else {
                    // Subbox, calculate start of input buffer
                    long y = map[pointer];
                    long ret = 0;
                    if(y <= minX) {
                        // Start of input of a simple xBox
                        ret = pointer+2;
                    }
                    else {
                        // Location + offset
                        ret = pointer + infoOffset;
                    }
                    return ret;
                }
            }
        }
        // Case 2) We found an element with the same key
        else if(map[forwardPointer+index*elementSize] == element) {
            ////cout << "Case 2\n";
            // Is this the real deal or a pointer?
            if(map[forwardPointer+index*elementSize+1] > 0) {
                // Return pointer to this element
                return forwardPointer+index*elementSize;
            }
            else {
                // I lied, we can actually have multiple elements with the same value.
                // Look ahead and see if we can still find the real deal.
                long tempIndex = index+1;
                while(map[forwardPointer+tempIndex*elementSize] == element) {
                    if(map[forwardPointer+tempIndex*elementSize+1] > 0) {
                        return forwardPointer+tempIndex*elementSize;
                    }
                    tempIndex++;
                }
                // No luck? Follow original pointer
                //return map[forwardPointer+index*elementSize+2];
                long value = map[forwardPointer+index*elementSize+1];
                long pointer = map[forwardPointer+index*elementSize+2];
                if(value == -2) {
                    // Forward
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    return pointer;
                }
                else {
                    // Subbox, calculate start of input buffer
                    long y = map[pointer];
                    long ret = 0;
                    if(y <= minX) {
                        // Start of input of a simple xBox
                        ret = pointer+2;
                    }
                    else {
                        // Location + offset
                        ret = pointer + infoOffset;
                    }
                    return ret;
                }
            }
        }
            // Case 3) We found an element with a larger key
        else if(map[forwardPointer+index*elementSize] > element) {
            ////cout << "Case 3\n";
            // Case 3a) We found a real element with a larger key
            if(map[forwardPointer+index*elementSize+1] > 0) {
                // Return the pointer of the reverse pointer
                ////cout << "Case 3a\n";
                //return map[map[forwardPointer+index*elementSize+2]+2];
                long reversePointer = map[forwardPointer+index*elementSize+2];
                if(reversePointer == 0) {
                    return 0; // Convention
                }
                long value = map[reversePointer+1];
                long pointer = map[reversePointer+2];
                if(value == -2) {
                    // Forward
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    return pointer;
                }
                else {
                    // Subbox, calculate start of input buffer
                    long y = map[pointer];
                    long ret = 0;
                    if(y <= minX) {
                        // Start of input of a simple xBox
                        ret = pointer+2;
                    }
                    else {
                        // Location + offset
                        ret = pointer + infoOffset;
                    }
                    return ret;
                }
            }
            else {
                if(index == 0) {
                    return 0; // Convention
                }
                index--;
                if(map[forwardPointer+index*elementSize+1] > 0) {
                    // Return the pointer of the reverse pointer
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    long reversePointer = map[forwardPointer+index*elementSize+2];
                    if(reversePointer == 0) {
                        return 0; // Convention
                    }
                    long value = map[reversePointer+1];
                    long pointer = map[reversePointer+2];
                    if(value == -2) {
                        // Forward
                        //return map[map[forwardPointer+index*elementSize+2]+2];
                        return pointer;
                    }
                    else {
                        // Subbox, calculate start of input buffer
                        long y = map[pointer];
                        long ret = 0;
                        if(y <= minX) {
                            // Start of input of a simple xBox
                            ret = pointer+2;
                        }
                        else {
                            // Location + offset
                            ret = pointer + infoOffset;
                        }
                        return ret;
                    }
                }
                else {
                    ////cout << "Case 3b\n";
                    /*//cout << "subbox element " << map[forwardPointer+index*elementSize] << " "
                         << map[forwardPointer+index*elementSize+1] << " " << map[forwardPointer+index*elementSize+2]
                         << " " << map[forwardPointer+index*elementSize+3] << "\n";*/
                    //return map[forwardPointer+index*elementSize+2]; // Return pointer
                    // This is either a forward pointer or a subbox pointer
                    long value = map[forwardPointer+index*elementSize+1];
                    long pointer = map[forwardPointer+index*elementSize+2];
                    if(value == -2) {
                        // Forward
                        //return map[map[forwardPointer+index*elementSize+2]+2];
                        return pointer;
                    }
                    else {
                        // Subbox, calculate start of input buffer
                        long y = map[pointer];
                        long ret = 0;
                        if(y <= minX) {
                            // Start of input of a simple xBox
                            ret = pointer+2;
                        }
                        else {
                            // Location + offset
                            ret = pointer + infoOffset;
                        }
                        return ret;
                    }
                }
            }
        }
        else {
            // You should never end up here, go away!
            //cout << "!!!!! ERROR IN SEARCH ON XBOX SIMPLE" << xBoxPointer << "\n";
        }
    }
    else {
        // Proper xBox

        /*//cout << "---\n";
        //cout << map[xBoxPointer] << " " << map[xBoxPointer+1] << " " << map[xBoxPointer+2] << " " << map[xBoxPointer+3] << " "
             << map[xBoxPointer+4] << " " << map[xBoxPointer+5] << " " << map[xBoxPointer+6] << " " << map[xBoxPointer+7] << " "
             << map[xBoxPointer+8] << " " << map[xBoxPointer+9] << "\n";

        long maxUpperLevel = map[xBoxPointer+4];
        long upB = map[xBoxPointer+6];
        for(int i = 0; i < maxUpperLevel; i++) {
            //cout << map[upB+i] << " ";
        }
        //cout << "\n";

        long maxLowerLevel = map[xBoxPointer+5];
        long lB = map[xBoxPointer+8];
        for(int i = 0; i < maxLowerLevel; i++) {
            //cout << map[lB+i] << " ";
        }
        //cout << "\n";


        //cout << "---\n";*/

        // ***Scan our input buffer starting at forwardPointer
        //cout << "Listing the elements we search in input of " << xBoxPointer << ": ";
        long index = 0;
        while(map[forwardPointer+index*elementSize] != -1 && map[forwardPointer+index*elementSize] < element) {
            /*//cout << "Input element " << map[forwardPointer+index*elementSize] << " "
                 << map[forwardPointer+index*elementSize+1] << " " << map[forwardPointer+index*elementSize+2]
                 << " " << map[forwardPointer+index*elementSize+3] << "\n";*/
            /*cout << map[forwardPointer+index*elementSize] << " " << map[forwardPointer+index*elementSize+1] << " "
                 << map[forwardPointer+index*elementSize+2] << " " << map[forwardPointer+index*elementSize+3] << " | ";*/
            index++;
        }
        /*cout << map[forwardPointer+index*elementSize] << " " << map[forwardPointer+index*elementSize+1] << " "
             << map[forwardPointer+index*elementSize+2] << " " << map[forwardPointer+index*elementSize+3] << " | ";
        cout << "\n";*/

        /*//cout << "Index Input = " << index << " " << map[forwardPointer+index*elementSize] << "\n";
        //cout << map[forwardPointer+index*elementSize] << " " << map[forwardPointer+index*elementSize+1] << " "
             << map[forwardPointer+index*elementSize+2] << " " << map[forwardPointer+index*elementSize+3] << "\n";*/

        // Case 1) We reached the end of the array
        if(map[forwardPointer+index*elementSize] == -1) {
            ////cout << "Case 1\n";
            index--;
            if(map[forwardPointer+index*elementSize+1] < 0) {
                // Return this pointer
                //return map[forwardPointer+index*elementSize+2];
                // This is either a forward pointer or a subbox pointer
                long value = map[forwardPointer+index*elementSize+1];
                long pointer = map[forwardPointer+index*elementSize+2];
                if(value == -2) {
                    // Forward
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    forwardPointer = pointer;
                }
                else {
                    // Subbox, calculate start of input buffer
                    long y = map[pointer];
                    long ret = 0;
                    if(y <= minX) {
                        // Start of input of a simple xBox
                        ret = pointer+2;
                    }
                    else {
                        // Location + offset
                        ret = pointer + infoOffset;
                    }
                    forwardPointer = ret;
                }
            }
            else {
                // Return the pointer of the reverse pointer
                // return map[map[forwardPointer+index*elementSize+2]+2];

                // This is either a forward pointer or a subbox pointer
                long reversePointer = map[forwardPointer+index*elementSize+2];
                if(reversePointer == 0) {
                    forwardPointer = 0; // Convention
                }
                else {
                    long value = map[reversePointer+1];
                    long pointer = map[reversePointer+2];
                    if(value == -2) {
                        // Forward
                        //return map[map[forwardPointer+index*elementSize+2]+2];
                        forwardPointer = pointer;
                    }
                    else {
                        // Subbox, calculate start of input buffer
                        long y = map[pointer];
                        long ret = 0;
                        if(y <= minX) {
                            // Start of input of a simple xBox
                            ret = pointer+2;
                        }
                        else {
                            // Location + offset
                            ret = pointer + infoOffset;
                        }
                        forwardPointer = ret;
                    }
                }

            }
        }
            // Case 2) We found an element with the same key
        else if(map[forwardPointer+index*elementSize] == element) {
            ////cout << "Case 2\n";
            // Is this the real deal or a pointer?
            if(map[forwardPointer+index*elementSize+1] > 0) {
                // Return pointer to this element
                return forwardPointer+index*elementSize;
            }
            else {
                // I lied, we can actually have multiple elements with the same value.
                // Look ahead and see if we can still find the real deal.
                long tempIndex = index+1;
                while(map[forwardPointer+tempIndex*elementSize] == element) {
                    if(map[forwardPointer+tempIndex*elementSize+1] > 0) {
                        return forwardPointer+tempIndex*elementSize;
                    }
                    tempIndex++;
                }
                // No luck? Follow original pointer
                //return map[forwardPointer+index*elementSize+2];
                long value = map[forwardPointer+index*elementSize+1];
                long pointer = map[forwardPointer+index*elementSize+2];
                if(value == -2) {
                    // Forward
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    forwardPointer = pointer;
                }
                else {
                    // Subbox, calculate start of input buffer
                    long y = map[pointer];
                    long ret = 0;
                    if(y <= minX) {
                        // Start of input of a simple xBox
                        ret = pointer+2;
                    }
                    else {
                        // Location + offset
                        ret = pointer + infoOffset;
                    }
                    forwardPointer = ret;
                }
            }
        }
            // Case 3) We found an element with a larger key
        else if(map[forwardPointer+index*elementSize] > element) {
            ////cout << "Case 3\n";
            // Case 3a) We found a real element with a larger key
            if(map[forwardPointer+index*elementSize+1] > 0) {
                // Return the pointer of the reverse pointer
                ////cout << "Case 3a\n";
                //return map[map[forwardPointer+index*elementSize+2]+2];
                long reversePointer = map[forwardPointer+index*elementSize+2];
                if(reversePointer == 0) {
                    forwardPointer = 0; // Convention
                }
                else {
                    long value = map[reversePointer+1];
                    long pointer = map[reversePointer+2];
                    if(value == -2) {
                        // Forward
                        //return map[map[forwardPointer+index*elementSize+2]+2];
                        forwardPointer = pointer;
                    }
                    else {
                        // Subbox, calculate start of input buffer
                        long y = map[pointer];
                        long ret = 0;
                        if(y <= minX) {
                            // Start of input of a simple xBox
                            ret = pointer+2;
                        }
                        else {
                            // Location + offset
                            ret = pointer + infoOffset;
                        }
                        forwardPointer = ret;
                    }
                }
            }
            else {
                if(index == 0) {
                    forwardPointer = 0;
                }
                else {
                    index--;
                    if(map[forwardPointer+index*elementSize+1] > 0) {
                        // Return the pointer of the reverse pointer
                        //return map[map[forwardPointer+index*elementSize+2]+2];
                        long reversePointer = map[forwardPointer+index*elementSize+2];
                        if(reversePointer == 0) {
                            forwardPointer = 0; // Convention
                        }
                        else {
                            long value = map[reversePointer+1];
                            long pointer = map[reversePointer+2];
                            if(value == -2) {
                                // Forward
                                //return map[map[forwardPointer+index*elementSize+2]+2];
                                forwardPointer = pointer;
                            }
                            else {
                                // Subbox, calculate start of input buffer
                                long y = map[pointer];
                                long ret = 0;
                                if(y <= minX) {
                                    // Start of input of a simple xBox
                                    ret = pointer+2;
                                }
                                else {
                                    // Location + offset
                                    ret = pointer + infoOffset;
                                }
                                forwardPointer = ret;
                            }
                        }
                    }
                    else {
                        ////cout << "Case 3b\n";
                        /*//cout << "subbox element " << map[forwardPointer+index*elementSize] << " "
                             << map[forwardPointer+index*elementSize+1] << " " << map[forwardPointer+index*elementSize+2]
                             << " " << map[forwardPointer+index*elementSize+3] << "\n";*/
                        //return map[forwardPointer+index*elementSize+2]; // Return pointer
                        // This is either a forward pointer or a subbox pointer
                        long value = map[forwardPointer+index*elementSize+1];
                        long pointer = map[forwardPointer+index*elementSize+2];
                        if(value == -2) {
                            // Forward
                            //return map[map[forwardPointer+index*elementSize+2]+2];
                            forwardPointer = pointer;
                        }
                        else {
                            // Subbox, calculate start of input buffer
                            long y = map[pointer];
                            long ret = 0;
                            if(y <= minX) {
                                // Start of input of a simple xBox
                                ret = pointer+2;
                            }
                            else {
                                // Location + offset
                                ret = pointer + infoOffset;
                            }
                            forwardPointer = ret;
                        }
                    }
                }
            }
        }
        else {
            // You should never end up here, go away!
            //cout << "!!!!! ERROR IN SEARCH ON XBOX INPUT" << xBoxPointer << "\n";
        }

        /*cout << "Input " << xBoxPointer << " forward = " << forwardPointer << " points to " << map[forwardPointer] << " " << map[forwardPointer+1] << " " << map[forwardPointer+2]
                << " " << map[forwardPointer+3] << "\n";*/

        // ***Recursively search the upper level subbox

        // Calculate which subbox we point into
        long sizeOfSubboxes = map[xBoxPointer+3];
        long numberOfMaxUpperLevel = map[xBoxPointer+4];
        long upperBoolean = map[xBoxPointer+6];
        long upperLevelStartsAt = upperBoolean + numberOfMaxUpperLevel*booleanSize;
        long subboxNumber = (forwardPointer-upperLevelStartsAt) / sizeOfSubboxes;
        if(forwardPointer == 0) {
            subboxNumber = 0;
        }
        long subboxPointer = upperLevelStartsAt+subboxNumber*sizeOfSubboxes;
        if(forwardPointer == 0) {
            if(map[subboxPointer] <= minX) {
                forwardPointer = subboxPointer+2;
            }
            else {
                forwardPointer = subboxPointer + infoOffset;
            }
        }

        if(map[upperBoolean+subboxNumber*booleanSize] == 0) {
            //cout << "Error, Search pointed to non-existing upper subbox\n";
            return -1;
        }

        /*cout << "UpperBool=" << upperBoolean << " upperLevelStartsAt=" << upperLevelStartsAt
             << " subboxNumber=" << subboxNumber << " subboxSize=" << sizeOfSubboxes
             << " subboxPointer=" << subboxPointer << " forward=" << forwardPointer << "\n";*/

        forwardPointer = search(subboxPointer,forwardPointer,element);
        if(forwardPointer == 0) {
            forwardPointer = map[xBoxPointer+7]; // Middle, convention
        }

        if(forwardPointer == -1) {
            //cout << "Error, upper subbox returned -1\n";
            return -1; // Error
        }

        if(map[forwardPointer] == element && map[forwardPointer+1] > 0) {
            //return map[forwardPointer+1];
            //cout << "Upper subbox pointed directly at element\n";
            return forwardPointer;
        }

        /*cout << "Search on upper subbox returned " << forwardPointer << " containing " << map[forwardPointer]
             << " " << map[forwardPointer+1] << " " << map[forwardPointer+2] << " " << map[forwardPointer+3] << "\n";*/

        // ***Scan the middle buffer starting at the forward pointer returned from the upper level
        //cout << "Listing the elements we scan in middle of " << xBoxPointer << ": ";
        index = 0;
        while(map[forwardPointer+index*elementSize] != -1 && map[forwardPointer+index*elementSize] < element) {
            /*cout << map[forwardPointer+index*elementSize] << " " << map[forwardPointer+index*elementSize+1] << " "
                 << map[forwardPointer+index*elementSize+2] << " " << map[forwardPointer+index*elementSize+3] << " | ";*/
            index++;
        }

        /*cout << map[forwardPointer+index*elementSize] << " " << map[forwardPointer+index*elementSize+1] << " "
             << map[forwardPointer+index*elementSize+2] << " " << map[forwardPointer+index*elementSize+3] << " | ";
        cout << "\n";*/

        /*//cout << "Index Middle = " << index << " " << map[forwardPointer+index*elementSize] << "\n";
        //cout << map[forwardPointer+index*elementSize] << " " << map[forwardPointer+index*elementSize+1] << " "
             << map[forwardPointer+index*elementSize+2] << " " << map[forwardPointer+index*elementSize+3] << "\n";*/

        // Case 1) We reached the end of the array
        if(map[forwardPointer+index*elementSize] == -1) {
            ////cout << "Case 1\n";
            index--;
            if(map[forwardPointer+index*elementSize+1] < 0) {
                // Return this pointer
                //return map[forwardPointer+index*elementSize+2];
                // This is either a forward pointer or a subbox pointer
                long value = map[forwardPointer+index*elementSize+1];
                long pointer = map[forwardPointer+index*elementSize+2];
                if(value == -2) {
                    // Forward
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    forwardPointer = pointer;
                }
                else {
                    // Subbox, calculate start of input buffer
                    long y = map[pointer];
                    long ret = 0;
                    if(y <= minX) {
                        // Start of input of a simple xBox
                        ret = pointer+2;
                    }
                    else {
                        // Location + offset
                        ret = pointer + infoOffset;
                    }
                    forwardPointer = ret;
                }
            }
            else {
                // Return the pointer of the reverse pointer
                // return map[map[forwardPointer+index*elementSize+2]+2];

                // This is either a forward pointer or a subbox pointer
                long reversePointer = map[forwardPointer+index*elementSize+2];
                if(reversePointer == 0) {
                    forwardPointer = 0; // Convention
                }
                else {
                    long value = map[reversePointer+1];
                    long pointer = map[reversePointer+2];
                    if(value == -2) {
                        // Forward
                        //return map[map[forwardPointer+index*elementSize+2]+2];
                        forwardPointer = pointer;
                    }
                    else {
                        // Subbox, calculate start of input buffer
                        long y = map[pointer];
                        long ret = 0;
                        if(y <= minX) {
                            // Start of input of a simple xBox
                            ret = pointer+2;
                        }
                        else {
                            // Location + offset
                            ret = pointer + infoOffset;
                        }
                        forwardPointer = ret;
                    }
                }

            }
        }
            // Case 2) We found an element with the same key
        else if(map[forwardPointer+index*elementSize] == element) {
            ////cout << "Case 2\n";
            // Is this the real deal or a pointer?
            if(map[forwardPointer+index*elementSize+1] > 0) {
                // Return pointer to this element
                return forwardPointer+index*elementSize;
            }
            else {
                // I lied, we can actually have multiple elements with the same value.
                // Look ahead and see if we can still find the real deal.
                long tempIndex = index+1;
                while(map[forwardPointer+tempIndex*elementSize] == element) {
                    if(map[forwardPointer+tempIndex*elementSize+1] > 0) {
                        return forwardPointer+tempIndex*elementSize;
                    }
                    tempIndex++;
                }
                // No luck? Follow original pointer
                //return map[forwardPointer+index*elementSize+2];
                long value = map[forwardPointer+index*elementSize+1];
                long pointer = map[forwardPointer+index*elementSize+2];
                if(value == -2) {
                    // Forward
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    forwardPointer = pointer;
                }
                else {
                    // Subbox, calculate start of input buffer
                    long y = map[pointer];
                    long ret = 0;
                    if(y <= minX) {
                        // Start of input of a simple xBox
                        ret = pointer+2;
                    }
                    else {
                        // Location + offset
                        ret = pointer + infoOffset;
                    }
                    forwardPointer = ret;
                }
            }
        }
            // Case 3) We found an element with a larger key
        else if(map[forwardPointer+index*elementSize] > element) {
            ////cout << "Case 3\n";
            // Case 3a) We found a real element with a larger key
            if(map[forwardPointer+index*elementSize+1] > 0) {
                // Return the pointer of the reverse pointer
                ////cout << "Case 3a\n";
                //return map[map[forwardPointer+index*elementSize+2]+2];
                long reversePointer = map[forwardPointer+index*elementSize+2];
                if(reversePointer == 0) {
                    forwardPointer = 0; // Convention
                }
                else {
                    long value = map[reversePointer+1];
                    long pointer = map[reversePointer+2];
                    if(value == -2) {
                        // Forward
                        //return map[map[forwardPointer+index*elementSize+2]+2];
                        forwardPointer = pointer;
                    }
                    else {
                        // Subbox, calculate start of input buffer
                        long y = map[pointer];
                        long ret = 0;
                        if(y <= minX) {
                            // Start of input of a simple xBox
                            ret = pointer+2;
                        }
                        else {
                            // Location + offset
                            ret = pointer + infoOffset;
                        }
                        forwardPointer = ret;
                    }
                }
            }
            else {
                if(index == 0) {
                    forwardPointer = 0;
                }
                else {
                    index--;
                    if(map[forwardPointer+index*elementSize+1] > 0) {
                        // Return the pointer of the reverse pointer
                        //return map[map[forwardPointer+index*elementSize+2]+2];
                        long reversePointer = map[forwardPointer+index*elementSize+2];
                        if(reversePointer == 0) {
                            forwardPointer = 0; // Convention
                        }
                        else {
                            long value = map[reversePointer+1];
                            long pointer = map[reversePointer+2];
                            if(value == -2) {
                                // Forward
                                //return map[map[forwardPointer+index*elementSize+2]+2];
                                forwardPointer = pointer;
                            }
                            else {
                                // Subbox, calculate start of input buffer
                                long y = map[pointer];
                                long ret = 0;
                                if(y <= minX) {
                                    // Start of input of a simple xBox
                                    ret = pointer+2;
                                }
                                else {
                                    // Location + offset
                                    ret = pointer + infoOffset;
                                }
                                forwardPointer = ret;
                            }
                        }
                    }
                    else {
                        ////cout << "Case 3b\n";
                        /*//cout << "subbox element " << map[forwardPointer+index*elementSize] << " "
                             << map[forwardPointer+index*elementSize+1] << " " << map[forwardPointer+index*elementSize+2]
                             << " " << map[forwardPointer+index*elementSize+3] << "\n";*/
                        //return map[forwardPointer+index*elementSize+2]; // Return pointer
                        // This is either a forward pointer or a subbox pointer
                        long value = map[forwardPointer+index*elementSize+1];
                        long pointer = map[forwardPointer+index*elementSize+2];
                        if(value == -2) {
                            // Forward
                            //return map[map[forwardPointer+index*elementSize+2]+2];
                            forwardPointer = pointer;
                        }
                        else {
                            // Subbox, calculate start of input buffer
                            long y = map[pointer];
                            long ret = 0;
                            if(y <= minX) {
                                // Start of input of a simple xBox
                                ret = pointer+2;
                            }
                            else {
                                // Location + offset
                                ret = pointer + infoOffset;
                            }
                            forwardPointer = ret;
                        }
                    }
                }
            }
        }
        else {
            // You should never end up here, go away!
            //cout << "!!!!! ERROR IN SEARCH ON XBOX MIDDLE" << xBoxPointer << "\n";
        }

        // ***Recursively search the lower level

        // Calculate which subbox we point into
        long numberOfMaxLowerLevel = map[xBoxPointer+5];
        long lowerBoolean = map[xBoxPointer+8];
        long lowerLevelStartsAt = lowerBoolean + numberOfMaxLowerLevel*booleanSize;
        subboxNumber = (forwardPointer-lowerLevelStartsAt) / sizeOfSubboxes;
        if(forwardPointer == 0) {
            subboxNumber = 0;
        }
        subboxPointer = lowerLevelStartsAt+subboxNumber*sizeOfSubboxes;
        if(forwardPointer == 0) {
            if(map[subboxPointer] <= minX) {
                forwardPointer = subboxPointer+2;
            }
            else {
                forwardPointer = subboxPointer + infoOffset;
            }
        }

        if(map[lowerBoolean+subboxNumber*booleanSize] == 0) {
            //cout << "Error, Search pointed to non-existing lower subbox\n";
            return -1;
        }

        //cout << "Searching lower subbox " << subboxPointer << " of xBox " << xBoxPointer << "\n";

        forwardPointer = search(subboxPointer,forwardPointer,element);

        /*if(element == 708) {
            cout << "Subbox returned forward " << forwardPointer << "\n";
        }*/

        if(forwardPointer == 0) {
            forwardPointer = map[xBoxPointer+9]; // Output, convention
        }

        if(forwardPointer == -1) {
            //cout << "Lower subbox in " << xBoxPointer << " returned -1\n";
            return -1; // Error
        }

        if(map[forwardPointer] == element && map[forwardPointer+1] > 0) {
            /*//cout << "Lower subbox pointed directly to " << map[forwardPointer] << " "
                 << map[forwardPointer+1] << " " << map[forwardPointer+2] << " " << map[forwardPointer+3] << "\n";*/
            //return map[forwardPointer+1];
            return forwardPointer;
        }

        /*//cout << "Search on lower subbox returned " << forwardPointer << " containing " << map[forwardPointer]
             << " " << map[forwardPointer+1] << " " << map[forwardPointer+2] << " " << map[forwardPointer+3] << "\n";*/

        // ***Finally scan the output buffer
        //cout << "Scanning output of " << xBoxPointer << ": ";
        index = 0;
        while(map[forwardPointer+index*elementSize] != -1 && map[forwardPointer+index*elementSize] < element) {
            /*cout << map[forwardPointer+index*elementSize] << " " << map[forwardPointer+index*elementSize+1] << " "
                 << map[forwardPointer+index*elementSize+2] << " " << map[forwardPointer+index*elementSize+3] << " | ";*/
            index++;
        }
        /*cout << map[forwardPointer+index*elementSize] << " " << map[forwardPointer+index*elementSize+1] << " "
             << map[forwardPointer+index*elementSize+2] << " " << map[forwardPointer+index*elementSize+3] << " | ";
        cout << "\n";*/

        /*if(element == 708) {
            cout << "Scanning output for 708 in " << xBoxPointer << "\n";
            cout << map[xBoxPointer+9] << " " << forwardPointer << "\n";
            long tInd = 0;
            while(map[forwardPointer+tInd*elementSize] != -1 && map[forwardPointer+tInd*elementSize] < element) {
                cout << map[forwardPointer+tInd*elementSize] << " " << map[forwardPointer+tInd*elementSize+1] << " "
                     << map[forwardPointer+tInd*elementSize+2] << " " << map[forwardPointer+tInd*elementSize+3] << " | ";
                tInd++;
            }
            cout << map[forwardPointer+tInd*elementSize] << " " << map[forwardPointer+tInd*elementSize+1] << " "
                 << map[forwardPointer+tInd*elementSize+2] << " " << map[forwardPointer+tInd*elementSize+3] << " | ";
            cout << "\n";
        }*/

        ////cout << "Index Output = " << index << " " << map[forwardPointer+index*elementSize] << "\n";

        // Case 1) We reached the end of the array
        if(map[forwardPointer+index*elementSize] == -1) {
            ////cout << "Case 1\n";
            index--;
            if(map[forwardPointer+index*elementSize+1] < 0) {
                // Return this pointer
                //return map[forwardPointer+index*elementSize+2];
                // This is either a forward pointer or a subbox pointer
                long value = map[forwardPointer+index*elementSize+1];
                long pointer = map[forwardPointer+index*elementSize+2];
                if(value == -2) {
                    // Forward
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    forwardPointer = pointer;
                }
                else {
                    // Subbox, calculate start of input buffer
                    long y = map[pointer];
                    long ret = 0;
                    if(y <= minX) {
                        // Start of input of a simple xBox
                        ret = pointer+2;
                    }
                    else {
                        // Location + offset
                        ret = pointer + infoOffset;
                    }
                    forwardPointer = ret;
                }
            }
            else {
                // Return the pointer of the reverse pointer
                // return map[map[forwardPointer+index*elementSize+2]+2];

                // This is either a forward pointer or a subbox pointer
                long reversePointer = map[forwardPointer+index*elementSize+2];
                if(reversePointer == 0) {
                    return 0; // Convention
                }
                long value = map[reversePointer+1];
                long pointer = map[reversePointer+2];
                if(value == -2) {
                    // Forward
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    forwardPointer = pointer;
                }
                else {
                    // Subbox, calculate start of input buffer
                    long y = map[pointer];
                    long ret = 0;
                    if(y <= minX) {
                        // Start of input of a simple xBox
                        ret = pointer+2;
                    }
                    else {
                        // Location + offset
                        ret = pointer + infoOffset;
                    }
                    forwardPointer = ret;
                }
            }
        }
            // Case 2) We found an element with the same key
        else if(map[forwardPointer+index*elementSize] == element) {
            ////cout << "Case 2\n";
            // Is this the real deal or a pointer?
            if(map[forwardPointer+index*elementSize+1] > 0) {
                // Return pointer to this element
                return forwardPointer+index*elementSize;
            }
            else {
                // I lied, we can actually have multiple elements with the same value.
                // Look ahead and see if we can still find the real deal.
                long tempIndex = index+1;
                while(map[forwardPointer+tempIndex*elementSize] == element) {
                    if(map[forwardPointer+tempIndex*elementSize+1] > 0) {
                        return forwardPointer+tempIndex*elementSize;
                    }
                    tempIndex++;
                }
                // No luck? Follow original pointer
                //return map[forwardPointer+index*elementSize+2];
                long value = map[forwardPointer+index*elementSize+1];
                long pointer = map[forwardPointer+index*elementSize+2];
                if(value == -2) {
                    // Forward
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    forwardPointer = pointer;
                }
                else {
                    // Subbox, calculate start of input buffer
                    long y = map[pointer];
                    long ret = 0;
                    if(y <= minX) {
                        // Start of input of a simple xBox
                        ret = pointer+2;
                    }
                    else {
                        // Location + offset
                        ret = pointer + infoOffset;
                    }
                    forwardPointer = ret;
                }
            }
        }
            // Case 3) We found an element with a larger key
        else if(map[forwardPointer+index*elementSize] > element) {
            ////cout << "Case 3\n";
            // Case 3a) We found a real element with a larger key
            if(map[forwardPointer+index*elementSize+1] > 0) {
                // Return the pointer of the reverse pointer
                ////cout << "Case 3a\n";
                //return map[map[forwardPointer+index*elementSize+2]+2];
                long reversePointer = map[forwardPointer+index*elementSize+2];
                if(reversePointer == 0) {
                    return 0; // Convention
                }
                long value = map[reversePointer+1];
                long pointer = map[reversePointer+2];
                if(value == -2) {
                    // Forward
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    forwardPointer = pointer;
                }
                else {
                    // Subbox, calculate start of input buffer
                    long y = map[pointer];
                    long ret = 0;
                    if(y <= minX) {
                        // Start of input of a simple xBox
                        ret = pointer+2;
                    }
                    else {
                        // Location + offset
                        ret = pointer + infoOffset;
                    }
                    forwardPointer = ret;
                }
            }
            else {
                if(index == 0) {
                    forwardPointer = 0;
                }
                else {
                    index--;
                    if(map[forwardPointer+index*elementSize+1] > 0) {
                        // Return the pointer of the reverse pointer
                        //return map[map[forwardPointer+index*elementSize+2]+2];
                        long reversePointer = map[forwardPointer+index*elementSize+2];
                        if(reversePointer == 0) {
                            return 0;
                        }
                        long value = map[reversePointer+1];
                        long pointer = map[reversePointer+2];
                        if(value == -2) {
                            // Forward
                            //return map[map[forwardPointer+index*elementSize+2]+2];
                            forwardPointer = pointer;
                        }
                        else {
                            // Subbox, calculate start of input buffer
                            long y = map[pointer];
                            long ret = 0;
                            if(y <= minX) {
                                // Start of input of a simple xBox
                                ret = pointer+2;
                            }
                            else {
                                // Location + offset
                                ret = pointer + infoOffset;
                            }
                            forwardPointer = ret;
                        }
                    }
                    else {
                        ////cout << "Case 3b\n";
                        /*//cout << "subbox element " << map[forwardPointer+index*elementSize] << " "
                             << map[forwardPointer+index*elementSize+1] << " " << map[forwardPointer+index*elementSize+2]
                             << " " << map[forwardPointer+index*elementSize+3] << "\n";*/
                        //return map[forwardPointer+index*elementSize+2]; // Return pointer
                        // This is either a forward pointer or a subbox pointer
                        long value = map[forwardPointer+index*elementSize+1];
                        long pointer = map[forwardPointer+index*elementSize+2];
                        if(value == -2) {
                            // Forward
                            //return map[map[forwardPointer+index*elementSize+2]+2];
                            forwardPointer = pointer;
                        }
                        else {
                            // Subbox, calculate start of input buffer
                            long y = map[pointer];
                            long ret = 0;
                            if(y <= minX) {
                                // Start of input of a simple xBox
                                ret = pointer+2;
                            }
                            else {
                                // Location + offset
                                ret = pointer + infoOffset;
                            }
                            forwardPointer = ret;
                        }
                    }
                }

            }
        }
        else {
            // You should never end up here, go away!
            //cout << "!!!!! ERROR IN SEARCH ON XBOX OUTPUT" << xBoxPointer << "\n";
        }

        // ***Return a pointer into the next xBox, or -1 if there are none.

        // TODO at this point we will have returned if we found the element.
        // TODO the enclosing method will know wether we are the last xBox,
        // TODO or if we return a valid pointer.

        ////cout << "Did we find anything in xBox " << xBoxPointer << "\n";

        return forwardPointer;
    }
}


/***********************************************************************************************************************
 * Extended Methods
 **********************************************************************************************************************/

/*
 * Assumes the subbox has already been flushed.
 * Will sample up afterwards.
 */
void XDict::splitSubbox(long pointerToSubbox, long subboxSize,long subboxIndex, long maxSubboxes, long pointerToBool) {

    //cout << "----- SPLIT on " << pointerToSubbox << "\n";

    long x = map[pointerToSubbox];

    // First move the bools back to make room
    long firstEmptyIndex = 0;
    long index = maxSubboxes-1;
    while(index > subboxIndex) {
        if(map[pointerToBool+index*booleanSize] == 0) {
            firstEmptyIndex = index;
        }
        map[pointerToBool+index*booleanSize] = map[pointerToBool+(index-1)*booleanSize];
        map[pointerToBool+index*booleanSize+1] = map[pointerToBool+(index-1)*booleanSize+1];
        index--;
    }

    // Layout new subbox
    //long location = pointerToSubbox+subboxSize; <-- Wrong
    long location = pointerToBool+maxSubboxes*booleanSize+firstEmptyIndex*subboxSize;
    layoutXBox(location,x);

    //cout << "New subbox will be located at " << location << "\n";

    map[pointerToBool+(subboxIndex+1)*booleanSize] = location; // Inserted subbox in parent
    long pointerToOutput;
    long pointerToNewOutput;

    if(x <= minX) {
        pointerToOutput = pointerToSubbox+2;
        pointerToNewOutput = location+2;
    }
    else {
        pointerToOutput = map[pointerToSubbox+9];
        pointerToNewOutput = map[location+9];
    }

    // Count output elements, real and pointers
    long counterRealElements = 0;
    index = 0;
    while(map[pointerToOutput+index*elementSize] != -1) {
        if(map[pointerToOutput+index*elementSize+1] > 0) {
            counterRealElements++;
        }
        index++;
    }

    long elementsToKeep = counterRealElements / 2;
    long elementsToTransfer = map[pointerToSubbox+1] - elementsToKeep;

    /*cout << "Split divided real elements " << elementsToKeep << " " << elementsToTransfer
         << " out of original " << map[pointerToSubbox+1] << "\n";*/

    long counterRealElementsMoved = 0;

    // Run through elements until we find where to split
    long counter = 0;
    index = 0;
    while(counter < elementsToKeep) {
        if(map[pointerToOutput+index*elementSize+1] > 0) {
            counter++;
        }
        index++;
    }

    // Move forward until there are no pointers matching this key!
    /*long key = map[pointerToOutput+(index-1)*elementSize];
    while(map[pointerToOutput+index*elementSize] == key) {
        if(map[pointerToOutput+index*elementSize+1] > 0 ) {
            elementsToTransfer--;
            elementsToKeep++;
        }
        index++;
        ////cout << "++\n";
    }*/

    // TODO means we cant split a subbox containing elements with only the same key! <-- Fixed

    long split = index;
    long writeIndex = 0;

    // Move elements
    // Now with correct pointers!
    long currentReversePointer = 0;
    while(map[pointerToOutput+index*elementSize] != -1) {
        map[pointerToNewOutput+writeIndex*elementSize] = map[pointerToOutput+index*elementSize];
        map[pointerToNewOutput+writeIndex*elementSize+1] = map[pointerToOutput+index*elementSize+1];
        if(map[pointerToNewOutput+writeIndex*elementSize+1] > 0) {
            map[pointerToNewOutput+writeIndex*elementSize+2] = currentReversePointer;
            map[pointerToNewOutput+writeIndex*elementSize+3] = 0;
        }
        else {
            map[pointerToNewOutput+writeIndex*elementSize+2] = map[pointerToOutput+index*elementSize+2];
            map[pointerToNewOutput+writeIndex*elementSize+3] = map[pointerToOutput+index*elementSize+3];
            currentReversePointer = pointerToNewOutput+writeIndex*elementSize;
        }

        writeIndex++;
        index++;
    }


    /*for(long i = 0; i < elementsToTransfer; i++) {
        if(map[pointerToOutput+(elementsToKeep+i)*elementSize+1] > 0) {
            counterRealElementsMoved++;
        }
        map[pointerToNewOutput+i*elementSize] = map[pointerToOutput+(elementsToKeep+i)*elementSize];
        map[pointerToNewOutput+i*elementSize+1] = map[pointerToOutput+(elementsToKeep+i)*elementSize+1];
        map[pointerToNewOutput+i*elementSize+2] = map[pointerToOutput+(elementsToKeep+i)*elementSize+2];
        map[pointerToNewOutput+i*elementSize+3] = map[pointerToOutput+(elementsToKeep+i)*elementSize+3];
    }*/

    // Mark end of buffers and update boolean
    /*map[pointerToNewOutput+elementsToTransfer*elementSize] = -1;
    map[pointerToBool+(subboxIndex+1)*booleanSize+1] = map[pointerToOutput+elementsToKeep*elementSize];
    map[pointerToOutput+elementsToKeep*elementSize] = -1;

    map[pointerToSubbox+1] = map[pointerToSubbox]*/

    map[pointerToNewOutput+writeIndex*elementSize] = -1;
    map[pointerToBool+(subboxIndex+1)*booleanSize+1] = map[pointerToOutput+split*elementSize];
    map[pointerToOutput+split*elementSize] = -1;

    //cout << "Old min element for subbox = " << map[pointerToBool+(subboxIndex)*booleanSize+1] << "\n";
    //cout << "Min element for new subbox = " << map[pointerToBool+(subboxIndex+1)*booleanSize+1] << "\n";

    map[pointerToSubbox+1] = elementsToKeep;
    map[location+1] = elementsToTransfer;

    //cout << "----- Split completed " << pointerToSubbox << " " << location << "\n";
    //cout << elementsToKeep << " " << elementsToTransfer << "\n";

    sampleUpAfterFlush(pointerToSubbox);
    sampleUpAfterFlush(location);

    //cout << "----- Split SampleUps completed\n";
}


/*
 * Handles the recursive BatchInsert (and SampleUp) on xBoxes in the xDict.
 * Creates new xBoxes as necessary.
 * Notice how this function operates at the xDict level,
 * whereas the BatchInsert method works at the xBox level.
 */
void XDict::recursivelyBatchInsertXBoxToXBox(long xBoxnumber) {


    // Setup
    if(xBoxnumber+1 == xBoxes->size()) {
        addXBox();
    }
    long xBoxPointer = xBoxes->at(xBoxnumber);
    long nextXBoxPointer = xBoxes->at(xBoxnumber+1);
    long startOfElementsToInsert;
    if(map[xBoxPointer] <= minX) {
        startOfElementsToInsert = xBoxPointer+2;
    }
    else {
        startOfElementsToInsert = map[xBoxPointer+9]; // Output buffer
    }

    //cout << "------------------------------ RecursiveBatchInsert on " << xBoxes->at(xBoxnumber) << " " << map[xBoxPointer] << "\n";

    /*if(map[0] != 8) {
        cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! xBox 0 has been overwritten before\n";
    }*/


    long start = startOfElementsToInsert; long end = startOfElementsToInsert; long counter = 0;
    long sizeOfBatch = map[nextXBoxPointer]/2;
    long maxOfNextXBox = ((long) pow(map[nextXBoxPointer],1+alpha) / 2);
    long index = 0;

    // Run through the elements, batch inserting as we go
    while(map[startOfElementsToInsert+index*elementSize] != -1) {
        if(map[startOfElementsToInsert+index*elementSize+1] > 0) {
            counter++; // Another real element
        }

        /*if(map[start] == 0 && map[start+1] > 0) {
            cout << "00Wrong input " << xBoxnumber << "\n";
        }*/

        if(counter == sizeOfBatch) {

            /*if(fileSize > 1967490 && map[1967490] == -1) {
                cout << "*** 11\n";
            }*/

            /*if(map[start] == 0 && map[start+1] > 0) {
                cout << "1Wrong input " << xBoxnumber << "\n";
            }*/

            end = startOfElementsToInsert+(index+1)*elementSize;
            if(map[nextXBoxPointer+1]+counter > maxOfNextXBox) {
                // Flush next xBox
                flush(nextXBoxPointer,true);

                /*if(fileSize > 1967490 && map[1967490] == -1) {
                    cout << "*** 12\n";
                }*/

                /*if(map[start] == 0 && map[start+1] > 0) {
                    cout << "11Wrong input " << xBoxnumber << "\n";
                }*/

                // Recursively batch insert
                /*cout << "1=== Checking xBox before recurSiveBatchInsert " << map[startOfElementsToInsert+index*elementSize] << "\n";
                recursivelyCheckxBox(xBoxes->at(xBoxnumber+1));
                cout << "1=== Check Completed\n";*/

                recursivelyBatchInsertXBoxToXBox(xBoxnumber+1);

                //cout << "1+++ Checking xBox after recurSiveBatchInsert\n";
                //recursivelyCheckxBox(xBoxes->at(xBoxnumber+1));
                //cout << "1+++ Check Done\n";

                /*if(fileSize > 1967490 && map[1967490] == -1) {
                    cout << "*** 13\n";
                }*/

                /*if(map[start] == 0 && map[start+1] > 0) {
                    cout << "12Wrong input " << xBoxnumber << "\n";
                }*/

                //cout << "===1 " << nextXBoxPointer << "\n";
                /*cout << "1=== Checking xBox before sampleUp\n";
                recursivelyCheckxBox(xBoxes->at(xBoxnumber+1));
                cout << "1=== Check Completed\n";*/

                // SampleUp on next xBox
                sampleUpAfterFlush(nextXBoxPointer);

                /*cout << "1+++ Checking xBox after sampleUp\n";
                recursivelyCheckxBox(xBoxes->at(xBoxnumber+1));
                cout << "1+++ Check Done\n";*/

                /*if(fileSize > 1967490 && map[1967490] == -1) {
                    cout << "*** 14\n";
                }*/

                /*if(map[start] == 0 && map[start+1] > 0) {
                    cout << "13Wrong input " << xBoxnumber << "\n";
                }*/

            }

            /*if(fileSize > 1967490 && map[1967490] == -1) {
                cout << "*** 15\n";
            }*/

            /*cout << "2=== Checking xBox before BatchInsert " << map[startOfElementsToInsert+index*elementSize] << "\n";
            recursivelyCheckxBox(xBoxes->at(xBoxnumber+1));
            cout << "2=== Check Completed\n";*/
            //cout << start << " " << end << " " << sizeOfBatch << " " << counter << "\n";
            /*if(map[start] == 0 && map[start+1] > 0) {
                cout << "2Wrong input " << xBoxnumber << "\n";
            }*/

            batchInsert(nextXBoxPointer,start,end,counter);

            /*cout << "2+++ Checking xBox after BatchInsert\n";
            recursivelyCheckxBox(xBoxes->at(xBoxnumber+1));
            cout << "2+++ Check Done\n";*/

            counter = 0;
            //cout << "Changing " << start << " " << end << "\n";
            start = end;

            /*if(fileSize > 1967490 && map[1967490] == -1) {
                cout << "*** 16\n";
            }*/
        }
        index++;
    }
    // Might be elements left that we did not batchInsert
    if(counter != 0) {
        // Index is already +1 from above.
        end = startOfElementsToInsert+(index)*elementSize;
        long elementsInNextXBox = map[nextXBoxPointer+1];
        long xOfNextXBox = map[nextXBoxPointer];

        /*if(fileSize > 1967490 && map[1967490] == -1) {
            cout << "*** 41\n";
        }*/

        if(map[nextXBoxPointer+1]+counter > maxOfNextXBox) {

            /*if(map[start] == 0 && map[start+1] > 0) {
                cout << "3Wrong input " << xBoxnumber << "\n";
            }*/

            // Flush next xBox
            flush(nextXBoxPointer,true);

            /*if(map[start] == 0 && map[start+1] > 0) {
                cout << "31Wrong input " << xBoxnumber << "\n";
            }*/

            /*cout << "3=== Checking xBox before recurSiveBatchInsert "  << map[startOfElementsToInsert+index*elementSize] << "\n";
            recursivelyCheckxBox(xBoxes->at(xBoxnumber+1));
            cout << "3=== Check Completed\n";*/

            // Recursively batch insert
            recursivelyBatchInsertXBoxToXBox(xBoxnumber+1);

            //cout << "3+++ Checking xBox after recurSiveBatchInsert\n";
            //recursivelyCheckxBox(xBoxes->at(xBoxnumber+1));
            //cout << "3+++ Check Done\n";

            /*if(map[start] == 0 && map[start+1] > 0) {
                cout << "32Wrong input " << xBoxnumber << "\n";
            }*/

            //cout << "===2 " << nextXBoxPointer << "\n";
            /*cout << "3=== Checking xBox before sampleUp "  << map[startOfElementsToInsert+index*elementSize] << "\n";
            recursivelyCheckxBox(xBoxes->at(xBoxnumber+1));
            cout << "3=== Check Completed\n";*/

            // SampleUp on next xBox
            sampleUpAfterFlush(nextXBoxPointer);

            /*cout << "3+++ Checking xBox after sampleUp\n";
            recursivelyCheckxBox(xBoxes->at(xBoxnumber+1));
            cout << "3+++ Check Done\n";*/

            /*if(map[start] == 0 && map[start+1] > 0) {
                cout << "33Wrong input " << xBoxnumber << "\n";
            }*/
        }

        /*if(fileSize > 1967490 && map[1967490] == -1) {
            cout << "*** 42\n";
        }*/

        /*cout << "4=== Checking xBox before BatchInsert " << map[startOfElementsToInsert+index*elementSize] << " " << start << " " << end << " " << counter << "\n";
        recursivelyCheckxBox(xBoxes->at(xBoxnumber+1));
        cout << "4=== Check Completed\n";*/

        batchInsert(nextXBoxPointer,start,end,counter);

        /*cout << "4+++ Checking xBox after recurSiveBatchInsert\n";
        recursivelyCheckxBox(xBoxes->at(xBoxnumber+1));
        cout << "4+++ Check Done\n";*/

        /*if(fileSize > 1967490 && map[1967490] == -1) {
            cout << "*** 43\n";
        }*/
    }

    /*if(map[0] != 8) {
        cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! xBox 0 was overwritten\n";
    }*/

    // Reset this xBox
    map[xBoxPointer+1] = 0;
    map[startOfElementsToInsert] = -1; // Empty array
    ////cout << "Wrote -1 to " << startOfElementsToInsert << "\n";

    /*cout << "5=== Checking xBox before sampleEmpty\n";
    recursivelyCheckxBox(xBoxPointer);
    cout << "5=== Check Completed\n";*/

    // Sample from the next xBox into this one
    sampleToEmptyBufferFromBuffer(startOfElementsToInsert,nextXBoxPointer+infoOffset,16);

    /*cout << "5+++ Checking xBox after sampleEmpty\n";
    recursivelyCheckxBox(xBoxPointer);
    cout << "5+++ Check Done\n";*/

    /*if(map[0] != 8) {
        cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!! xBox 0 was overwritten\n";
    }*/

    //cout << "------------------------------ RecursiveBatchInsert COMPLETED on " << xBoxes->at(xBoxnumber) << "\n";

}

/*
 * Samples from a full buffer to an empty buffer.
 * Used to sample from one xbox to another.
 */
void XDict::sampleToEmptyBufferFromBuffer(long pointerToDestination, long pointerToSource, long sampleEveryNth) {

    long counter = sampleEveryNth-1;
    long index = 0;
    long writeIndex = 0;
    long key;
    while(map[pointerToSource+index*elementSize] != -1) {
        counter++;
        if(counter == sampleEveryNth) {
            key = map[pointerToSource+index*elementSize];
            ////cout << "Sampling " << key << " to " << pointerToDestination+writeIndex*elementSize << "\n";
            map[pointerToDestination+writeIndex*elementSize] = key;
            map[pointerToDestination+writeIndex*elementSize+1] = -2; // Forward
            map[pointerToDestination+writeIndex*elementSize+2] = pointerToSource+index*elementSize;
            map[pointerToDestination+writeIndex*elementSize+3] = 0;
            writeIndex++;
            counter = 0;
        }
        index++;
    }
    map[pointerToDestination+writeIndex*elementSize] = -1;
}




/***********************************************************************************************************************
 * Helper Methods
 **********************************************************************************************************************/

/*
 * Recursively calculates the size of an xBox.
 */
long XDict::recursivelyCalculateSize(long x) {

    if(x <= minX) {
        //return minSize;
        return pow(x,1+alpha) * elementSize + 3;
    }

    long y = (long) sqrt(x);
    /*if(y < minX) {
        y = minX;
    }*/
    //long numberOfUpperSubboxes = y/4; // *(1/4)
    long numberOfUpperSubboxes = sqrt(x)/4; // *(1/4)
    if(numberOfUpperSubboxes < 1) {
        numberOfUpperSubboxes = 1;
    }
    long numberOfLowerSubboxes = (long) pow(x,0.5 + alpha/2)/4; // *(1/4)
    if(numberOfLowerSubboxes < 1) {
        numberOfLowerSubboxes = 1;
    }
    long sizeOfMiddleBuffer = elementSize* (long) pow(x,1+alpha/2) +1;
    long sizeOfOutputBuffer = elementSize* (long) pow(x,1+alpha) +1;

    long subboxSize = recursivelyCalculateSize(y);
    long upperLevelSize = numberOfUpperSubboxes*subboxSize;
    long lowerLevelSize = numberOfLowerSubboxes*subboxSize;

    // Info+ Input + Upper Bool + Upper Subboxes + Middle + Lower Bool + Lower subboxes + output
    return infoOffset +  x*elementSize+1 + numberOfUpperSubboxes * booleanSize + upperLevelSize + sizeOfMiddleBuffer
            + numberOfLowerSubboxes * booleanSize + lowerLevelSize + sizeOfOutputBuffer;


}

/*
 * Lays out a new xBox starting at the location of pointer
 */
long XDict::layoutXBox(long pointer, long x) {

    // Base case
    if(x <= minX) {
        map[pointer] = x;
        map[pointer+1] = 0; // No elements
        map[pointer+2] = -1; // Empty array
        /*map[pointer+minSize-1] = -1;
        return pointer+minSize;*/

        long tempMin = pow(x,1+alpha) * elementSize + 3;
        map[pointer+tempMin-1] = -1; // Backstopper
        return pointer+tempMin;
    }

    // Layout the xbox. Subboxes will be created as needed, just make space for them.
    long numberOfRealElements = 0;
    long y = (long) sqrt(x);
    /*if(y < minX) {
        y = minX;
    }*/
    long sizeOfSubboxes;
    long numberOfUpperSubboxes;
    long numberOfLowerSubboxes;
    long pointerToBoolUpper;
    //long pointerToFirstUpper;
    long pointerToMiddleBuffer;
    long pointerToBoolLower;
    //long pointerToFirstLower;
    long pointerToOutputBuffer;

    // Calculate size of subboxes
    sizeOfSubboxes = recursivelyCalculateSize(y);

    // Calculate number of subboxes
    //numberOfUpperSubboxes = y/4; // *(1/4)
    numberOfUpperSubboxes = (long) (sqrt(x)/4);
    if(numberOfUpperSubboxes < 1) {
        numberOfUpperSubboxes = 1;
    }
    numberOfLowerSubboxes = (long) pow(x,0.5 + alpha/2) / 4; // *(1/4)
    if(numberOfLowerSubboxes < 1) {
        numberOfLowerSubboxes = 1;
    }

    // Calculate size of arrays
    // Input buffer is obviously of size x
    long sizeOfMiddleBuffer = elementSize* (long) pow(x,1+alpha/2) +1;
    long sizeOfOutputBuffer = elementSize* (long) pow(x,1+alpha) +1;

    // Fill in info
    map[pointer] = x;
    map[pointer+1] = numberOfRealElements;
    map[pointer+2] = y;
    map[pointer+3] = sizeOfSubboxes;
    map[pointer+4] = numberOfUpperSubboxes;
    map[pointer+5] = numberOfLowerSubboxes;
    pointerToBoolUpper = pointer+infoOffset+x*elementSize+1; // Start + info + input buffer + 1
    map[pointer+6] = pointerToBoolUpper;
    /*pointerToFirstUpper = pointerToBoolUpper+numberOfUpperSubboxes;
    map[pointer+7] = pointerToFirstUpper;*/
    pointerToMiddleBuffer = pointerToBoolUpper + numberOfUpperSubboxes * booleanSize + (numberOfUpperSubboxes*sizeOfSubboxes);
    map[pointer+7] = pointerToMiddleBuffer;
    pointerToBoolLower = pointerToMiddleBuffer + sizeOfMiddleBuffer;
    map[pointer+8] = pointerToBoolLower;
    /*pointerToFirstLower = pointerToBoolLower + numberOfLowerSubboxes;
    map[pointer+10] = pointerToFirstLower;*/
    pointerToOutputBuffer = pointerToBoolLower + numberOfLowerSubboxes * booleanSize + (numberOfLowerSubboxes*sizeOfSubboxes);
    map[pointer+9] = pointerToOutputBuffer;


    // Indicate input buffer is empty
    map[pointer+infoOffset] = -1;
    map[pointerToBoolUpper-1] = -1; // Backstopper for input

    // Indicate that upper level subboxes are not in use
    for(int i = 0; i < numberOfUpperSubboxes*booleanSize; i++) {
        map[pointerToBoolUpper+i] = 0;
    }

    // Indicate that middle buffer is empty
    map[pointerToMiddleBuffer] = -1;
    map[pointerToBoolLower-1] = -1; // Backstopper for middle

    // Indicate that lower level subboxes are not in use
    for(int i = 0; i < numberOfLowerSubboxes*booleanSize; i++) {
        map[pointerToBoolLower+i] = 0;
    }

    // Indicate that output buffer is empty
    map[pointerToOutputBuffer] = -1;
    map[pointerToOutputBuffer+sizeOfOutputBuffer-1] = -1; // Backstopper for output

    //cout << "Wrote to " << pointerToOutputBuffer+sizeOfOutputBuffer-1 << " fileSize=" << fileSize << "\n";

    //cout << "************************ NEW XBOX ************************\n";
    /*cout << "------------------------ NEW XBOX ------------------------\n";
    cout << "placement = " << pointer << "\n";
    cout << "x = " << x << "\n";
    cout << "#realEle = " << numberOfRealElements << "\n";
    cout << "y = " << y << "\n";
    cout << "sizeSubboxes = " << sizeOfSubboxes << "\n";
    cout << "#upperSubboxes = " << numberOfUpperSubboxes << "\n";
    cout << "#lowerSubboxes = " << numberOfLowerSubboxes << "\n";
    cout << "size input = " << x*elementSize << "\n";
    cout << "size middle = " << sizeOfMiddleBuffer << "\n";
    cout << "size output = " << sizeOfOutputBuffer << "\n";
    cout << "----------------------------------------------------------\n";*/
    /*//cout << pow(x,(1/2 + alpha/2)) << " " << pow(x,1/2 + alpha/2) / 4 << "\n";
    //cout << alpha/2 << " " << pow(x,(1/2)) << " " << pow(x,(1/2 + 1/2)) << " " << pow(x,1) << "\n";
    double calc = 0.5 + alpha/2;
    //cout << pow(x,calc) << " " << calc << "\n";*/

    return pointerToOutputBuffer+sizeOfOutputBuffer; // Output Buffer is the last part of this structure, return end+1.

}

/* OLD
 * Pointer must point to a boolean array of subboxes (its pointers really),
 * that has a max size of size. Assumes all subboxes have been flushed.
 * Returns a pointer INTO THE BOOLEAN ARRAY, which then points
 * to the subbox whose output buffer contains the smallest element.
 * If none can be found it returns -1.
 * You have to find the subbox and output buffer yourself,
 * based on the return value.
 */

/*
 * Returns a pointer into the boolean array to the subbox with the smallest
 * element. Normally this would be the first entry, but we temporarily allow
 * subboxes to be zeroed out during flush, with non zero subboxes following them.
 */
long XDict::findNextSubboxForMerge(long pointer, long size) {

    long index = 0;
    long smallest = LONG_MAX;
    long smallestPointer = pointer;
    bool min = false;

    // Redid how boolean is laid out
    // First non zero subbox in the list is the smallest
    for(index; index < size; index++) {
        if(map[pointer+index*2] != 0) {
            //long subboxPointer = map[pointer+index*2];
            long minElementInSubbox = map[pointer+index*2+1];
            smallest = minElementInSubbox;
            smallestPointer = pointer+index*2;
            break; // Found smallest
        }
    }

    if(smallest != LONG_MAX) {
        //cout << "--- " << smallestPointer << "\n";
        return smallestPointer;
    }
    else {
        return -1;
    }


    ////cout << "Finding first subbox at position " << pointer << "\n";

    // Find the first subbox
    /*for(index; index < size; index++) {
        ////cout << map[pointer+index] << " ";
        if(map[pointer+index] != 0) {
            // We have a subbox!
            long subboxPointer = map[pointer+index];
            ////cout << "x=" << map[subboxPointer] << "\n";
            if(map[subboxPointer] <= minX) {
                min = true;
                if(map[subboxPointer+2] < smallest) {
                    smallest = map[subboxPointer+2];
                    smallestPointer = pointer+index;
                }

            }
            else {
                long outputBuffer = map[subboxPointer+9];
                if(map[outputBuffer] < smallest) {
                    smallest = map[outputBuffer];
                    ////cout << "New smallest = " << smallest << "\n";
                    smallestPointer = pointer+index;
                }
            }
        }
    }

    ////cout << " Found first " << smallestPointer << " " <<  smallest << "\n";

    // Check!
    if(smallest == LONG_MAX) {
        return -1;
    }
    else if(index+1 == size) {
        return smallestPointer;
    }

    // Now that we have established a temporary smallest
    // find the actual smallest element in the output buffers.
    index++;

    if(min) {
        for(index; index < size; index++) {
            if(map[pointer+index] != 0) {
                // We have a subbox!
                long subboxPointer = map[pointer+index];
                if(smallest > map[subboxPointer+2]) {
                    smallest = map[subboxPointer+2];
                    smallestPointer = pointer+index;
                }
            }
        }
    }
    else {
        for(index; index < size; index++) {
            if(map[pointer+index] != 0) {
                // We have a subbox!
                long subboxPointer = map[pointer+index];
                long outputBuffer = map[subboxPointer+9];
                if(smallest > map[outputBuffer]) {
                    smallest = map[outputBuffer];
                    smallestPointer = pointer+index;
                }
            }
        }
    }

    ////cout << " Found " << smallestPointer << " " <<  smallest << "\n";
    return smallestPointer;*/
}

/*
 * Extends the file and mapping up to and including the pointer location,
 * which is just a filesize.
 */
void XDict::extendMapTo(long pointer) {

    /*map = (long*) mmap(0, fileSize*sizeof(long), PROT_READ | PROT_WRITE, MAP_SHARED, fileDescriptor, 0);
    if (map == MAP_FAILED) {
        close(fileDescriptor);
        perror("Error mmap original file");
        exit(EXIT_FAILURE);
    }*/


    /*__off_t result = lseek(fileDescriptor, (pointer+1)*sizeof(long), SEEK_HOLE);
    if (result == -1) {
        cout << "Failed extending to size " << (pointer + 1) << " " << (pointer + 1) * sizeof(long) << "\n";
        close(fileDescriptor);
        perror("Error mremap lseek");
        exit(EXIT_FAILURE);
    }*/

    __off_t result = lseek64(fileDescriptor, (pointer+1)*sizeof(long), SEEK_SET);
    if (result == -1) {
        cout << "Failed extending to size " << (pointer+1) << " " << (pointer+1)*sizeof(long) << "\n" << std::flush;
        close(fileDescriptor);
        perror("Error mremap lseek");
        exit(EXIT_FAILURE);
    }

    /*__off_t result = ftruncate(fileDescriptor, (pointer+1)*sizeof(long));
    if (result == -1) {
        cout << "Failed extending to size " << (pointer+1) << " " << (pointer+1)*sizeof(long) << "\n";
        close(fileDescriptor);
        perror("Error mremap ftruncate");
        exit(EXIT_FAILURE);
    }*/

    result = write(fileDescriptor, "", 1);
    if (result != 1) {
        close(fileDescriptor);
        perror("Error mremap write to EOF");
        exit(EXIT_FAILURE);
    }

    map = (long*) mremap(map, fileSize*sizeof(long), (pointer+1)*sizeof(long), MREMAP_MAYMOVE);
    if (map == MAP_FAILED) {
        close(fileDescriptor);
        perror("Error mremap file");
        exit(EXIT_FAILURE);
    }

    fileSize = pointer+1;

}

/*
 * Use this method to verify the correctness of our structure, or alternatively find any errors.
 * Returns number of real elements in xBox, so we can recursively verify it.
 */
long XDict::recursivelyCheckxBox(long pointer) {


    bool error = false;


    if(map[pointer] <= minX) {

        long x = map[pointer];
        long numberOfRealElements = map[pointer+1];

        cout << "--- Checking Simple xBox " << pointer << "\n";
        long index = 0;
        long mostRecent = 0;
        long actualNumberOfRealElements = 0;
        while(map[pointer+2+index*elementSize] != -1) {
            if(map[pointer+2+index*elementSize] < mostRecent) {
                cout << "********** Wrong order of keys in input " << mostRecent << " " << map[pointer+2+index*elementSize] << "\n";
                error = true;
            }
            if(map[pointer+2+index*elementSize] < 0) {
                cout << "********** Key too small in input " << map[pointer+2+index*elementSize] << "\n";
                error = true;
            }
            if(map[pointer+2+index*elementSize+1] == 0) {
                cout << "********** Value was zero in input\n";
                error = true;
            }
            if(map[pointer+2+index*elementSize] == 0 && map[pointer+2+index*elementSize+1] > 0) {
                cout << "********** Key was zero in input\n";
                error = true;
            }
            if(map[pointer+2+index*elementSize+1] > 0) {
                actualNumberOfRealElements++;
                if(map[pointer+2+index*elementSize] != map[pointer+2+index*elementSize+1]) {
                    cout << "********** Key and Value of real element doesnt match in input\n";
                    error = true;
                }
            }
            mostRecent = map[pointer+2+index*elementSize];
            index++;
        }
        if(index > (long) pow(x,1+alpha)) {
            cout << "********** Input was too long " << index << " " << (long) pow(x,1+alpha) << "\n";
            error = true;
        }

        if(actualNumberOfRealElements != numberOfRealElements) {
            cout << "********** Wrong number of real elements in xBox " << pointer << " " << actualNumberOfRealElements << " " << numberOfRealElements << "\n";
            error = true;
        }

        if(error) {
            index = 0;
            while(map[pointer+2+index*elementSize] != -1) {
                cout << map[pointer+2+index*elementSize] << " ";
                cout << map[pointer+2+index*elementSize+1] << " ";
                cout << map[pointer+2+index*elementSize+2] << " ";
                cout << map[pointer+2+index*elementSize+3] << " | ";
                index++;
            }
            cout << "\n";
        }

        return actualNumberOfRealElements;
    }

    cout << "--- Checking xBox " << pointer << "\n";



    // Load in info
    long x = map[pointer];
    long numberOfRealElements = map[pointer+1];
    long y = map[pointer+2];
    long subboxSize = map[pointer+3];
    long maxNumberOfUpperLevel = map[pointer+4];
    long maxNumberOfLowerLevel = map[pointer+5];
    long pointerToInput = pointer+infoOffset;
    long pointerToUpperBool = map[pointer+6];
    long pointerToMiddle = map[pointer+7];
    long pointerToLowerBool = map[pointer+8];
    long pointerToOutput = map[pointer+9];

    long actualNumberOfRealElements = 0;

    // Verify that info pointers are correct!
    if(x < minX) {
        cout << "********** Error, x too small: " << x << "\n";
    }
    if(y != (long) pow(x,0.5) ) {
        cout << "********** Error, y is not square root x: " << x << " " << y << "\n";
    }
    if(subboxSize != recursivelyCalculateSize(y)) {
        cout << "********** Error, wrong subbox size: " << subboxSize << "\n";
    }
    if(maxNumberOfUpperLevel != (long) pow(x,0.5)/4) {
        cout << "********** Error, wrong maximum number of maxUpperLevel: " << maxNumberOfUpperLevel << "\n";
    }
    if(maxNumberOfLowerLevel != (long) pow(x,0.5 + alpha/2)/4) {
        cout << "********** Error, wrong maximum number of maxLowerLevel: " << maxNumberOfLowerLevel << "\n";
    }
    if(map[pointerToUpperBool-1] != -1) {
        cout << "********** Error, input not correctly terminated\n";
    }
    if(map[pointerToLowerBool-1] != -1) {
        cout << "********** Error, middle not correctly terminated\n";
    }

    if(pointerToUpperBool != pointer+infoOffset+x*elementSize+1) {
        cout << "********** Error, upper bool pointer not correct\n";
    }

    if(pointerToMiddle != pointerToUpperBool + maxNumberOfUpperLevel * booleanSize + maxNumberOfUpperLevel*subboxSize) {
        cout << "********** Error, middle pointer not correct\n";
    }

    long sizeOfMiddleBuffer = elementSize* (long) pow(x,1+alpha/2) +1;
    long sizeOfOutputBuffer = elementSize* (long) pow(x,1+alpha) +1;

    if(pointerToLowerBool != pointerToMiddle + sizeOfMiddleBuffer) {
        cout << "********** Error, lower bool pointer not correct\n";
    }

    if(pointerToOutput != pointerToLowerBool + maxNumberOfLowerLevel*booleanSize + maxNumberOfLowerLevel*subboxSize) {
        cout << "********** Error, output pointer not correct\n";
    }

    // Verify that bools are correct
    long mostRecent = 0;
    bool ok = true;
    long location = pointerToUpperBool+maxNumberOfUpperLevel*booleanSize;
    for(long i = 0; i < maxNumberOfUpperLevel; i++) {
        if(map[pointerToUpperBool+i*booleanSize] != 0) {
            /*if(map[pointerToUpperBool+i*booleanSize] != location) {
                cout << "********** Wrong location of upper subbox " << map[pointerToUpperBool+i*booleanSize] << " " << location << "\n";
            }*/
            if(map[pointerToUpperBool+i*booleanSize+1] < mostRecent) {
                cout << "********** Wrong minKey of upper subbox " << mostRecent << " " << map[pointerToUpperBool+i*booleanSize+1] << " " << map[pointerToUpperBool+i*booleanSize] << " " << i << "\n";
                cout << pointerToUpperBool+i*booleanSize+1 << "\n";
            }
            mostRecent = map[pointerToUpperBool+i*booleanSize+1];
            location = location+subboxSize;
        }
        else {
            break;
        }
    }

    mostRecent = 0;
    location = pointerToLowerBool + maxNumberOfLowerLevel*booleanSize;
    for(long i = 0; i < maxNumberOfLowerLevel; i++) {
        if(map[pointerToLowerBool+i*booleanSize] != 0) {
            /*if(map[pointerToLowerBool+i*booleanSize] != location) {
                cout << "********** Wrong location of lower subbox " << map[pointerToLowerBool+i*booleanSize] << " " << location << "\n";
            }*/
            if(map[pointerToLowerBool+i*booleanSize+1] < mostRecent) {
                cout << "********** Wrong minKey of lower subbox " << mostRecent << " " << map[pointerToLowerBool+i*booleanSize+1] << " " << map[pointerToLowerBool+i*booleanSize] << " " << i << "\n";
                cout << pointerToLowerBool+i*booleanSize+1 << "\n";
            }
            mostRecent = map[pointerToLowerBool+i*booleanSize+1];
            location = location+subboxSize;
        }
        else {
            break;
        }
    }

    // TODO verify that keys are placed in the right order, and that no value = 0!

    long index = 0;
    mostRecent = 0;
    while(map[pointerToInput+index*elementSize] != -1) {
        if(map[pointerToInput+index*elementSize] < mostRecent) {
            cout << "********** Wrong order of keys in input " << mostRecent << " " << map[pointerToInput+index*elementSize] << "\n";
        }
        if(map[pointerToInput+index*elementSize] < 0) {
            cout << "********** Key too small in input " << map[pointerToInput+index*elementSize] << "\n";
        }
        if(map[pointerToInput+index*elementSize+1] == 0) {
            cout << "********** Value was zero in input\n";
        }
        if(map[pointerToInput+index*elementSize+1] > 0) {
            actualNumberOfRealElements++;
            if(map[pointerToInput+index*elementSize] != map[pointerToInput+index*elementSize+1]) {
                cout << "********** Key and Value of real element doesnt match in input\n";
            }
        }
        if(map[pointerToInput+index*elementSize+1] > 0 && map[pointerToInput+index*elementSize+2] != 0
           && map[pointerToInput+index*elementSize+2] < pointerToInput) {
            cout << "********** Reverse pointer points outside array in input\n";
        }
        mostRecent = map[pointerToInput+index*elementSize];
        index++;
    }
    if(index > x) {
        cout << "********** Input was too long " << index << " " << x << "\n";
    }

    if(pointerToInput+index*elementSize >= pointerToUpperBool) {
        cout << "********** Input was too long " << index << " " << x << " " << pointerToUpperBool << "\n";
    }

    index = 0;
    mostRecent = 0;
    while(map[pointerToMiddle+index*elementSize] != -1) {
        if(map[pointerToMiddle+index*elementSize] < mostRecent) {
            cout << "********** Wrong order of keys in middle " << mostRecent << " " << map[pointerToMiddle+index*elementSize] << "\n";
        }
        if(map[pointerToMiddle+index*elementSize] < 0) {
            cout << "********** Key too small in middle " << map[pointerToMiddle+index*elementSize] << "\n";
        }
        if(map[pointerToMiddle+index*elementSize+1] == 0) {
            cout << "********** Value was zero in middle\n";
        }
        if(map[pointerToMiddle+index*elementSize+1] > 0) {
            actualNumberOfRealElements++;
            if(map[pointerToMiddle+index*elementSize] != map[pointerToMiddle+index*elementSize+1]) {
                cout << "********** Key and Value of real element doesnt match in middle\n";
            }
        }
        if(map[pointerToMiddle+index*elementSize+1] > 0 && map[pointerToMiddle+index*elementSize+2] != 0
           && map[pointerToMiddle+index*elementSize+2] < pointerToMiddle) {
            cout << "********** Reverse pointer points outside array in middle\n";
        }

        mostRecent = map[pointerToMiddle+index*elementSize];
        index++;
    }
    if(index > (long) pow(x,1+alpha/2)) {
        cout << "********** Middle was too long " << index << " " << (long) pow(x,1+alpha/2) << "\n";
    }

    if(pointerToMiddle+index*elementSize >= pointerToLowerBool) {
        cout << "********** Middle was too long " << index << " " << (long) pow(x,1+alpha/2) << " " << pointerToLowerBool << "\n";
    }

    index = 0;
    mostRecent = 0;
    while(map[pointerToOutput+index*elementSize] != -1) {
        if(map[pointerToOutput+index*elementSize] < mostRecent) {
            cout << "********** Wrong order of keys in output " << mostRecent << " " << map[pointerToOutput+index*elementSize] << "\n";
        }
        if(map[pointerToOutput+index*elementSize] < 0) {
            cout << "********** Key too small in output " << mostRecent << " " << map[pointerToOutput+index*elementSize] << "\n";
        }
        if(map[pointerToOutput+index*elementSize+1] == 0) {
            cout << "********** Value was zero in output\n";
        }
        if(map[pointerToOutput+index*elementSize+1] > 0) {
            actualNumberOfRealElements++;
            if(map[pointerToOutput+index*elementSize] != map[pointerToOutput+index*elementSize+1]) {
                cout << "********** Key and Value of real element doesnt match in output\n";
            }
        }
        if(map[pointerToOutput+index*elementSize+1] > 0 && map[pointerToOutput+index*elementSize+2] != 0
           && map[pointerToOutput+index*elementSize+2] < pointerToOutput) {
            cout << "********** Reverse pointer points outside array in output\n";
        }
        mostRecent = map[pointerToOutput+index*elementSize];
        index++;
    }
    if(index > (long) pow(x,1+alpha)) {
        cout << "********** Output was too long " << index << " " << (long) pow(x,1+alpha) << "\n";
    }

    long realElementsInUpperSubboxes = 0;

    // Recursively verify subboxes
    for(long i = 0; i < maxNumberOfUpperLevel; i++) {
        if(map[pointerToUpperBool+i*booleanSize] != 0) {
            realElementsInUpperSubboxes += recursivelyCheckxBox(map[pointerToUpperBool+i*booleanSize]);
        }
        else {
            break;
        }
    }

    long realElementsInLowerSubboxes = 0;
    for(long i = 0; i < maxNumberOfLowerLevel; i++) {
        if(map[pointerToLowerBool+i*booleanSize] != 0) {
            realElementsInLowerSubboxes += recursivelyCheckxBox(map[pointerToLowerBool+i*booleanSize]);
        }
        else {
            break;
        }
    }

    // Use the return values from subboxes to verify number of real elements.
    // Verify the 3 main buffers while we count real elements.

    if(actualNumberOfRealElements + realElementsInUpperSubboxes + realElementsInLowerSubboxes != numberOfRealElements) {
        cout << "********** Wrong number of real elements in xBox " << pointer << " " << actualNumberOfRealElements << " "
             << realElementsInUpperSubboxes << " " << realElementsInLowerSubboxes << " "
             << actualNumberOfRealElements+realElementsInUpperSubboxes+realElementsInUpperSubboxes << " " << numberOfRealElements << "\n";
    }

    return actualNumberOfRealElements+realElementsInUpperSubboxes+realElementsInLowerSubboxes;


}

void XDict::printXDict() {

    for(int i = 0; i < xBoxes->size(); i++) {
        cout << "===== Printing xBox in xDict\n";
        recursivelyPrintXBox(xBoxes->at(i));
    }
}

void XDict::recursivelyPrintXBox(long pointer) {

    cout << "---- Printing xBox " << pointer << "\n";

    if(map[pointer] <= minX) {

        cout << "Simple xBox containing: ";
        long index = 0;
        while(map[pointer+2+index*elementSize] != -1) {
            cout << map[pointer+2+index*elementSize] << " " << map[pointer+2+index*elementSize+1] << " "
                 << map[pointer+2+index*elementSize+2] << " " << map[pointer+2+index*elementSize+3] << " | ";
            index++;
        }
        cout << "\n";

        return;
    }

    cout << "Printing input: ";
    long index = 0;
    long pointerToInput = pointer+infoOffset;
    while(map[pointerToInput+index*elementSize] != -1) {
        cout << map[pointerToInput+index*elementSize] << " " << map[pointerToInput+index*elementSize+1] << " "
             << map[pointerToInput+index*elementSize+2] << " " << map[pointerToInput+index*elementSize+3] << " | ";
        index++;
    }
    cout << "\n";

    cout << "Printing middle: ";
    index = 0;
    long pointerToMiddle = map[pointer+7];
    while(map[pointerToMiddle+index*elementSize] != -1) {
        cout << map[pointerToMiddle+index*elementSize] << " " << map[pointerToMiddle+index*elementSize+1] << " "
             << map[pointerToMiddle+index*elementSize+2] << " " << map[pointerToMiddle+index*elementSize+3] << " | ";
        index++;
    }
    cout << "\n";

    cout << "Printing output: ";
    index = 0;
    long pointerToOutput = map[pointer+9];
    while(map[pointerToOutput+index*elementSize] != -1) {
        cout << map[pointerToOutput+index*elementSize] << " " << map[pointerToOutput+index*elementSize+1] << " "
             << map[pointerToOutput+index*elementSize+2] << " " << map[pointerToOutput+index*elementSize+3] << " | ";
        index++;
    }
    cout << "\n";

    cout << "Recursively printing upper level of " << pointer << "\n";
    long maxUpperLevel = map[pointer+4];
    long upperBool = map[pointer+6];
    for(int i = 0; i < maxUpperLevel; i++) {
        if(map[upperBool+i*2] != 0) {
            recursivelyPrintXBox(map[upperBool+i*2]);
        }
    }

    cout << "Recursively printing lower level of " << pointer << "\n";
    long maxLowerLevel = map[pointer+5];
    long lowerBool = map[pointer+8];
    for(int i = 0; i < maxLowerLevel; i++) {
        if(map[lowerBool+i*2] != 0) {
            recursivelyPrintXBox(map[lowerBool+i*2]);
        }
    }

}