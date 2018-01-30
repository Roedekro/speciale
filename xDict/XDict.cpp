//
// Created by martin on 1/15/18.
//
// Note that we use "KeyValue" elements with a forward and reverse pointer.
// That is each element in a buffer, except on the minimum sized xBox, consists
// of 4 longs.
//

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <cmath>
#include <climits>
#include <cstring>
#include <iostream>
#include "XDict.h"

using namespace std;

/*
 * Set up an empty xBox consisting of an array of
 * 194 longs, the first being the number of real elements
 * in the xBox and the rest holding keys and values of the
 * elements.
 */
XDict::XDict(float a) {

    alpha = a;
    elementSize = 4;
    booleanSize = 2;

    //minX = 64;
    minX = 8; // Absolute minimum, since a proper xBox needs to be at least 64.
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
    fileDescriptor = open(fileName.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    if (fileDescriptor == -1) {
        perror("Error creating original file");
        exit(EXIT_FAILURE);
    }

    // Extend file to appropriate size
    int result = (int) lseek(fileDescriptor, fileSize*sizeof(long), SEEK_SET);
    if (result == -1) {
        close(fileDescriptor);
        perror("Error original lseek");
        exit(EXIT_FAILURE);
    }

    // Write to end of file
    // Empty string = single byte containing '0'.
    result = (int) write(fileDescriptor, "", 1);
    if (result != 1) {
        close(fileDescriptor);
        perror("Error original write to EOF");
        exit(EXIT_FAILURE);
    }

    // Map file
    map = (long*) mmap(0, fileSize*sizeof(long), PROT_READ | PROT_WRITE, MAP_SHARED, fileDescriptor, 0);
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

    //*** First merge the element into the first xBox

    // Scan the array until we reach the end
    long index = map[1]; // This is the absolute minimum index without pointers.
    while(map[2+index*elementSize] != -1) {
        index++;
    }

    long endOfElements = index;
    // Insert -1 at new end
    map[2+(index+1)*elementSize] = -1;

    // Run backwards moving elements back until we can insert the element
    while(kvt.key > map[2+index*elementSize] && index > 0) {
        map[2+index*elementSize] = map[2+(index-1)*elementSize];
        map[2+index*elementSize+1] = map[2+(index-1)*elementSize+1];
        map[2+index*elementSize+2] = map[2+(index-1)*elementSize+2];
        map[2+index*elementSize+3] = map[2+(index-1)*elementSize+3];
        index--;
    }
    // Insert element
    map[2+index*elementSize] = kvt.key;
    map[2+index*elementSize+1] = kvt.value;

    // ++
    map[1] = map[1]+1;

    long forward = 0;
    long backward = 0;

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
    else {
        // We are the last element
        if(map[2+(index-1)*elementSize+1] > 0) {
            backward = map[2+(index-1)*elementSize+2];
        }
        else {
            backward = 2+(index-1)*elementSize;
        }
    }
    map[2+index*elementSize+2] = backward;
    map[2+index*elementSize+3] = forward;

    // We saved minX^(1+alpha) in insertionCap
    //*** Are we at capacity?
    if(map[1] >= insertionCap) {
        recursivelyBatchInsertXBoxToXBox(0);
    }
}

void XDict::addXBox() {

    // Expand mapping
    long x = pow(latestX,1+alpha);
    long pointer = fileSize;
    long newSize = recursivelyCalculateSize(x);
    extendMapTo(fileSize+newSize);

    latestX = x;

    // Layout xBox
    long endBox = layoutXBox(pointer,x);

    cout << "New xBox ending before " << endBox << "\n";

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
    ret = search(xBoxPointer,2,element);

    if(map[ret] == element && map[ret+1] > 0) {
        return map[ret+1]; // If only it was always this easy!
    }

    // We now have a pointer into the first real xBox.
    // Recursively use this pointer to search down the xDict.
    for(int i = 1; i < xBoxes->size(); i++) {
        xBoxPointer = xBoxes->at(i);
        ret = search(xBoxPointer,ret,element);
        if(map[ret] == element && map[ret+1] > 0) {
            return map[ret+1];
        }
    }

    // TODO Correct? Write the search method first
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
 */
void XDict::batchInsert(long xBoxPointer, long pointerStart, long pointerEnd, long numberOfRealElements) {
    // TODO

    // *** Update number of real elements in this xBox with the number of inserted elements
    map[xBoxPointer+1] = map[xBoxPointer+1] + numberOfRealElements;

    // *** Merge elements into input buffer while removing lookahead pointers.

    // Find the subbox with the smallest element while we scan, for use later.
    bool subboxSet = false;
    long pointerToCurrentSubbox = 0;
    long pointerDirectlyToBooleanForCurrentSubbox = 0;
    long upperLevelSubboxCounter = 0;

    // First scan the input buffer to count the elements.
    long index = 0;
    long pointerToInputBuffer = xBoxPointer+infoOffset;
    while(map[pointerToInputBuffer+index*elementSize] != -1) {
        if(map[pointerToInputBuffer+index*elementSize] == -1) {
            upperLevelSubboxCounter++;
            if(!subboxSet) {
                pointerToCurrentSubbox = map[pointerToInputBuffer+index*elementSize+2];
                pointerDirectlyToBooleanForCurrentSubbox = map[pointerToInputBuffer+index*elementSize+3];
                subboxSet = true;
            }
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

    mergeArray[2] = map[pointerStart];
    mergeArray[3] = map[pointerStart+1];

    long writeIndex = 0;
    long insertIndex = 0;
    long originalIndex = 0;
    long insertCounter = 0;

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
            if(insertIndex < numberOfRealElements) {
                mergeArray[2] = map[pointerStart+insertIndex*elementSize];
                mergeArray[3] = map[pointerStart+insertIndex*elementSize+1];
            }
            else {
                // We already wrote out all the real elements to be inserted.
                mergeArray[-1] = map[pointerStart+insertIndex*elementSize];
            }

        }
    }

    // Insert -1 at the end of input.
    map[pointerStart+writeIndex*elementSize] = -1;

    // *** Insert elements into upper level subboxes, updating subbox pointers!
    // +
    // *** Handle splits
    index = 0;
    long start = 0; long end = 0;
    long counter = 0;
    long sizeOfBatch = (map[xBoxPointer+2] / 2); // y/2
    long maxRealElementsInSubbox = (pow(map[xBoxPointer+2],1+alpha) / 2); // Size of subbox output / 2.
    long maxNumberOfUpperLevelSubboxes = map[xBoxPointer+4];
    while(map[pointerToInputBuffer+index*elementSize] != 1) {
        if(map[pointerToInputBuffer+index*elementSize+1] > 0) {
            counter++;
        }
        else {
            // Check if we need to update current subbox
            if(map[pointerToInputBuffer+index*elementSize+1] == -1) {
                pointerToCurrentSubbox = map[pointerToInputBuffer+index*elementSize+2];
                pointerDirectlyToBooleanForCurrentSubbox = map[pointerToInputBuffer+index*elementSize+3];
                // TODO if this isnt the first subbox abort this batch range
            }
        }

        // TODO

        if(counter >= sizeOfBatch) {
            end = pointerToInputBuffer+(index+1)*elementSize;
            // Check if we need to split
            // TODO trick, if we split insert new subbox pointer. Create space by inserting a single element.
            // TODO This element is the first before the current subbox pointer?
            if(map[pointerToCurrentSubbox+1] + counter >= maxRealElementsInSubbox) {
                // Do we have room to split?
                if(upperLevelSubboxCounter < maxNumberOfUpperLevelSubboxes) {


                    //

                    upperLevelSubboxCounter++;
                    // Reset and scan this range again
                }
                else {
                    // Break and flush upper level. Then merge input + upper level to middle.
                }

            }
            else {
                // Batch insert into subbox
                start = end;
                counter = 0;
            }
        }




    }




    // *** If we finish inserting into the upper level without running out of subboxes we now
    // *** sample from the input buffer of the upper layer, restoring pointers, and return.

    // *** Else we ran out of subboxes. Flush upper level, then merge input and upper level into middle,
    // *** while removing pointers.

    // *** Insert elements into lower level subboxes, updating subbox pointers!
    // +
    // *** Handle splits

    // *** If we finish inserting into the lower level without running out of subboxes we now
    // *** sample from the input buffer of the lower layer, restoring pointers.
    // *** Notice that the upper level must be empty, sample up from the middle to the input,
    // *** creating the necessary subboxes. Then return.

    // *** Else we ran out of subboxes. Flush lower level, then merge middle and lower level into output,
    // *** while removing pointers

    // Sample up the entire structure.

}



/*
 * Flush the xBox pointed at.
 * Recursive indicates wether we should bother restoring pointers,
 * since a recursive call discards them.
 */
void XDict::flush(long pointer, bool recursive) {

    cout << "FLUSH on xBox at position " << pointer << "\n";

    if(map[pointer] <= minX) {
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
                map[pointerToUpperBoolean+i*2] = 0;
                map[pointerToUpperBoolean+i*2+1] = 0;
            }
            map[pointerToMiddleBuffer] = -1;
            for(long i = 0; i < numberOfLowerLevelSubboxes; i++) {
                map[pointerToLowerBoolean+i*2] = 0;
                map[pointerToLowerBoolean+i*2+1] = 0;
            }
            return; // We are already flushed
        }

        // 2) Move the elements in the output buffer back to make room for the remaining real elements.
        long deltaRealElements = numberOfRealElements-numberOfRealElementsInOutputBuffer;
        long elementsToMove = writeIndex; // Includes forward pointers!
        long moveByThisMuch = elementSize * deltaRealElements;

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
        long upperSubbox,upperMax,upperBuffer;
        if(upperPointer != -1) {
            upperSubbox = map[upperPointer];
            upperMax = map[upperSubbox+1]; // Real elements in subbox
            upperBuffer = map[upperSubbox+9]; // Its output buffer
        }*/
        long upperSubbox,upperMax,upperBuffer;
        upperSubbox = map[pointerToUpperBoolean+upperSubboxCounter*booleanSize];
        if(upperSubbox != 0) {
            upperBuffer = upperSubbox+2; // Its output buffer
            //cout << "Upper buffer placed at " << upperBuffer << " " << upperSubbox << "\n";
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
            }
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
            //cout << mergeArray[0] << " " << mergeArray[2] << " " << mergeArray[4] << " " << mergeArray[6] << " " << mergeArray[8] << "\n";
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

        map[pointerToInputBuffer] = -1;
        map[pointerToMiddleBuffer] = -1;
        map[pointerToOutputBuffer+totalElementsToOutput*elementSize] = -1;

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
                map[pointerToUpperBoolean+i*2] = 0;
                map[pointerToUpperBoolean+i*2+1] = 0;
            }
            map[pointerToMiddleBuffer] = -1;
            for(long i = 0; i < numberOfLowerLevelSubboxes; i++) {
                map[pointerToLowerBoolean+i*2] = 0;
                map[pointerToLowerBoolean+i*2+1] = 0;
            }
            return; // We are already flushed
        }

        // 2) Move the elements in the output buffer back to make room for the remaining real elements.
        long deltaRealElements = numberOfRealElements-numberOfRealElementsInOutputBuffer;
        long elementsToMove = writeIndex; // Includes forward pointers!
        long moveByThisMuch = elementSize * deltaRealElements;

        /*cout << "Move " << numberOfRealElements << " " << numberOfRealElementsInOutputBuffer << " "
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
        //cout << "Upper Pointer = " << upperPointer << "\n";
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
            //cout << "Upper buffer placed at " << upperBuffer << " " << upperSubbox << "\n";
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
                //cout << "Key upper = " << mergeArray[2] << "\n";
                //cout << "Value upper = " << mergeArray[3] << "\n";
            }
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
            }
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

        //cout << "Going to output " << totalElementsToOutput << " elements\n";

        for(writeIndex = 0; writeIndex < totalElementsToOutput; writeIndex++) {

            // Select smallest element by scanning mergeArray
            smallest = LONG_MAX;
            smallestIndex = 0;
            for(int i = 0; i < 5; i++) {
                //cout << mergeArray[i*2] << " ";
                if(mergeArray[i*2] != -1 && mergeArray[i*2] < smallest) {
                    smallestIndex = i;
                    smallest = mergeArray[i*2];
                }
            }
            //cout << "\n";

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
                    //cout << "Aborted index 0. " << inputIndex << " " << map[pointerToInputBuffer+inputIndex*elementSize] << "\n";
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
                    //cout << "Placing subbox key " << map[upperBuffer+upperIndex*2] << " from index " << upperIndex << "\n";
                    mergeArray[2] = map[upperBuffer+upperIndex*elementSize];
                    mergeArray[3] = map[upperBuffer+upperIndex*elementSize+1];
                }
                else {
                    // Else load in new subbox

                    // First mark the old subbox as deleted
                    map[pointerToUpperBoolean+upperSubboxCounter*booleanSize] = 0; // upperPointed pointed into our array of upper level subboxes.
                    map[pointerToUpperBoolean+upperSubboxCounter*booleanSize+1] = 0;
                    /*upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
                    cout << "=== " << upperPointer << "\n";*/
                    upperSubboxCounter++;
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

        map[pointerToInputBuffer] = -1;
        map[pointerToMiddleBuffer] = -1;
        map[pointerToOutputBuffer+totalElementsToOutput*elementSize] = -1;

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

    if(map[pointerToXBox] <= minX) {

        cout << "Elements in subbox located at " << pointerToXBox << ": ";
        long index = 0;
        while(map[pointerToXBox+2+index*elementSize] != -1) {
            cout << map[pointerToXBox+2+index*elementSize] << " ";
            if(index > minX*minX) {
                cout << "\n";
                cout << "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! ERROR IN SUBBOX\n";
            }
            index++;
        }
        cout << "\n";

        return; // The surrounding xBox gave us pointers and will sample from us.
    }

    long x = map[pointerToXBox];
    long y = map[pointerToXBox+2];

    // Count the number of real elements + forward pointers from the xBox below.
    long elementsInOutput = 0;
    long pointerToOutput = map[pointerToXBox+9];
    while(map[pointerToOutput+elementsInOutput*elementSize] != -1) {
        elementsInOutput++;
    }

    // Create the appropriate amount of lower level subboxes
    long numberOfPointersToCreate = elementsInOutput/16;
    if(numberOfPointersToCreate < 1) {
        numberOfPointersToCreate = 1;
    }
    long power = (long) pow(x,0.5 + alpha/2); // Size of output buffer of subbox
    if(y <= minX) {
        power = pow(y,1 + alpha); // Single array
    }

    cout << elementsInOutput << " " << numberOfPointersToCreate << " " << power <<  "\n";

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

    cout << "Going to create " << lowerLevelSubboxesToCreate << " new lower level subboxes of y = " << y << "\n";
    for(int i = 0; i < lowerLevelSubboxesToCreate; i++) {
        //cout << "Subbox will be located at " << latestLocation << "\n";
        map[pointerToLowerBoolean+i*booleanSize] = latestLocation;
        latestLocation = layoutXBox(latestLocation,y);
        //cout << latestLocation << "\n";
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

    // Run through output creating pointers
    while(map[pointerToOutput+index*elementSize] != -1) {
        sixteenth++;
        if(sixteenth == 16) {
            // Create pointer
            if(counter < pointersToEachSubbox) {
                // Create pointer in current subbox
                map[currentSubboxOutputBuffer+counter*elementSize] = map[pointerToOutput+index*4];
                map[currentSubboxOutputBuffer+counter*elementSize+1] = -2; // Forward pointer
                map[currentSubboxOutputBuffer+counter*elementSize+2] = pointerToOutput+index*4; // Element in output buffer
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

                // Insert this pointer
                map[currentSubboxOutputBuffer+counter*elementSize] = map[pointerToOutput+index*4];
                map[currentSubboxOutputBuffer+counter*elementSize+1] = -2; // Forward pointer
                map[currentSubboxOutputBuffer+counter*elementSize+2] = pointerToOutput+index*4; // Element in output buffer
                map[currentSubboxOutputBuffer+counter*elementSize+3] = 0; // Empty
            }
            sixteenth = 0;
            counter++;
        }
        index++;
    }

    // Insert -1 at the end of the last subbox's buffer
    map[currentSubboxOutputBuffer+counter*elementSize] = -1;

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

    cout << "Sampling every " << sampleEveryNthElement << "th element in lower level to middle\n";

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
        }
        // Update
        map[pointerToLowerBoolean+i*booleanSize+1] = smallestKeyInSubbox;
        // Create subbox pointer
        writeIndex++;
        map[pointerToMiddle+writeIndex*elementSize] = smallestKeyInSubbox; // Key
        map[pointerToMiddle+writeIndex*elementSize+1] = -1; // Subbox pointer
        map[pointerToMiddle+writeIndex*elementSize+2] = pointerToSubbox;
        map[pointerToMiddle+writeIndex*elementSize+3] = pointerToLowerBoolean+i;

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
        cout << "Counter=" << counter << "\n";
    }
    // Insert -1 at end of array
    writeIndex++;
    map[pointerToMiddle+writeIndex*elementSize] = -1;

    cout << "Created " << writeIndex << " pointers in middle buffer\n";


    // Create the appropriate amount of upper level subboxes
    numberOfPointersToCreate = writeIndex/16;
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

    long maxNumberOfUpperLevel = map[pointerToXBox+4];
    long pointerToUpperBoolean = map[pointerToXBox+6];
    latestLocation = pointerToUpperBoolean+maxNumberOfUpperLevel*booleanSize; // First subbox

    cout << "Going to create " << upperLevelSubboxesToCreate << " new upper level subboxes of y = " << y << "\n";
    for(int i = 0; i < upperLevelSubboxesToCreate; i++) {
        cout << "Subbox will be located at " << latestLocation << "\n";
        map[pointerToUpperBoolean+i*booleanSize] = latestLocation;
        latestLocation = layoutXBox(latestLocation,y);
        //cout << latestLocation << "\n";
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

    long tempIndex = -1;
    cout << "Middle consists of: ";
    do {
        tempIndex++;
        cout << map[pointerToMiddle+tempIndex*elementSize] << " ";
    }
    while(map[pointerToMiddle+tempIndex*elementSize] != -1);
    cout << "\n";

    cout << "Pointers to each subbox = " << pointersToEachSubbox << "\n";

    // Run through middle creating pointers
    while(map[pointerToMiddle+index*elementSize] != -1) {
        sixteenth++;
        if(sixteenth == 16) {
            // Create pointer
            if(counter < pointersToEachSubbox) {
                // Create pointer in current subbox
                //cout << "Writing " << map[pointerToMiddle+index*4] << " to " << map[pointerToUpperBoolean+currentSubboxIndex] << "\n";
                map[currentSubboxOutputBuffer+counter*elementSize] = map[pointerToMiddle+index*4];
                map[currentSubboxOutputBuffer+counter*elementSize+1] = -2; // Forward pointer
                map[currentSubboxOutputBuffer+counter*elementSize+2] = pointerToMiddle+index*4; // Element in output buffer
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
                    currentSubboxOutputBuffer = map[map[pointerToUpperBoolean+currentSubboxIndex]+9];
                }
                else {
                    // Subboxes are just arrays
                    currentSubboxOutputBuffer = map[pointerToUpperBoolean+currentSubboxIndex] + 2;
                }

                // Insert this pointer
                map[currentSubboxOutputBuffer+counter*elementSize] = map[pointerToMiddle+index*4];
                map[currentSubboxOutputBuffer+counter*elementSize+1] = -2; // Forward pointer
                map[currentSubboxOutputBuffer+counter*elementSize+2] = pointerToMiddle+index*4; // Element in output buffer
                map[currentSubboxOutputBuffer+counter*elementSize+3] = 0; // Empty
            }
            sixteenth = 0;
            counter++;
        }
        index++;
    }

    // Insert -1 at the end of the last subbox's buffer
    map[currentSubboxOutputBuffer+counter*elementSize] = -1;


    // Recursively call SampleUp on the upper level subboxes
    for(int i = 0; i < upperLevelSubboxesToCreate; i++) {
        sampleUpAfterFlush(map[pointerToUpperBoolean+i*booleanSize]);
    }

    // Sample from the upper level subboxes into the input buffer
    cout << "Sampling every " << sampleEveryNthElement << "th element in upper level to input\n";

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
        map[pointerToInput+writeIndex*elementSize+3] = pointerToLowerBoolean+i;

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
        cout << "Counter=" << counter << "\n";
    }
    // Insert -1 at end of array
    writeIndex++;
    map[pointerToInput+writeIndex*elementSize] = -1;

    cout << "Created " << writeIndex << " pointers in input buffer\n";

    tempIndex = -1;
    cout << "Input consists of: ";
    do {
        tempIndex++;
        cout << map[pointerToInput+tempIndex*elementSize] << " ";
    }
    while(map[pointerToInput+tempIndex*elementSize] != -1);
    cout << "\n";
}

/*
 * Queries an xBox at the given location, recursively.
 * ForwardPointer points into the given xBox's input buffer.
 * Will return a pointer. This is either a pointer to the element,
 * or a pointer into the next xBox. This differs from the paper where
 * we return a pointer into this xBox's output buffer.
 */
long XDict::search(long xBoxPointer, long forwardPointer, long element) {

    //cout << "Search on xBox " << xBoxPointer << " with forward pointer " << forwardPointer << "\n";

    if(map[xBoxPointer] <= minX) {
        // Special case, scan the array at forward pointer
        long index = 0;
        while(map[forwardPointer+index*elementSize] != -1 && map[forwardPointer+index*elementSize] < element) {
            index++;
        }

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
            //cout << "Case 1\n";
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
            //cout << "Case 2\n";
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
            //cout << "Case 3\n";
            // Case 3a) We found a real element with a larger key
            if(map[forwardPointer+index*elementSize+1] > 0) {
                // Return the pointer of the reverse pointer
                //cout << "Case 3a\n";
                //return map[map[forwardPointer+index*elementSize+2]+2];
                long reversePointer = map[forwardPointer+index*elementSize+2];
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
                index--;
                if(map[forwardPointer+index*elementSize+1] > 0) {
                    // Return the pointer of the reverse pointer
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    long reversePointer = map[forwardPointer+index*elementSize+2];
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
                    //cout << "Case 3b\n";
                    /*cout << "subbox element " << map[forwardPointer+index*elementSize] << " "
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
            cout << "!!!!! ERROR IN SEARCH ON XBOX SIMPLE" << xBoxPointer << "\n";
        }
    }
    else {
        // Proper xBox

        /*cout << "---\n";
        cout << map[xBoxPointer] << " " << map[xBoxPointer+1] << " " << map[xBoxPointer+2] << " " << map[xBoxPointer+3] << " "
             << map[xBoxPointer+4] << " " << map[xBoxPointer+5] << " " << map[xBoxPointer+6] << " " << map[xBoxPointer+7] << " "
             << map[xBoxPointer+8] << " " << map[xBoxPointer+9] << "\n";

        long maxUpperLevel = map[xBoxPointer+4];
        long upB = map[xBoxPointer+6];
        for(int i = 0; i < maxUpperLevel; i++) {
            cout << map[upB+i] << " ";
        }
        cout << "\n";

        long maxLowerLevel = map[xBoxPointer+5];
        long lB = map[xBoxPointer+8];
        for(int i = 0; i < maxLowerLevel; i++) {
            cout << map[lB+i] << " ";
        }
        cout << "\n";


        cout << "---\n";*/

        // ***Scan our input buffer starting at forwardPointer
        long index = 0;
        while(map[forwardPointer+index*elementSize] != -1 && map[forwardPointer+index*elementSize] < element) {
            /*cout << "Input element " << map[forwardPointer+index*elementSize] << " "
                 << map[forwardPointer+index*elementSize+1] << " " << map[forwardPointer+index*elementSize+2]
                 << " " << map[forwardPointer+index*elementSize+3] << "\n";*/
            index++;
        }

        /*cout << "Index Input = " << index << " " << map[forwardPointer+index*elementSize] << "\n";
        cout << map[forwardPointer+index*elementSize] << " " << map[forwardPointer+index*elementSize+1] << " "
             << map[forwardPointer+index*elementSize+2] << " " << map[forwardPointer+index*elementSize+3] << "\n";*/

        // Case 1) We reached the end of the array
        if(map[forwardPointer+index*elementSize] == -1) {
            //cout << "Case 1\n";
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
            //cout << "Case 2\n";
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
            //cout << "Case 3\n";
            // Case 3a) We found a real element with a larger key
            if(map[forwardPointer+index*elementSize+1] > 0) {
                // Return the pointer of the reverse pointer
                //cout << "Case 3a\n";
                //return map[map[forwardPointer+index*elementSize+2]+2];
                long reversePointer = map[forwardPointer+index*elementSize+2];
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
                index--;
                if(map[forwardPointer+index*elementSize+1] > 0) {
                    // Return the pointer of the reverse pointer
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    long reversePointer = map[forwardPointer+index*elementSize+2];
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
                    //cout << "Case 3b\n";
                    /*cout << "subbox element " << map[forwardPointer+index*elementSize] << " "
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
        else {
            // You should never end up here, go away!
            cout << "!!!!! ERROR IN SEARCH ON XBOX INPUT" << xBoxPointer << "\n";
        }

        /*cout << "Input forward = " << forwardPointer << " points to " << map[forwardPointer] << " " << map[forwardPointer+1] << " " << map[forwardPointer+2]
                << " " << map[forwardPointer+3] << "\n";*/

        // ***Recursively search the upper level subbox

        // Calculate which subbox we point into
        long sizeOfSubboxes = map[xBoxPointer+3];
        long numberOfMaxUpperLevel = map[xBoxPointer+4];
        long upperBoolean = map[xBoxPointer+6];
        long upperLevelStartsAt = upperBoolean + numberOfMaxUpperLevel*booleanSize;
        long subboxNumber = (forwardPointer-upperLevelStartsAt) / sizeOfSubboxes;
        long subboxPointer = upperLevelStartsAt+subboxNumber*sizeOfSubboxes;

        /*cout << "UpperBool=" << upperBoolean << " upperLevelStartsAt=" << upperLevelStartsAt
             << " subboxNumber=" << subboxNumber << " subboxSize=" << sizeOfSubboxes
             << " subboxPointer=" << subboxPointer << " forward=" << forwardPointer << "\n";*/

        forwardPointer = search(subboxPointer,forwardPointer,element);
        if(map[forwardPointer] == element && map[forwardPointer+1] > 0) {
            //return map[forwardPointer+1];
            //cout << "Upper subbox pointed directly at element\n";
            return forwardPointer;
        }

        /*cout << "Search on upper subbox returned " << forwardPointer << " containing " << map[forwardPointer]
             << " " << map[forwardPointer+1] << " " << map[forwardPointer+2] << " " << map[forwardPointer+3] << "\n";*/

        // ***Scan the middle buffer starting at the forward pointer returned from the upper level
        index = 0;
        while(map[forwardPointer+index*elementSize] != -1 && map[forwardPointer+index*elementSize] < element) {
            index++;
        }

        /*cout << "Index Middle = " << index << " " << map[forwardPointer+index*elementSize] << "\n";
        cout << map[forwardPointer+index*elementSize] << " " << map[forwardPointer+index*elementSize+1] << " "
             << map[forwardPointer+index*elementSize+2] << " " << map[forwardPointer+index*elementSize+3] << "\n";*/

        // Case 1) We reached the end of the array
        if(map[forwardPointer+index*elementSize] == -1) {
            //cout << "Case 1\n";
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
            //cout << "Case 2\n";
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
            //cout << "Case 3\n";
            // Case 3a) We found a real element with a larger key
            if(map[forwardPointer+index*elementSize+1] > 0) {
                // Return the pointer of the reverse pointer
                //cout << "Case 3a\n";
                //return map[map[forwardPointer+index*elementSize+2]+2];
                long reversePointer = map[forwardPointer+index*elementSize+2];
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
                index--;
                if(map[forwardPointer+index*elementSize+1] > 0) {
                    // Return the pointer of the reverse pointer
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    long reversePointer = map[forwardPointer+index*elementSize+2];
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
                    //cout << "Case 3b\n";
                    /*cout << "subbox element " << map[forwardPointer+index*elementSize] << " "
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
        else {
            // You should never end up here, go away!
            cout << "!!!!! ERROR IN SEARCH ON XBOX MIDDLE" << xBoxPointer << "\n";
        }

        // ***Recursively search the lower level

        // Calculate which subbox we point into
        long numberOfMaxLowerLevel = map[xBoxPointer+5];
        long lowerBoolean = map[xBoxPointer+8];
        long lowerLevelStartsAt = lowerBoolean + numberOfMaxLowerLevel*booleanSize;
        subboxNumber = (forwardPointer-lowerLevelStartsAt) / sizeOfSubboxes;
        subboxPointer = lowerLevelStartsAt+subboxNumber*sizeOfSubboxes;

        forwardPointer = search(subboxPointer,forwardPointer,element);
        if(map[forwardPointer] == element && map[forwardPointer+1] > 0) {
            /*cout << "Lower subbox pointed directly to " << map[forwardPointer] << " "
                 << map[forwardPointer+1] << " " << map[forwardPointer+2] << " " << map[forwardPointer+3] << "\n";*/
            //return map[forwardPointer+1];
            return forwardPointer;
        }

        /*cout << "Search on lower subbox returned " << forwardPointer << " containing " << map[forwardPointer]
             << " " << map[forwardPointer+1] << " " << map[forwardPointer+2] << " " << map[forwardPointer+3] << "\n";*/

        // ***Finally scan the output buffer
        index = 0;
        while(map[forwardPointer+index*elementSize] != -1 && map[forwardPointer+index*elementSize] < element) {
            index++;
        }

        //cout << "Index Output = " << index << " " << map[forwardPointer+index*elementSize] << "\n";

        // Case 1) We reached the end of the array
        if(map[forwardPointer+index*elementSize] == -1) {
            //cout << "Case 1\n";
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
            //cout << "Case 2\n";
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
            //cout << "Case 3\n";
            // Case 3a) We found a real element with a larger key
            if(map[forwardPointer+index*elementSize+1] > 0) {
                // Return the pointer of the reverse pointer
                //cout << "Case 3a\n";
                //return map[map[forwardPointer+index*elementSize+2]+2];
                long reversePointer = map[forwardPointer+index*elementSize+2];
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
                index--;
                if(map[forwardPointer+index*elementSize+1] > 0) {
                    // Return the pointer of the reverse pointer
                    //return map[map[forwardPointer+index*elementSize+2]+2];
                    long reversePointer = map[forwardPointer+index*elementSize+2];
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
                    //cout << "Case 3b\n";
                    /*cout << "subbox element " << map[forwardPointer+index*elementSize] << " "
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
        else {
            // You should never end up here, go away!
            cout << "!!!!! ERROR IN SEARCH ON XBOX OUTPUT" << xBoxPointer << "\n";
        }

        // ***Return a pointer into the next xBox, or -1 if there are none.

        // TODO at this point we will have returned if we found the element.
        // TODO the enclosing method will know wether we are the last xBox,
        // TODO or if we return a valid pointer.

        //cout << "Did we find anything in xBox " << xBoxPointer << "\n";

        return forwardPointer;
    }
}


/***********************************************************************************************************************
 * Extended Methods
 **********************************************************************************************************************/

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


    long start = startOfElementsToInsert; long end = 0; long counter = 0;
    long sizeOfBatch = map[nextXBoxPointer]/2;
    long maxOfNextXBox = (pow(map[nextXBoxPointer],1+alpha) / 2);
    long index = 0;
    // Run through the elements, batch inserting as we go
    while(map[startOfElementsToInsert+index*elementSize] != -1) {
        if(map[startOfElementsToInsert+index*elementSize+1] > 0) {
            counter++; // Another real element
        }
        if(counter == sizeOfBatch) {
            end = startOfElementsToInsert+(index+1)*elementSize;
            if(map[nextXBoxPointer+1]+counter > maxOfNextXBox) {
                // Flush next xBox
                flush(nextXBoxPointer,false);

                // Recursively batch insert
                recursivelyBatchInsertXBoxToXBox(xBoxnumber+1);
            }
            batchInsert(nextXBoxPointer,start,end,counter);
            counter = 0;
            start = end;
        }
        index++;
    }
    // Might be elements left that we did not batchInsert
    if(counter != 0) {
        end = startOfElementsToInsert+(index)*elementSize;
        if(map[nextXBoxPointer+1]+counter > maxOfNextXBox) {
            // Flush next xBox
            flush(nextXBoxPointer,false);

            // Recursively batch insert
            recursivelyBatchInsertXBoxToXBox(xBoxnumber+1);
        }
        batchInsert(nextXBoxPointer,start,end,counter);
    }

    // Reset this xBox
    map[xBoxPointer+1] = 0;
    map[startOfElementsToInsert] = -1; // Empty array

    // Sample from the next xBox into this one
    sampleToEmptyBufferFromBuffer(startOfElementsToInsert,nextXBoxPointer+infoOffset,16);

}

/*
 * Samples from a full buffer to an empty buffer.
 * Used to sample from one xbox to another.
 */
void XDict::sampleToEmptyBufferFromBuffer(long pointerToDestination, long pointerToSource, long sampleEveryNth) {
    // TODO
}




/***********************************************************************************************************************
 * Helper Methods
 **********************************************************************************************************************/

/*
 * Recursively calculates the size of an xBox.
 */
long XDict::recursivelyCalculateSize(long x) {

    if(x <= minX) {
        return minSize;
    }

    long y = (long) sqrt(x);
    long numberOfUpperSubboxes = y/4; // *(1/4)
    long numberOfLowerSubboxes = (long) pow(x,0.5 + alpha/2)/4; // *(1/4)
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
        map[pointer+minSize-1] = -1; // Backstopper
        return pointer+minSize;
    }

    // Layout the xbox. Subboxes will be created as needed, just make space for them.
    long numberOfRealElements = 0;
    long y = (long) sqrt(x);
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
    numberOfUpperSubboxes = y/4; // *(1/4)
    numberOfLowerSubboxes = (long) pow(x,0.5 + alpha/2) / 4; // *(1/4)

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

    cout << "Wrote to " << pointerToOutputBuffer+sizeOfOutputBuffer-1 << " fileSize=" << fileSize << "\n";

    cout << "************************ NEW XBOX ************************\n";
    cout << "x = " << x << "\n";
    cout << "#realEle = " << numberOfRealElements << "\n";
    cout << "y = " << y << "\n";
    cout << "sizeSubboxes = " << sizeOfSubboxes << "\n";
    cout << "#upperSubboxes = " << numberOfUpperSubboxes << "\n";
    cout << "#lowerSubboxes = " << numberOfLowerSubboxes << "\n";
    cout << "size input = " << x*elementSize << "\n";
    cout << "size middle = " << sizeOfMiddleBuffer << "\n";
    cout << "size output = " << sizeOfOutputBuffer << "\n";
    cout << "**********************************************************\n";
    /*cout << pow(x,(1/2 + alpha/2)) << " " << pow(x,1/2 + alpha/2) / 4 << "\n";
    cout << alpha/2 << " " << pow(x,(1/2)) << " " << pow(x,(1/2 + 1/2)) << " " << pow(x,1) << "\n";
    double calc = 0.5 + alpha/2;
    cout << pow(x,calc) << " " << calc << "\n";*/

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
        cout << "--- " << smallestPointer << "\n";
        return smallestPointer;
    }
    else {
        return -1;
    }


    //cout << "Finding first subbox at position " << pointer << "\n";

    // Find the first subbox
    /*for(index; index < size; index++) {
        //cout << map[pointer+index] << " ";
        if(map[pointer+index] != 0) {
            // We have a subbox!
            long subboxPointer = map[pointer+index];
            //cout << "x=" << map[subboxPointer] << "\n";
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
                    //cout << "New smallest = " << smallest << "\n";
                    smallestPointer = pointer+index;
                }
            }
        }
    }

    //cout << " Found first " << smallestPointer << " " <<  smallest << "\n";

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

    //cout << " Found " << smallestPointer << " " <<  smallest << "\n";
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

    int result = (int) lseek(fileDescriptor, (pointer+1)*sizeof(long), SEEK_SET);
    if (result == -1) {
        close(fileDescriptor);
        perror("Error mremap lseek");
        exit(EXIT_FAILURE);
    }
    result = (int) write(fileDescriptor, "", 1);
    if (result != 1) {
        close(fileDescriptor);
        perror("Error mremap write to EOF");
        exit(EXIT_FAILURE);
    }

    map = (long*) mremap(map, fileSize*sizeof(long), (pointer+1)*sizeof(long), MREMAP_MAYMOVE);
    if (map == MAP_FAILED) {
        close(fileDescriptor);
        perror("Error mmap original file");
        exit(EXIT_FAILURE);
    }

    fileSize = pointer+1;

}