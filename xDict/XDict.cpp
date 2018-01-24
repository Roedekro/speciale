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

    minX = 64;
    minSize = minX*elementSize+3; // x, real elements, and -1 at the end
    infoOffset = 10;

    latestSize = minSize;
    latestX = minX;

    fileSize = latestSize;
    fileName = "xDict";

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


/***********************************************************************************************************************
 * Standard Methods, from the paper
 **********************************************************************************************************************/

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

    // Flush all subboxes
    for(int i = 0; i < numberOfUpperLevelSubboxes; i++) {
        if(map[pointerToUpperBoolean+i] != 0) {
            flush(map[pointerToUpperBoolean+i],true);
        }
    }

    for(int i = 0; i < numberOfLowerLevelSubboxes; i++) {
        if(map[pointerToLowerBoolean+i] != 0) {
            flush(map[pointerToLowerBoolean+i],true);
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
        long upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
        long upperSubbox,upperMax,upperBuffer;
        if(upperPointer != -1) {
            upperSubbox = map[upperPointer];
            upperMax = map[upperSubbox+1]; // Real elements in subbox
            upperBuffer = map[upperSubbox+9]; // Its output buffer
        }

        long lowerPointer = findNextSubboxForMerge(pointerToLowerBoolean,numberOfLowerLevelSubboxes);
        // Its now -1 if none was found, but proceed as if it found one.
        long lowerSubbox,lowerMax,lowerBuffer;
        if(lowerPointer != -1) {
            lowerSubbox = map[lowerPointer];
            lowerMax = map[lowerSubbox+1]; // Real elements in subbox
            lowerBuffer = map[lowerSubbox+9]; // Its output buffer
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
        if(upperPointer == -1) {
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
        if(lowerPointer == -1) {
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
                    map[upperPointer] = 0; // upperPointed pointed into our array of upper level subboxes.
                    upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
                    // Its now -1 if none was found, but proceed as if it found one.
                    upperSubbox = map[upperPointer];
                    upperMax = map[upperSubbox+1]; // Real elements in subbox
                    upperBuffer = map[upperSubbox+9]; // Its output buffer
                    upperIndex = 0;

                    // Did we find a subbox?
                    if(upperPointer == -1) {
                        mergeArray[2] = -1;
                        mergeArray[3] = -1;
                    }
                    else {
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
                    map[lowerPointer] = 0; // Mark old subbox as deleted.
                    lowerPointer = findNextSubboxForMerge(pointerToLowerBoolean,numberOfLowerLevelSubboxes);
                    // Its now -1 if none was found, but proceed as if it found one.
                    lowerSubbox = map[lowerPointer];
                    lowerMax = map[lowerSubbox+1]; // Real elements in subbox
                    lowerBuffer = map[lowerSubbox+9]; // Its output buffer
                    lowerIndex = 0;

                    // Did we find a subbox?
                    if(lowerPointer == -1) {
                        mergeArray[6] = -1;
                        mergeArray[7] = -1;
                    }
                    else {
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
        if(upperPointer != -1) {
            map[upperPointer] = 0;
        }
        if(lowerPointer != -1) {
            map[lowerPointer] = 0;
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
        long upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
        //cout << "Upper Pointer = " << upperPointer << "\n";
        long upperSubbox,upperMax,upperBuffer;
        if(upperPointer != -1) {
            upperSubbox = map[upperPointer];
            upperMax = map[upperSubbox+1]; // Real elements in subbox
            upperBuffer = upperSubbox+2; // Its output buffer
        }

        long lowerPointer = findNextSubboxForMerge(pointerToLowerBoolean,numberOfLowerLevelSubboxes);
        // Its now -1 if none was found, but proceed as if it found one.
        long lowerSubbox,lowerMax,lowerBuffer;
        if(lowerPointer != -1) {
            lowerSubbox = map[lowerPointer];
            lowerMax = map[lowerSubbox+1]; // Real elements in subbox
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
        if(upperPointer == -1) {
            // We found no upper level subbox
            mergeArray[2] = -1;
            mergeArray[3] = -1;
        }
        else {
            // Find first element that isnt a pointer
            while(map[upperBuffer+upperIndex*2+1] < 0 &&
                  map[upperBuffer+upperIndex*2] != -1) {
                upperIndex++;
            }
            if(map[upperBuffer+upperIndex*2] == -1) {
                mergeArray[2] = -1;
                mergeArray[3] = -1;
            }
            else {
                mergeArray[2] = map[upperBuffer+upperIndex*2];
                mergeArray[3] = map[upperBuffer+upperIndex*2+1];
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
        if(lowerPointer == -1) {
            // We found no lower level subbox
            mergeArray[6] = -1;
            mergeArray[7] = -1;
        }
        else {
            // Find first element that isnt a pointer
            while(map[lowerBuffer+lowerIndex*2+1] < 0 &&
                  map[lowerBuffer+lowerIndex*2] != -1) {
                lowerIndex++;
            }
            if(map[lowerBuffer+lowerIndex*2] == -1) {
                mergeArray[6] = -1;
                mergeArray[7] = -1;
            }
            else {
                mergeArray[6] = map[lowerBuffer+lowerIndex*2];
                mergeArray[7] = map[lowerBuffer+lowerIndex*2+1];
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
                    map[upperPointer] = 0; // upperPointed pointed into our array of upper level subboxes.
                    upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);

                    // Did we find a subbox?
                    if(upperPointer == -1) {
                        mergeArray[2] = -1;
                        mergeArray[3] = -1;
                    }
                    else {
                        upperSubbox = map[upperPointer];
                        upperMax = map[upperSubbox+1]; // Real elements in subbox
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
                    map[lowerPointer] = 0; // Mark old subbox as deleted.
                    lowerPointer = findNextSubboxForMerge(pointerToLowerBoolean,numberOfLowerLevelSubboxes);

                    // Did we find a subbox?
                    if(lowerPointer == -1) {
                        mergeArray[6] = -1;
                        mergeArray[7] = -1;
                    }
                    else {
                        lowerSubbox = map[lowerPointer];
                        lowerMax = map[lowerSubbox+1]; // Real elements in subbox
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
        if(upperPointer != -1) {
            map[upperPointer] = 0;
        }
        if(lowerPointer != -1) {
            map[lowerPointer] = 0;
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
 * Assumes all elements in an xBox is located in the output buffer, and
 * that the xBox is otherwise empty with no subboxes allocated.
 * We create up to half the subboxes and fill each up with forward pointers equivalent to
 * half the elements it can contain in its output buffer.
 * We then recursively call SampleUp on these subboxes.
 * We sample into the middle buffer.
 * We perform the same procedure for the upper level.
 * And finally we sample up into the input buffer.
 */
void XDict::sampleUp(long pointerToXBox) {

    if(map[pointerToXBox] <= minX) {
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
    long power = (long) pow(x,0.5 + alpha/2); // Size of output buffer of subbox
    if(y <= minX) {
        power = minX; // Single array
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
    long latestLocation = pointerToLowerBoolean+maxNumberOfLowerLevel; // First subbox

    cout << "Going to create " << lowerLevelSubboxesToCreate << " new lower level subboxes of y = " << y << "\n";
    for(int i = 0; i < lowerLevelSubboxesToCreate; i++) {
        //cout << "Subbox will be located at " << latestLocation << "\n";
        map[pointerToLowerBoolean+i] = latestLocation;
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
        currentSubboxOutputBuffer = map[map[pointerToLowerBoolean+currentSubboxIndex]+9];
    }
    else {
        // Subboxes are just arrays
        currentSubboxOutputBuffer = map[pointerToLowerBoolean+currentSubboxIndex] + 2;
    }

    // Run through output creating pointers
    while(map[pointerToOutput+index*4] != -1) {
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
                    currentSubboxOutputBuffer = map[map[pointerToLowerBoolean+currentSubboxIndex]+9];
                }
                else {
                    // Subboxes are just arrays
                    currentSubboxOutputBuffer = map[pointerToLowerBoolean+currentSubboxIndex] + 2;
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
        sampleUp(map[pointerToLowerBoolean+i]);
    }

    // Sample into the middle buffer while counting elements
    // TODO Create subbox pointers while at it! Not for the first subbox though, its implicit.




    // Create the appropriate amount of upper level subboxes
    long upperLevelSubboxesToCreate = 0;
    long pointerToUpperBoolean = map[pointerToXBox+6];

    // Sample up into the upper level subboxes


    // Recursively call SampleUp on the upper level subboxes
    for(int i = 0; i < upperLevelSubboxesToCreate; i++) {
        sampleUp(map[pointerToUpperBoolean+i]);
    }

    // Sample from the upper level subboxes into the input buffer



}


/***********************************************************************************************************************
 * Extended Methods
 **********************************************************************************************************************/

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
    return infoOffset +  x*elementSize+1 + numberOfUpperSubboxes + upperLevelSize + sizeOfMiddleBuffer
            + numberOfLowerSubboxes + lowerLevelSize + sizeOfOutputBuffer;


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
    pointerToMiddleBuffer = pointerToBoolUpper + numberOfUpperSubboxes + (numberOfUpperSubboxes*sizeOfSubboxes);
    map[pointer+7] = pointerToMiddleBuffer;
    pointerToBoolLower = pointerToMiddleBuffer + sizeOfMiddleBuffer;
    map[pointer+8] = pointerToBoolLower;
    /*pointerToFirstLower = pointerToBoolLower + numberOfLowerSubboxes;
    map[pointer+10] = pointerToFirstLower;*/
    pointerToOutputBuffer = pointerToBoolLower + numberOfLowerSubboxes + (numberOfLowerSubboxes*sizeOfSubboxes);
    map[pointer+9] = pointerToOutputBuffer;


    // Indicate input buffer is empty
    map[pointer+infoOffset] = -1;
    map[pointerToBoolUpper-1] = -1; // Backstopper for input

    // Indicate that upper level subboxes are not in use
    for(int i = 0; i < numberOfUpperSubboxes; i++) {
        map[pointerToBoolUpper+i] = 0;
    }

    // Indicate that middle buffer is empty
    map[pointerToMiddleBuffer] = -1;
    map[pointerToBoolLower-1] = -1; // Backstopper for middle

    // Indicate that lower level subboxes are not in use
    for(int i = 0; i < numberOfLowerSubboxes; i++) {
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

/*
 * Pointer must point to a boolean array of subboxes (its pointers really),
 * that has a max size of size. Assumes all subboxes have been flushed.
 * Returns a pointer INTO THE BOOLEAN ARRAY, which then points
 * to the subbox whose output buffer contains the smallest element.
 * If none can be found it returns -1.
 * You have to find the subbox and output buffer yourself,
 * based on the return value.
 */
long XDict::findNextSubboxForMerge(long pointer, long size) {

    long index = 0;
    long smallest = LONG_MAX;
    long smallestPointer = pointer;
    bool min = false;

    //cout << "Finding first subbox at position " << pointer << "\n";

    // Find the first subbox
    for(index; index < size; index++) {
        //cout << map[pointer+index] << " ";
        if(map[pointer+index] != 0) {
            // We have a subbox!
            long subboxPointer = map[pointer+index];
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
    return smallestPointer;
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