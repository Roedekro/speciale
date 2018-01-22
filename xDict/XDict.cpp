//
// Created by martin on 1/15/18.
//

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include <cmath>
#include "XDict.h"

/*
 * Set up an empty xBox consisting of an array of
 * 194 longs, the first being the number of real elements
 * in the xBox and the rest holding keys and values of the
 * elements.
 */
XDict::XDict(float a) {

    alpha = a;
    latestSize = 194;
    latestX = 64;

    minX = 64;
    minSize = 194; // 3*64 + 2
    infoOffset = 10;

    fileSize = latestSize;
    fileName = "xDict";

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
    xBoxes->push_back(0);
}

XDict::~XDict() {

}

/***********************************************************************************************************************
 * General Methods
 **********************************************************************************************************************/

/***********************************************************************************************************************
 * Standard Methods, from the paper
 **********************************************************************************************************************/

/*
 * Flush the xBox pointed at.
 */
void XDict::flush(long pointer) {

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
    long pointerToOutputBufer = map[pointer+9];

    // Flush all subboxes
    for(int i = 0; i < numberOfUpperLevelSubboxes; i++) {
        if(map[pointerToUpperBoolean+i] != 0) {
            flush(map[pointerToUpperBoolean+i]);
        }
    }

    for(int i = 0; i < numberOfLowerLevelSubboxes; i++) {
        if(map[pointerToLowerBoolean+i] != 0) {
            flush(map[pointerToLowerBoolean+i]);
        }
    }

    // Elements are now placed in potentially five places.
    // The three main buffers and the output buffers of all subboxes.
    // The output buffers of each level of subboxes can be concidered an array.
    // Perform a five way merge to place all elements into the output buffer
    if(y > minX) {
        // Merge with normal subboxes

        // Special case, subboxes are merely arrays

        // Find upper level subbox with smallest element in output buffer
        long upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
        // Its now -1 if none was found, but proceed as if it found one.
        long upperSubbox = map[upperPointer];
        long upperMax = map[upperSubbox+1]; // Real elements in subbox
        long upperBuffer = map[upperSubbox+9]; // Its output buffer
        long upperIndex = 0;

        long lowerPointer = findNextSubboxForMerge(pointerToLowerBoolean,numberOfLowerLevelSubboxes);
        // Its now -1 if none was found, but proceed as if it found one.
        long lowerSubbox = map[lowerPointer];
        long lowerMax = map[lowerSubbox+1]; // Real elements in subbox
        long lowerBuffer = map[lowerSubbox+9];
        long lowerIndex = 0;

        long inputIndex = 0;
        long middleIndex = 0;
        long outputIndex = 0;

        long writeIndex = 0;


    }
    else {
        // Special case, subboxes are merely arrays

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
        while(map[pointerToOutputBufer+writeIndex*3] != -1) {
            writeIndex++;
            if(map[pointerToOutputBufer+writeIndex*3+1] > 0) {
                // Real element
                numberOfRealElementsInOutputBuffer++;
            }
        }

        if(numberOfRealElements == numberOfRealElementsInOutputBuffer) {
            return; // We are already flushed
        }

        // 2) Move the elements in the output buffer back to make room for the remaining real elements.
        long deltaRealElements = numberOfRealElements-numberOfRealElementsInOutputBuffer;
        long elementsToMove = writeIndex-1; // Includes forward pointers!
        long moveByThisMuch = 3 * deltaRealElements;

        for(long i = elementsToMove; i > 0; i--) {
            map[pointerToOutputBufer + moveByThisMuch + i*3+2] = map[pointerToOutputBufer + i*3+2];
            map[pointerToOutputBufer + moveByThisMuch + i*3+1] = map[pointerToOutputBufer + i*3+1];
            map[pointerToOutputBufer + moveByThisMuch + i*3] = map[pointerToOutputBufer + i*3];
        }






        // 3) Perform the five way merge

        // Find upper level subbox with smallest element in output buffer
        long upperPointer = findNextSubboxForMerge(pointerToUpperBoolean,numberOfUpperLevelSubboxes);
        // Its now -1 if none was found, but proceed as if it found one.
        long upperSubbox = map[upperPointer];
        long upperMax = map[upperSubbox+1]; // Real elements in subbox
        long upperBuffer = map[upperSubbox+9]; // Its output buffer
        long upperIndex = 0;

        long lowerPointer = findNextSubboxForMerge(pointerToLowerBoolean,numberOfLowerLevelSubboxes);
        // Its now -1 if none was found, but proceed as if it found one.
        long lowerSubbox = map[lowerPointer];
        long lowerMax = map[lowerSubbox+1]; // Real elements in subbox
        long lowerBuffer = map[lowerSubbox+9];
        long lowerIndex = 0;

        long inputIndex = 0;
        long middleIndex = 0;
        long outputIndex = 0;

        long writeIndex = 0;




    }



    // TODO: When finished with a subbox, set its boolean to zero

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

    long y = sqrt(x);
    long numberOfUpperSubboxes = (1/4) * y;
    long numberOfLowerSubboxes = (1/4) * (long) pow(x,1/2 + alpha/2);
    long sizeOfMiddleBuffer = 3* pow(x,1+alpha/2);
    long sizeOfOutputBuffer = 3* pow(x,1+alpha);

    long subboxSize = recursivelyCalculateSize(y);
    long upperLevelSize = numberOfUpperSubboxes*subboxSize;
    long lowerLevelSize = numberOfLowerSubboxes*subboxSize;

    // Info+ Input + Upper Bool + Upper Subboxes + Middle + Lower Bool + Lower subboxes + output
    return infoOffset +  x*3 + numberOfUpperSubboxes + upperLevelSize + sizeOfMiddleBuffer
            + numberOfLowerSubboxes + lowerLevelSize + sizeOfOutputBuffer;


}

/*
 * Lays out a new xBox starting at the location of pointer
 */
void XDict::layoutXBox(long pointer, long x) {

    // Base case
    if(x <= minX) {
        map[pointer] = x;
        map[pointer+1] = 0; // No elements
        map[pointer+2] = -1; // Empty array
        return;
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
    numberOfUpperSubboxes = (1/4) * y;
    numberOfLowerSubboxes = (1/4) * (long) pow(x,1/2 + alpha/2);

    // Calculate size of arrays
    // Input buffer is obviously of size x
    long sizeOfMiddleBuffer = 3* pow(x,1+alpha/2);
    long sizeOfOutputBuffer = 3* pow(x,1+alpha);

    // Fill in info
    map[pointer] = x;
    map[pointer+1] = numberOfRealElements;
    map[pointer+2] = y;
    map[pointer+3] = sizeOfSubboxes;
    map[pointer+4] = numberOfUpperSubboxes;
    map[pointer+5] = numberOfLowerSubboxes;
    pointerToBoolUpper = pointer+infoOffset+x*3; // Start + info + input buffer
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

    // Indicate that upper level subboxes are not in use
    for(int i = 0; i < numberOfUpperSubboxes; i++) {
        map[pointerToBoolUpper+i] = 0;
    }

    // Indicate that middle buffer is empty
    map[pointerToMiddleBuffer] = -1;

    // Indicate that lower level subboxes are not in use
    for(int i = 0; i < numberOfLowerSubboxes; i++) {
        map[pointerToBoolLower+i] = 0;
    }

    // Indicate that output buffer is empty
    map[pointerToOutputBuffer] = -1;

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
    long smallest = -1;
    long smallestPointer = pointer;
    bool min = false;

    // Find the first subbox
    for(index; index < size; index++) {
        if(map[pointer+index] != 0) {
            // We have a subbox!
            long subboxPointer = map[pointer+index];
            if(map[subboxPointer] <= minX) {
                min = true;
                smallest = map[subboxPointer+2];
                smallestPointer = pointer+index;
            }
            else {
                long outputBuffer = map[subboxPointer+9];
                smallest = map[outputBuffer];
                smallestPointer = pointer+index;
            }

        }
    }

    // Check!
    if(smallest == -1) {
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


    return smallestPointer;
}