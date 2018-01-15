//
// Created by martin on 1/15/18.
// Does not have the appropriate memory layout needed for
// being optimal. This is used as a stepping stone into
// the real xBox.
//

#include "NaiveInternalXBox.h"

#include <cmath>
#include <algorithm>

using namespace std;

NaiveInternalXBox::NaiveInternalXBox(int size, float a) {

    if(size > 128) {
        x = size;
        alpha = a;
        leaf = false; // We have subboxes
        realElements = 0;
        maxRealElements = (1/2) * (pow(x,1 + alpha));
        y = (int) sqrt(x); // Size of subboxes, x^(1/2)

        /*upperLevelCounter = 0; // # of upper level subboxes
        lowerLevelCounter = 0; // # of lower level subboxes
        maxNumberOfUpperLevelSubboxes = (1/4) * y; // 1/4 x^(1/2)
        maxNumberOfLowerLevelSubboxes = (1/4) * (pow(x,1/2 + alpha/2));
        upperLevel = InternalXBox[maxNumberOfUpperLevelSubboxes];*/

        upperLevel = new vector<NaiveInternalXBox*>();
        lowerLevel = new vector<NaiveInternalXBox*>();
        inputBuffer = new vector<KeyValueTime>();
        middleBuffer = new vector<KeyValueTime>();
        outputBuffer = new vector<KeyValueTime>();
    }
    else {
        x = size;
        leaf = true;
        inputBuffer = new vector<KeyValueTime>();
    }


}

NaiveInternalXBox::~NaiveInternalXBox() {

}

/*
 * Insert sorted array of KeyValueTimes into this xbox.
 * Size of toInsert is at most 1/2 x.
 */
void NaiveInternalXBox::batchInsert(std::vector<KeyValueTime> *toInsert) {

    // Update real elements
    realElements += toInsert->size();

    // Merge into input array
    // TODO: OBS, can not be translated into non-naive, since we handle an array.
    mergeVectors(inputBuffer,toInsert);

    // TODO: Should remove lookahead pointers when merging


    if(!leaf) {

        // Input array now implicitly partitioned by the subbox pointers (if any).
        // For a range with >= 1/2 sqrt(x) elements insert them into the subbox.

        if(maxNumberOfUpperLevelSubboxes != 0) {
            // Go through each of them and insert if necessary.

            // If inserting elements into subbox would exceed it limit
            // first "split" the subbox.
            // If we want to split and there are no more empty subboxs
            // left we abort the insertion, to be resumed after we
            // have flushed the subboxes into the middle buffer

            // Scan input
            for(int i = 0; i < inputBuffer->size(); i++) {

                // Count elements that fits into next subbox

                // If we reach 1/2 sqrt(x) elements insert into subbox
                if() {

                    // If this will cause it to exceed max size split
                    if() {

                        // Check if max upper level, else split
                        if(upperLevelCounter != maxNumberOfUpperLevelSubboxes) {

                        }
                        else {

                            middleBatchInsert();
                            // TODO: Check that we did not flush the entire xbox and are now empty

                        }
                    }
                    else {
                        // Else insert into the upper level subbox, there is space
                    }

                }
            }

            // Restore lookahead pointers of the input buffer




        }
        else {
            // Create a subbox and insert into it
            NaiveInternalXBox* newSubbox = new NaiveInternalXBox(y,alpha);
            upperLevelCounter++;
            upperLevel->push_back(newSubbox);




            // Sample lookahead pointers from new subbox

        }
    }

    // Outside scope above we are a leaf
    // which means we do nothing.
}

/*
 * Extension method to batchInsert, to keep the logic for the inputBuffer
 * and the middle buffer separate.
 */
void NaiveInternalXBox::middleBatchInsert() {

    // Flush into middle buffer

    // Flush all upper level subboxes

    // Gather elements into middle buffer

    // Scan middle buffer and insert into lower level subboxes
    for() {

        // If we reach 1/2 sqrt(x) elements insert into subbox
        if() {

            // If this insertion will cause it to exceed its max size split
            if() {

                // Do we have room to split the lower level subbx?
                if(lowerLevelCounter != maxNumberOfLowerLevelSubboxes) {
                    // Split
                }
                else {
                    // Flush to output buffer

                    // First flush all lower level subboxes

                    // Now merge into the output buffer


                }
            }
            else {
                // Room in lower level subbox to insert
            }


        }
    }

    // Restore lookahead pointers for the middle buffer

}

/***********************************************************************************************************************
 * Helper Methods
 **********************************************************************************************************************/

/*
 * Merge v2 into v1, and by merge I mean we sort. Its internal memory and doesnt matter.
 */
void NaiveInternalXBox::mergeVectors(std::vector<KeyValueTime> *v1, std::vector<KeyValueTime> *v2) {

    for(int i = 0; i < v2->size(); i++) {
        KeyValueTime kvt = v2->at(i);
        v1->push_back(kvt);
    }
    sort(v1->begin(),v1->end());

}