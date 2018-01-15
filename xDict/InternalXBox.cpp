//
// Created by martin on 1/15/18.
//

#include <cmath>
#include <algorithm>
#include "InternalXBox.h"

using namespace std;

InternalXBox::InternalXBox(int size, float a) {

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

        upperLevel = new vector<InternalXBox>();
        lowerLevel = new vector<InternalXBox>();;
        inputBuffer = new vector<KeyValueTime>();
        middleBuffer = new vector<KeyValueTime>();
        outputBuffer = new vector<KeyValueTime>();
    }
    else {
        x = size;
        leaf = true;
        upperLevel = new vector<InternalXBox>();
    }


}

InternalXBox::~InternalXBox() {

}

/*
 * Insert sorted array of KeyValueTimes into this xbox.
 * Size of toInsert is at most 1/2 x.
 */
void InternalXBox::batchInsert(std::vector<KeyValueTime> *toInsert) {

    // Update real elements
    realElements += toInsert->size();

    // Merge into input array
    mergeVectors(inputBuffer,toInsert);

    // TODO: Should remove lookahead pointers when merging

    // Input array now implicitly partitioned by the subbox pointers (if any).
    // For a range with >= 1/2 sqrt(x) elements insert them into the subbox.

    if(maxNumberOfUpperLevelSubboxes != 0) {
        // Go through each of them and insert if necessary.

        // If inserting elements into subbox would exceed it limit
        // first "split" the subbox.





        // Restore lookahead pointers

    }
    else {
        // Create a subbox and insert into it


        // Sample lookahead pointers from new subbox

    }






}

/***********************************************************************************************************************
 * Helper Methods
 **********************************************************************************************************************/

/*
 * Merge v2 into v1, and by merge I mean we sort. Its internal memory and doesnt matter.
 */
void InternalXBox::mergeVectors(std::vector<KeyValueTime> *v1, std::vector<KeyValueTime> *v2) {

    for(int i = 0; i < v2->size(); i++) {
        KeyValueTime kvt = v2->at(i);
        v1->push_back(kvt);
    }
    sort(v1->begin(),v1->end());

}