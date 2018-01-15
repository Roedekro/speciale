//
// Created by martin on 1/15/18.
//

#ifndef SPECIALE_NAIVEINTERNALXBOX_H
#define SPECIALE_NAIVEINTERNALXBOX_H


#include <vector>
#include "../BufferedBTree/keyValueTime.h"

class NaiveInternalXBox {
public:
    NaiveInternalXBox(int size, float a);
    ~NaiveInternalXBox();
    int x; // Size of this box
    float alpha;
    bool leaf; // "Leaf" as in no subboxes
    int realElements;
    int maxRealElements;
    int y; // Size of subboxes
    int upperLevelCounter; // # of upper level subboxes
    int lowerLevelCounter; // # of lower level subboxes
    int maxNumberOfUpperLevelSubboxes;
    int maxNumberOfLowerLevelSubboxes;
    /*InternalXBox* upperLevel;
    InternalXBox* lowerLevel;
    KeyValueTime* inputBuffer;
    KeyValueTime* middleBuffer;
    KeyValueTime* outputBuffer;*/
    std::vector<NaiveInternalXBox*>* upperLevel;
    std::vector<NaiveInternalXBox*>* lowerLevel;
    std::vector<KeyValueTime>* inputBuffer;
    std::vector<KeyValueTime>* middleBuffer;
    std::vector<KeyValueTime>* outputBuffer;

    /*
     * Standard methods of the xBox
     */
    void batchInsert(std::vector<KeyValueTime>* toInsert);
    void middleBatchInsert();
    void flush();
    void sampleUp();
    int query(int element);

    /*
     * Helper methods
     */
    void mergeVectors(std::vector<KeyValueTime> *v1, std::vector<KeyValueTime> *v2);

};


#endif //SPECIALE_NAIVEINTERNALXBOX_H
