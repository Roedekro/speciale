//
// Created by martin on 1/15/18.
//

#ifndef SPECIALE_INTERNALXBOX_H
#include <vector>
#include "../Btree/KeyValue.h"
#include "../BufferedBTree/keyValueTime.h"

#define SPECIALE_INTERNALXBOX_H




class InternalXBox {
public:
    InternalXBox(int size, float a);
    ~InternalXBox();
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
    std::vector<InternalXBox>* upperLevel;
    std::vector<InternalXBox>* lowerLevel;
    std::vector<KeyValueTime>* inputBuffer;
    std::vector<KeyValueTime>* middleBuffer;
    std::vector<KeyValueTime>* outputBuffer;

    /*
     * Standard methods of the xBox
     */
    void batchInsert(std::vector<KeyValueTime>* toInsert);
    void flush();
    void sampleUp();
    int query(int element);

    /*
     * Helper methods
     */
    void mergeVectors(std::vector<KeyValueTime> *v1, std::vector<KeyValueTime> *v2);



};


#endif //SPECIALE_INTERNALXBOX_H
