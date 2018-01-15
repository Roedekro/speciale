//
// Created by martin on 1/15/18.
//

#ifndef SPECIALE_XDICT_H
#define SPECIALE_XDICT_H

#include <string>
#include <vector>
#include "../Btree/KeyValue.h"

class XDict {
public:
    long* map; // The mapped xDict
    std::string fileName; // contains the entire structure.
    int fileDescriptor;
    long fileSize;
    std::vector<long>* xBoxes; // Contains longs that acts as pointers to the xBoxes of the xDict.
    float a;
    long latestSize; // Holds the size of the latest xBox. Used to calculate size of new xBox.
    long latestX; // Holds the x of the latest xBox
    long minX = 64; // The minimum x of any xBox, at which point its just an array.

    XDict(float alpha);
    ~XDict();

    /*
     * General Methods
     */
    void insert(KeyValue kvt);
    void addXBox(); // Extends the file and relevant structures and variables with another xbox

    /*
     * Standard methods from the paper
     */
    void batchInsert();
    void flush();
    void sampleUp();
    long query(long element);

    /*
     * Extended methods
     */



    /*
     * Helper methods
     */
    void calculateNewSize(int x); // Each time its called we calculate the size of the next xBox in line into size
    void layoutXBox(long pointer, int x);

};


#endif //SPECIALE_XDICT_H
