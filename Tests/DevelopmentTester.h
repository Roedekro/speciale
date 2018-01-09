//
// Created by martin on 11/8/17.
//

#ifndef SPECIALE_DEVELOPMENTTESTER_H
#define SPECIALE_DEVELOPMENTTESTER_H


#include "../Btree/ModifiedInternalNode.h"

class DevelopmentTester {
public:
    DevelopmentTester();
    ~DevelopmentTester();
    void test();

private:
    int streamtestread(long n, int increment);
    int streamtestread2(long n, int increment);
    int streamtestwrite(long n, int increment);
    int streamtestwrite2(long n, int increment);
    void BtreeInsertTest();
    void simpleInsertAndQueryTest();
    void insertAndQueryTest();
    void insertDeleteAndQueryTest();
    void writeToFileTest();
    void manipulateArray(int* array);
    void manipulateArrayTest();
    void pointerSize();
    void recursivePrintModifiedNode(ModifiedInternalNode* node);
    void internalModifiedBtreeInsertAndQueryTest();
    void internalModifiedBtreeInsertDeleteAndQueryTest();
    void BufferedBtreeInsertDeleteAndQueryTest(); // Deprecated
    void internalNodeCalculation();
    void sortTest();
    void sizeTest();
    void sortAndRemoveTest();
    void BufferWriteReadAppendTest();
    void sortAndRemoveDuplicatesExternalBufferTest();
    void testLeafReadWriteAndSortRemove();
    void externalBufferedBTreeInsertDeleteQuery();
    void testHandleDeletesExternalLeaf();
    void testNewLeafBufferOverflowMethod();
    void handleEmptyRootLeafBufferOverflow();
    void testMedianOfMedians();
    void testTruncatedInsertQueryDelete();
    void testBinarySearchFracList();
    void readDiskstatsTest();
    void buildModifiedBTreeTest();
    void initialTestBufferedBTree();
    void advancedTestBufferedBTree();



};


#endif //SPECIALE_DEVELOPMENTTESTER_H
