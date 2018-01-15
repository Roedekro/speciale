//
// Created by martin on 11/21/17.
//

#ifndef SPECIALE_TREETESTER_H
#define SPECIALE_TREETESTER_H


class TreeTester {
public:
    TreeTester();
    ~TreeTester();
    void truncatedBufferTree(int B, int M, int delta, int N, int runs);
    void truncatedDeltaTest(int B, int M, int N, int runs);
    void modifiedBTreeTest(int B, int M, int N, int runs);
    void bufferedBTreeDeltaTest(int B, int M, int N, int runs);
    void bufferedBTreeTest(int B, int M, int N, int runs, float delta);
    void bufferedBTreeTestSpecialQuery(int B, int M, int N, int runs, float delta);

};


#endif //SPECIALE_TREETESTER_H
