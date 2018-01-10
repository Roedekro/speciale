//
// Created by martin on 1/10/18.
// Enables us to build a BufferedBTree in O(sort).
//

#ifndef SPECIALE_BUFFEREDBTREEBUILDER_H
#include <string>
#define SPECIALE_BUFFEREDBTREEBUILDER_H

class BufferedBTreeBuilder {
public:
    int numberOfNodes;
    BufferedBTreeBuilder();
    ~BufferedBTreeBuilder();
    int build(int n, int B, int M, float delta);
    std::string generate(int n, int B, int M);
};


#endif //SPECIALE_BUFFEREDBTREEBUILDER_H
