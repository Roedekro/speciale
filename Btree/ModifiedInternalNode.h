//
// Created by martin on 10/26/17.
//

#ifndef SPECIALE_MODIFIEDINTERNALNODE_H
#define SPECIALE_MODIFIEDINTERNALNODE_H


class ModifiedInternalNode {
public:
    int id;
    bool externalChildren;
    int nodeSize; // # of keys
    int height;
    int* keys;
    int* values;
    ModifiedInternalNode** children;
    ModifiedInternalNode(int id, int height, int size, bool externalChildren);
    ~ModifiedInternalNode();
};


#endif //SPECIALE_MODIFIEDINTERNALNODE_H
