//
// Created by martin on 10/11/17.
//

#ifndef SPECIALE_BTREE_H
struct keyValue {
    int key;
    int value;
};
#define SPECIALE_BTREE_H


class Btree {
public:
    int B;
    int size;
    int root;
    int numberOfNodes;
    long iocounter;
    Btree(int B);
    virtual ~Btree();
    void insert(keyValue element);
    void insertIntoNonFull(keyValue element, int id, int height, int nodeSize, int* keys, int* values);
    void splitChild(int height, int nodeSize, int* keys, int* values, int childNumber,
                    int* cKeys, int* cValues, int* newKeys, int* newValues);
    int query(int element);
    keyValue deleteElement(int element);
};


#endif //SPECIALE_BTREE_H
