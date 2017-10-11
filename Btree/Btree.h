//
// Created by martin on 10/11/17.
//

#ifndef SPECIALE_BTREE_H
#define SPECIALE_BTREE_H


class Btree {
public:
    int B;
    int M;
    int size;
    int root;
    int numberOfNodes;
    long iocounter;
    Btree(int B, int M);
    virtual ~Btree();
    void insert(int element);
    void insertInternal(int node, int child1, int child2, int max1, int max2);
    int query(int element);
    void deleteElement(int element);
};


#endif //SPECIALE_BTREE_H
