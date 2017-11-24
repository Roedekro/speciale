//
// Created by martin on 10/27/17.
//

#ifndef SPECIALE_KEYVALUE_H

#define SPECIALE_KEYVALUE_H

struct KeyValue {
    int key;
    int value;
    KeyValue(int k, int v) {
        key = k;
        value = v;
    }
    KeyValue(){}
    ~KeyValue(){}
};

#endif //SPECIALE_KEYVALUE_H
