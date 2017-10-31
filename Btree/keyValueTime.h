//
// Created by martin on 10/30/17.
//

#ifndef SPECIALE_KEYVALUETIME_H
#define SPECIALE_KEYVALUETIME_H

struct KeyValueTime {
    int key;
    int value; // Positive value denotes insert, negative value denotes delete.
    int time;
    KeyValueTime(int k, int v, int t) {
        key = k;
        value = v;
        time = t;
    }
};

#endif //SPECIALE_KEYVALUETIME_H
