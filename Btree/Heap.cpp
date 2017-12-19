//
// Created by martin on 12/18/17.
//

#include "Heap.h"
#include <cstdlib>

using namespace std;

Heap::Heap() {

}

Heap::~Heap() {
    // TODO Auto-generated destructor stub
}

KeyValue* Heap::SWOPHeap(KeyValue** a, int n, KeyValue* in) {
    KeyValue* out = NULL;
    if(n < 1) out = in;
    else if (in->key <= a[1]->key) {
        out = in;}
    else {
        int i = 1;
        a[n+1] = in;
        out = a[1];
        bool b = true;
        int j;
        KeyValue* temp;
        KeyValue* temp1;
        while(b) {
            j = i+i;
            if(j <= n) {
                temp = a[j];
                temp1 = a[j+1];
                if(temp1->key < temp->key) {
                    temp = temp1;
                    j++;
                }
                if(temp->key < in->key) {
                    a[i] = temp;
                    i = j;
                }
                else {
                    b = false;
                }
            }
            else {b=false;}
            a[i] = in;
        }
    }
    return out;
}

void Heap::inheap(KeyValue** a, int n, KeyValue* in) {
    n++;
    int i = n;
    int j;
    bool b = true;
    while(b) {
        if (i > 1) {
            j = i / 2;
            if(in->key < a[j]->key) {
                a[i] = a[j];
                i = j;
            }
            else {
                b = false;
            }
        }
        else {b=false;}
        a[i] = in;
    }
}

KeyValue* Heap::outheap(KeyValue** a, int n) {
    KeyValue* in = a[n];
    n--;
    KeyValue* out = SWOPHeap(a, n, in);
    return out;
}

void Heap::setheap(KeyValue** a, int n) {
    int j = 1;
    bool b = true;
    if(j >= n) b = false;
    while(b) {
        inheap(a,j,a[j+1]);
        j++;
        if(j >= n) b = false;
        //if(j >= n) b = false;
    }
}