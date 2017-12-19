//
// Created by martin on 12/18/17.
//

#ifndef SPECIALE_MODIFIEDBUILDER_H
#define SPECIALE_MODIFIEDBUILDER_H


class ModifiedBuilder {
public:
    int numberOfNodes;
    ModifiedBuilder();
    ~ModifiedBuilder();
    int build(int n, int B, int M);
    std::string generate(int n, int B, int M);
};


#endif //SPECIALE_MODIFIEDBUILDER_H
