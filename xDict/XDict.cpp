//
// Created by martin on 1/15/18.
//

#include <fcntl.h>
#include <unistd.h>
#include <sys/mman.h>
#include "XDict.h"

/*
 * Set up an empty xBox consisting of an array of
 * 129 longs, the first being the number of real elements
 * in the xBox and the rest holding keys and values of the
 * elements.
 */
XDict::XDict(float alpha) {

    a = alpha;
    latestSize = 129;
    latestX = 128;

    fileSize = 129;
    fileName = "xDict";

    // Open original file
    fileDescriptor = open(fileName.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0777);
    if (fileDescriptor == -1) {
        perror("Error creating original file");
        exit(EXIT_FAILURE);
    }

    // Extend file to appropriate size
    int result = lseek(fileDescriptor, fileSize-1, SEEK_SET);
    if (result == -1) {
        close(fileDescriptor);
        perror("Error original lseek");
        exit(EXIT_FAILURE);
    }

    // Write to end of file
    result = write(fileDescriptor, 0, 1);
    if (result != 1) {
        close(fileDescriptor);
        perror("Error original write to EOF");
        exit(EXIT_FAILURE);
    }

    // Map file
    map = (long*) mmap(0, fileSize, PROT_READ | PROT_WRITE, MAP_SHARED, fileDescriptor, 0);
    if (map == MAP_FAILED) {
        close(fileDescriptor);
        perror("Error mmap original file");
        exit(EXIT_FAILURE);
    }
}