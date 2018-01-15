cmake_minimum_required(VERSION 3.8)
project(speciale)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES main.cpp Streams/InputStream.h Streams/OutputStream.h Streams/BufferedInputStream.cpp Streams/BufferedInputStream.h Streams/BufferedOutputStream.cpp Streams/BufferedOutputStream.h Streams/NaiveInputStream.cpp Streams/NaiveInputStream.h Streams/NaiveOutputStream.cpp Streams/NaiveOutputStream.h Streams/SystemInputStream.cpp Streams/SystemInputStream.h Streams/SystemOutputStream.cpp Streams/SystemOutputStream.h Streams/MapInputStream.cpp Streams/MapInputStream.h Streams/MapOutputStream.cpp Streams/MapOutputStream.h Btree/Btree.cpp Btree/Btree.h Btree/Old.cpp Btree/Old.h Btree/ModifiedBtree.cpp Btree/ModifiedBtree.h Btree/ModifiedInternalNode.cpp Btree/ModifiedInternalNode.h Btree/KeyValue.h BufferedBTree/BufferedBTree.cpp BufferedBTree/BufferedBTree.h BufferedBTree/keyValueTime.h BufferedBTree/OldBufferedInternalNode.cpp BufferedBTree/OldBufferedInternalNode.h BufferedBTree/InternalBufferedBTree.cpp BufferedBTree/InternalBufferedBTree.h BufferedBTree/BufferedInternalNode.cpp BufferedBTree/BufferedInternalNode.h TruncatedBufferTree/ExternalBufferedBTree.cpp TruncatedBufferTree/ExternalBufferedBTree.h Tests/DevelopmentTester.cpp Tests/DevelopmentTester.h TruncatedBufferTree/TruncatedBufferTree.cpp TruncatedBufferTree/TruncatedBufferTree.h Tests/TreeTester.cpp Tests/TreeTester.h Btree/ModifiedBuilder.cpp Btree/ModifiedBuilder.h Btree/Heap.cpp Btree/Heap.h BufferedBTree/BufferedBTreeBuilder.cpp BufferedBTree/BufferedBTreeBuilder.h xDict/InternalXBox.cpp xDict/InternalXBox.h xDict/NaiveInternalXBox.cpp xDict/NaiveInternalXBox.h xDict/XDict.cpp xDict/XDict.h)

SET(CMAKE_FIND_LIBRARY_SUFFIXES ".a")
SET(BUILD_SHARED_LIBRARIES OFF)
SET(CMAKE_EXE_LINKER_FLAGS "-static")

add_executable(speciale ${SOURCE_FILES})