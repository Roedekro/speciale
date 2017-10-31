cmake_minimum_required(VERSION 3.8)
project(speciale)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES main.cpp Streams/InputStream.h Streams/OutputStream.h Streams/BufferedInputStream.cpp Streams/BufferedInputStream.h Streams/BufferedOutputStream.cpp Streams/BufferedOutputStream.h Streams/NaiveInputStream.cpp Streams/NaiveInputStream.h Streams/NaiveOutputStream.cpp Streams/NaiveOutputStream.h Streams/SystemInputStream.cpp Streams/SystemInputStream.h Streams/SystemOutputStream.cpp Streams/SystemOutputStream.h Streams/MapInputStream.cpp Streams/MapInputStream.h Streams/MapOutputStream.cpp Streams/MapOutputStream.h Btree/Btree.cpp Btree/Btree.h Btree/Old.cpp Btree/Old.h Btree/ModifiedBtree.cpp Btree/ModifiedBtree.h Btree/ModifiedInternalNode.cpp Btree/ModifiedInternalNode.h Btree/KeyValue.h Btree/BufferedBTree.cpp Btree/BufferedBTree.h Btree/keyValueTime.h Btree/OldBufferedInternalNode.cpp Btree/OldBufferedInternalNode.h Btree/InternalBufferedBTree.cpp Btree/InternalBufferedBTree.h Btree/BufferedInternalNode.cpp Btree/BufferedInternalNode.h)
add_executable(speciale ${SOURCE_FILES})