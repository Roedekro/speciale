cmake_minimum_required(VERSION 3.8)
project(speciale)

set(CMAKE_CXX_STANDARD 11)

set(SOURCE_FILES main.cpp Streams/InputStream.h Streams/OutputStream.h Streams/BufferedInputStream.cpp Streams/BufferedInputStream.h Streams/BufferedOutputStream.cpp Streams/BufferedOutputStream.h Streams/NaiveInputStream.cpp Streams/NaiveInputStream.h Streams/NaiveOutputStream.cpp Streams/NaiveOutputStream.h Streams/SystemInputStream.cpp Streams/SystemInputStream.h Streams/SystemOutputStream.cpp Streams/SystemOutputStream.h Streams/MapInputStream.cpp Streams/MapInputStream.h Streams/MapOutputStream.cpp Streams/MapOutputStream.h)
add_executable(speciale ${SOURCE_FILES})