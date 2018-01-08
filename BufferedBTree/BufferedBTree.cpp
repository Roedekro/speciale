//
// Created by martin on 10/30/17.
// Buffered BTree with the top of the tree stored in
// internal memory. The buffer size is O(B), and
// a fanout of O(delta/log(N)) where delta
// log(N) < delta <= B log(N).
// Delta can then be used as a tradeoff between
// insert and query time.
//
// NOTE: Each node will use max 2*B space + Buffer of B.
//
// Note: Tree should update value upon insert of key
// already present in the tree. This feature is removed
// to enable insertion of more elements.
//

#include "BufferedBTree.h"
#include "../Streams/OutputStream.h"
#include "../Streams/BufferedOutputStream.h"
#include "../Streams/InputStream.h"
#include "../Streams/BufferedInputStream.h"
#include <sstream>
#include <iostream>
#include <unistd.h>
#include <cmath>
#include <algorithm>

using namespace std;

/***********************************************************************************************************************
 * General Methods
 **********************************************************************************************************************/

/*
 * Create a B-tree by creating an empty root.
 */
BufferedBTree::BufferedBTree(int B, int M, int N, float delta) {
    // Create root
    this->B = B;
    this->M = M;

    // Fanout must be Theta(delta/log N)
    size = delta / log2(N);
    // Each node takes up max 3B space, 2B for keys/values and B for the buffer.
    size = (B/sizeof(int))/4; // #values. Max #keys will be 4*size-1. Min = size.

    maxBufferSize = (int) B/sizeof(KeyValueTime);
    numberOfNodes = 1;
    currentNumberOfNodes = 1;
    root = new BufferedInternalNode(numberOfNodes,1,size,true,maxBufferSize);
    internalNodeCounter=1;
    externalNodeHeight = -1; // Height the external nodes begin <--------------------------------------------------------------------------!!!!!!!!!!!!!!
    // How many nodes can we store in internal Memory?
    // Three ints, an array of keys, and an array of pointers, pr. Node. + a buffer.
    int internalNodeSize = ((size*4-1)*sizeof(int)) + (size*4*sizeof(BufferedInternalNode*)) + B; // Keys, children, buffer
    maxInternalNodes = M/internalNodeSize;
    int internalHeight = (int) (log(maxInternalNodes) / log(4*size));
    if(internalHeight - (log(maxInternalNodes) / log(4*size)) != 0) {
        //
        internalHeight++;
        //cout << "Increased height\n";
    }
    minInternalNodes = (int) pow(4*size,internalHeight-1);
    iocounter = 0;
    cout << "Created root " << root->id << "\n";
    cout << "Tree will have size = " << size << " resulting in #keys = " << 4*size-1 << " with int size = " << sizeof(int) << " and max buffer size "
            << maxBufferSize << "\n";
    cout << "Max internal nodes = " << maxInternalNodes << " with internal node size = " << internalNodeSize << "\n";
    cout << "Internal height is " << internalHeight << " and min internal nodes is " << minInternalNodes << "\n";

    //Create empty leaf as child of root
    numberOfNodes++;
    root->values->push_back(numberOfNodes);
    root->leafInfo->push_back(0);
    // Create empty file
    BufferedOutputStream out(B);
    string name = "Leaf";
    name += to_string(numberOfNodes);
    out.create(name.c_str());
    out.close();

}

BufferedBTree::~BufferedBTree() {

}

void BufferedBTree::insert(KeyValueTime element) {

    // Node is internal
    root->buffer->push_back(element);

    // Check size
    if(root->buffer->size() == maxBufferSize) {

        // Sort buffer - No, done in flush
        //sort(root->buffer->begin(), root->buffer->end());

        // Flush buffer
        flushInternalNode(root);

        // Check root size
        if(root->keys->size() > 4*size-1) {
            cout << "Creating new root " << numberOfNodes+1 << "\n";
            // Create new root
            numberOfNodes++;
            internalNodeCounter++;
            currentNumberOfNodes++;
            BufferedInternalNode* newRoot = new BufferedInternalNode(numberOfNodes,root->height+1,size,
                                                                     false,maxBufferSize);
            newRoot->children->push_back(root);
            root = newRoot;
            // Split old root
            splitInternalNodeInternalChildren(root,0);

            cout << "New root has children " << root->children->at(0)->id << " " << root->children->at(1)->id << "\n";
            cout << "New children have size " << root->children->at(0)->nodeSize << " " << root->children->at(1)->nodeSize << "\n";
        }

        // Check if we have exceeded maximum amount of internal children
        //externalize();
        if(internalNodeCounter > maxInternalNodes) {
            cout << "EXTERNALIZE\n";
            externalize();
        }
    }
}

int BufferedBTree::query(int element) {

    // Scan each buffer on the path down
    int result = 0;
    // Internal
    BufferedInternalNode* internalNode = root;
    while(true) {
        // Scan buffer
        for(int i = 0; i < internalNode->buffer->size(); i ++) {
            if(internalNode->buffer->at(i).key == element) {
                result = internalNode->buffer->at(i).value;
                break; // Break for loop
            }
        }
        // Check result
        if(result != 0) {
            return result;
        }
        if(!internalNode->externalChildren) {
            // Follow element down according to keys
            int index = internalNode->nodeSize;
            while(index > 0 && element <= internalNode->keys->at(index-1)) {
                index--;
            }
            internalNode = internalNode->children->at(index);
        }
        else {
            // Shift to external
            break;
        }
    }

    // We reached external node or external leaf
    if(internalNode->height == 1) {
        // Find correct left
        int index = internalNode->nodeSize;
        while(index > 0 && element <= internalNode->keys->at(index-1)) {
            index--;
        }
        int leafID= internalNode->values->at(index);

        //Scan leaf
        vector<KeyValueTime>* leaf = new vector<KeyValueTime>();
        readLeaf(leafID,leaf,internalNode->leafInfo->at(index));
        for(int i  = 0; i < leaf->size(); i++) {
            if(leaf->at(i).key == element) {
                result = leaf->at(i).value;
                break;
            }
        }
        leaf->clear();
        delete(leaf);
        return result;
    }
    else {
        // Continue search externally

        // First scan the values
        int index = internalNode->nodeSize;
        while(index > 0 && element <= internalNode->keys->at(index-1)) {
            index--;
        }
        int currentNode = internalNode->values->at(index);
        int height = 1000;
        int nodeSize, bufferSize;
        int* ptr_height = &height;
        int* ptr_nodeSize = &nodeSize;
        int* ptr_bufferSize = &bufferSize;
        vector<int>* keys = new vector<int>();
        keys->reserve(4*size);
        vector<int>* values = new vector<int>();
        values->reserve(4*size);

        //readNode(currentNode,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

        // Search down to the bottom layer
        // Switched to correct do while
        int parent = internalNode->id;
        do {
            readNode(currentNode,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

            // TODO Scan buffer


            index = nodeSize;
            while(index > 0 && element <= keys->at(index-1)) {
                index--;
            }
            parent = currentNode;
            currentNode = values->at(index);
        }
        while(height != 1);

        /*while(height != 1) {
            index = nodeSize;
            while(index > 0 && element <= keys->at(index-1)) {
                index--;
            }
            currentNode = values->at(index);
            readNode(currentNode,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);
        }

        index = nodeSize;
        while(index > 0 && element <= keys->at(index-1)) {
            index--;
        }*/

        // Clean up parent
        delete(keys);
        delete(values);

        // We are now at the leaf level, read in leaf size
        vector<int>* leafSizes = new vector<int>();
        leafSizes->reserve(4*size);
        readLeafInfo(parent,leafSizes);
        int leafSize = leafSizes->at(index);
        delete(leafSizes);

        // Read in leaf
        vector<KeyValueTime>* leaf = new vector<KeyValueTime>();
        leaf->reserve(4*size);
        readLeaf(currentNode,leaf,leafSize);

        // Scan leaf
        for(int i = 0; i < leafSize; i++) {
            KeyValueTime kvt = leaf->at(i);
            if(kvt.key == element) {
                result = kvt.value;
                break;
            }
        }

        // Clean up
        delete(leaf);

        return result;
    }

    return -1;
}




/***********************************************************************************************************************
 * Tree Structural Methods
 **********************************************************************************************************************/

/*
 * Flushes the buffer of the internal node down into
 * children/leafs and rebalances the tree below the node.
 * Returns the resulting size of the node.
 */
int BufferedBTree::flushInternalNode(BufferedInternalNode *node) {

    cout << "Flush internal node " << node->id << " size " << node->nodeSize << "\n";
    cout << "Buffer size " << node->buffer->size() << "\n";

    // Sort buffer
    sort(node->buffer->begin(), node->buffer->end());

    // Special case if we only have one child, such as when we insert the
    // first elements into the tree
    int index = 0;
    if(node->nodeSize != 0) {
        // Find the child whom we can push the most elements to
        cout << "#keys = " << node->keys->size() << "\n";
        //cout << node->children->at(1)->id << "\n";
        int indexKey = node->keys->at(0);
        int indexCounter = 0; // Counter for size of current index
        int largestIndex = 0; // Largest index so far
        int largest = 0; // Size of largest index so far
        int current;
        bool last = false; // Special case, last child doesnt have a key
        for(int i = 0; i < node->buffer->size(); i++) {
            current = node->buffer->at(i).key;
            if(last) {
                indexCounter++;
            }
            else if(current <= indexKey) {
                indexCounter++;
            }
            else {
                // Evaluate index
                if(indexCounter > largest) {
                    largestIndex = index;
                }
                index++;
                // Special case if we reached last child
                if(index == node->nodeSize) {
                    last = true;
                }
                else {
                    indexKey = node->keys->at(index);
                }
                indexCounter = 0;
                i--; // Because we might not be part of the range of the next child
            }
        }
        // Evaluate last index
        if(indexCounter > largest) {
            largestIndex = index;
        }
    }

    // Create new vector with elements to child
    vector<KeyValueTime>* toChild = new vector<KeyValueTime>();
    if(node->nodeSize != 0) {
        if(index != node->nodeSize && index != 0) {
            // If we are not pushing to the first or last child
            int firstKey = node->keys->at(index-1); // Elements must be larger than this key
            int secondKey = node->keys->at(index); // Elements must be smaller or equal to this key
            int startOfElements=-1, endOfElements=-1;
            // Add elements to toChild
            for(int i = 0; i < node->buffer->size(); i++) {
                KeyValueTime kvt = node->buffer->at(i);
                // Do we add it to toChild?
                if(firstKey < kvt.key && kvt.key <= secondKey) {
                    toChild->push_back(kvt);
                }
                // Should we set startOfElements?
                if(startOfElements == -1 && kvt.key < firstKey) {
                    startOfElements = i;
                }
                // Are we done?
                if(kvt.key <= secondKey) {
                    endOfElements = i;
                }
                else {
                    break; // No more elements <= secondKey
                }
            }
            // Remove elements from buffer
            // We know the start and end from above
            node->buffer->erase(node->buffer->begin()+startOfElements, node->buffer->begin() + endOfElements + 1);
        }
        else if(index == 0) {
            // Special case for the first child
            int firstKey = node->keys->at(index); // Elements smaller or equal to this key
            int endOfElements=-1;
            // Add elements toChild
            for(int i = 0; i < node->buffer->size(); i++) {
                KeyValueTime kvt = node->buffer->at(i);
                // Do we add it to toChild?
                if(kvt.key <= firstKey) {
                    toChild->push_back(kvt);
                }
                else {
                    endOfElements = i-1;
                    break; // No more elements <= secondKey
                }
            }
            // endOfElements was not set if the last element in the buffer is added toChild
            if(endOfElements == -1) {
                endOfElements = (int) node->buffer->size();
            }
            // Remove elements from buffer
            // We know the start and end from above
            node->buffer->erase(node->buffer->begin(), node->buffer->begin() + endOfElements + 1);
        }
        else {
            // Special case for last child
            int firstKey = node->keys->at(index-1); // Elements must be larger than this key
            int startOfelements=-1;
            // Add elements to toChild
            for(int i = 0; i < node->buffer->size(); i++) {
                KeyValueTime kvt = node->buffer->at(i);
                // Do we add it to toChild?
                if(kvt.key > firstKey) {
                    toChild->push_back(kvt);
                    if(startOfelements == -1) {
                        startOfelements = i;
                    }
                }
            }
            // Remove elements from buffer
            // We know the start and end from above
            node->buffer->erase(node->buffer->begin()+startOfelements, node->buffer->end());
        }
    }
    else {
        // Very special case when we have but one child
        for(int i = 0; i < node->buffer->size(); i++) {
            KeyValueTime kvt = node->buffer->at(i);
            toChild->push_back(kvt);
        }
        node->buffer->erase(node->buffer->begin(), node->buffer->end());
    }

    // 3 cases for children: External leafs, internal children, external children.
    if(node->height == 1) {
        // Handle leafs
        cout << "#values = " << node->values->size() << "\n";
        int child = node->values->at(index);
        // Read in leaf
        vector<KeyValueTime>* leaf = new vector<KeyValueTime>();
        readLeaf(child,leaf,node->leafInfo->at(index));

        cout << "Original leaf size " << leaf->size() << "\n";

        // Merge with buffer
        mergeVectors(leaf,toChild); // Places result in leaf

        int leafSize = (int) leaf->size();

        cout << "After insertion leaf will have size " << leafSize << "\n";

        // Update parent node
        (*node->leafInfo)[index] = leafSize;

        // Write out new leaf
        writeLeaf(child,leaf);

        // Reset our buffer
        //node->buffer->clear(); // Preserves capacity
        delete(toChild);

        while(leafSize >= 4*size-1) {
            // Call appropriate split
            leafSize = splitLeafInternalParent(node,index);
        }
    }
    else if(node->externalChildren) {
        // Handle flush to external children
        cout << "Flush to external children\n";

        int child = node->values->at(index);
        // Push to largest child
        cout << "Appending " << toChild->size() << " elements to external child B" << child << "\n";
        int bSize = appendBuffer(child,toChild);

        // Reset our buffer
        //node->buffer->clear(); // Preserves capacity
        delete(toChild);

        // Read in and write out childs updated node info
        int cHeight, cSize, cBufferSize;
        int* ptr_cHeight = &cHeight;
        int* ptr_cSize = &cSize;
        int* ptr_cBufferSize = &cBufferSize;
        readNodeInfo(child,ptr_cHeight,ptr_cSize,ptr_cBufferSize);
        cout << "Old buffer size was " << cBufferSize << " new buffer size " << bSize << "\n";
        writeNodeInfo(child,cHeight,cSize,bSize);

        // Check if we need to flush
        if(bSize >= maxBufferSize) {
            cSize = flushExternalNode(child);
            if(cSize >= 4*size-1) {
                // Read in child
                vector<int>* cKeys = new vector<int>();
                cKeys->reserve(4*size);
                vector<int>* cValues = new vector<int>();
                cValues->reserve(4*size);
                readNode(child,ptr_cHeight,ptr_cSize,ptr_cBufferSize,cKeys,cValues);

                while(cSize >= 4*size-1) {
                    cSize = splitInternalNodeExternalChildren(node,index,cHeight,cSize,ptr_cBufferSize,cKeys,cValues);
                }

                // Write out child

                cout << "Internal Node external Child Writing out node " << child << " of nodeSize " << cSize <<
                     " and " << cKeys->size() << " " << cValues->size() << "\n";

                writeNode(child,cHeight,cSize,cBufferSize,cKeys,cValues);
            }
        }
    }
    else {
        // Handle flush to internal children

        // Push to largest child
        BufferedInternalNode* child = node->children->at(index);
        cout << "Internal child: Pushing elements from " << node->id << " to " << child->id << "\n";
        for(int i = 0; i < toChild->size(); i++) {
            // Can not push directly
            KeyValueTime kvt = toChild->at(i);
            //cout << "Pusing element " << kvt.key << " to node " << child->id << "\n";
            child->buffer->push_back(kvt);
        }

        // Reset our buffer
        //node->buffer->clear(); // Preserves capacity
        delete(toChild);

        // Check if we need to flush child
        if(child->buffer->size() >= maxBufferSize) {
            flushInternalNode(child);
        }

        // Rebalance child if necessary
        while(child->nodeSize >= 4*size-1) {
            // Call appropriate split
            splitInternalNodeInternalChildren(node,index);
        }
    }
    return node->nodeSize;
}

/*
 * Flushes the buffer of the external node down into
 * children/leafs and rebalances the tree below the node.
 * Returns the resulting size of the node.
 */
int BufferedBTree::flushExternalNode(int node) {

    cout << "Flush external node " << node  << "\n";

    // Read in node
    int height, nodeSize, bufferSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* ptr_bufferSize = &bufferSize;
    vector<int>* keys = new vector<int>();
    keys->reserve(4*size);
    vector<int>* values = new vector<int>();
    values->reserve(4*size);
    readNode(node,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

    cout << "Buffer size " << bufferSize << "\n";

    // Read in buffer
    vector<KeyValueTime>* buffer = new vector<KeyValueTime>();
    buffer->reserve(maxBufferSize);
    readBuffer(node,bufferSize,buffer);

    // Sort buffer
    sort(buffer->begin(),buffer->end());

    // Find child we can push most to
    int index = 0;
    int indexKey = keys->at(0);
    int indexCounter = 0; // Counter for size of current index
    int largestIndex = 0; // Largest index so far
    int largest = 0; // Size of largest index so far
    int current;
    bool last = false; // Special case, last child doesnt have a key
    for(int i = 0; i < buffer->size(); i++) {
        current = buffer->at(i).key;
        if(last) {
            indexCounter++;
        }
        else if(current <= indexKey) {
            indexCounter++;
        }
        else {
            // Evaluate index
            if(indexCounter > largest) {
                largestIndex = index;
            }
            index++;
            // Special case if we reached last child
            if(index == nodeSize) {
                last = true;
            }
            else {
                indexKey = keys->at(index);
            }
            indexCounter = 0;
            i--; // Because we might not be part of the range of the next child
        }
    }
    // Evaluate last index
    if(indexCounter > largest) {
        largestIndex = index;
    }

    // Create new vector with elements to child
    vector<KeyValueTime>* toChild = new vector<KeyValueTime>();
    if(index != nodeSize && index != 0) {
        // If we are not pushing to the first or last child
        int firstKey = keys->at(index-1); // Elements must be larger than this key
        int secondKey = keys->at(index); // Elements must be smaller or equal to this key
        int startOfElements=-1, endOfElements=-1;
        // Add elements to toChild
        for(int i = 0; i < buffer->size(); i++) {
            KeyValueTime kvt = buffer->at(i);
            // Do we add it to toChild?
            if(firstKey < kvt.key && kvt.key <= secondKey) {
                toChild->push_back(kvt);
            }
            // Should we set startOfElements?
            if(startOfElements == -1 && kvt.key < firstKey) {
                startOfElements = i;
            }
            // Are we done?
            if(kvt.key <= secondKey) {
                endOfElements = i;
            }
            else {
                break; // No more elements <= secondKey
            }
        }
        // Remove elements from buffer
        // We know the start and end from above
        cout << "case1 " << startOfElements << " " << endOfElements << "\n";
        //buffer->erase(buffer->begin()+startOfElements, buffer->begin() + endOfElements + 1);
        buffer->erase(buffer->begin()+startOfElements, buffer->begin() + endOfElements);
    }
    else if(index == 0) {
        // Special case for the first child
        int firstKey = keys->at(index); // Elements smaller or equal to this key
        int endOfElements=-1;
        // Add elements toChild
        for(int i = 0; i < buffer->size(); i++) {
            KeyValueTime kvt = buffer->at(i);
            // Do we add it to toChild?
            if(kvt.key <= firstKey) {
                toChild->push_back(kvt);
            }
            else {
                endOfElements = i-1;
                break; // No more elements <= secondKey
            }
        }
        // endOfElements was not set if the last element in the buffer is added toChild
        if(endOfElements == -1) {
            endOfElements = (int) buffer->size();
        }
        // Remove elements from buffer
        // We know the start and end from above
        cout << "case2 " << endOfElements << "\n";
        //buffer->erase(buffer->begin(), buffer->begin() + endOfElements + 1);
        buffer->erase(buffer->begin(), buffer->begin() + endOfElements);
    }
    else {
        // Special case for last child
        int firstKey = keys->at(index-1); // Elements must be larger than this key
        int startOfelements=-1;
        // Add elements to toChild
        for(int i = 0; i < buffer->size(); i++) {
            KeyValueTime kvt = buffer->at(i);
            // Do we add it to toChild?
            if(kvt.key > firstKey) {
                toChild->push_back(kvt);
                if(startOfelements == -1) {
                    startOfelements = i;
                }
            }
        }
        // Remove elements from buffer
        // We know the start and end from above
        cout << "case3\n";
        buffer->erase(buffer->begin()+startOfelements, buffer->end());
    }

    int childID = values->at(index);

    cout << buffer->size() << " " << toChild->size() << "\n";

    // Child is either a node or a leaf
    if(height == 1) {
        // Leaf

        cout << "Pushing " << toChild->size() << " elements to leaf " << childID << "\n";

        // Get leaf info
        vector<int>* leafSizes = new vector<int>();
        leafSizes->reserve(4*size);
        readLeafInfo(node,leafSizes);

        int leafSize = leafSizes->at(index);

        // Read in leaf
        vector<KeyValueTime>* leaf = new vector<KeyValueTime>();
        leaf->reserve(4*size);
        readLeaf(childID,leaf,leafSize);

        mergeVectors(leaf,toChild); // Places elements sorted in leaf
        delete(toChild);
        int newLeafSize = leaf->size();
        (*leafSizes)[index] = newLeafSize;

        while(newLeafSize > 4*size) {
            cout << "Splitleaf size " << newLeafSize << "\n";
            newLeafSize = splitLeafExternalParent(nodeSize,keys,values,index,leafSizes, leaf);
            nodeSize++;
        }

        writeLeaf(childID,leaf); // Deletes leaf
        writeLeafInfo(node,leafSizes); // Deletes leafSizes
        writeNode(node,height,nodeSize,buffer->size(),keys,values);

        if(buffer->size() != 0) {
            writeBuffer(node,buffer);
        }
        else {
            delete(buffer);
            string name = "Buf";
            name += to_string(node);
            if(FILE *file = fopen(name.c_str(), "r")) {
                fclose(file);
                remove(name.c_str());
            }
        }
    }
    else {
        cout << "Pushing " << toChild->size() << " elements to external child B" << childID << "\n";

        // Push to child
        int newChildBufferSize = appendBuffer(childID,toChild);

        // Update childs buffer size
        int cHeight, cSize, cBufferSize;
        int* ptr_cHeight = &cHeight;
        int* ptr_cSize = &cSize;
        int* ptr_cBufferSize = &cBufferSize;
        cout << "Reading child info\n";
        readNodeInfo(childID,ptr_cHeight,ptr_cSize,ptr_cBufferSize);
        cout << "Writing child info\n";
        writeNodeInfo(childID,cHeight,cSize,newChildBufferSize);

        // Clean up parent for now
        writeNodeInfo(node,height,nodeSize,(int)buffer->size()); // Write out new buffer size
        if(bufferSize != 0) {
            writeBuffer(node,buffer); // Write out buffer again
        }
        else {
            delete(buffer);
            string name = "Buf";
            name += to_string(node);
            if(FILE *file = fopen(name.c_str(), "r")) {
                fclose(file);
                remove(name.c_str());
            }
        }
        delete(keys); // Clean up so we dont recursively build up keys down the height of the tree.
        delete(values); // As above

        // Recurse
        if(newChildBufferSize >= maxBufferSize) {
            flushExternalNode(childID);
        }

        // Recheck child and possibly increment nodeSize
        readNodeInfo(childID,ptr_cHeight,ptr_cSize,ptr_cBufferSize);
        if(cSize > 4*size-1) {
            // Read in parent
            keys = new vector<int>();
            values = new vector<int>();
            readNode(node,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

            // Read in child
            vector<int>* cKeys = new vector<int>();
            cKeys->reserve(4*size);
            vector<int>* cValues = new vector<int>();
            cValues->reserve(4*size);
            readNode(childID,ptr_cHeight,ptr_cSize,ptr_cBufferSize,cKeys,cValues);

            // Split until child has appropriate size
            // New children will get appropriate size by default
            while(cSize > 4*size-1) {
                cSize = splitExternalNodeExternalChildren(nodeSize, keys, values, index, cHeight, cSize, ptr_cBufferSize,
                                                          cKeys, cValues);
                nodeSize++;
            }

            // Clean up child
            writeNode(childID,cHeight,cSize,cBufferSize,cKeys,cValues);

            // Clean up parent
            writeNode(node,height,nodeSize,bufferSize,keys,values);
        }
    }

    // Return nodes possibly updated size
    return nodeSize;
}

/*
 * Updates parent and children completely.
 * Creates new sibling of appropriate size,
 * recheck original child afterwards with
 * the return value.
 */
int BufferedBTree::splitInternalNodeInternalChildren(BufferedInternalNode *node, int childNumber) {

    cout << "Split internal node internal children, node " << node->id << " childNumber " << childNumber << "\n";

    BufferedInternalNode* child = node->children->at(childNumber);
    int childSize = child->nodeSize;
    /*int newSize = (childSize-1)/2;
    int cSize = childSize/2;*/
    int newSize = 2*size;
    int cSize = childSize-newSize-1; // We throw away the partitioning key.

    numberOfNodes++;
    internalNodeCounter++;
    currentNumberOfNodes++;
    BufferedInternalNode* newChild = new BufferedInternalNode(numberOfNodes,child->height,size,
                                                              child->externalChildren,maxBufferSize);

    // Fill out new childs keys
    for(int j = 0; j < newSize; j++) {
        newChild->keys->push_back((*child->keys)[cSize+1+j]);
    }

    // Move parents keys by one
    node->keys->push_back(0);
    for(int j = node->nodeSize; j > childNumber; j--) {
        (*node->keys)[j] = (*node->keys)[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    (*node->keys)[childNumber] = (*child->keys)[cSize]; // was cSize-1

    // Remove excess keys from child
    for(int j = 0; j < newSize; j++) {
        child->keys->pop_back();
    }

    // Split values
    if(child->externalChildren){
        // Copy values
        for(int j = 0; j < newSize+1; j++) {
            //newValues[j] = cValues[cSize+1+j];
            newChild->values->push_back((*child->values)[cSize+1+j]);
        }
        for(int j = 0; j < newSize+1; j++) {
            child->values->pop_back();
        }
    }
    else {
        for(int j = 0; j < newSize+1; j++) {
            //newValues[j] = cValues[cSize+1+j];
            newChild->children->push_back((*child->children)[cSize+1+j]);
        }
        for(int j = 0; j < newSize+1; j++) {
            child->children->pop_back();
        }
    }

    // split leafInfo
    if(child->height == 1) {
        // Copy leafInfo
        for(int j = 0; j < newSize+1; j++) {
            //newValues[j] = cValues[cSize+1+j];
            newChild->leafInfo->push_back((*child->leafInfo)[cSize+1+j]);
        }
        for(int j = 0; j < newSize+1; j++) {
            child->leafInfo->pop_back();
        }
    }

    // Move parents children back by one
    node->children->push_back(0);
    for(int j = node->nodeSize+1; j > childNumber; j--) {
        (*node->children)[j] = (*node->children)[j-1];
    }
    // Insert pointer to new child
    (*node->children)[childNumber+1] = newChild;

    (node->nodeSize)++;

    // Update childrens nodeSize
    child->nodeSize = cSize;
    newChild->nodeSize = newSize;

    // Handle buffer
    if(child->buffer->size() != 0) {
        // Possibly split elements
        int splitKey = (*node->keys)[childNumber];
        // Buffer is sorted, as a split only occurs as a result of a flush
        int startOfElements = -1;
        for(int i = 0; i < child->buffer->size(); i++) {
            KeyValueTime kvt = child->buffer->at(i);
            if(kvt.key > splitKey) {
                newChild->buffer->push_back(kvt);
                if(startOfElements == -1) {
                    startOfElements = i;
                }
            }
        }
        // Delete elements from childs buffer
        if(startOfElements != -1) {
            child->buffer->erase(child->buffer->begin()+startOfElements,child->buffer->end());
        }
    }

    return cSize;
}

/*
 * Changed: Updates all nodes involved,
 * but only writes out the new child to disk,
 * since the old child might need to be split again.
 * New child will have appropriate size.
 * Updates cBufferSize.
 *
 * Old: Updates parent and children completely
 * Creates a new node of appropriate size,
 * recheck original child afterwards with
 * the return value.
 */
int BufferedBTree::splitInternalNodeExternalChildren(BufferedInternalNode *node, int childIndex,
                                                     int cHeight, int cNodeSize, int* cBuffersize,
                                                     std::vector<int>* cKeys, std::vector<int>* cValues) {

    // Load in child
    int childID = node->values->at(childIndex);
    /*int cHeight, cNodeSize, cBufferSize;
    int* ptr_cHeight = &cHeight;
    int* ptr_cNodeSize = &cNodeSize;
    int* ptr_cBufferSize = &cBufferSize;
    vector<int>* cKeys = new vector<int>();
    cKeys->reserve(4*size);
    vector<int>* cValues = new vector<int>();
    cValues->reserve(4*size);
    readNode(childID,ptr_cHeight,ptr_cNodeSize,ptr_cBufferSize,cKeys,cValues);*/

    // Create new child
    numberOfNodes++;
    int newID = numberOfNodes;
    int nHeight = cHeight;
    int nSize = 2*size;
    int nBuffer = 0;
    vector<int>* nKeys = new vector<int>();
    vector<int>* nValues = new vector<int>();

    // Calculate new child size
    int cSize = cNodeSize - nSize - 1;

    // Fill out new childs keys
    for(int j = 0; j < nSize; j++) {
        nKeys->push_back((*cKeys)[cSize+1+j]);
    }

    // Move parents keys by one
    node->keys->push_back(0);
    for(int j = node->nodeSize; j > childIndex; j--) {
        (*node->keys)[j] = (*node->keys)[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    (*node->keys)[childIndex] = (*cKeys)[cSize];

    // Remove excess keys from child
    for(int j = 0; j < nSize+1; j++) {
        cKeys->pop_back();
    }

    // Split values
    for(int j = 0; j < nSize+1; j++) {
        nValues->push_back((*cValues)[cSize+1+j]);
    }
    for(int j = 0; j < nSize+1; j++) {
        cValues->pop_back();
    }

    // split leafInfo
    if(cHeight == 1) {
        // Read in leaf info
        vector<int>* leafs = new vector<int>();
        readLeafInfo(childID,leafs);

        vector<int>* nLeafs = new vector<int>();

        // Copy leafInfo
        for(int j = 0; j < nSize+1; j++) {
            //newValues[j] = cValues[cSize+1+j];
            nLeafs->push_back((*leafs)[cSize+1+j]);
        }
        for(int j = 0; j < nSize+1; j++) {
            leafs->pop_back();
        }

        // Removes old leafinfo
        writeLeafInfo(childID,leafs);
        writeLeafInfo(newID,nLeafs);
    }

    // Move parents children back by one
    node->values->push_back(0);
    for(int j = node->nodeSize+1; j > childIndex; j--) {
        (*node->values)[j] = (*node->values)[j-1];
    }
    // Insert pointer to new child
    (*node->values)[childIndex+1] = newID;

    // Increase node size
    (node->nodeSize)++;

    // Check buffer
    int newChildsBuffersize = 0;
    if(*cBuffersize != 0) {
        vector<KeyValueTime>* buffer = new vector<KeyValueTime>();
        buffer->reserve(maxBufferSize);
        readBuffer(childID,*cBuffersize,buffer);
        vector<KeyValueTime>* newBuffer = new vector<KeyValueTime>();
        newBuffer->reserve(maxBufferSize);
        int splitKey = (*node->keys)[childIndex];
        int startOfElements = -1;
        for(int i = 0; i < buffer->size(); i++) {
            KeyValueTime kvt = buffer->at(i);
            if(kvt.key > splitKey) {
                newBuffer->push_back(kvt);
                if(startOfElements == -1) {
                    startOfElements = i;
                }
            }
        }
        if(startOfElements != -1) {
            buffer->erase(buffer->begin()+startOfElements,buffer->end());
            *cBuffersize = buffer->size();
            newChildsBuffersize = newBuffer->size();
            // Write out both childrens buffers
            writeBuffer(childID,buffer);
            writeBuffer(newID,newBuffer);
        }
    }

    // Write out external nodes, use cSize
    // Deletes arrays.
    //writeNode(childID,cHeight,cSize,0,cKeys,cValues); // Updated in calling method

    cout << "Writing out node " << newID << " of theoretical size " << nSize <<
         " and actual size " << nKeys->size() << " " << nValues->size() << "\n";

    writeNode(newID,nHeight,nSize,newChildsBuffersize,nKeys,nValues);

    // Notice we changed the value of cBuffersize if relevant above.
    return cSize;

}

/*
 * Updates new child only, not parent and original child.
 * Creates a new node of appropriate size,
 * recheck size of original child with return value.
 */
int BufferedBTree::splitExternalNodeExternalChildren(int nodeSize, std::vector<int> *keys, std::vector<int> *values,
                                                     int childIndex, int cHeight, int cNodeSize, int* cBuffersize,
                                                     std::vector<int> *cKeys, std::vector<int> *cValues) {

    int childID = values->at(childIndex);

    // Create new child
    numberOfNodes++;
    int nChildID = numberOfNodes;
    int nSize, nBufferSize;
    nSize = 2*size;
    nBufferSize = 0;
    int cSize = cNodeSize - nSize - 1;
    vector<int>* nKeys = new vector<int>();
    vector<int>* nValues = new vector<int>();

    // Fill out new childs keys
    for(int j = 0; j < nSize; j++) {
        nKeys->push_back((*cKeys)[cSize+1+j]);
    }

    // Move parents keys by one
    keys->push_back(0);
    for(int j = nodeSize; j > childIndex; j--) {
        (*keys)[j] = (*keys)[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    (*keys)[childIndex] = (*cKeys)[cSize];

    // Remove excess keys from child
    for(int j = 0; j < nSize; j++) {
        cKeys->pop_back();
    }

    // Split values
    for(int j = 0; j < nSize+1; j++) {
        nValues->push_back((*cValues)[cSize+1+j]);
    }
    for(int j = 0; j < nSize+1; j++) {
        cValues->pop_back();
    }

    // split leafInfo
    if(cHeight == 1) {
        // Read in leaf info
        vector<int>* leafs = new vector<int>();
        readLeafInfo(childID,leafs);

        vector<int>* nLeafs = new vector<int>();

        // Copy leafInfo
        for(int j = 0; j < nSize+1; j++) {
            //newValues[j] = cValues[cSize+1+j];
            nLeafs->push_back((*leafs)[cSize+1+j]);
        }
        for(int j = 0; j < nSize+1; j++) {
            leafs->pop_back();
        }

        // Removes old leafinfo
        writeLeafInfo(childID,leafs);
        writeLeafInfo(nChildID,nLeafs);
    }

    // Move parents children back by one
    values->push_back(0);
    for(int j = nodeSize+1; j > childIndex; j--) {
        (*values)[j] = (*values)[j-1];
    }
    // Insert pointer to new child
    (*values)[childIndex+1] = nChildID;

    // Check buffer
    if(*cBuffersize != 0) {
        vector<KeyValueTime>* buffer = new vector<KeyValueTime>();
        buffer->reserve(maxBufferSize);
        readBuffer(childID,*cBuffersize,buffer);
        vector<KeyValueTime>* newBuffer = new vector<KeyValueTime>();
        newBuffer->reserve(maxBufferSize);
        int splitKey = (*keys)[childIndex];
        int startOfElements = -1;
        for(int i = 0; i < buffer->size(); i++) {
            KeyValueTime kvt = buffer->at(i);
            if(kvt.key > splitKey) {
                newBuffer->push_back(kvt);
                if(startOfElements == -1) {
                    startOfElements = i;
                }
            }
        }
        if(startOfElements != -1) {
            buffer->erase(buffer->begin()+startOfElements,buffer->end());
            *cBuffersize = buffer->size();
            nBufferSize = newBuffer->size();
            // Write out both childrens buffers
            writeBuffer(childID,buffer);
            writeBuffer(nChildID,newBuffer);
        }
    }

    // Write out the new node
    writeNode(nChildID,cHeight,nSize,nBufferSize,nKeys,nValues);

    //cout << "Original child will have size " <<

    // Return new size of original child, for possible recursion
    return cSize;
}

/*
 * Splits a leaf into a new leaf of the proper size,
 * recheck original leaf with return value.
 */
int BufferedBTree::splitLeafInternalParent(BufferedInternalNode *node, int leafNumber) {

    cout << "Split leaf internal parent on node " << node->id << "\n";

    int leafID = node->values->at(leafNumber);
    int leafSize = node->leafInfo->at(leafNumber);
    numberOfNodes++;
    int newLeafID = numberOfNodes;
    int newSize = 2*size;
    int cSize = leafSize-newSize;
    vector<KeyValueTime>* leaf = new vector<KeyValueTime>();
    readLeaf(leafID,leaf,node->leafInfo->at(leafNumber));

    /*cout << "Leaf size " << leaf->size() << " and content is\n";
    for(int i = 0; i < leaf->size(); i++) {
        cout << leaf->at(i).key << "\n";
    }*/

    vector<KeyValueTime>* newLeaf = new vector<KeyValueTime>();
    for(int i = 0; i < newSize; i++) {
        KeyValueTime kvt = leaf->at(cSize+i);
        newLeaf->push_back(kvt);
    }
    for(int i = 0; i < newSize; i++) {
        leaf->pop_back();
    }

    // Update parent
    node->leafInfo->push_back(0);
    node->values->push_back(0);
    node->keys->push_back(0);
    for(int i = node->nodeSize+1; i > leafNumber; i--) {
        (*node->leafInfo)[i] = (*node->leafInfo)[i-1];
        (*node->values)[i] = (*node->values)[i-1];
    }
    for(int i = node->nodeSize; i > leafNumber; i--) {
        (*node->keys)[i] = (*node->keys)[i-1];
    }
    (*node->keys)[leafNumber] = leaf->at(cSize-1).key;
    (*node->leafInfo)[leafNumber] = cSize;
    (*node->leafInfo)[leafNumber+1] = newSize;
    (*node->values)[leafNumber+1] = newLeafID;
    (node->nodeSize)++;

    writeLeaf(leafID,leaf);
    writeLeaf(newLeafID, newLeaf);
    return cSize;
}

/*
 * Splits the leaf and writes out new leaf.
 * You must clean up original leaf and parent.
 * Returns the new size of the old leaf.
 */
int BufferedBTree::splitLeafExternalParent(int nodeSize, std::vector<int> *keys, std::vector<int> *values, int leafNumber,
                                           std::vector<int> *leafSizes, std::vector<KeyValueTime> *leaf) {

    int origLeafSize = leafSizes->at(leafNumber);
    numberOfNodes++;
    int newLeafID = numberOfNodes;
    int nSize = 2*size;
    int leafSize = origLeafSize - nSize;
    vector<KeyValueTime>* nLeaf = new vector<KeyValueTime>();
    nLeaf->reserve(2*size);

    for(int i = 0; i < nSize; i++) {
        KeyValueTime kvt = leaf->at(leafSize+i);
        nLeaf->push_back(kvt);
    }
    for(int i = 0; i < nSize; i++) {
        leaf->pop_back();
    }

    // Update parent
    leafSizes->push_back(0);
    values->push_back(0);
    keys->push_back(0);
    for(int i = nodeSize+1; i > leafNumber; i--) {
        (*leafSizes)[i] = (*leafSizes)[i-1];
        (*values)[i] = (*values)[i-1];
    }
    for(int i = nodeSize; i > leafNumber; i--) {
        (*keys)[i] = (*keys)[i-1];
    }
    (*keys)[leafNumber] = leaf->at(leafSize-1).key;
    (*leafSizes)[leafNumber] = leafSize;
    (*leafSizes)[leafNumber+1] = nSize;
    (*values)[leafNumber+1] = newLeafID;

    // Write out new leaf
    writeLeaf(newLeafID,nLeaf);

    return leafSize;
}



/*
 * Checks if we should externalize internal nodes
 */
void BufferedBTree::externalize() {

    if(externalNodeHeight == -1) {
        // Special case, first time we externalize
        externalNodeHeight = 1;
    }
    else {
        externalNodeHeight++;
    }

    // Recursively travel down to the external node height +1
    // and externalize the children of the nodes at this level.
    recursiveExternalize(root);
}

void BufferedBTree::recursiveExternalize(BufferedInternalNode *node) {

    if(node->height != externalNodeHeight+1) {
        // Recurse on children
        for(int i = 0; i < node->nodeSize+1; i++) {
            recursiveExternalize(node->children->at(i));
        }
    }
    else {
        // Externalize children
        node->values = new vector<int>();
        node->values->reserve(size*4);

        int cHeight, cSize;
        int* cKeys = new int[size*2-1];
        int* cValues = new int[size*2];
        BufferedInternalNode* child;
        for(int i = 0; i < node->nodeSize+1; i++) {
            child = node->children->at(i);
            //writeNode(child->id,child->height,child->nodeSize,child->buffer->size(),child->keys,child->values);
            writeNodeNoDelete(child);
            cout << "Externalized node " << child->id << "\n";
            node->values->push_back(0);
            (*node->values)[i] = child->id;
            //writeBufferNoDelete(node->id,node->buffer); // Will be deleted next line // Now handled in writeNodeNoDelete
            delete(child);
            internalNodeCounter--;
        }
        //node->children->clear(); // Deleted above
        delete(node->children);
        node->externalChildren = true;
    }
}


/***********************************************************************************************************************
 * Buffer Handling Methods
 **********************************************************************************************************************/

/*
 * Merges the two vectors into the first.
 * And by merging I mean we sort, because
 * we dont really care about spending
 * linear vs. n log n internally.
 * The vectors have size O(B) anyways.
 */
void BufferedBTree::mergeVectors(std::vector<KeyValueTime> *v1, std::vector<KeyValueTime> *v2) {

    for(int i = 0; i < v2->size(); i++) {
        KeyValueTime kvt = v2->at(i);
        v1->push_back(kvt);
    }
    sort(v1->begin(),v1->end());

}

/*
 * Append an external buffer with the elements in kvts.
 * Return the size of the resulting buffer.
 */
int BufferedBTree::appendBuffer(int node, std::vector<KeyValueTime> *kvts) {

    cout << "Append to node " << node << "\n";

    string name = "Buf";
    name += to_string(node);
    BufferedOutputStream* out = new BufferedOutputStream(B);

    // Buffer might not exist
    /* Doesnt matter, open will create if not present
     * if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        out->open(name.c_str());
    }
    else {
        out->create(name.c_str());
    }*/

    out->open(name.c_str());

    cout << "Appending\n";

    // Append
    for(int i = 0; i < kvts->size(); i++) {
        KeyValueTime kvt = kvts->at(i);
        out->write(&kvt.key);
        out->write(&kvt.value);
        out->write(&kvt.time);
    }
    out->close();
    delete(out);

    // Get original buffer size
    int height, nodeSize, bufferSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* ptr_bufferSize = &bufferSize;
    cout << "Reading\n";
    readNodeInfo(node,ptr_height,ptr_nodeSize,ptr_bufferSize);
    int newBufferSize = kvts->size() + bufferSize;
    cout << "----- Writing " << bufferSize << " " << newBufferSize << "\n";
    writeNodeInfo(node,height,nodeSize,newBufferSize);
    return newBufferSize;

}

/*
 * Reads in a buffer of specified size
 */
void BufferedBTree::readBuffer(int id, int bufferSize, std::vector<KeyValueTime> *buffer) {

    string name = "Buf";
    name += to_string(id);
    InputStream* is = new BufferedInputStream(B);
    is->open(name.c_str());
    int key, value, time;
    for(int i = 0; i < bufferSize; i++) {
        key = is->readNext();
        value = is->readNext();
        time = is->readNext();
        buffer->push_back(KeyValueTime(key,value,time));
    }
    is->close();
    delete(is);

}

/*
 * Writes out a buffer to disk.
 * Deletes the given vector.
 */
void BufferedBTree::writeBuffer(int id, std::vector<KeyValueTime> *kvts) {

    string name = "Buf";
    name += to_string(id);

    // Remove previous buffer if present
    if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }

    // Create new buffer
    OutputStream* os = new BufferedOutputStream(B);
    os->create(name.c_str());
    for(int i = 0; i < kvts->size(); i++) {
        KeyValueTime kvt = kvts->at(i);
        os->write(&(kvt.key));
        os->write(&(kvt.value));
        os->write(&(kvt.time));
    }

    os->close();
    delete(os);
    delete(kvts);
}


/***********************************************************************************************************************
 * Utility Methods
 ***********************************************************************************************************************/

/*
 * Reads in a node.
 */
void BufferedBTree::readNode(int id, int *height, int *nodeSize, int *bufferSize, std::vector<int> *keys,
                             std::vector<int> *values) {

    InputStream* is = new BufferedInputStream(B);
    string name = "B";
    name += to_string(id);
    is->open(name.c_str());
    *height = is->readNext();
    *nodeSize = is->readNext();
    *bufferSize = is->readNext();

    for(int i = 0; i < *nodeSize; i++) {
        keys->push_back(is->readNext());
    }

    for(int i = 0; i < (*nodeSize)+1; i++) {
        values->push_back(is->readNext());
    }

    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);

}


/*
 * Reads only the first three integers of a node
 */
void BufferedBTree::readNodeInfo(int id, int *height, int *nodeSize, int *bufferSize) {

    string name = "B";
    name += to_string(id);
    InputStream* is = new BufferedInputStream(B);
    is->open(name.c_str());
    *height = is->readNext();
    *nodeSize = is->readNext();
    *bufferSize = is->readNext();
    is->close();
    delete(is);
}

/*
 * Deletes old node, writes out new, and cleans up vectors.
 */
void BufferedBTree::writeNode(int id, int height, int nodeSize, int bufferSize, std::vector<int> *keys,
                              std::vector<int> *values) {

    OutputStream* os = new BufferedOutputStream(B);
    string node = "B";
    node += to_string(id);

    // Remove old file
    /* No need, nodeSize constrains when we read in
    if(FILE *file = fopen(node.c_str(), "r")) {
        fclose(file);
        remove(node.c_str());
    }*/

    os->create(node.c_str()); // Overwrites old file if present
    os->write(&height);
    os->write(&nodeSize);
    os->write(&bufferSize);

    for(int k = 0; k < nodeSize; k++) {
        os->write(&(keys->at(k)));
    }

    for(int k = 0; k < nodeSize+1; k++) {
        os->write(&(values->at(k)));
    }

    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);
    delete(keys);
    delete(values);
}

/*
 * Write node info out to disk.
 */
void BufferedBTree::writeNodeInfo(int id, int height, int nodeSize, int bufferSize) {

    OutputStream* os = new BufferedOutputStream(B);
    string node = "B";
    node += to_string(id);
    os->create(node.c_str()); // Overwrites old file if present
    os->write(&height);
    os->write(&nodeSize);
    os->write(&bufferSize);
    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);
}


/*
 * Writes out an internal node to external.
 * Does not delete anything in the node.
 */
void BufferedBTree::writeNodeNoDelete(BufferedInternalNode *node) {

    cout << "WriteNodeNoDelete " << node->id << " " << node->nodeSize << "\n";

    // Write out node
    OutputStream* os = new BufferedOutputStream(B);
    string name = "B";
    name += to_string(node->id);
    os->create(name.c_str());
    os->write(&node->height);
    os->write(&node->nodeSize);
    int* pNodeSize = &node->nodeSize;
    cout << *pNodeSize << "\n";
    int bSize = node->buffer->size();
    os->write(&(bSize));
    for(int k = 0; k < node->nodeSize; k++) {
        os->write(&(*node->keys)[k]);
    }
    for(int k = 0; k < node->nodeSize+1; k++) {
        os->write(&(*node->values)[k]);
    }

    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);

    // Write out buffer
    name = "Buf";
    name += to_string(node->id);
    os = new BufferedOutputStream(B);
    os->create(name.c_str());
    for(int i = 0; i < node->buffer->size(); i++) {
        KeyValueTime kvt = node->buffer->at(i);
        os->write(&kvt.key);
        os->write(&kvt.value);
        os->write(&kvt.time);
    }
    os->close();
    delete(os);

    if(node->height == 1) {
        // Write out leaf info
        name = "Leaf";
        name += to_string(node->id);
        os = new BufferedOutputStream(B);
        os->create(name.c_str());
        for(int i = 0; i < node->leafInfo->size(); i++) {
            os->write(&node->leafInfo->at(i));
        }
        os->close();
        delete(os);
    }


}

/*
 * Pushes the elements of the leaf to the vector
 */
void BufferedBTree::readLeaf(int leaf, std::vector<KeyValueTime> *ret, int leafSize) {

    string name = "Leaf";
    name += to_string(leaf);
    InputStream* in = new BufferedInputStream(B);
    in->open(name.c_str());
    int key,value,time;
    for(int i = 0; i < leafSize; i++) {
        key = in->readNext();
        value = in->readNext();
        time = in->readNext();
        ret->push_back(KeyValueTime(key,value,time));
    }
    /*while(!in->endOfStream()) {
        key = in->readNext();
        value = in->readNext();
        time = in->readNext();
        ret->push_back(KeyValueTime(key,value,time));
    }*/
    in->close();
    delete(in);
}


/*
 * Deletes kvts
 */
void BufferedBTree::writeLeaf(int leaf, std::vector<KeyValueTime> *kvts) {

    string name = "Leaf";
    name += to_string(leaf);
    if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }
    OutputStream* out = new BufferedOutputStream(B);
    out->create(name.c_str());
    for(int i = 0; i < kvts->size(); i++) {
        KeyValueTime kvt = kvts->at(i);
        out->write(&(kvt.key));
        out->write(&(kvt.value));
        out->write(&(kvt.time));
    }
    out->close();
    delete(out);
    kvts->clear();
    delete(kvts);
}

int BufferedBTree::readLeafInfo(int id, std::vector<int> *leafs) {

    InputStream* is = new BufferedInputStream(B);
    string name = "Leaf";
    name += to_string(id);
    is->open(name.c_str());
    int leafSize;
    int counter = 0; // Could just check vector size
    while(!is->endOfStream()) {
        leafSize = is->readNext();
        leafs->push_back(leafSize);
        counter++;
    }
    is->close();
    delete(is);
    return counter;
}

void BufferedBTree::writeLeafInfo(int id, std::vector<int> *leafs) {

    // Check and remove old leaf info
    string name = "Leaf";
    name += to_string(id);
    if(FILE *file = fopen(name.c_str(), "r")) {
        fclose(file);
        remove(name.c_str());
    }
    OutputStream* out = new BufferedOutputStream(B);
    out->create(name.c_str());
    for(int i = 0; i < leafs->size(); i++) {
        int leaf = leafs->at(i);
        out->write(&(leaf));

    }
    out->close();
    delete(out);
    delete(leafs);
}

/*
 * Prints the tree.
 */
void BufferedBTree::printTree(BufferedInternalNode *node) {

    cout << "=== I " << node->id << "\n";
    cout << node->height << "\n";
    cout << node->nodeSize << "\n";
    cout << node->buffer->size() << "\n";
    cout << "---\n";
    for(int i = 0; i < node->nodeSize; i++) {
        cout << (*node->keys)[i] << "\n";
    }
    if(node->height > 1 && node->height != externalNodeHeight+1) {
        cout << "---C\n";
        for(int i = 0; i < node->nodeSize+1; i++) {
            cout << (*node->children)[i]->id << "\n";
        }
        cout << "---B\n";
        for(int i = 0; i < node->buffer->size(); i++) {
            KeyValueTime kvt = node->buffer->at(i);
            cout << kvt.key << " " << kvt.value << " " << kvt.time  << "\n";
        }
        for(int i = 0; i < node->nodeSize+1; i++) {
            printTree((*node->children)[i]);
        }
    }
    else if( node->height == 1 && externalNodeHeight <= 0) {
        cout << "---V1\n";
        for(int i = 0; i < node->nodeSize+1; i++) {
            cout << (*node->values)[i] << "\n";
        }
        cout << "---B\n";
        for(int i = 0; i < node->buffer->size(); i++) {
            KeyValueTime kvt = node->buffer->at(i);
            cout << kvt.key << " " << kvt.value << " " << kvt.time  << "\n";
        }
        for(int i = 0; i < node->nodeSize+1; i++) {
            printLeaf((*node->values)[i],node->leafInfo->at(i));
        }
    }
    else {
        cout << "---V\n";
        for(int i = 0; i < node->nodeSize+1; i++) {
            cout << (*node->values)[i] << "\n";
        }
        cout << "---B\n";
        for(int i = 0; i < node->buffer->size(); i++) {
            KeyValueTime kvt = node->buffer->at(i);
            cout << kvt.key << " " << kvt.value << " " << kvt.time  << "\n";
        }
        for(int i = 0; i < node->nodeSize+1; i++) {
            printExternal((*node->values)[i]);
        }
    }

}

void BufferedBTree::printExternal(int node) {

    int height, nodeSize, bufferSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* ptr_bufferSize = &bufferSize;
    vector<int>* keys = new vector<int>();
    vector<int>* values = new vector<int>();
    readNode(node,ptr_height,ptr_nodeSize,ptr_bufferSize,keys,values);

    cout << "=== E " << node << "\n";
    cout << height << "\n";
    cout << nodeSize << "\n";
    cout << bufferSize << "\n";
    cout << "---\n";
    for(int i = 0; i < nodeSize; i++) {
        cout << (*keys)[i] << "\n";
    }
    cout << "---\n";
    if(height == 1) {
        for(int i = 0; i < nodeSize+1; i++) {
            cout << (*values)[i] << "\n";
        }

        cout << "---\n";
        if(bufferSize != 0){
            vector<KeyValueTime>* buffer = new vector<KeyValueTime>();
            readBuffer(node,bufferSize,buffer);
            for(int i = 0; i < buffer->size(); i++) {
                KeyValueTime kvt = buffer->at(i);
                cout << kvt.key << " " << kvt.value << " " << kvt.time  << "\n";
            }
            delete(buffer);
        }



        vector<int>* leafs = new vector<int>();
        readLeafInfo(node,leafs);
        for(int i = 0; i < nodeSize+1; i++) {
            printLeaf((*values)[i],(*leafs)[i]);
        }
        delete(leafs);
    }
    else {
        for(int i = 0; i < nodeSize+1; i++) {
            cout << (*values)[i] << "\n";
        }
        cout << "---\n";
        if(bufferSize != 0){
            vector<KeyValueTime>* buffer = new vector<KeyValueTime>();
            readBuffer(node,bufferSize,buffer);
            for(int i = 0; i < buffer->size(); i++) {
                KeyValueTime kvt = buffer->at(i);
                cout << kvt.key << " " << kvt.value << " " << kvt.time  << "\n";
            }
            delete(buffer);
        }


        for(int i = 0; i < nodeSize+1; i++) {
            printExternal((*values)[i]);
        }
    }

    delete(keys);
    delete(values);
}

void BufferedBTree::printLeaf(int leafID, int leafSize) {

    string name = "Leaf";
    name += to_string(leafID);
    InputStream* is = new BufferedInputStream(B);
    is->open(name.c_str());
    cout << "=== L " << leafID << "\n";
    cout << leafSize << "\n";
    cout << "---\n";
    for(int i = 0; i < leafSize; i++) {
        cout << is->readNext() << " " << is->readNext() << " " << is->readNext() << "\n";
    }
    /*while(!is->endOfStream()) {
        cout << is->readNext() << " " << is->readNext() << " " << is->readNext() << "\n";
    }*/

}

void BufferedBTree::cleanup() {

    for(int i = 1; i <= numberOfNodes; i++) {
        string node = "B";
        string buffer = "Buf";
        string leaf = "Leaf";
        node += to_string(i);
        buffer += to_string(i);
        leaf += to_string(i);
        if(FILE *file = fopen(node.c_str(), "r")) {
            fclose(file);
            remove(node.c_str());
        }
        if(FILE *file = fopen(buffer.c_str(), "r")) {
            fclose(file);
            remove(buffer.c_str());
        }
        if(FILE *file = fopen(leaf.c_str(), "r")) {
            fclose(file);
            remove(leaf.c_str());
        }
    }
}





/***********************************************************************************************************************
 * OLD METHODS BELOW
 * DEPRECATED
 * ETC.
 * !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
 **********************************************************************************************************************/



/*
 * Responsible for externalizing the lowest level of the internal nodes.
 * Should be called when internalNodeCount > maxInternalNodes.
 * The extra nodes will never be greater than O(log(M/B)), and
 * will in practice be much smaller.
 */
/*void BufferedBTree::externalize() {

    cout << "Externalizing #internalNodes " << internalNodeCounter << " maxInternalNodes " << maxInternalNodes << "\n";
    cout << "#nodes in tree " << currentNumberOfNodes << " External node height is " << externalNodeHeight << "\n";

    if(externalNodeHeight == -1) {
        // Special case, first time we externalize
        externalNodeHeight = 1;
    }
    else {
        externalNodeHeight++;
    }

    // Recursively travel down to the external node height +1
    // and externalize the children of the nodes at this level.
    recursiveExternalize(root);

    cout << "Externalization completed, new #internalNodes " << internalNodeCounter << " external node height " << externalNodeHeight << "\n";
}

void BufferedBTree::recursiveExternalize(BufferedInternalNode *node) {

    if(node->height != externalNodeHeight+1) {
        // Recurse on children
        for(int i = 0; i < node->nodeSize+1; i++) {
            recursiveExternalize(node->children->at(i));
        }
    }
    else {
        // Externalize children
        node->values = new vector<int>();
        node->values->reserve(4*size);
        int cHeight, cSize;
        vector<int>* cKeys = new vector<int>();
        cKeys->reserve(size*4);
        vector<int>* cValues = new vector<int>();
        cValues->reserve(4*size);
        BufferedInternalNode* child;
        for(int i = 0; i < node->nodeSize+1; i++) {
            child = node->children->at(i);
            writeNode(child->id,child->height,child->nodeSize,child->keys,child->values);
            //cout << "Externalized node " << child->id << " from parent " << node->id << "\n";
            (*node->values)[i] = child->id;
            delete(child);
            internalNodeCounter--;
        }
        delete[] node->children;
    }
}

/*
 * Inserts key/value pair.
 * If key already present it updates value.
 */
/*void BufferedBTree::insert(KeyValueTime element) {

    // Commented out, work in progress

    /*root->buffer->push_back(*element);

    if(root->buffer->size() < bufferSize) {
        // Do nothing
        return;
    }
    else {
        // Sort the buffer of the root
        sortInternal(root->buffer);

        // Check if we need to split the root
        // In which case the buffer also gets split,
        // and no further action is needed.
        if(root->nodeSize == 4*size-1) {
            numberOfNodes++;
            internalNodeCounter++;
            currentNumberOfNodes++;
            BufferedInternalNode* newRoot = new BufferedInternalNode(numberOfNodes,root->height+1,size,false,bufferSize);
            (*newRoot->children)[0] = root;
            // Note that children of the new root will always be internal
            // Even if they are supposed to be external we dont externalize them
            // until after the insertion
            splitChildInternal(newRoot,root,0);
            root = newRoot;

            // Special case if root was leaf???

        }
        else {
            insertIntoNonFullInternal(root);
        }

        // Check if we need to externalize
        if(internalNodeCounter > maxInternalNodes) {
            externalize();
        }
    }
     */
/*}

/*
 * Assumes the input nodes buffer is sorted.
 */
/*void BufferedBTree::insertIntoNonFullInternal(KeyValueTime element, BufferedInternalNode *node) {

    // commented out because work in progress


    // Split the sorted buffer of node to its children
    /*if(node->height == 2) {
        // Children are leafs

        if(externalNodeHeight <= 0) {
            // Internal children who are leafs

            // Use two temporary vectors to merge
            // childrens buffer with ours
            std::vector<KeyValueTime>* toChild = new std::vector<KeyValueTime>();
            toChild->reserve(bufferSize);
            std::vector<KeyValueTime>* temp;

            // Distribute to children
            BufferedInternalNode* child;
            int bufferIndex = 0;
            for(int i = 0; i < node->nodeSize; i++) {
                child = node->children->at(i);
                int divider = node->values->at(i);
                // Run through buffer, adding to toChild
                while(bufferIndex < node->buffer->size() && (*node->buffer)[bufferIndex].key <= divider) {
                    toChild->push_back((*node->buffer)[bufferIndex]);
                }
                temp = child->buffer;
                child->buffer = new std::vector<KeyValueTime>();
                child->buffer->reserve(bufferSize);
                // Merge toChild and temp into new child buffer
                int toChildIndex = 0;
                int tempIndex = 0;
                while(toChildIndex < toChild->size() || tempIndex < temp->size()) {
                    // Pick
                    if(toChildIndex >= toChild->size()) {
                        // toChild done, add from temp
                        KeyValueTime kvt = temp->at(tempIndex);
                        child->buffer->push_back(kvt);
                        tempIndex++;
                    }
                    else if(tempIndex >= temp->size()) {
                        KeyValueTime kvt = toChild->at(toChildIndex);
                        child->buffer->push_back(kvt);
                        toChildIndex++;
                    }
                    else {
                        if(temp->at(tempIndex).key == toChild->at(toChildIndex).key) {
                            // Special case, update

                        }
                        else if(temp->at(tempIndex).key < toChild->at(toChildIndex).key) {
                            KeyValueTime kvt = temp->at(tempIndex);
                            child->buffer->push_back(kvt);
                            tempIndex++;
                        }
                        else {
                            KeyValueTime kvt = toChild->at(toChildIndex);
                            child->buffer->push_back(kvt);
                            toChildIndex++;
                        }
                    }
                }
            }
            // Last child


        }
        else {
            // Children are external leafs

        }


    }
    else {

    }

    //cout << "Insert Non Full Internal " << element->key << " Node ID = " << node->id << "\n";

    /*int i = node->nodeSize;
    if(node->height == 1) {
        // Insert into this leaf

        // Check if key is present
        int j = 0;
        while(j < node->nodeSize && node->keys[j] != element->key) {
            j++;
        }
        if(node->keys[j] == element->key) {
            // Special case, just update value
            node->values[j] = element->value;
            //cout << "Node = " <<  node->id << " Updated key = " << element->key << " to value = " << element->value << "\n";
            return;
        }
        else {
            // Move keys and values back, insert new pair
            while(i > 0 && element->key < node->keys[i-1]) {
                node->keys[i] = node->keys[i-1];
                node->values[i] = node->values[i-1];
                i--;
            }
            node->keys[i] = element->key;
            node->values[i] = element->value;
            (node->nodeSize)++;
            //cout << "Node = " <<  node->id << " Inserted key = " << element->key << " with value = " << element->value << "\n";
            return;
        }


    }
    else {
        // Search for node to insert into

        // Find child
        while(i > 0 && element->key <= node->keys[i-1]) {
            i--;
        }
        //int child = node->values[i];


        if(node->height != externalNodeHeight+1) {
            // Internal children

            BufferedInternalNode* child = node->children[i];
            if(child->nodeSize == size*4-1) {
                // Split child
                splitChildInternal(node,child,i);
                //(node->nodeSize)++;

                if(element->key > node->keys[i]) {
                    child = node->children[i+1];
                }
            }
            insertIntoNonFullInternal(element,child);

        }
        else {
            // External children

            int child = node->values[i];
            // Load in child
            int cHeight, cSize;
            int* ptr_cHeight = &cHeight;
            int* ptr_cSize = &cSize;
            int* cKeys = new int[size*4-1];
            int* cValues = new int[size*4];
            readNode(child,ptr_cHeight,ptr_cSize,cKeys,cValues);
            bool cWrite = false;

            // Check if we need to split the child
            if(cSize == 4*size-1) {

                cWrite = true;
                int newSize = 2*size-1;
                int* newKeys = new int[size*4-1];
                int* newValues = new int[size*4];

                // Special
                splitChildBorder(node,i,ptr_cSize, cKeys, cValues,
                                 newKeys, newValues);

                //cout << "Split children new sizes " << cSize << " " << newSize << "\n";

                // Check which of the two children we need to recurse upon.
                if(element->key > node->keys[i]) {

                    // Write out old child
                    //cout << "Writing out old child " << child << " with size " << cSize << "\n";
                    writeNode(child,cHeight,cSize,cKeys,cValues);

                    // Switch child to new child.
                    i++;
                    child = node->values[i];
                    cKeys = newKeys;
                    cValues = newValues;
                    cSize = newSize;

                }
                else {
                    // Write out new child
                    writeNode(numberOfNodes,cHeight,newSize,newKeys,newValues);
                }
            }

            // Insert into child - will switch to external functions
            insertIntoNonFull(element,child,cHeight,cSize,cKeys,cValues,cWrite);
        }
    }
    */
/*}

/*
 * Inserts into an internal node or a leaf.
 * Write denotes whether a node was changed, ans thus needs to be written to disk.
 */

/*void BufferedBTree::insertIntoNonFull(KeyValueTime element, int id, int height, int nodeSize, int *keys, int *values, bool write) {

    int i = nodeSize;
    if(height == 1) {
        // Insert into this leaf

        // Check if key is present
        int j = 0;
        while(j < nodeSize && keys[j] != element.key) {
            j++;
        }
        /*if(keys[j] == element.key) {
            // Special case, just update value
            values[j] = element.value;
            writeNode(id,height,nodeSize,keys,values);
            //cout << "Node = " <<  id << " Updated key = " << element->key << " to value = " << element->value << "\n";
            return;
        }
        else {
            // Move keys and values back, insert new pair
            while(i > 0 && element.key < keys[i-1]) {
                keys[i] = keys[i-1];
                values[i] = values[i-1];
                i--;
            }
            keys[i] = element.key;
            values[i] = element.value;
            nodeSize++;
            writeNode(id,height,nodeSize,keys,values);
            //cout << "Node = " <<  id << " Inserted key = " << element->key << " with value = " << element->value << "\n";
            return;
        }*/
        // Move keys and values back, insert new pair
        /*while(i > 0 && element.key < keys[i-1]) {
            keys[i] = keys[i-1];
            values[i] = values[i-1];
            i--;
        }
        keys[i] = element.key;
        values[i] = element.value;
        nodeSize++;
        writeNode(id,height,nodeSize,keys,values);
        //cout << "Node = " <<  id << " Inserted key = " << element->key << " with value = " << element->value << "\n";
        return;
    }
    else {
        // Internal node

        // Find child
        while(i > 0 && element.key <= keys[i-1]) {
            i--;
        }
        int child = values[i];

        // Load in child
        int cHeight, cSize;
        int* ptr_cHeight = &cHeight;
        int* ptr_cSize = &cSize;
        int* cKeys = new int[size*4-1];
        int* cValues = new int[size*4];
        readNode(child,ptr_cHeight,ptr_cSize,cKeys,cValues);
        bool cWrite = false;

        // Check if we need to split the child
        if(cSize == 4*size-1) {

            write = true;
            cWrite = true;

            int newSize = 2*size-1;
            int* newKeys = new int[size*4-1];
            int* newValues = new int[size*4];

            splitChild(height, nodeSize, keys, values, i, ptr_cSize, cKeys, cValues,
                       newKeys, newValues);
            nodeSize++;

            //cout << "Split children new sizes " << cSize << " " << newSize << "\n";

            // Check which of the two children we need to recurse upon.
            if(element.key > keys[i]) {

                // Write out old child
                //cout << "Writing out old child " << child << " with size " << cSize << "\n";
                writeNode(child,cHeight,cSize,cKeys,cValues);

                // Switch child to new child.
                i++;
                child = values[i];
                cKeys = newKeys;
                cValues = newValues;
                cSize = newSize;

            }
            else {
                // Write out new child
                writeNode(numberOfNodes,cHeight,newSize,newKeys,newValues);
            }
        }

        // Write node to disk, if necessary
        if(write) {
            writeNode(id,height,nodeSize,keys,values);
        }

        // Insert into child
        insertIntoNonFull(element,child,cHeight,cSize,cKeys,cValues,cWrite);
    }
}

/*
 * Split internal children.
 * Will handle if childrens children are external.
 */
/*void BufferedBTree::splitChildInternal(BufferedInternalNode* parent, BufferedInternalNode *child, int childNumber) {

    //cout << "Split Internal Child on parent " << parent->id << " child " << child->id << " childnumber " << childNumber <<"\n";
    //cout << parent->nodeSize << " " << externalNodeHeight << " " << child->height << "\n";

    numberOfNodes++;
    currentNumberOfNodes++;
    internalNodeCounter++;
    bool externalChildren = (child->height == externalNodeHeight+1);
    BufferedInternalNode* newChild;
    // Special case if this is a leaf
    if(child->height == 1) {
        newChild = new BufferedInternalNode(numberOfNodes,child->height,size,true,bufferSize);
    }
    else {
        newChild = new BufferedInternalNode(numberOfNodes,child->height,size,externalChildren,bufferSize);
    }

    // Fill out new childs keys
    for(int j = 0; j < 2*size-1; j++) {
        newChild->keys[j] = child->keys[2*size+j];
    }

    // Fill out values
    if(!externalChildren) {
        if(child->height == 1) {
            // Children are internal leafs
            for(int j = 0; j < 2*size-1; j++) {
                newChild->values[j] = child->values[2*size+j];
            }
        }
        else {
            // Children are internal nodes
            for(int j = 0; j < 2*size; j++) {
                newChild->children[j] = child->children[2*size+j];
            }
        }
    }
    else {
        // Childrens children are external nodes
        for(int j = 0; j < 2*size; j++) {
            newChild->values[j] = child->values[2*size+j];
        }

    }

    // Move parents keys by one
    for(int j = parent->nodeSize; j > childNumber; j--) {
        parent->keys[j] = parent->keys[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    parent->keys[childNumber] = child->keys[2*size-1];

    // Also update size of children.
    if(child->height == 1) {
        child->nodeSize = 2*size;
    }
    else {
        child->nodeSize = 2*size-1;
    }
    newChild->nodeSize = 2*size-1;

    // Move parents values back by one
    for(int j = parent->nodeSize+1; j > childNumber; j--) {
        parent->children[j] = parent->children[j-1];
    }
    // Insert pointer to new child
    parent->children[childNumber+1] = newChild;

    // Increase parent size
    (parent->nodeSize)++;

}

/*
 * Special case where the parent is a node in internal memory and the children are external nodes
 */
/*void BufferedBTree::splitChildBorder(BufferedInternalNode *parent, int childNumber, int *childSize, int *cKeys,
                                     int *cValues, int *newKeys, int *newValues) {

    numberOfNodes++;
    currentNumberOfNodes++;
    int newChild = numberOfNodes;

    // Fill out new childs keys
    for(int j = 0; j < 2*size-1; j++) {
        newKeys[j] = cKeys[2*size+j];
    }

    // Copy values
    if(parent->height == 2) {
        // Children are leafs
        for(int j = 0; j < 2*size-1; j++) {
            newValues[j] = cValues[2*size+j]; // <---?
        }
    }
    else {
        // Children are internal nodes
        for(int j = 0; j < 2*size; j++) {
            newValues[j] = cValues[2*size+j];
        }
    }

    // Move parents keys by one
    for(int j = parent->nodeSize; j > childNumber; j--) {
        parent->keys[j] = parent->keys[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    parent->keys[childNumber] = cKeys[2*size-1];

    // Also update size of old child.
    if(parent->height == 2) {
        *childSize = 2*size;
    }
    else {
        *childSize = 2*size-1;
    }

    // Move parents values back by one
    for(int j = parent->nodeSize+1; j > childNumber; j--) {
        parent->values[j] = parent->values[j-1];
    }
    // Insert pointer to new child
    parent->values[childNumber+1] = newChild;

    (parent->nodeSize)++;

}

/*
 * Splits a child of a node into a new child.
 * Does not clean up parent, existing child or new child, responsibility is left to calling function.
 * You must increase parents size by one, and set the old child and new childs size to size-1;
 * Will update nodeSize of original child correctly, depending on if its a leaf or not.
 * New child will have nodeSize = size-1;
 */
/*void BufferedBTree::splitChild(int height, int nodeSize, int *keys, int *values, int childNumber, int* childSize,
                               int *cKeys, int *cValues, int *newKeys, int *newValues) {

    numberOfNodes++;
    currentNumberOfNodes++;
    int newChild = numberOfNodes;

    // Fill out new childs keys
    for(int j = 0; j < 2*size-1; j++) {
        newKeys[j] = cKeys[2*size+j];
    }

    // Copy values
    if(height == 2) {
        // Children are leafs
        for(int j = 0; j < 2*size-1; j++) {
            newValues[j] = cValues[2*size+j]; // <---?
        }
    }
    else {
        // Children are internal nodes
        for(int j = 0; j < 2*size; j++) {
            newValues[j] = cValues[2*size+j];
        }
    }

    // Move parents keys by one
    for(int j = nodeSize; j > childNumber; j--) {
        keys[j] = keys[j-1];
    }
    // New child inherits key from old child. Assign new key to old child.
    keys[childNumber] = cKeys[2*size-1];

    // Also update size of old child.
    if(height == 2) {
        *childSize = 2*size;
    }
    else {
        *childSize = 2*size-1;
    }

    // Move parents values back by one
    for(int j = nodeSize+1; j > childNumber; j--) {
        values[j] = values[j-1];
    }
    // Insert pointer to new child
    values[childNumber+1] = newChild;

}

/*int BufferedBTree::query(int element) {

    // Root is in internal memory
    BufferedInternalNode* node = root;
    while(node->height > externalNodeHeight+1 && node->height != 1) {
        // Find position
        int i = node->nodeSize;
        while(i > 0 && element <= node->keys[i-1]) {
            i--;
        }
        node = node->children[i];
    }

    // Two cases
    // 1) Hit internal leaf
    // 2) Hit internal node on height externalNodeHeight+1

    if(node->height == 1) {
        // Internal leaf, just report element
        int j = 0;
        while(j < node->nodeSize && node->keys[j] != element) {
            j++;
        }
        return node->values[j];
    }
    else {

        // Search externally

        // First find the approriate child as root of the subtree
        int i = node->nodeSize;
        while(i > 0 && element <= node->keys[i-1]) {
            i--;
        }
        int currentNode = node->values[i];

        // Load in child
        int height, nodeSize;
        int* ptr_height = &height;
        int* ptr_nodeSize = &nodeSize;
        int* keys = new int[size*4-1];
        int* values = new int[size*4];
        readNode(currentNode,ptr_height,ptr_nodeSize,keys,values);

        // Go down in tree until we hit the leaf
        while(height != 1) {

            //cout << "---Inside node " << currentNode << "\n";

            // Find position
            int i = nodeSize;
            while(i > 0 && element <= keys[i-1]) {
                i--;
            }
            currentNode = values[i];

            //cout << "Found node " << currentNode << " in place " << i << "\n";

            readNode(currentNode,ptr_height,ptr_nodeSize,keys,values);
        }

        //cout << "Searching leaf, node " << currentNode << "\n";

        // In leaf
        int j = 0;
        while(j < nodeSize && keys[j] != element) {
            j++;
        }

        // Postpone return, clean up first.
        int ret = -1;
        if(keys[j] == element) {
            ret = values[j];
        }

        delete[] keys;
        delete[] values;

        return ret;
    }
}

void BufferedBTree::internalize() {

    if(currentNumberOfNodes == internalNodeCounter) {
        // Do nothing, no external nodes to internalize
        return;
    }
    else if(externalNodeHeight > 0) {
        cout << "Internalize, #internalNodes = " << internalNodeCounter << "\n";
        externalNodeHeight--;
        recursiveInternalize(root);
    }
}

void BufferedBTree::recursiveInternalize(BufferedInternalNode *node) {

    if(node->height != externalNodeHeight+2) {
        // Recurse internally
        for(int i = 0; i < node->nodeSize+1; i++) {
            recursiveInternalize(node->children[i]);
        }
    }
    else {
        // Children are external, internalize them
        cout << "=Internalizing children of node " << node->id << "\n";
        node->children = new BufferedInternalNode*[4*size];
        for(int i = 0; i < node->nodeSize+1; i++) {
            int id = node->values[i];
            cout << "Internalizing node " << id << "\n";
            BufferedInternalNode* child = new BufferedInternalNode(id,0,size,true,bufferSize);
            int* ptr_height = &(child->height);
            int* ptr_nodeSize = &(child->nodeSize);
            readNode(id,ptr_height,ptr_nodeSize,child->keys,child->values);
            node->children[i] = child;
            internalNodeCounter++;
        }
        delete[] node->values;
    }
}

void BufferedBTree::deleteElement(int element) {

    // Root always in internal memory
    // Perform check to see if we need to shrink the tree.
    if(root->height != 1 && root->nodeSize == 0) {
        cout << "Deleting root " << root->id << "\n";
        BufferedInternalNode* oldRoot = root;
        if(root->height == externalNodeHeight+1) {
            // Special case
            cout << "Special case " << root->id << " " << root->values[0] << "\n";
            // Special case where root is the only node in internal memory,
            // and we must internalize its child
            int child = root->values[0];
            root = new BufferedInternalNode(root->values[0],root->height-1,size,true,bufferSize);
            int height, nodeSize;
            int* ptr_height = &height;
            int* ptr_nodeSize = &nodeSize;
            readNode(root->id,ptr_height,ptr_nodeSize,root->keys,root->values);
            root->height = height;
            root->nodeSize = nodeSize;
            cout << root->id << " " << root->height << " " << root->nodeSize << "\n";
            delete[] oldRoot->keys;
            delete[] oldRoot->values;
            delete(oldRoot);
            externalNodeHeight--;
        }
        else {
            root = root->children[0];
            delete[] oldRoot->keys;
            delete[] oldRoot->children;
            delete(oldRoot);
        }
        internalNodeCounter--;
        currentNumberOfNodes--;
        cout << "New root is node " << root->id << "\n";
        deleteElement(element);
    }
    else {
        deleteNonSparseInternal(element,root);
    }

    if(internalNodeCounter < minInternalNodes) {
        internalize();
    }
}

void BufferedBTree::deleteNonSparseInternal(int element, BufferedInternalNode *node) {

    cout << "Delete Non Sparse Internal " << node->id << "\n";
    if(node->height == 1) {
        cout << "Found internal leaf\n";
        // Leaf
        // Find key position
        int i = 0;
        while(i < node->nodeSize && node->keys[i] != element) {
            i++;
        }
        if(node->keys[i] == element) {
            (node->nodeSize)--;
            while(i < node->nodeSize) {
                // Move array indexes forward one place
                node->keys[i] = node->keys[i+1];
                node->values[i] = node->values[i+1];
                i++;
            }
            //cout << "Deleted key = " << element << " from node " << id << "\n";
            return;
        }
        else {
            // Key was not present
            //cout << "Key not present in node " << id << "\n";
            return;
        }
    }
    else if(node->height > externalNodeHeight+1){
        cout << "Internal child\n";
        // Children are internal
        int i = node->nodeSize;
        while(i > 0 && element <= node->keys[i-1]) {
            i--;
        }
        BufferedInternalNode* child = node->children[i];

        // Check if child is too small
        if(child->nodeSize == size-1) {
            // Fuse child
            fuseChildInternal(node,child,i);

            // Recheck child to recurse upon
            int i = node->nodeSize;
            while(i > 0 && element <= node->keys[i-1]) {
                i--;
            }
            child = node->children[i];
        }
        deleteNonSparseInternal(element,child);
    }
    else {

        // Children are external
        int i = node->nodeSize;
        while(i > 0 && element <= node->keys[i-1]) {
            i--;
        }
        int child = node->values[i];

        cout << "External child " << child << " " << i << " " << element << " " << node->nodeSize << "\n";



        // Load in child
        int cHeight, cSize;
        int* ptr_cHeight = &cHeight;
        int* ptr_cSize = &cSize;
        int* cKeys = new int[size*4-1];
        int* cValues = new int[size*4];
        readNode(child,ptr_cHeight,ptr_cSize,cKeys,cValues);
        bool cWrite = false;

        if(cSize == size-1) {

            cWrite = true;
            cout << "!!!!!!\n";
            // Fuse child to ensure place for deletion
            fuseChildBorder(node,i,ptr_cSize,cKeys,cValues);

            // Recheck what child to recurse upon
            // Fusechild will update cKeys and cValues
            i = node->nodeSize;
            while(i > 0 && element <= node->keys[i-1]) {
                i--;
            }
            child = node->values[i];
        }
        deleteNonSparse(element,child,cHeight,cSize,cKeys,cValues,cWrite);
    }
}

/*
 * Responsible for cleaning up arguments!
 * Write indicates whether a node was changed, and thus needs to be written to disk.
 */
/*void BufferedBTree::deleteNonSparse(int element, int id, int height, int nodeSize, int *keys, int *values, bool write) {

    cout << "Delete on node " << id << " height " << height << " of size " << nodeSize << "\n";

    if(height == 1) {
        // Leaf, delete

        // Find key position
        int i = 0;
        while(i < nodeSize && keys[i] != element) {
            i++;
        }
        if(keys[i] == element) {
            nodeSize--;
            while(i < nodeSize) {
                // Move array indexes forward one place
                keys[i] = keys[i+1];
                values[i] = values[i+1];
                i++;
            }
            writeNode(id,height,nodeSize,keys,values);
            //cout << "Deleted key = " << element << " from node " << id << "\n";
            return;
        }
        else {
            // Key was not present
            if(write) {
                writeNode(id,height,nodeSize,keys,values);
            }
            //cout << "Key not present in node " << id << "\n";
            return;
        }
    }
    else {
        // Internal node, find child to recurse upon
        int i = nodeSize;
        while(i > 0 && element <= keys[i-1]) {
            i--;
        }
        int child = values[i];

        // Load in child
        int cHeight, cSize;
        int* ptr_cHeight = &cHeight;
        int* ptr_cSize = &cSize;
        int* cKeys = new int[size*4-1];
        int* cValues = new int[size*4];
        readNode(child,ptr_cHeight,ptr_cSize,cKeys,cValues);
        bool cWrite = false;

        if(cSize == size-1) {

            write = true;
            cWrite = true;

            // Fuse child to ensure place for deletion
            int* ptr_nodeSize = &nodeSize;
            fuseChild(height,ptr_nodeSize,keys,values,i,ptr_cSize,cKeys,cValues);

            // Recheck what child to recurse upon
            // Fusechild will update cKeys and cValues
            i = nodeSize;
            while(i > 0 && element <= keys[i-1]) {
                i--;
            }
            child = values[i];
        }

        if(write) {
            writeNode(id,height,nodeSize,keys,values);
        }
        deleteNonSparse(element,child,cHeight,cSize,cKeys,cValues,cWrite);
    }
}


// Reponsible for fusing internal children, cleans up and adjusts all values in involved nodes.
// Must handle children having external children.
void BufferedBTree::fuseChildInternal(BufferedInternalNode *parent, BufferedInternalNode *child, int childNumber) {

    /*
     * Two possible scenarios:
     * a) The child has a neighbouring sibling who also contains size-1 keys.
     * b) The child has a neighbouring sibling containing >= size keys.
     * We prefer case a over case b, since it allows for faster consecutive deletes.
     */

    /*cout << "Fuse child internal " << child->id << "\n";

    // Check case and placement
    BufferedInternalNode* sibling;
    bool caseA = false;
    bool leftSibling = false;
    if(childNumber == 0) {
        // Leftmost child, check right sibling
        sibling = parent->children[1];
        if(sibling->nodeSize == size-1) {
            caseA = true;
        }
    }
    else if (childNumber == parent->nodeSize) {
        // Rightmost child, check left sibling
        sibling = parent->children[(parent->nodeSize)-1];
        if(sibling->nodeSize == size-1) {
            caseA = true;
        }
        leftSibling = true;
    }
    else {
        // Check both left and right sibling
        sibling = parent->children[childNumber+1];
        if(sibling->nodeSize == size-1) {
            caseA = true;
        }
        else {
            sibling = parent->children[childNumber-1];
            if(sibling->nodeSize == size-1) {
                caseA = true;
            }
            leftSibling = true;
        }
    }

    if(caseA) {
        // Case a, fuse child with its sibling
        // Fuse right child over into left child
        if(leftSibling) {
            if(parent->height != 2) {
                if(child->height != externalNodeHeight+1) {
                    // Internal children
                    // Fuse child into left sibling

                    // We lack one key to separate the two sets of keys.
                    // This key is the left siblings rightmost childs
                    // rightmost key. This key can be found separating
                    // the two children in the parent!

                    sibling->keys[sibling->nodeSize] = parent->keys[childNumber-1];

                    for(int i = 0; i < child->nodeSize; i++) {
                        sibling->keys[sibling->nodeSize+1+i] = child->keys[i];
                    }
                    for(int i = 0; i < child->nodeSize+1; i++) {
                        sibling->children[sibling->nodeSize+1+i] = child->children[i];
                    }
                    sibling->nodeSize = sibling->nodeSize+1+child->nodeSize;
                }
                else {
                    sibling->keys[sibling->nodeSize] = parent->keys[childNumber-1];

                    for(int i = 0; i < child->nodeSize; i++) {
                        sibling->keys[sibling->nodeSize+1+i] = child->keys[i];
                    }
                    for(int i = 0; i < child->nodeSize+1; i++) {
                        sibling->values[sibling->nodeSize+1+i] = child->values[i];
                    }
                    sibling->nodeSize = sibling->nodeSize+1+child->nodeSize;
                }
            }
            else {
                // Children are leafs, and can not be external
                // Fuse child into left sibling
                for(int i = 0; i < child->nodeSize; i++) {
                    sibling->keys[sibling->nodeSize+i] = child->keys[i];
                    sibling->values[sibling->nodeSize+i] = child->values[i];
                }
                sibling->nodeSize = sibling->nodeSize+child->nodeSize;
            }

            // Update parent
            for(int i = childNumber-1; i < parent->nodeSize; i++) {
                parent->keys[i] = parent->keys[i+1];
            }
            for(int i = childNumber; i < parent->nodeSize; i++) {
                parent->children[i] = parent->children[i+1];
            }
            (parent->nodeSize)--;

            // Delete original child
            delete[] child->keys;
            if(child->height != externalNodeHeight+1) {
                delete[] child->children;
            }
            else {
                delete[] child->values;
            }
            delete(child);
            return;
        }
        else {
            if(parent->height != 2) {
                if(child->height != externalNodeHeight+1) {
                    // Internal children with internal children
                    // Fuse right sibling into child
                    child->keys[child->nodeSize] = parent->keys[childNumber];

                    for(int i = 0; i < sibling->nodeSize; i++) {
                        child->keys[child->nodeSize+1+i] = sibling->keys[i];
                    }
                    for(int i = 0; i < sibling->nodeSize+1; i++) {
                        child->children[child->nodeSize+1+i] = sibling->children[i];
                    }
                    child->nodeSize = child->nodeSize+1+sibling->nodeSize;
                }
                else {
                    // Internal children with external children
                    // Fuse right sibling into child
                    child->keys[child->nodeSize] = parent->keys[childNumber];

                    for(int i = 0; i < sibling->nodeSize; i++) {
                        child->keys[child->nodeSize+1+i] = sibling->keys[i];
                    }
                    for(int i = 0; i < sibling->nodeSize+1; i++) {
                        child->values[child->nodeSize+1+i] = sibling->values[i];
                    }
                    child->nodeSize = child->nodeSize+1+sibling->nodeSize;
                }
            }
            else {
                // Children are leafs, and can not be external
                // Fuse right sibling into child
                for(int i = 0; i < sibling->nodeSize; i++) {
                    child->keys[child->nodeSize+i] = sibling->keys[i];
                    child->values[child->nodeSize+i] = sibling->values[i];
                }
                child->nodeSize = child->nodeSize+sibling->nodeSize;
            }

            // Update parent
            for(int i = childNumber; i < parent->nodeSize; i++) {
                parent->keys[i] = parent->keys[i+1];
            }
            for(int i = childNumber+1; i < parent->nodeSize+1; i++) {
                parent->children[+i] = parent->children[i+1];
            }
            (parent->nodeSize)--;

            // Effectively deleted right sibling
            delete[] sibling->keys;
            if(sibling->height != externalNodeHeight+1) {
                delete[] sibling->children;
            }
            else {
                delete[] sibling->values;
            }
            delete(sibling);
            return;
        }
        // Fused two internal children
        internalNodeCounter--;
        currentNumberOfNodes--;
    }
    else {
        // Case b, steal a key and a value from the sibling
        if(leftSibling) {
            // Steal rightmost value from left sibling
            if(parent->height != 2) {
                if(child->height != externalNodeHeight+1) {
                    // Internal nodes with internal children
                    // Make room for new value and key
                    for(int i = child->nodeSize; i > 0; i--) {
                        child->keys[i] = child->keys[i-1];
                    }
                    for(int i = child->nodeSize+1; i > 0; i--) {
                        child->children[i] = child->children[i-1];
                    }
                    child->keys[0] = parent->keys[childNumber-1];
                    child->children[0] = sibling->children[sibling->nodeSize];
                    (child->nodeSize)++;
                    (sibling->nodeSize)--;
                    parent->keys[childNumber-1] = sibling->keys[sibling->nodeSize];
                }
                else {
                    // Internal nodes with external children
                    // Make room for new value and key
                    for(int i = child->nodeSize; i > 0; i--) {
                        child->keys[i] = child->keys[i-1];
                    }
                    for(int i = child->nodeSize+1; i > 0; i--) {
                        child->values[i] = child->values[i-1];
                    }
                    child->keys[0] = parent->keys[childNumber-1];
                    child->values[0] = sibling->values[sibling->nodeSize];
                    (child->nodeSize)++;
                    (sibling->nodeSize)--;
                    parent->keys[childNumber-1] = sibling->keys[sibling->nodeSize];
                }
            }
            else {
                // Leafs, cant be external
                for(int i = child->nodeSize; i > 0; i--) {
                    child->keys[i] = child->keys[i-1];
                }
                for(int i = child->nodeSize+1; i > 0; i--) {
                    child->values[i] = child->values[i-1];
                }
                //cKeys[0] = keys[childNumber-1];
                child->keys[0] = sibling->keys[sibling->nodeSize-1];
                child->values[0] = sibling->values[sibling->nodeSize-1];
                (child->nodeSize)++;
                (sibling->nodeSize)--;
                parent->keys[childNumber-1] = sibling->keys[sibling->nodeSize-1];
            }
        }
        else {
            // Steal leftmost value from right sibling
            if(parent->height != 2) {
                if(child->height != externalNodeHeight+1) {
                    child->keys[child->nodeSize] = parent->keys[childNumber];
                    child->children[child->nodeSize+1] = sibling->children[0];
                    (child->nodeSize)++;
                    parent->keys[childNumber] = sibling->keys[0];
                    // Move siblings keys and values forward in arrays
                    for(int i = 0; i < sibling->nodeSize-1; i++) {
                        sibling->keys[i] = sibling->keys[i+1];
                    }
                    for(int i = 0; i < sibling->nodeSize; i++) {
                        sibling->children[i] = sibling->children[i+1];
                    }
                    (sibling->nodeSize)--;
                }
                else {
                    child->keys[child->nodeSize] = parent->keys[childNumber];
                    child->values[child->nodeSize+1] = sibling->values[0];
                    (child->nodeSize)++;
                    parent->keys[childNumber] = sibling->keys[0];
                    // Move siblings keys and values forward in arrays
                    for(int i = 0; i < sibling->nodeSize-1; i++) {
                        sibling->keys[i] = sibling->keys[i+1];
                    }
                    for(int i = 0; i < sibling->nodeSize; i++) {
                        sibling->values[i] = sibling->values[i+1];
                    }
                    (sibling->nodeSize)--;
                }

            }
            else {
                // Leafs
                //cKeys[*childSize] = keys[childNumber];
                child->keys[child->nodeSize] = sibling->keys[0];
                child->values[child->nodeSize] = sibling->values[0];
                (child->nodeSize)++;
                parent->keys[childNumber] = sibling->keys[0];
                // Move siblings keys and values forward in arrays
                for(int i = 0; i < sibling->nodeSize-1; i++) {
                    sibling->keys[i] = sibling->keys[i+1];
                    sibling->values[i] = sibling->values[i+1];
                }
                (sibling->nodeSize)--;
            }
        }
        return;
    }
}

// Internal node with external children
void BufferedBTree::fuseChildBorder(BufferedInternalNode *parent, int childNumber, int *childSize, int *cKeys,
                                    int *cValues) {

    /*
     * Two possible scenarios:
     * a) The child has a neighbouring sibling who also contains size-1 keys.
     * b) The child has a neighbouring sibling containing >= size keys.
     * We prefer case a over case b, since it allows for faster consecutive deletes.
     */

    /*cout << "Fuse child border " << parent->values[childNumber] << "\n";

    int siblingID;
    int siblingHeight, siblingSize;
    int* ptr_siblingHeight = &siblingHeight;
    int* ptr_siblingSize = &siblingSize;
    int* siblingKeys = new int[size*4-1];
    int* siblingValues = new int[size*4];

    // Check case and placement
    bool caseA = false;
    bool leftSibling = false;
    if(childNumber == 0) {
        // Leftmost child, check right sibling
        siblingID = parent->values[1];
        readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
        if(siblingSize == size-1) {
            caseA = true;
        }
    }
    else if (childNumber == (parent->nodeSize)) {
        // Rightmost child, check left sibling
        siblingID = parent->values[(parent->nodeSize)-1];
        readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
        if(siblingSize == size-1) {
            caseA = true;
        }
        leftSibling = true;
    }
    else {
        // Check both left and right sibling
        siblingID = parent->values[childNumber+1];
        readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
        if(siblingSize == size-1) {
            caseA = true;
        }
        else {
            siblingID = parent->values[childNumber-1];
            readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
            if(siblingSize == size-1) {
                caseA = true;
            }
            leftSibling = true;
        }
    }

    //cout << "CaseA = " << caseA << " leftSibling = " << leftSibling << " siblingSize = " << siblingSize << "\n";

    // Perform the necessary adjustments
    if(caseA) {
        // Case a, fuse child with its sibling
        // Fuse right child over into left child
        if(leftSibling) {
            if(parent->height != 2) {
                // Internal children
                // Fuse child into left sibling

                // We lack one key to separate the two sets of keys.
                // This key is the left siblings rightmost childs
                // rightmost key. This key can be found separating
                // the two children in the parent!

                siblingKeys[siblingSize] = parent->keys[childNumber-1];

                for(int i = 0; i < *childSize; i++) {
                    siblingKeys[siblingSize+1+i] = cKeys[i];
                }
                for(int i = 0; i < (*childSize)+1; i++) {
                    siblingValues[siblingSize+1+i] = cValues[i];
                }
                siblingSize = siblingSize+1+(*childSize);
            }
            else {
                // Children are leafs
                // Fuse child into left sibling
                for(int i = 0; i < *childSize; i++) {
                    siblingKeys[siblingSize+i] = cKeys[i];
                    siblingValues[siblingSize+i] = cValues[i];
                }
                siblingSize = siblingSize+(*childSize);
            }

            // Update parent
            for(int i = childNumber-1; i < parent->nodeSize; i++) {
                parent->keys[i] = parent->keys[i+1];
            }
            for(int i = childNumber; i < (parent->nodeSize); i++) {
                parent->values[i] = parent->values[i+1];
            }
            (parent->nodeSize)--;

            // "Return" left sibling
            *childSize = siblingSize;

            if(parent->height != 2) {
                for(int i = 0; i < siblingSize; i++) {
                    cKeys[i] = siblingKeys[i];
                }
                for(int i = 0; i < siblingSize+1; i++) {
                    cValues[i] = siblingValues[i];
                }
            }
            else {
                for(int i = 0; i < siblingSize; i++) {
                    cKeys[i] = siblingKeys[i];
                    cValues[i] = siblingValues[i];
                }
            }
            delete[] siblingKeys;
            delete[] siblingValues;
            return;
        }
        else {
            if(parent->height != 2) {
                // Internal children
                // Fuse right sibling into child
                cKeys[*childSize] = parent->keys[childNumber];

                for(int i = 0; i < siblingSize; i++) {
                    cKeys[(*childSize)+1+i] = siblingKeys[i];
                }
                for(int i = 0; i < siblingSize+1; i++) {
                    cValues[(*childSize)+1+i] = siblingValues[i];
                }
                (*childSize) = (*childSize)+1+siblingSize;
            }
            else {
                // Children are leafs
                // Fuse right sibling into child
                for(int i = 0; i < siblingSize; i++) {
                    cKeys[(*childSize)+i] = siblingKeys[i];
                    cValues[(*childSize)+i] = siblingValues[i];
                }
                (*childSize) = (*childSize)+siblingSize;
            }

            // Update parent
            for(int i = childNumber; i < parent->nodeSize; i++) {
                parent->keys[i] = parent->keys[i+1];
            }
            for(int i = childNumber+1; i < (parent->nodeSize)+1; i++) {
                parent->values[+i] = parent->values[i+1];
            }
            (parent->nodeSize)--;

            // Effectively deleted right sibling
            delete[] siblingKeys;
            delete[] siblingValues;
            return;
        }
        currentNumberOfNodes--;
    }
    else {
        // Case b, steal a key and a value from the sibling
        if(leftSibling) {
            // Steal rightmost value from left sibling
            if(parent->height != 2) {
                // Internal nodes
                // Make room for new value and key
                for(int i = *childSize; i > 0; i--) {
                    cKeys[i] = cKeys[i-1];
                }
                for(int i = (*childSize)+1; i > 0; i--) {
                    cValues[i] = cValues[i-1];
                }
                cKeys[0] = parent->keys[childNumber-1];
                cValues[0] = siblingValues[siblingSize];
                (*childSize)++;
                siblingSize--;
                parent->keys[childNumber-1] = siblingKeys[siblingSize];
            }
            else {
                // Leafs
                for(int i = *childSize; i > 0; i--) {
                    cKeys[i] = cKeys[i-1];
                }
                for(int i = (*childSize)+1; i > 0; i--) {
                    cValues[i] = cValues[i-1];
                }
                //cKeys[0] = keys[childNumber-1];
                cKeys[0] = siblingKeys[siblingSize-1];
                cValues[0] = siblingValues[siblingSize-1];
                (*childSize)++;
                siblingSize--;
                parent->keys[childNumber-1] = siblingKeys[siblingSize-1];
            }
        }
        else {
            // Steal leftmost value from right sibling
            if(parent->height != 2) {
                cKeys[*childSize] = parent->keys[childNumber];
                cValues[(*childSize)+1] = siblingValues[0];
                (*childSize)++;
                parent->keys[childNumber] = siblingKeys[0];
                // Move siblings keys and values forward in arrays
                for(int i = 0; i < siblingSize-1; i++) {
                    siblingKeys[i] = siblingKeys[i+1];
                }
                for(int i = 0; i < siblingSize; i++) {
                    siblingValues[i] = siblingValues[i+1];
                }
                siblingSize--;
            }
            else {
                //cKeys[*childSize] = keys[childNumber];
                cKeys[*childSize] = siblingKeys[0];
                cValues[*childSize] = siblingValues[0];
                (*childSize)++;
                parent->keys[childNumber] = siblingKeys->at(0);
                // Move siblings keys and values forward in arrays
                for(int i = 0; i < siblingSize-1; i++) {
                    siblingKeys[i] = siblingKeys[i+1];
                    siblingValues[i] = siblingValues[i+1];
                }
                siblingSize--;
            }
        }
        // Write out sibling
        writeNode(siblingID,siblingHeight,siblingSize,siblingKeys,siblingValues);
        return;
    }
}

/*
 * Fuses a child with an appropriate neighbour.
 * Will update parent as appropriate.
 * If child fused into other child, keys and values are replaced.
 */
/*void BufferedBTree::fuseChild(int height, int* nodeSize, int *keys, int *values, int childNumber, int *childSize,
                              int *cKeys, int *cValues) {

    /*
     * Two possible scenarios:
     * a) The child has a neighbouring sibling who also contains size-1 keys.
     * b) The child has a neighbouring sibling containing >= size keys.
     * We prefer case a over case b, since it allows for faster consecutive deletes.
     */

    /*int siblingID;
    int siblingHeight, siblingSize;
    int* ptr_siblingHeight = &siblingHeight;
    int* ptr_siblingSize = &siblingSize;
    int* siblingKeys = new int[size*4-1];
    int* siblingValues = new int[size*4];

    // Check case and placement
    bool caseA = false;
    bool leftSibling = false;
    if(childNumber == 0) {
        // Leftmost child, check right sibling
        siblingID = values[1];
        readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
        if(siblingSize == size-1) {
            caseA = true;
        }
    }
    else if (childNumber == (*nodeSize)) {
        // Rightmost child, check left sibling
        siblingID = values[(*nodeSize)-1];
        readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
        if(siblingSize == size-1) {
            caseA = true;
        }
        leftSibling = true;
    }
    else {
        // Check both left and right sibling
        siblingID = values[childNumber+1];
        readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
        if(siblingSize == size-1) {
            caseA = true;
        }
        else {
            siblingID = values[childNumber-1];
            readNode(siblingID,ptr_siblingHeight,ptr_siblingSize,siblingKeys,siblingValues);
            if(siblingSize == size-1) {
                caseA = true;
            }
            leftSibling = true;
        }
    }

    //cout << "CaseA = " << caseA << " leftSibling = " << leftSibling << " siblingSize = " << siblingSize << "\n";

    // Perform the necessary adjustments
    if(caseA) {
        // Case a, fuse child with its sibling
        // Fuse right child over into left child
        if(leftSibling) {
            if(height != 2) {
                // Internal children
                // Fuse child into left sibling

                // We lack one key to separate the two sets of keys.
                // This key is the left siblings rightmost childs
                // rightmost key. This key can be found separating
                // the two children in the parent!

                siblingKeys[siblingSize] = keys[childNumber-1];

                for(int i = 0; i < *childSize; i++) {
                    siblingKeys[siblingSize+1+i] = cKeys[i];
                }
                for(int i = 0; i < (*childSize)+1; i++) {
                    siblingValues[siblingSize+1+i] = cValues[i];
                }
                siblingSize = siblingSize+1+(*childSize);
            }
            else {
                // Children are leafs
                // Fuse child into left sibling
                for(int i = 0; i < *childSize; i++) {
                    siblingKeys[siblingSize+i] = cKeys[i];
                    siblingValues[siblingSize+i] = cValues[i];
                }
                siblingSize = siblingSize+(*childSize);
            }

            // Update parent
            for(int i = childNumber-1; i < *nodeSize; i++) {
                keys[i] = keys[i+1];
            }
            for(int i = childNumber; i < (*nodeSize); i++) {
                values[i] = values[i+1];
            }
            (*nodeSize)--;

            /*cout << "===Printing out child\n";
            cout << height-1 << "\n";
            cout << *childSize << "\n";
            for(int j = 0; j < *childSize; j++) {
                cout << cKeys[j] << "\n";
            }
            for(int j = 0; j < *childSize; j++) {
                cout << cValues[j] << "\n";
            }
            cout << "===Printing out sibling\n";
            cout << height-1 << "\n";
            cout << siblingSize << "\n";
            for(int j = 0; j < siblingSize; j++) {
                cout << siblingKeys[j] << "\n";
            }
            for(int j = 0; j < siblingSize; j++) {
                cout << siblingValues[j] << "\n";
            }*/

            // Dont delete and reassign, copy over and delete instead
            //delete[] cKeys;
            //delete[] cValues;
            //cKeys = siblingKeys;
            //cValues = siblingValues;

            // "Return" left sibling
/*
            *childSize = siblingSize;

            if(height != 2) {
                for(int i = 0; i < siblingSize; i++) {
                    cKeys[i] = siblingKeys[i];
                }
                for(int i = 0; i < siblingSize+1; i++) {
                    cValues[i] = siblingValues[i];
                }
            }
            else {
                for(int i = 0; i < siblingSize; i++) {
                    cKeys[i] = siblingKeys[i];
                    cValues[i] = siblingValues[i];
                }
            }
            delete[] siblingKeys;
            delete[] siblingValues;
            return;
        }
        else {
            if(height != 2) {
                // Internal children
                // Fuse right sibling into child
                cKeys[*childSize] = keys[childNumber];

                for(int i = 0; i < siblingSize; i++) {
                    cKeys[(*childSize)+1+i] = siblingKeys[i];
                }
                for(int i = 0; i < siblingSize+1; i++) {
                    cValues[(*childSize)+1+i] = siblingValues[i];
                }
                (*childSize) = (*childSize)+1+siblingSize;
            }
            else {
                // Children are leafs
                // Fuse right sibling into child
                for(int i = 0; i < siblingSize; i++) {
                    cKeys[(*childSize)+i] = siblingKeys[i];
                    cValues[(*childSize)+i] = siblingValues[i];
                }
                (*childSize) = (*childSize)+siblingSize;
            }

            // Update parent
            for(int i = childNumber; i < *nodeSize; i++) {
                keys[i] = keys[i+1];
            }
            for(int i = childNumber+1; i < (*nodeSize)+1; i++) {
                values[+i] = values[i+1];
            }
            (*nodeSize)--;

            // Effectively deleted right sibling
            delete[] siblingKeys;
            delete[] siblingValues;
            return;
        }
        currentNumberOfNodes--;
    }
    else {
        // Case b, steal a key and a value from the sibling
        if(leftSibling) {
            // Steal rightmost value from left sibling
            if(height != 2) {
                // Internal nodes
                // Make room for new value and key
                for(int i = *childSize; i > 0; i--) {
                    cKeys[i] = cKeys[i-1];
                }
                for(int i = (*childSize)+1; i > 0; i--) {
                    cValues[i] = cValues[i-1];
                }
                cKeys[0] = keys[childNumber-1];
                cValues[0] = siblingValues[siblingSize];
                (*childSize)++;
                siblingSize--;
                keys[childNumber-1] = siblingKeys[siblingSize];
            }
            else {
                // Leafs
                for(int i = *childSize; i > 0; i--) {
                    cKeys[i] = cKeys[i-1];
                }
                for(int i = (*childSize)+1; i > 0; i--) {
                    cValues[i] = cValues[i-1];
                }
                //cKeys[0] = keys[childNumber-1];
                cKeys[0] = siblingKeys[siblingSize-1];
                cValues[0] = siblingValues[siblingSize-1];
                (*childSize)++;
                siblingSize--;
                keys[childNumber-1] = siblingKeys[siblingSize-1];
            }
        }
        else {
            // Steal leftmost value from right sibling
            if(height != 2) {
                cKeys[*childSize] = keys[childNumber];
                cValues[(*childSize)+1] = siblingValues[0];
                (*childSize)++;
                keys[childNumber] = siblingKeys[0];
                // Move siblings keys and values forward in arrays
                for(int i = 0; i < siblingSize-1; i++) {
                    siblingKeys[i] = siblingKeys[i+1];
                }
                for(int i = 0; i < siblingSize; i++) {
                    siblingValues[i] = siblingValues[i+1];
                }
                siblingSize--;
            }
            else {
                //cKeys[*childSize] = keys[childNumber];
                cKeys[*childSize] = siblingKeys[0];
                cValues[*childSize] = siblingValues[0];
                (*childSize)++;
                keys[childNumber] = siblingKeys[0];
                // Move siblings keys and values forward in arrays
                for(int i = 0; i < siblingSize-1; i++) {
                    siblingKeys[i] = siblingKeys[i+1];
                    siblingValues[i] = siblingValues[i+1];
                }
                siblingSize--;
            }
        }

        // Write out sibling
        writeNode(siblingID,siblingHeight,siblingSize,siblingKeys,siblingValues);
        return;
    }
}

/*
 * Writes out the node to file "B<id>".
 * Deletes key and value arrays.
 * Handles that leafs have size-1 values.
 */
/*void BufferedBTree::writeNode(int id, int height, int nodeSize, int *keys, int *values) {

    OutputStream* os = new BufferedOutputStream(B);
    string node = "B";
    node += to_string(id);
    os->create(node.c_str());
    os->write(&height);
    os->write(&nodeSize);
    for(int k = 0; k < nodeSize; k++) {
        os->write(&keys[k]);
    }
    if(height > 1) {
        for(int k = 0; k < nodeSize+1; k++) {
            os->write(&values[k]);
        }
    }
    else {
        // Leaf
        for(int k = 0; k < nodeSize; k++) {
            os->write(&values[k]);
        }
    }

    os->close();
    iocounter = iocounter + os->iocounter;
    delete(os);
    delete[] keys;
    delete[] values;
}

/*
 * Reads the node of id into the pointers height and size, as well as fills
 * out the arrays keys and values. Handles leafs (height == 1) appropriately.
 */
/*void BufferedBTree::readNode(int id, int* height, int* nodeSize, int *keys, int *values) {


    InputStream* is = new BufferedInputStream(B);
    string name = "B";
    name += to_string(id);
    is->open(name.c_str());
    *height = is->readNext();
    *nodeSize = is->readNext();

    for(int i = 0; i < *nodeSize; i++) {
        keys[i] = is->readNext();
    }

    if(*height > 1) {
        for(int i = 0; i < *nodeSize+1; i++) {
            values[i] = is->readNext();
        }
    }
    else {
        for(int i = 0; i < *nodeSize; i++) {
            values[i] = is->readNext();
        }
    }
    is->close();
    iocounter = iocounter + is->iocounter;
    delete(is);
}

void BufferedBTree::printTree(BufferedInternalNode *node) {

    cout << "=== I " << node->id << "\n";
    cout << node->height << "\n";
    cout << node->nodeSize << "\n";
    cout << "---\n";
    for(int i = 0; i < node->nodeSize; i++) {
        cout << node->keys->at(i) << "\n";
    }
    if(node->height > 1 && node->height != externalNodeHeight+1) {
        cout << "---C\n";
        for(int i = 0; i < node->nodeSize+1; i++) {
            cout << node->children->at(i)->id << "\n";
        }
        for(int i = 0; i < node->nodeSize+1; i++) {
            printTree(node->children->at(i));
        }
    }
    else if( node->height == 1 && externalNodeHeight <= 0) {
        cout << "---V1\n";
        for(int i = 0; i < node->nodeSize; i++) {
            cout << node->values->at(i) << "\n";
        }
    }
    else {
        cout << "---V\n";
        for(int i = 0; i < node->nodeSize+1; i++) {
            cout << node->values->at(i) << "\n";
        }
        for(int i = 0; i < node->nodeSize+1; i++) {
            printExternal(node->values->at(i));
        }
    }

}

void BufferedBTree::printExternal(int node) {

    int height, nodeSize;
    int* ptr_height = &height;
    int* ptr_nodeSize = &nodeSize;
    int* keys = new int[size*4-1];
    int* values = new int[size*4];
    readNode(node,ptr_height,ptr_nodeSize,keys,values);

    cout << "=== E " << node << "\n";
    cout << height << "\n";
    cout << nodeSize << "\n";
    cout << "---\n";
    for(int i = 0; i < nodeSize; i++) {
        cout << keys[i] << "\n";
    }
    cout << "---\n";
    if(height == 1) {
        for(int i = 0; i < nodeSize; i++) {
            cout << values[i] << "\n";
        }
    }
    else {
        for(int i = 0; i < nodeSize+1; i++) {
            cout << values[i] << "\n";
        }
        for(int i = 0; i < nodeSize+1; i++) {
            printExternal(values[i]);
        }
    }
}

// Clean up external files
void BufferedBTree::cleanup() {
    for(int i = 1; i <= numberOfNodes; i++) {
        string name = "B";
        name += to_string(i);
        // Doesnt always work
        /*if(access( name.c_str(), F_OK ) != -1) {
            remove(name.c_str());
        }*/
        // This always works
        /*if(FILE *file = fopen(name.c_str(), "r")) {
            fclose(file);
            remove(name.c_str());
        }
    }
}

/*
 * Sort an array of KeyValueTime by key and time
 */
/*void BufferedBTree::sortInternal(std::vector<KeyValueTime>* buffer) {


    std::sort(buffer->begin(), buffer->end(),[](KeyValueTime a, KeyValueTime b) -> bool
    {
        if(a.key == b.key) {
            return a.time < b.time;
        }
        else {
            return a.key < b.key;
        }
    } );

    /*std::sort(buffer->begin(), buffer->end(),[](KeyValueTime a, KeyValueTime b) -> bool
    {
        if(a->key == b->key) {
            return a->time < b->time;
        }
        else {
            return a->key < b->key;
        }
    } );

    /*bool ValueCmp(KeyValueTime const & a, KeyValueTime const & b)
    {
        return a.key < b.key;
    }
    std::sort(buffer,buffer+bSize,ValueCmp);*/
//}