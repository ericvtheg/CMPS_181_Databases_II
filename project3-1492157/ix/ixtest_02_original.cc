#include <iostream>

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <cassert>

#include "ix.h"
#include "ix_test_util.h"

IndexManager *indexManager;

int testCase_2(const string &indexFileName, const Attribute &attribute)
{
    // Functions tested
    // 1. Open Index file
    // 2. Insert entry **
    // 3. Disk I/O check of Insertion - CollectCounterValues **
    // 4. print B+ Tree **
    // 5. Close Index file
    // NOTE: "**" signifies the new functions being tested in this test case.
    cerr << endl << "***** In IX Test Case 2 *****" << endl;

    RID rid;
    int key = 200;
    rid.pageNum = 500;
    rid.slotNum = 20;

    unsigned readPageCount = 0;
    unsigned writePageCount = 0;
    unsigned appendPageCount = 0;
    unsigned readPageCountAfter = 0;
    unsigned writePageCountAfter = 0;
    unsigned appendPageCountAfter = 0;
    unsigned readDiff = 0;
    unsigned writeDiff = 0;
    unsigned appendDiff = 0;

    // open index file
    IXFileHandle ixfileHandle;
    RC rc = indexManager->openFile(indexFileName, ixfileHandle);
    assert(rc == success && "indexManager::openFile() should not fail.");

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCount, writePageCount, appendPageCount);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << endl << "Before Insert - R W A: " << readPageCount << " " <<  writePageCount << " " << appendPageCount << endl;

    // insert entry
    rc = indexManager->insertEntry(ixfileHandle, attribute, &key, rid);
    assert(rc == success && "indexManager::insertEntry() should not fail.");

    // collect counters
    rc = ixfileHandle.collectCounterValues(readPageCountAfter, writePageCountAfter, appendPageCountAfter);
    assert(rc == success && "indexManager::collectCounterValues() should not fail.");

    cerr << "After Insert - R W A: " << readPageCountAfter << " " <<  writePageCountAfter << " " << appendPageCountAfter << endl;

    readDiff = readPageCountAfter - readPageCount;
    writeDiff = writePageCountAfter - writePageCount;
    appendDiff = appendPageCountAfter - appendPageCount;

    cerr << "Page I/O count of single insertion - R W A: " << readDiff << " " << writeDiff << " " << appendDiff << endl;

    if (readDiff == 0 && writeDiff == 0 && appendDiff == 0) {
        cerr << "Insertion should generate some page I/O. The implementation is not correct." << endl;
        rc = indexManager->closeFile(ixfileHandle);
        return fail;
    } 

    // print BTree, by this time the BTree should have only one node
    cerr << endl;
    indexManager->printBtree(ixfileHandle, attribute);

    // close index file
    rc = indexManager->closeFile(ixfileHandle);
    assert(rc == success && "indexManager::closeFile() should not fail.");

    return success;
}

int main()
{
    // Global Initialization
    indexManager = IndexManager::instance();

    const string indexFileName = "age_idx";
    Attribute attrAge;
    attrAge.length = 4;
    attrAge.name = "age";
    attrAge.type = TypeInt;

    RC result = testCase_2(indexFileName, attrAge);
    if (result == success) {
        cerr << "***** IX Test Case 2 finished. The result will be examined. *****" << endl;
        return success;
    } else {
        cerr << "***** [FAIL] IX Test Case 2 failed. *****" << endl;
        return fail;
    }

}
