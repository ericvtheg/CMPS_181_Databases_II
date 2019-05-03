#include <iostream>
#include <string>
#include <cassert>
#include <sys/stat.h>
#include <stdlib.h>
#include <string.h>
#include <stdexcept>
#include <stdio.h>

#include "pfm.h"
#include "rbfm.h"
#include "test_util.h"

using namespace std;

int RBFTest_DELETE2(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case DELETE *****" << endl;

    RC rc;
    string fileName = "DELETE2";

    // Create a file named "DELETE"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

	rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "DELETE"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid;
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);
    void *returnedData2 = malloc(100);

    void *pageDataV = malloc(PAGE_SIZE);
    // char *pageData = (char *)pageDataV;

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);

    vector<Attribute> recordDescriptor2;
    createRecordDescriptor(recordDescriptor2);


    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // check offsets in beginning
    fileHandle.readPage(rid.pageNum, pageDataV);
    unsigned freeSpaceSizeV2 = rbfm->getPageFreeSpaceSize(pageDataV); 
    cout << endl << "freeSpaceSizeV2:" << freeSpaceSizeV2 << endl;
    

    // Insert a record into a file and print the record
    prepareRecord(recordDescriptor.size(), nullsIndicator, 8, "UCSCSlug", 24, 170.1, 5000, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm->printRecord(recordDescriptor, record);

    rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
    assert(rc == success && "Inserting a record should not fail.");


    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
    assert(rc == success && "Reading a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor, returnedData);

    // check offsets in the middle
    
    fileHandle.readPage(rid.pageNum, pageDataV);
    unsigned freeSpaceSizeV3 = rbfm->getPageFreeSpaceSize(pageDataV); 
    cout << endl << "freeSpaceSizeV3:" << freeSpaceSizeV3 << endl;

    RID mips;

    // Insert a record into a file and print the record
    prepareRecord(recordDescriptor2.size(), nullsIndicator, 10, "UCSCCoding", 20, 180.1, 6969, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm->printRecord(recordDescriptor2, record);

    rc = rbfm->insertRecord(fileHandle, recordDescriptor2, record, mips);
    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor2, mips, returnedData2);
    assert(rc == success && "Reading a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor2, returnedData2);

    // check offsets in the middle
    fileHandle.readPage(mips.pageNum, pageDataV);
    unsigned freeSpaceSizeV4 = rbfm->getPageFreeSpaceSize(pageDataV); 
    cout << endl << "freeSpaceSizeV4:" << freeSpaceSizeV4 << endl;
    

    // DELETE RECORD
    rc = rbfm->deleteRecord(fileHandle, recordDescriptor2, rid);
    assert(rc == success && "Deleting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, mips, returnedData2);
    assert(rc == success && "Reading a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor, returnedData);


    // check the slot of the deleted directory 
    cout << "test" << endl; 

    // check offsets in the end

    fileHandle.readPage(rid.pageNum, pageDataV);
    unsigned freeSpaceSizeV5 = rbfm->getPageFreeSpaceSize(pageDataV); 

    cout << endl << "freeSpaceOffSizeV5:" << freeSpaceSizeV5 << endl;
    

   // Compare wfther the two memory blocks are the different
    // if(memcmp(record, returnedData, recordSize) == 0)
    // {
    //     cout << "[FAIL] Test Case DELETE Failed!" << endl << endl;
    //     free(record);
    //     free(returnedData);
    //     return -1;
    // }

    return 0;
}

int main()
{
    // To test the functionality of the record-based file manager
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    remove("DELETE2");

    RC rcmain = RBFTest_DELETE2(rbfm);
    return rcmain;
}
