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

int RBFTest_update(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case update *****" << endl;

    RC rc;
    string fileName = "update";

    // Create a file named "update"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

	rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file should not fail.");

    // Open the file "update"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid;
    int recordSize = 0;
    void *record = malloc(100);
    void *returnedData = malloc(100);
    void *returnedData2 = malloc(100);
    void *returnedData3 = malloc(100);

    void *pageDataV = malloc(PAGE_SIZE);
    // char *pageData = (char *)pageDataV;

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);

    vector<Attribute> recordDescriptor2;
    createRecordDescriptor(recordDescriptor2);

    vector<Attribute> recordDescriptor3;
    createRecordDescriptor(recordDescriptor3);


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


    RID soup;

    // Insert a record into a file and print the record
    prepareRecord(recordDescriptor2.size(), nullsIndicator, 10, "thirdsoups", 15, 183.1, 6688, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rbfm->printRecord(recordDescriptor2, record);

    rc = rbfm->insertRecord(fileHandle, recordDescriptor2, record, soup);
    assert(rc == success && "Inserting a record should not fail.");

    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor2, soup, returnedData2);
    assert(rc == success && "Reading a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor2, returnedData2);

    // check offsets in the middle
    fileHandle.readPage(soup.pageNum, pageDataV);
    unsigned freeSpaceSizeV5 = rbfm->getPageFreeSpaceSize(pageDataV); 
    cout << endl << "freeSpaceSizeV5:" << freeSpaceSizeV5 << endl;
    
    
    // update RECORD
    prepareRecord(recordDescriptor2.size(), nullsIndicator, 8, "12345678", 10, 14, 4848, record, &recordSize);
    cout << endl << "Inserting Data:" << endl;
    rc = rbfm->updateRecord(fileHandle, recordDescriptor3, record, mips);
    assert(rc == success && "Updating a record should not fail.");
    rbfm->printRecord(recordDescriptor3, record);


    // Given the rid, read the record from file
    rc = rbfm->readRecord(fileHandle, recordDescriptor, mips, returnedData3);
    assert(rc == success && "Reading a record should not fail.");

    cout << endl << "Returned Data:" << endl;
    rbfm->printRecord(recordDescriptor3, returnedData3);

     // check offsets in the end
    fileHandle.readPage(soup.pageNum, pageDataV);
    unsigned freeSpaceSizeV6 = rbfm->getPageFreeSpaceSize(pageDataV); 
    cout << endl << "freeSpaceSizeV6:" << freeSpaceSizeV6 << endl;


    // check the slot of the updated directory 
    cout << "test" << endl; 

    

   // Compare wfther the two memory blocks are the different
    // if(memcmp(record, returnedData, recordSize) == 0)
    // {
    //     cout << "[FAIL] Test Case update Failed!" << endl << endl;
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

    remove("update");

    RC rcmain = RBFTest_update(rbfm);
    return rcmain;
}
