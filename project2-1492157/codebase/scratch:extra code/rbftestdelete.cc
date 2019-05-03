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

int RBFTest_DELETE(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Record
    // 4. Read Record
    // 5. Close Record-Based File
    // 6. Destroy Record-Based File
    cout << endl << "***** In RBF Test Case DELETE *****" << endl;

    RC rc;
    string fileName = "DELETE";

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

    void *pageDataV = malloc(PAGE_SIZE);
    // char *pageData = (char *)pageDataV;

    vector<Attribute> recordDescriptor;
    createRecordDescriptor(recordDescriptor);

    // Initialize a NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
	memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // check offsets in beginning
    fileHandle.readPage(rid.pageNum, pageDataV);
    unsigned freeSpaceOffsetV2 = rbfm->getPageFreeSpaceSize(pageDataV); 
    cout << endl << "freeSpaceOffsetV2:" << freeSpaceOffsetV2 << endl;
    

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
    unsigned freeSpaceOffsetV3 = rbfm->getPageFreeSpaceSize(pageDataV); 
    cout << endl << "freeSpaceOffsetV3:" << freeSpaceOffsetV3 << endl;
    

    void *attrData = malloc(100);
    // READ ATTRIBUTE
     rc = rbfm->readAttribute(fileHandle, recordDescriptor, rid, "Age", attrData);
     assert(rc == success && "Reading an Attribute should not fail.");


    // DELETE RECORD
    // rc = rbfm->deleteRecord(fileHandle, recordDescriptor, rid);
    // assert(rc == success && "Deleting a record should not fail.");


    // check the slot of the deleted directory 
    cout << "test" << endl; 

    // check offsets in the end

    fileHandle.readPage(rid.pageNum, pageDataV);
    unsigned freeSpaceOffsetV4 = rbfm->getPageFreeSpaceSize(pageDataV); 

    cout << endl << "freeSpaceOffsetV4:" << freeSpaceOffsetV4 << endl;
    

    // READ RECORD AGAIN, Given the rid, read the record from file
    // rc = rbfm->readRecord(fileHandle, recordDescriptor, rid, returnedData);
    // assert(rc == success && "Reading a record should not fail.");

    // cout << endl << "Returned Data:" << endl;
    // rbfm->printRecord(recordDescriptor, returnedData);

   // Compare whether the two memory blocks are the different
    // if(memcmp(record, returnedData, recordSize) == 0)
    // {
    //     cout << "[FAIL] Test Case DELETE Failed!" << endl << endl;
    //     free(record);
    //     free(returnedData);
    //     return -1;
    // }

 //    cout << endl;

 //    // Close the file "DELETE"
 //    rc = rbfm->closeFile(fileHandle);
 //    assert(rc == success && "Closing the file should not fail.");

 //    // Destroy the file
 //    rc = rbfm->destroyFile(fileName);
 //    assert(rc == success && "Destroying the file should not fail.");

	// rc = destroyFileShouldSucceed(fileName);
 //    assert(rc == success  && "Destroying the file should not fail.");

 //    free(record);
 //    free(returnedData);

 //    cout << "RBF Test Case DELETE Finished! The result will be examined." << endl << endl;

    return 0;
}

int main()
{
    // To test the functionality of the record-based file manager
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance();

    remove("DELETE");

    RC rcmain = RBFTest_DELETE(rbfm);
    return rcmain;
}
