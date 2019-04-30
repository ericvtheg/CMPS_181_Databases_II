#include <fstream>
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

int RBFTest_scan(RecordBasedFileManager *rbfm) {
    // Functions tested
    // 1. Create Record-Based File
    // 2. Open Record-Based File
    // 3. Insert Multiple Records
    // 4. Close Record-Based File
    cout << endl << "***** In RBF Test Case SCAN *****" << endl;
   
    RC rc;
    string fileName = "SCAN";

    // Create a file named "test9"
    rc = rbfm->createFile(fileName);
    assert(rc == success && "Creating the file should not fail.");

    rc = createFileShouldSucceed(fileName);
    assert(rc == success && "Creating the file failed.");

    // Open the file "test9"
    FileHandle fileHandle;
    rc = rbfm->openFile(fileName, fileHandle);
    assert(rc == success && "Opening the file should not fail.");

    RID rid; 
    void *record = malloc(1000);
    int numRecords = 100;

    vector<Attribute> recordDescriptor;
    createLargeRecordDescriptor(recordDescriptor);

    for(unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        cout << "Attr Name: " << recordDescriptor[i].name << " Attr Type: " << (AttrType)recordDescriptor[i].type << " Attr Len: " << recordDescriptor[i].length << endl;
    }
    cout << endl;
    
    // NULL field indicator
    int nullFieldsIndicatorActualSize = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullFieldsIndicatorActualSize);
    memset(nullsIndicator, 0, nullFieldsIndicatorActualSize);

    // Insert 2000 records into file
    for(int i = 0; i < numRecords; i++)
    {
        // Test insert Record
        int size = 0;
        memset(record, 0, 1000);
        prepareLargeRecord(recordDescriptor.size(), nullsIndicator, i, record, &size);

        rc = rbfm->insertRecord(fileHandle, recordDescriptor, record, rid);
        rbfm -> printRecord(recordDescriptor, record);
        assert(rc == success && "Inserting a record should not fail.");
       
    }

    vector<string> attrName;
    //void *attrData = malloc(100);
    createLargeWantedAttrNameVector(attrName);
     //rc = rbfm->readRecordWithAttrs(fileHandle, recordDescriptor, attrName, rid, attrData);
    // assert(rc == success && "Reading a record should not fail.");

    vector<Attribute> attrDesc ;
    createLargeWantedAttrNameDesc(attrDesc);
    // cout << endl << "Returned Data:" << endl;
    //rbfm->printRecord(attrDesc, attrData);

    //TESTING THE SCAN ITERATOR
    cout << " returned scan records: " << endl;
    int age  = 24;
    void *returnedDataScan = malloc(100);
    RBFM_ScanIterator rbfmsi;
    rc = rbfm-> scan(fileHandle, recordDescriptor, "Int0", LE_OP, &age, attrName, rbfmsi);  
    while(rbfmsi.getNextRecord(rid, returnedDataScan) != RBFM_EOF){
       // fetch one tuple at at time and process the data;
        rbfm->printRecord(attrDesc, returnedDataScan);
        memset(record, 0, 1000);

    }
    //rbfmsi.close();

    free(record);
    rc = rbfm->closeFile(fileHandle);
    assert(rc == success && "Closing the file should not fail.");


    return 0;
}

int main()
{
    // To test the functionality of the record-based file manager 
    RecordBasedFileManager *rbfm = RecordBasedFileManager::instance(); 

    remove("SCAN");

    RC rcmain = RBFTest_scan(rbfm);
    return rcmain;
}