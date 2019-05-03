
#include "rm.h"

RelationManager* RelationManager::_rm = NULL;
RecordBasedFileManager *RelationManager::_rbf_manager = NULL;

RelationManager* RelationManager::instance()
{
    if(!_rm)
        _rm = new RelationManager();

    return _rm;
}

RelationManager::RelationManager()
{
    _rbf_manager = RecordBasedFileManager::instance();
}

RelationManager::~RelationManager()
{

}

RC RelationManager::createCatalog()
{
    FileHandle fileHandle;
    string fileName = "Tables.tbl";

    // if (_rbf_manager->openFile(fileName, fileHandle)){
    //     ;
    // }else{
    //     cout << "Error: Table table already exists." << endl;
    //     return RBFM_CREATE_FAILED;
    // }

    // initialize catalog tables
    if (_rbf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    if (_rbf_manager->openFile(fileName.c_str(), fileHandle))
        return RBFM_OPEN_FAILED;

    vector<Attribute> sysTblVec;
    createTableDesc(sysTblVec);
    int nullIndicatorSize = int(ceil((double) sysTblVec.size()/ CHAR_BIT));
    unsigned char * nullsIndicator = (unsigned char *)(malloc(nullIndicatorSize));
    memset(nullsIndicator, 0, nullIndicatorSize);

    void * data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    int datasize = 0;
    RID rid;

    prepareTableRecord((int) sysTblVec.size(), nullsIndicator, 1, 6, "Tables", 10, "Tables.tbl", data, &datasize);
    ////cout << "size: "<<datasize << endl;
    // _rbf_manager->printRecord(sysTblVec, data);
    _rbf_manager->insertRecord(fileHandle, sysTblVec, data, rid );
    //cout << "rid: (" <<  rid.slotNum << " ," << rid.pageNum <<")" << endl;


    vector<Attribute> sysColVec;
    createColumnDesc(sysColVec);
    memset(nullsIndicator, 0, nullIndicatorSize);
    memset(data, 0, PAGE_SIZE);

    prepareTableRecord(sysTblVec.size(), nullsIndicator, 2, 7, "Columns", 11 , "Columns.tbl", data, &datasize);
    // _rbf_manager->printRecord(sysTblVec, data);
    _rbf_manager->insertRecord(fileHandle, sysTblVec, data, rid );
    //cout << "rid: (" <<  rid.slotNum << " ," << rid.pageNum <<")" << endl;
    _rbf_manager->closeFile(fileHandle);

    fileName = "Columns.tbl";


    // if (_rbf_manager->openFile(fileName, fileHandle)){
    //
    // }else{
    //     cout << "Error: Column table already exists." << endl;
    //     return RBFM_CREATE_FAILED;
    // }

    if (_rbf_manager->createFile(fileName)){
    	free(data);
        free(nullsIndicator);
        return RM_CREATE_FAILED;
    }


    if (_rbf_manager->openFile(fileName.c_str(), fileHandle)){
        free(data);
    	free(nullsIndicator);
        return RBFM_OPEN_FAILED;
    }

    for(unsigned i = 0; i < sysTblVec.size() ; i++){
        memset(nullsIndicator, 0, nullIndicatorSize);
        memset(data, 0, PAGE_SIZE);
        prepareColumnRecord((int)sysColVec.size(), nullsIndicator, 1, sysTblVec[i].name.length(), sysTblVec[i].name , sysTblVec[i].type, sysTblVec[i].length ,i + 1, data, &datasize);
        //prepareColumnRecord(sysColVec.size(), nullsIndicator, 1,varcharlen, sysTblVec[i].name , sysTblVec[i].type, sysTblVec[i].length ,i + 1, data, datasize);
        _rbf_manager->insertRecord(fileHandle, sysColVec, data, rid );
        //cout << "rid: (" <<  rid.slotNum << " ," << rid.pageNum <<")" << endl;
        // _rbf_manager->printRecord(sysColVec, data);

    }

    for(unsigned i = 0; i < sysColVec.size() ; i++){
        memset(nullsIndicator, 0, nullIndicatorSize);
        memset(data, 0, PAGE_SIZE);
        prepareColumnRecord((int)sysColVec.size(), nullsIndicator, 2, sysColVec[i].name.length(), sysColVec[i].name , sysColVec[i].type, sysColVec[i].length ,i + 1, data, &datasize);
        //prepareColumnRecord(sysColVec.size(), nullsIndicator, 2, varcharlen, sysColVec[i].name , sysColVec[i].type, sysColVec[i].length ,i + 1, data, datasize);

        _rbf_manager->insertRecord(fileHandle, sysColVec, data, rid );
        //cout << "rid: (" <<  rid.slotNum << " ," << rid.pageNum <<")" << endl;
        // _rbf_manager->printRecord(sysColVec, data);

    }

    _rbf_manager->closeFile(fileHandle);

free(data);
free(nullsIndicator);
return SUCCESS;

}

// Create an employee table
RC RelationManager::createTableDesc(vector<Attribute> &retVec)
{

    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    retVec.push_back(attr);

    attr.name = "table-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    retVec.push_back(attr);

    attr.name = "file-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    retVec.push_back(attr);

    return SUCCESS;
}

// Create an employee table
RC RelationManager::createColumnDesc(vector<Attribute> &retVec)
{

    Attribute attr;

    attr.name = "table-id";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    retVec.push_back(attr);

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    retVec.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    retVec.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    retVec.push_back(attr);

    attr.name = "column-position";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    retVec.push_back(attr);

    return SUCCESS;
}

// Create an employee table
RC RelationManager::createNewDesc(vector<Attribute> &retVec)
{

    Attribute attr;

    attr.name = "column-name";
    attr.type = TypeVarChar;
    attr.length = (AttrLength)50;
    retVec.push_back(attr);

    attr.name = "column-type";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    retVec.push_back(attr);

    attr.name = "column-length";
    attr.type = TypeInt;
    attr.length = (AttrLength)4;
    retVec.push_back(attr);

    return SUCCESS;
}


// Function to prepare the data in the correct form to be inserted/read
void RelationManager::prepareTableRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int tableid, const int namesize, const string &name, const int filenamesize, const string &filename, void *buffer, int *recordSize)
{
    int offset = 0;

    // Null-indicators
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);
    //cout <<"nullIndicatorSize: " << nullFieldsIndicatorActualSize << endl;

    // Null-indicator for the fields
    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // Beginning of the actual data
    // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
    // e.g., if a record consists of four fields and they are all nulls, then the bit representation will be: [11110000]
    nullBit = nullFieldsIndicator[0] & (1 << 7);
    ////cout << "nullbit1: " << nullBit << endl;
    if (!nullBit) {
        memcpy((char *)buffer + offset, &tableid, sizeof(int));
        offset += sizeof(int);
        //cout << "int offset: " << offset << endl;
    }
    nullBit = nullFieldsIndicator[0] & (1 << 6);
    ////cout << "nullbit2: " << nullBit << endl;
    if (!nullBit) {
        memcpy((char *)buffer + offset, &namesize, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, name.c_str(), namesize);
        offset += namesize;
        //cout << "namesize offset: " << offset << endl;
    }
    nullBit = nullFieldsIndicator[0] & (1 << 5);
    ////cout << "nullbit3: " << nullBit << endl;
    if (!nullBit) {
        memcpy((char *)buffer + offset, &filenamesize, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, filename.c_str(), filenamesize);
        offset += filenamesize;
        //cout << "filenamesize offset: " << offset << endl;
    }

    *recordSize = offset;
}


void RelationManager::prepareColumnRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int tableid, const int columnnamesize, const string &columnname, const int columntype, const int columnlength,const int columnposition, void *buffer, int *recordSize)
{
    int offset = 0;

    // Null-indicators
    bool nullBit = false;
    int nullFieldsIndicatorActualSize = ceil((double) fieldCount / CHAR_BIT);


    // Null-indicator for the fields
    memcpy((char *)buffer + offset, nullFieldsIndicator, nullFieldsIndicatorActualSize);
    offset += nullFieldsIndicatorActualSize;

    // Beginning of the actual data
    // Note that the left-most bit represents the first field. Thus, the offset is 7 from right, not 0.
    // e.g., if a record consists of four fields and they are all nulls, then the bit representation will be: [11110000]

    nullBit = nullFieldsIndicator[0] & (1 << 7);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &tableid, sizeof(int));
        offset += sizeof(int);
    }

    nullBit = nullFieldsIndicator[0] & (1 << 6);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &columnnamesize, sizeof(int));
        offset += sizeof(int);
        memcpy((char *)buffer + offset, columnname.c_str(), columnnamesize);
        offset += columnnamesize;
    }

    nullBit = nullFieldsIndicator[0] & (1 << 5);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &columntype, sizeof(int));
        offset += sizeof(int);
    }

    nullBit = nullFieldsIndicator[0] & (1 << 4);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &columnlength, sizeof(int));
        offset += sizeof(int);
    }

    nullBit = nullFieldsIndicator[0] & (1 << 3);
    if (!nullBit) {
        memcpy((char *)buffer + offset, &columnposition, sizeof(int));
        offset += sizeof(int);
    }

    *recordSize = offset;

}

RC RelationManager::deleteCatalog()
{

    // deleteTable("Tables.tbl");
    // deleteTable("Columns.tbl");
    _rbf_manager->destroyFile("Tables.tbl");
    _rbf_manager->destroyFile("Columns.tbl");

    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{

    FileHandle fileHandle;

    // edit the catalog
     // if (_rbf_manager->openFile("Tables.tbl", fileHandle)){
     //
     // }else{
     //     cout << "Error: table already exists." << endl;
     //     return RBFM_CREATE_FAILED;
     // }


    // create new table, should start after catalogs were made// why do we have so many tables
    if (_rbf_manager->createFile(tableName))
        return RM_CREATE_FAILED;

        // edit the catalog
         if (_rbf_manager->openFile("Tables.tbl", fileHandle))
            return RM_CREATE_FAILED;

    //Setting up the recordDescriptors for insertion of new data
    vector<Attribute> sysTblVec;
    vector<Attribute> sysColVec;
    createTableDesc(sysTblVec);
    createColumnDesc(sysColVec);

    //Set up Null indicators
    int nullIndicatorSize = int(ceil((double) sysTblVec.size()/ CHAR_BIT));
    unsigned char * nullsIndicator = (unsigned char *)(malloc(nullIndicatorSize));
    memset(nullsIndicator, 0, nullIndicatorSize);


    void * data = malloc(PAGE_SIZE);
    memset(data, 0, PAGE_SIZE);
    int datasize = 0;
    //Given the Table.tbl file  find the most recent id to give the new table

    int lastTblID;
    RID rid;
    getLastTblID(fileHandle,  sysTblVec , &lastTblID);
    //cout << "Laste Records: " << lastTblID << endl;

    memset(nullsIndicator, 0, nullIndicatorSize);
    memset(data, 0, PAGE_SIZE);

    prepareTableRecord((int) sysTblVec.size(), nullsIndicator, lastTblID + 1 , tableName.length(), tableName , tableName.length(), tableName , data, &datasize);
    //cout << "size: "<<datasize << endl;
    // _rbf_manager->printRecord(sysTblVec, data);
    _rbf_manager->insertRecord(fileHandle, sysTblVec, data, rid );

    _rbf_manager->closeFile(fileHandle);

     if (_rbf_manager->openFile("Columns.tbl", fileHandle))
        return RM_CREATE_FAILED;

    for(unsigned i = 0; i < attrs.size() ; i++){
        memset(nullsIndicator, 0, nullIndicatorSize);
        memset(data, 0, PAGE_SIZE);
        prepareColumnRecord((int)sysColVec.size(), nullsIndicator, lastTblID + 1, attrs[i].name.length(), attrs[i].name , attrs[i].type, attrs[i].length ,i + 1, data, &datasize);
        //prepareColumnRecord(sysColVec.size(), nullsIndicator, 2, varcharlen, sysColVec[i].name , sysColVec[i].type, sysColVec[i].length ,i + 1, data, datasize);

        _rbf_manager->insertRecord(fileHandle, sysColVec, data, rid );
        //cout << "rid: (" <<  rid.slotNum << " ," << rid.pageNum <<")" << endl;
        // _rbf_manager->printRecord(sysColVec, data);

    }

    //cout << "HERE" << endl;
    _rbf_manager->closeFile(fileHandle);
    free(data);
    free(nullsIndicator);

    //cout << "HERE" << endl;

    return SUCCESS;
}

RC RelationManager::getLastTblID(FileHandle &fileHandle,  const vector<Attribute> &recordDescriptor, void * data){

   void *pageData = malloc(PAGE_SIZE);

   if (pageData == NULL)
       return RBFM_MALLOC_FAILED;

   //Grabd the very last page
   unsigned numPages = fileHandle.getNumberOfPages();
   if (fileHandle.readPage(numPages - 1, pageData))
           return RBFM_READ_FAILED;

   SlotDirectoryHeader slotHeader = _rbf_manager->getSlotDirectoryHeader(pageData);

   //Using the header, grab the last record entry for a read
   SlotDirectoryRecordEntry recordEntry = _rbf_manager->getSlotDirectoryRecordEntry(pageData, slotHeader.recordEntriesNumber - 1);

   // Retrieve the actual tableID
   _rbf_manager->getRecordAttrAtOffset(pageData, recordEntry.offset , recordDescriptor, "table-id", data);

   return SUCCESS;

}

RC RelationManager::deleteTable(const string &tableName)
{
    FileHandle fileHandle;
    vector<Attribute> s;
    createTableDesc(s);
    vector<string> n;
    RBFM_ScanIterator rbfmsi;
    void * returnedDataScan = malloc(PAGE_SIZE);
    RID new_rid;

    // if ( _rbf_manager->openFile(tableName, fileHandle))
    //     return RBFM_OPEN_FAILED;
    // _rbf_manager->closeFile(fileHandle);


    n.push_back("table-name");

    void *valuePointer = malloc(PAGE_SIZE);
    int bytes = tableName.length();

    memcpy(valuePointer, &bytes, INT_SIZE);
    memcpy((char*) valuePointer + INT_SIZE, tableName.c_str(), bytes);

    // edit the catalog
    // _rbf_manager->scan(fileHandle, s, "table-name", EQ_OP, valuePointer, n, rbfmsi);
    // while(rbfmsi.getNextRecord(new_rid, returnedDataScan) == SUCCESS){
    // }

    //cout << "hit before delete record *****************************************" << endl;

    // destroy given table
    if ( _rbf_manager->destroyFile(tableName))
        return RM_CREATE_FAILED;

    // _rbf_manager->deleteRecord(fileHandle, s,new_rid);


    //cout << "hit after delete record *****************************************" << endl;

    free(returnedDataScan);
    free(valuePointer);
    return SUCCESS;
}


RC RelationManager::parseDataScan(void * returnedDataScan, vector<Attribute> &newColDesc, vector<Attribute> &newColAttr){
    char * iter_ptr = (char * )returnedDataScan;
    Attribute newAtt;
    int varCharSize = 0;
    int offset = 1;

    memcpy(&varCharSize, iter_ptr + offset, INT_SIZE);
    offset += INT_SIZE;
    char * data_ptr = (char*) malloc(newColDesc[0].length);
    memcpy(data_ptr, iter_ptr + offset, varCharSize );
    data_ptr[varCharSize] = '\0';
    string dataString(data_ptr);
    newAtt.name = dataString;
    //cout << "newAtt.name: " << newAtt.name << endl;
    offset += varCharSize;

    memcpy(&newAtt.type, iter_ptr + offset, INT_SIZE );
    //cout << "newAtt.type: " << newAtt.type << endl;
    offset += INT_SIZE;

    memcpy(&newAtt.length, iter_ptr + offset, INT_SIZE );
    //cout << "newAtt.length: " << newAtt.length << endl;
    offset += INT_SIZE;

    newColAttr.push_back(newAtt);

    //memcpy(&newAtt.position, iter_ptr + offset, INT_SIZE );
    ////cout << "newAtt.position: " << newAtt.position << endl;

    free(data_ptr);

    return SUCCESS;

}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{
    // initalize
    vector<Attribute> sysTblVec;
    vector<Attribute> sysColVec;
    createTableDesc(sysTblVec);
    createColumnDesc(sysColVec);

    FileHandle fileHandle;
    RID new_rid;

    _rbf_manager->openFile("Tables.tbl", fileHandle);

    RBFM_ScanIterator rbfmsi;

    vector<string> attrNames;
    attrNames.push_back("table-id");
    void *returnedDataScan = malloc(PAGE_SIZE);

    // cout << "tableName length: "<<  tableName.length() << endl;
    // cout << "tableName size: "<<  sizeof(tableName) << endl;

    void *valuePointer = malloc(PAGE_SIZE);
    int32_t bytes = tableName.length();

    memcpy(valuePointer, &bytes, INT_SIZE);
    memcpy((char*) valuePointer + INT_SIZE, tableName.c_str(), bytes);

    _rbf_manager->scan(fileHandle, sysTblVec, "table-name", EQ_OP, valuePointer, attrNames, rbfmsi);

    // cout << "valuePointer: " << valuePointer << endl;
    // cout << "returnedDataScan: " << returnedDataScan << endl;
    // cout << "hit before 1st getNextRecord" << endl;
    // _rbf_manager->printRecord(sysTblVec, returnedDataScan);
    while( rbfmsi.getNextRecord(new_rid, returnedDataScan) != EOF) {
        // _rbf_manager->printRecord(sysTblVec, returnedDataScan);
        //cout << "hit in 1st getNextRecord" << endl;
    }
     // cout << "hit after 1st getNextRecord" << endl;
    // _rbf_manager->printRecord(sysTblVec,returnedDataScan);

    _rbf_manager->closeFile(fileHandle);
    _rbf_manager->openFile("Columns.tbl", fileHandle);

    int tableid = 0;
    memcpy(&tableid, (char *)returnedDataScan + 1, INT_SIZE );
    // cout << "tableid after initial scan: " << tableid << endl;

    vector<string> colAttr;
    colAttr.push_back("column-name");
    colAttr.push_back("column-type");
    colAttr.push_back("column-length");
    colAttr.push_back("column-position");

    vector<Attribute> newColDesc;
    createNewDesc(newColDesc);

    _rbf_manager->scan(fileHandle, sysColVec, "table-id", EQ_OP, &tableid, colAttr, rbfmsi);

    // cout << "right before big while loop" << endl;
    while( rbfmsi.getNextRecord(new_rid, returnedDataScan) == SUCCESS) {
        //_rbf_manager->printRecord(sysColVec, returnedDataScan);
        parseDataScan(returnedDataScan, newColDesc, attrs);
        //cout << "hit in 1st getNextRecord" << endl;
    }
    // cout << "right after big while loop" << endl;


    free(valuePointer);
    free(returnedDataScan);

    _rbf_manager->closeFile(fileHandle);
    return SUCCESS;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    FileHandle fileHandle;

    vector<Attribute> recordDescriptor;
    if(_rbf_manager->openFile(tableName, fileHandle))
        return RBFM_OPEN_FAILED;
    getAttributes(tableName, recordDescriptor);
    _rbf_manager->insertRecord(fileHandle, recordDescriptor, data, rid);
    _rbf_manager->closeFile(fileHandle);
    return SUCCESS;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    FileHandle fileHandle;

    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    _rbf_manager->openFile(tableName, fileHandle);
    _rbf_manager->deleteRecord(fileHandle, recordDescriptor, rid);
    _rbf_manager->closeFile(fileHandle);
    return SUCCESS;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{
    FileHandle fileHandle;

    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    _rbf_manager->openFile(tableName, fileHandle);
    _rbf_manager->updateRecord(fileHandle, recordDescriptor, data, rid);
    _rbf_manager->closeFile(fileHandle);
    return SUCCESS;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    FileHandle fileHandle;

    if(_rbf_manager->openFile(tableName, fileHandle))
        return RBFM_OPEN_FAILED;

    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    RC rc = _rbf_manager->readRecord(fileHandle, recordDescriptor, rid, data);
    _rbf_manager->closeFile(fileHandle);
    return rc;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    _rbf_manager->printRecord(attrs, data);

	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    FileHandle fileHandle;

    vector<Attribute> recordDescriptor;
    getAttributes(tableName, recordDescriptor);
    _rbf_manager->openFile(tableName, fileHandle);
    RC rc = _rbf_manager->readAttribute(fileHandle, recordDescriptor, rid, attributeName, data);
    _rbf_manager->closeFile(fileHandle);
    return rc;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,
      const void *value,
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    vector<Attribute> sysTblVec;
    vector<Attribute> sysColVec;
    createTableDesc(sysTblVec);
    createColumnDesc(sysColVec);

    // vector<string> attrNames;
    // attrNames.push_back("table-id");
    FileHandle fileHandle;

    if(_rbf_manager->openFile(tableName, fileHandle))
        return RBFM_OPEN_FAILED;

    vector<Attribute> attrs;

    RBFM_ScanIterator rbfmsi;

    getAttributes(tableName, attrs);
    _rbf_manager->scan(fileHandle, attrs, conditionAttribute, compOp, value, attributeNames, rbfmsi);
    _rbf_manager->closeFile(fileHandle);
    return SUCCESS;
}
