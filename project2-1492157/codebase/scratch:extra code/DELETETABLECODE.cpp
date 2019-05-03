RC RelationManager::deleteTable(const string &tableName)
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

    void *valuePointer = malloc(PAGE_SIZE);
    void *returnedDataScan = malloc(PAGE_SIZE);
    int32_t bytes = tableName.length();

    memcpy(valuePointer, &bytes, INT_SIZE);
    memcpy((char*) valuePointer + INT_SIZE, tableName.c_str(), bytes);

    _rbf_manager->scan(fileHandle, sysTblVec, "table-name", EQ_OP, valuePointer, attrNames, rbfmsi);

    while( rbfmsi.getNextRecord(new_rid, returnedDataScan) == SUCCESS) {
    }
    cout << "(RID ) " << new_rid.slotNum << ", " <<new_rid.pageNum <<endl;
    _rbf_manager->deleteRecord(fileHandle, sysTblVec ,new_rid);

    _rbf_manager->closeFile(fileHandle);

    if ( _rbf_manager->destroyFile(tableName))
        return RM_CREATE_FAILED;

    int tableid = 0;
    memcpy(&tableid, (char *)returnedDataScan + 1, INT_SIZE );
    cout << "tableid after initial scan: " << tableid << endl;
  
    _rbf_manager->openFile("Columns.tbl", fileHandle);

    vector<string> colAttr;
    colAttr.push_back("column-name");
    colAttr.push_back("column-type");
    colAttr.push_back("column-length");
    colAttr.push_back("column-position");

    _rbf_manager->scan(fileHandle, sysColVec, "table-id", EQ_OP, &tableid, colAttr, rbfmsi);

    memset(returnedDataScan, 0 , PAGE_SIZE);
    // cout << "right before big while loop" << endl;
    while( rbfmsi.getNextRecord(new_rid, returnedDataScan) != EOF) {
        //_rbf_manager->printRecord(sysColVec, returnedDataScan);
        
        _rbf_manager->deleteRecord(fileHandle, sysColVec ,new_rid);
        cout << "(RID ) " << new_rid.slotNum << ", " <<new_rid.pageNum <<endl;
        memset(returnedDataScan, 0 , PAGE_SIZE);
        //cout << "hit in 1st getNextRecord" << endl;
    }


    _rbf_manager->closeFile(fileHandle);

    //cout << "hit after delete record *****************************************" << endl;

    free(returnedDataScan);
    free(valuePointer);
    return SUCCESS;
}