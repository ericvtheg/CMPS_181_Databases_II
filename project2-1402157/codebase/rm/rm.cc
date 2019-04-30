
#include "rm.h"

RelationManager* RelationManager::_rm = 0;
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
    // initialize catalog tables
    if (_rbf_manager->createFile("Tables.tbl"))
        return RM_CREATE_FAILED;

    if (_rbf_manager->createFile("Columns.tbl"))
        return RM_CREATE_FAILED;

return 0;

}

RC RelationManager::deleteCatalog()
{

    _rbf_manager->destroyFile("Tables.tbl");
    _rbf_manager->destroyFile("Columns.tbl");

    return 0;
}

RC RelationManager::createTable(const string &tableName, const vector<Attribute> &attrs)
{

    FileHandle fileHandle; 

    // create new table, should start after catalogs were made// why do we have so many tables
    if (_rbf_manager->createFile(tableName + ".tbl"))
        return RM_CREATE_FAILED;

    // edit the catalog
     if (_rbf_manager->openFile("Tables.tbl", fileHandle))
        return RM_CREATE_FAILED;


    // "with a vector of attributes (attrs)"
    // idk how we want to implement this
    vector<Attribute> table_attrs; 
    vector<Attribute> column_attrs;

    return -1;
}

RC RelationManager::deleteTable(const string &tableName)
{   
    FileHandle fileHandle;

    // destroy given table given
    if ( _rbf_manager->destroyFile(tableName + ".tbl"))
        RM_CREATE_FAILED;

    // edit the catalog
    if (_rbf_manager->openFile("Tables.tbl", fileHandle))
        _rbf_manager->closeFile(fileHandle);
        return RM_CREATE_FAILED;

    return -1;
}

RC RelationManager::getAttributes(const string &tableName, vector<Attribute> &attrs)
{

    // not sure what this does over a read?
   return 0;
    // return -1;
}

RC RelationManager::insertTuple(const string &tableName, const void *data, RID &rid)
{
    _rbf_manager->insertRecord(fh, recordDescriptor, data, rid);
    return -1;
}

RC RelationManager::deleteTuple(const string &tableName, const RID &rid)
{
    _rbf_manager->deleteRecord(fh, recordDescriptor, rid);
    return -1;
}

RC RelationManager::updateTuple(const string &tableName, const void *data, const RID &rid)
{

    _rbf_manager->updateRecord(fileHandle, recordDescriptor, data, rid);
    return -1;
}

RC RelationManager::readTuple(const string &tableName, const RID &rid, void *data)
{
    return -1;
}

RC RelationManager::printTuple(const vector<Attribute> &attrs, const void *data)
{
    _rbf_manager->printRecord(attrs, data);
	
	return 0;
}

RC RelationManager::readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data)
{
    
    return -1;
}

RC RelationManager::scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  
      const void *value,                    
      const vector<string> &attributeNames,
      RM_ScanIterator &rm_ScanIterator)
{
    return -1;
}



