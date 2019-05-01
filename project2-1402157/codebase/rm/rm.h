
#ifndef _rm_h_
#define _rm_h_

#include <string>
#include <cstring>
#include <vector>
#include <cmath>
#include <iostream>

#include "../rbf/rbfm.h"

#define RM_CREATE_FAILED 1

using namespace std;

# define RM_EOF (-1)  // end of a scan operator

// RM_ScanIterator is an iteratr to go through tuples
class RM_ScanIterator {
public:
  RM_ScanIterator() {};
  ~RM_ScanIterator() {};

  // "data" follows the same format as RelationManager::insertTuple()
  RC getNextTuple(RID &rid, void *data) { return RM_EOF; };
  RC close() { return -1; };
};


// Relation Manager
class RelationManager
{
public:
  static RelationManager* instance();
friend class RecordBasedFileManager;
  RC createCatalog();

  RC createTableDesc(vector<Attribute> &retVec);

  void prepareTableRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int tableid, const int namesize, const string &name, const int filenamesize, const string &filename, void *buffer, int *recordSize);

  RC createColumnDesc(vector<Attribute> &retVec);

  void prepareColumnRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int tableid, const int columnnamesize, const string &columnname, const int columntype, const int columnlength,const int columnposition, void *buffer, int *recordSize);

  RC getLastTblID(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, void * data);
 
  RC deleteCatalog();

  RC createTable(const string &tableName, const vector<Attribute> &attrs);

  RC deleteTable(const string &tableName);

  RC getAttributes(const string &tableName, vector<Attribute> &attrs);

  RC insertTuple(const string &tableName, const void *data, RID &rid);

  RC deleteTuple(const string &tableName, const RID &rid);

  RC updateTuple(const string &tableName, const void *data, const RID &rid);

  RC readTuple(const string &tableName, const RID &rid, void *data);

  RC prepareRecord(int fieldCount, unsigned char *nullFieldsIndicator, const int tableid, const int namesize, const string &name, const int filenamesize, const string $filename, void *buffer, int *recordSize);

  // Print a tuple that is passed to this utility method.
  // The format is the same as printRecord().
  RC printTuple(const vector<Attribute> &attrs, const void *data);

  RC readAttribute(const string &tableName, const RID &rid, const string &attributeName, void *data);

  // Scan returns an iterator to allow the caller to go through the results one by one.
  // Do not store entire results in the scan iterator.
  RC scan(const string &tableName,
      const string &conditionAttribute,
      const CompOp compOp,                  // comparison type such as "<" and "="
      const void *value,                    // used in the comparison
      const vector<string> &attributeNames, // a list of projected attributes
      RM_ScanIterator &rm_ScanIterator);




protected:
  RelationManager();
  ~RelationManager();

private:
  static RelationManager *_rm;
  static RecordBasedFileManager *_rbf_manager;
};

#endif
