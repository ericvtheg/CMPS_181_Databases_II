#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;

RecordBasedFileManager* RecordBasedFileManager::instance()
{
    if(!_rbf_manager)
        _rbf_manager = new RecordBasedFileManager();

    return _rbf_manager;
}

RecordBasedFileManager::RecordBasedFileManager()
{
}

RecordBasedFileManager::~RecordBasedFileManager()
{
}

/*
This method creates an empty-paged file called fileName.
The file should not already exist. This method should not
create any pages in the file.
*/
RC RecordBasedFileManager::createFile(const string &fileName) {

    return -1;
}

/*
This method destroys the paged file whose name is fileName.
The file should already exist.
*/

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return -1;
}

/*
This method opens the paged file whose name is fileName.
The file must already exist (and been created using the
createFile method). If the open method is successful, the
fileHandle object whose address is passed in as a parameter
now becomes a "handle" for the open file. This file handle
is used to manipulate the pages of the file (see the FileHandle
class description below). It is an error if fileHandle is already
a handle for some open file when it is passed to the openFile
method. It is not an error to open the same file more than once
if desired, but this would be done by using a different fileHandle
object each time. Each call to the openFile method creates a new
"instance" of the open file. Warning: Opening a file more than
once for data modification is not prevented by the PF component,
but doing so is likely to corrupt the file structure and may
crash the PF component. (You do not need to try and prevent this,
as you can assume the layer above is "friendly" in that regard.)
Opening a file more than once for reading is no problem.
*/

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return -1;
}

/*
This method closes the open file instance referred to by
fileHandle. (The file should have been opened using the
openFile method.) All of the file's pages are flushed to
disk when the file is closed.
*/

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return -1;
}

/*
Given a record descriptor, read the record identified by the given rid.
*/

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    return -1;
}

/*
Given a record descriptor, delete the record identified
by the given rid. Also, each time when a record is
deleted, you will need to compact that page. That is,
keep the free space in the middle of the page -- the
slot table will be at one end, the record data area
will be at the other end, and the free space should be in the middle.
*/

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    return -1;
}

/*
This is a utility method that will be mainly used for
debugging/testing. It should be able to interpret the
bytes of each record using the passed-in record descriptor
and then print its content to the screen. For instance,
suppose a record consists of two fields: age (int) and
height (float), which means the record will be of size 9
(1 byte for the null-fields-indicator, 4 bytes for int,
and 4 bytes for float). The printRecord method should
recognize the record format using the record descriptor.
It should then check the null-fields-indicator to skip
certain fields if there are any NULL fields. Then, it
should be able to convert the four bytes after the first
byte into an int object and the last four bytes to a float
object and print their values. It should also print NULL for
those fields that are skipped because they are null. Thus,
an example for three records would be:
*/

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
    return -1;
}
