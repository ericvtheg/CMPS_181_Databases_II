#include "pfm.h"

PagedFileManager* PagedFileManager::_pf_manager = 0;

PagedFileManager* PagedFileManager::instance()
{
    if(!_pf_manager)
        _pf_manager = new PagedFileManager();

    return _pf_manager;
}

PagedFileManager::PagedFileManager()
{
}

PagedFileManager::~PagedFileManager()
{
}

/*
This method creates a record-based file called fileName.
The file should not already exist. Please note that this
method should internally use the method
PagedFileManager::createFile (const char *fileName).
*/

RC PagedFileManager::createFile(const string &fileName)
{
    return -1;
}

/*
This method destroys the record-based file whose name is
fileName. The file should exist. Please note that this
method should internally use the method
PagedFileManager::destroyFile (const char *fileName).
*/

RC PagedFileManager::destroyFile(const string &fileName)
{
    return -1;
}

/*
This method opens the record-based file whose name is
fileName. The file must already exist and it must have
been created using the RecordBasedFileManager::createFile
method. If the method is successful, the fileHandle object
whose address is passed as a parameter becomes a "handle"
for the open file. The file handle rules in the method
PagedFileManager::openFile apply here too. Also note that
this method should internally use the method
PagedFileManager::openFile(const char *fileName,
FileHandle &fileHandle).
*/

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    return -1;
}

/*
This method closes the open file instance referred to by
fileHandle. The file must have been opened using the
RecordBasedFileManager::openFile method. Note that this
method should internally use the method
PagedFileManager::closeFile(FileHandle &fileHandle).
*/

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{
    return -1;
}

FileHandle::FileHandle()
{
	readPageCounter = 0;
	writePageCounter = 0;
	appendPageCounter = 0;
}

FileHandle::~FileHandle()
{
}

/*
This method reads the page into the memory block pointed
to by data. The page should exist. Note that page numbers
start from 0.
*/

RC FileHandle::readPage(PageNum pageNum, void *data)
{
    return -1;
}

/*
This method writes the given data into a page specified
by pageNum. The page should exist. Page numbers start from 0.
*/

RC FileHandle::writePage(PageNum pageNum, const void *data)
{
    return -1;
}

/*
This method appends a new page to the end of the file
and writes the given data into the newly allocated page.
*/

RC FileHandle::appendPage(const void *data)
{
    return -1;
}

/*
This method returns the total number of pages currently
in the file.
*/

unsigned FileHandle::getNumberOfPages()
{
    return -1;
}

/*
This method should return the current counter values
of this FileHandle in the three given variables. Here
is some example code that gives you an idea how it will
be applied.
*/

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
	return -1;
}
