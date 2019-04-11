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
    FILE *fp;

    char * cstr = new char [fileName.length()+1];
    strcpy(cstr, fileName.c_str());
    fp = fopen(cstr, "r");

    if(fp == nullptr){
        fp = fopen(cstr, "w+");
        delete[] cstr;
        return 0;
    }else{
        fclose(fp);
        delete[] cstr;
        return -1;
    }


}

/*
This method destroys the record-based file whose name is
fileName. The file should exist. Please note that this
method should internally use the method
PagedFileManager::destroyFile (const char *fileName).
*/

RC PagedFileManager::destroyFile(const string &fileName)
{

    FILE *fp;
    int ret_val;

    char * cstr = new char [fileName.length()+1];
    strcpy(cstr, fileName.c_str());
    fp = fopen(cstr, "r");

    if(fp == nullptr){
        delete[] cstr;
        return 1;
    }else{
        fclose(fp);
        ret_val = remove(cstr);
        delete[] cstr;
        return (ret_val);
    }
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
    FILE *fp;

    char * cstr = new char [fileName.length()+1];
    strcpy(cstr, fileName.c_str());
    fp = fopen(cstr, "r");

    if(fp == nullptr){
        delete[] cstr;
        return 1;
    }else{
        fclose(fp);
        fopen(cstr, "r+");
        delete[] cstr;
        fileHandle.setfpV2(fp);
        return 0;
    }
}

/*
This method closes the open file instance referred to by
fileHandle. The file must have been opened using the
RecordBasedFileManager::openFile method. Note that this
method should internally use the method
PagedFileManager::closeFile(FileHandle &fileHandle).
*/

RC PagedFileManager::closeFile(FileHandle &fileHandle)
{   FILE * fp = fileHandle.getfpV2();
    return (fclose(fp));
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
    size_t sz = PAGE_SIZE;
    fseek(fpV2, pageNum*PAGE_SIZE, SEEK_SET);
    size_t ret_val = fread(data, 1, sz, fpV2);
    std::cout << "fread bytes: " << ret_val  << "  " << sz << std::endl;
    if(ret_val == sz){
        readPageCounter++;
        return 0;
    }else{
        return 1;
    }

}

/*
This method writes the given data into a page specified
by pageNum. The page should exist. Page numbers start from 0.
*/

RC FileHandle::writePage(PageNum pageNum, const void *data)
{   //ASK TA
    //ADD A CHECK FOR IS PAGE EXISTS also, should we clear the page first?
    //Assume how large the data is suppose to be?
    size_t sz = PAGE_SIZE;
    fseek(fpV2, pageNum*PAGE_SIZE, SEEK_SET);
    size_t ret_val = fwrite(data, 1, sz, fpV2);
    std::cout << "fread bytes: " << ret_val  << "  " << sz << std::endl;
    if(!(ferror(fpV2))){
        writePageCounter++;
        return 0;
    }else{
        return 1;
    }
}

/*
This method appends a new page to the end of the file
and writes the given data into the newly allocated page.
*/

RC FileHandle::appendPage(const void *data)
{
    //maybe check size of data :)
    size_t ret_val;
    //char * pp;
    //does this give us right size
    //pp = (char *) malloc(sizeof(*((char *)data)));
    size_t sz = PAGE_SIZE;
    fseek(fpV2, 0L, SEEK_END);
    ret_val = fwrite( data, 1, sz, fpV2);
    std::cout << "fwrite bytes: " << ret_val  << "  " << sz << std::endl;
    if(!(ferror(fpV2))){
        appendPageCounter++;
        return 0;
    }else{
        return 1;
    }

}

/*
This method returns the total number of pages currently
in the file.
*/

unsigned FileHandle::getNumberOfPages()
{
    size_t num_pages;
    FILE * fp = getfpV2();

    fseek(fp, 0L, SEEK_END);
    size_t sz = ftell(fp);
    fseek(fp, 0L, SEEK_SET);

    num_pages = ceil(sz/PAGE_SIZE);

    std::cout << "num_pages: "<< num_pages << " " << sz << std::endl;

    return num_pages;
}

/*
This method should return the current counter values
of this FileHandle in the three given variables. Here
is some example code that gives you an idea how it will
be applied.
*/

RC FileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    // #dont fail pls
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}
