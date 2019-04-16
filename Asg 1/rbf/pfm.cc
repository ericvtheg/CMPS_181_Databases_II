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
This method creates an empty-paged file called fileName. The file should not already exist. This 
method should not create any pages in the file.
*/
//ASK IF THIS IS OKAY? CREATE SIZE
RC PagedFileManager::createFile(const string &fileName)
{
    FILE *fp;

    char * cstr = new char [fileName.length()+1];
    strcpy(cstr, fileName.c_str());
    fp = fopen(cstr, "r");
    if(fp == nullptr){
        fp = fopen(cstr, "w+");
        fclose(fp);
        delete[] cstr;
        return 0;
    }else{
        fclose(fp);
        delete[] cstr;
        return 1;
    }


}

/*
This method destroys the paged file whose name is fileName. The file should already exist. 
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
This method opens the paged file whose name is fileName. The file must already exist (and been 
created using the createFile method). If the open method is successful, the fileHandle object 
whose address is passed in as a parameter now becomes a "handle" for the open file. This file 
handle is used to manipulate the pages of the file (see the FileHandle class description below). It 
is an error if fileHandle is already a handle for some open file when it is passed to the openFile 
method. It is not an error to open the same file more than once if desired, but this would be done 
by using a different fileHandle object each time. Each call to the openFile method creates a new 
"instance" of the open file. Warning: Opening a file more than once for data modification is not 
prevented by the PF component, but doing so is likely to corrupt the file structure and may crash 
the PF component. (You do not need to try and prevent this, as you can assume the layer above is 
"friendly" in that regard.) Opening a file more than once for reading is no problem
*/

RC PagedFileManager::openFile(const string &fileName, FileHandle &fileHandle)
{
    FILE *fp;
    //If fileHandle is not null, already has a handle, return error
    if(fileHandle.getfpV2() != nullptr) return 1;

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
This method closes the open file instance referred to by fileHandle. (The file should have been 
opened using the openFile method.) All of the file's pages are flushed to disk when the file is 
closed.
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
    //Check the size of the file to verify the existence of a page
	fseek(fpV2, 0L, SEEK_END);
    size_t sz = ftell(fpV2);
    fseek(fpV2, 0L, SEEK_SET);
    size_t num_pages = ceil(sz/PAGE_SIZE);

    //If pagenum is within range 0 to numpages, good
    if(pageNum >= 0 and num_pages >= pageNum){
	    fseek(fpV2, pageNum*PAGE_SIZE, SEEK_SET);
	    size_t ret_val = fread(data, 1, PAGE_SIZE, fpV2);
	    //std::cout << "fread bytes: " << ret_val  << "  " << sz << std::endl;
	    if(ret_val == PAGE_SIZE){
	        readPageCounter++;
	        return 0;
	    }else{
	        return 1;
	    }
    }else{
        return 1;
    }
}

/*
This method writes the given data into a page specified
by pageNum. The page should exist. Page numbers start from 0.
*/

RC FileHandle::writePage(PageNum pageNum, const void *data)
{  
    //ASK ADD A CHECK FOR IS PAGE EXISTS also, should we clear the page first?
    //Assume how large the data is suppose to be?
    fseek(fpV2, 0L, SEEK_END);
    size_t sz = ftell(fpV2);
    fseek(fpV2, 0L, SEEK_SET);
    size_t num_pages = ceil(sz/PAGE_SIZE);

    //If pagenum is within range 0 to numpages, good
    if(pageNum >= 0 and num_pages >= pageNum){
	    fseek(fpV2, pageNum*PAGE_SIZE, SEEK_SET);
	    fwrite(data, 1, PAGE_SIZE, fpV2);
	    //std::cout << "fread bytes: " << ret_val  << "  " << sz << std::endl;
	    if(!(ferror(fpV2))){
	        writePageCounter++;
	        return 0;
	    }else{
	        return 1;
	    }
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
    // ASK: Do we need to verify the size of data?
    size_t num_pages = getNumberOfPages();
    char * buffer = (char *) calloc(sizeof(char), PAGE_SIZE);
    fwrite(buffer, sizeof(char), PAGE_SIZE, fpV2);
    fseek(fpV2, PAGE_SIZE * num_pages, SEEK_SET);
    if(data != nullptr){
        fwrite( data, 1, PAGE_SIZE, fpV2);
    }
    //std::cout << "fwrite bytes: " << ret_val  << "  " << sz << std::endl;
    free(buffer);
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

    //Geting the total size of the file
    fseek(fp, 0L, SEEK_END);
    size_t sz = ftell(fp);
    fseek(fp, 0L, SEEK_SET);
    
    if(sz < PAGE_SIZE){ 
       num_pages = 0;
    }else{
       num_pages = ceil(sz/PAGE_SIZE);
    }
    //std::cout << "num_pages: "<< num_pages << " " << sz << std::endl;

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
    readPageCount = this->readPageCounter;
    writePageCount = this->writePageCounter;
    appendPageCount = this->appendPageCounter;
    return 0;
}
