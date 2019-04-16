#include "rbfm.h"

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = 0;
PagedFileManager*  _pf_manager = PagedFileManager::instance();

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
    SlotHeader header;
    header.slotsV2   = 0;
    header.freeSpace = 0; //offset to start of free space
    // cout << sizeof( header) << " " << sizeof(directory);
    _pf_manager->createFile(fileName);
    FILE *fp = fopen(fileName.c_str(), "r+" );
    fseek(fp, -1 * (sizeof(header)), SEEK_END);
    fwrite(&header, sizeof(header), 1, fp);
    fclose(fp);
    return 0;
}

/*
This method destroys the paged file whose name is fileName.
The file should already exist.
*/

RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
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
       return _pf_manager->openFile(fileName, fileHandle);
}

/*
This method closes the open file instance referred to by
fileHandle. (The file should have been opened using the
openFile method.) All of the file's pages are flushed to
disk when the file is closed.
*/

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

/*
Given a record descriptor, read the record identified by the given rid.
*/

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    //calculate size of new slot + size of record to add
    SlotHeader header;
    size_t free_bytes = 0;
    size_t total_used = 0; //-8 to accomdate for allocated slot
    char* data_ptr = (char *) data;
    bool nullBit = false;

    int nullBytes = getActualByteForNullsIndicator(recordDescriptor.size());
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullBytes);
    memset(nullsIndicator, 0, nullBytes);
    memcpy(nullsIndicator, data, sizeof(nullBytes));
    total_used += nullBytes;

    fread(&header, sizeof(header), 1, fileHandle.getfpV2());
    pair<unsigned,unsigned> directory[header.slotsV2 + 1];

    if(header.slotsV2 != 0 ){
       fread(&directory, sizeof(directory), 1, fileHandle.getfpV2());
    }

    // cout << "Header Info:" << header.slotsV2 << " " << header.freeSpace <<  " " <<  sizeof(directory)<<endl;

    // - 8 bytes to account for newly allocated memory in directory array
    free_bytes = PAGE_SIZE - (sizeof(directory) + sizeof(header));

    for(size_t i = 0; i<recordDescriptor.size(); i++){
        // cout << "Total_used_iter: " << total_used << endl;
        int nullIndex = i/8;
        nullBit = nullsIndicator[nullIndex] & (1 << (7 - (i%8)));
        if (recordDescriptor[i].type == TypeInt){
            if (!nullBit){
	            total_used += INT_SIZE;
            }
        }
        else if (recordDescriptor[i].type == TypeReal){
            if (!nullBit){
	           total_used += FLOAT_SIZE;
	        }
        }
        else if (recordDescriptor[i].type == TypeVarChar){
        	if(!nullBit){
	            int varcharsize;
	            memcpy(&varcharsize, (data_ptr + total_used), INT_SIZE);
	            total_used += INT_SIZE;
                total_used += varcharsize;
                // cout << "Var_char size: " << varcharsize << endl;
            }
        }
        // cout << "Total_used_iter: " << total_used << endl;
    }

    free_bytes = free_bytes - total_used;

    if(free_bytes >= 0){
        cout << "Total_used: " << total_used << " bytes" << endl;
        header.slotsV2 += 1;
        fseek(fileHandle.getfpV2(), header.freeSpace, SEEK_SET);
        fwrite(data, 1, total_used, fileHandle.getfpV2());

        header.freeSpace += total_used;
        fseek(fileHandle.getfpV2(), -1 * (sizeof(header)), SEEK_END);
        fwrite(&header, 1, sizeof(header), fileHandle.getfpV2());
        directory[header.slotsV2 + 1] = { header.freeSpace, total_used};
        fseek(fileHandle.getfpV2(), -1 * (sizeof(directory) + sizeof(header)), SEEK_END);
        fwrite(&directory, 1, (sizeof(directory) + sizeof(header)), fileHandle.getfpV2());
    }

    rid.pageNum = 0;
    rid.slotNum = header.slotsV2 + 1;
    // cout << "free_bytes: " << free_bytes << endl;

        free(nullsIndicator);
        return 0;
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
    SlotHeader header;
    int page_offset;

    cout << "hit 1" << endl;

    // find end of page and then move header bytes back
    fseek(fileHandle.getfpV2(), -1 * (sizeof(header)), SEEK_END);
    fread(&header, sizeof(header), 1, fileHandle.getfpV2());

    cout << "hit 2" << endl;

    // this line below is causing seg fault
    // header.slotsV2 value is way off
    pair<unsigned,unsigned> directory[header.slotsV2];

    // find end of page and then move both size of directory and header back
    fseek(fileHandle.getfpV2(), -1 * (sizeof(directory)), SEEK_END);
    fread(&directory, sizeof(directory), 1, fileHandle.getfpV2());

    cout << "hit 3" << endl;

    // getting to appropiate page then adding offset of record
    page_offset = (rid.pageNum * PAGE_SIZE) + directory[rid.slotNum].second;

    // ask salami about where to cpy data
    fseek(fileHandle.getfpV2(), page_offset, SEEK_SET);
    fread(&data, directory[rid.slotNum].second, 1, fileHandle.getfpV2());

    cout << "hit 4" << endl;

    return 0;
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
	int nullBytes = getActualByteForNullsIndicator(recordDescriptor.size());
    int offset = nullBytes ;
    char* data_ptr = (char *) data;
    bool nullBit = false;
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullBytes);
    memset(nullsIndicator, 0, nullBytes);
    memcpy(nullsIndicator, data, sizeof(nullBytes));

    for(size_t i = 0; i<recordDescriptor.size(); i++){
        int nullIndex = i/8;
        nullBit = nullsIndicator[nullIndex] & (1 << (7 - (i%8)));
        if (recordDescriptor[i].type == TypeInt){
            if (!nullBit){
	            int buffer;
	            memcpy(&buffer, (data_ptr += offset), INT_SIZE);
	            offset = INT_SIZE;
	            cout << recordDescriptor[i].name << " : "<< buffer << " " ;
            }else{
                cout << recordDescriptor[i].name << " : NULL ";
            }
        }
        else if (recordDescriptor[i].type == TypeReal){
            if (!nullBit){
	            float buffer;
	            memcpy(&buffer, (data_ptr += offset), FLOAT_SIZE);
	            offset = FLOAT_SIZE;
	            cout << recordDescriptor[i].name << " : "<< buffer << " ";
	        }else{
     	        cout << recordDescriptor[i].name << " : NULL ";
            }
        }
        else if (recordDescriptor[i].type == TypeVarChar){
        	if(!nullBit){
	            int varcharsize;
	            memcpy(&varcharsize, (data_ptr += offset), INT_SIZE);
	            offset = INT_SIZE;
	            //+1 to add the null terminating character
	            char buffer[varcharsize + 1];
	            memcpy(&buffer, (data_ptr += offset), varcharsize);
	            buffer[varcharsize] ='\0';
	            offset = varcharsize;
	            cout << recordDescriptor[i].name << " : " << buffer << " ";
            }else{
	            cout << recordDescriptor[i].name << " : NULL ";
            }
        }
        else{
        	free(nullsIndicator);
            return -1;
        }
        cout << endl;
    }
    free(nullsIndicator);
    return 0;
}
