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
This method creates a record-based file called fileName. The file should not already exist. Please 
note that this method should internally use the method PagedFileManager::createFile (const char 
*fileName).
*/
RC RecordBasedFileManager::createFile(const string &fileName) {
    SlotHeader header;
    header.slotsV2   = 0;
    header.freeSpace = 0; //offset to start of free space
    // cout << sizeof( header) << " " << sizeof(directory);
    //Create an insert the header
    _pf_manager->createFile(fileName);
    return 0;
}

/*
This method destroys the record-based file whose name is fileName. The file should exist. Please 
note that this method should internally use the method PagedFileManager::destroyFile (const 
char *fileName).  
*/
RC RecordBasedFileManager::destroyFile(const string &fileName) {
    return _pf_manager->destroyFile(fileName);
}

/*
This method opens the record-based file whose name is fileName. The file must already exist 
and it must have been created using the RecordBasedFileManager::createFile method. If the 
method is successful, the fileHandle object whose address is passed as a parameter becomes a 
"handle" for the open file. The file handle rules in the method PagedFileManager::openFile apply 
here too. Also note that this method should internally use the method 
PagedFileManager::openFile(const char *fileName, FileHandle &fileHandle). 
*/
RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
    return _pf_manager->openFile(fileName, fileHandle);
}

/*
This method closes the open file instance referred to by fileHandle. The file must have been 
opened using the RecordBasedFileManager::openFile method. Note that this method should 
internally use the method PagedFileManager::closeFile(FileHandle &fileHandle).
*/
RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
    return _pf_manager->closeFile(fileHandle);
}

/*
Given a record descriptor, insert a new record into the file identified by the provided handle. You 
can assume that the input is always correct and free of error. That is, you do not need to check to 
see if the input record has the right number of attributes or if the attribute types match. However, 
in order to deal with NULL values in the attributes, the first part in *data contains n bytes for 
passing the null information about each field. The value n can be calculated by using this 
formula: ceil(number of fields in a record / 8). For example, if there are 5 fields, ceil(5/8) = 1 
byte. If there are 20 fields, the size will be ceil(20/8) = 3 bytes. The left-most bit in the first byte 
corresponds to the first field. The right-most bit in the first byte corresponds to the eighth field. If 
there are more than eight fields, the left-most bit in the second byte corresponds to the ninth field 
and so on. If the bit corresponding to a field is set to 1, then the actual data does not contain any 
value for this field. For example, if there are three fields in a record and the second field contains 
NULL, the bit representation in a byte is 0100000. In addition, in the actual data, the incoming 
record contains the first and the third values only. That is, the third field value is placed right 
after the first field value in this case. 
This format (null-fields-indicator + actual data) is to be used for all record manipulation 
operations (unless stated differently). For example, when you read a record, the first part of what 
you return should contain a null-fields-indicator that provides the information about null fields, 
and the actual data should not contain null field values. 
Your file structure is a heap file, and you may use a system-sequenced file organization. That is, 
if the last (current) page has enough space, insert a new record into this page. If not, find the first 
page with free space large enough to store the record, e.g., looking from the beginning of the file, 
and store the record at that location. An RID here is the record ID which is used to uniquely 
identify records in a file. An RID consists of: 1) the page number that the record resides in within 
the file, and 2) the slot number that the record resides in within the page. The insertRecord 
method accepts an RID object and fills it with the RID of the record that is the target for 
insertion; this lets the caller know what the system-determined RID was for each newly inserted 
record. For managing free space within pages, you should keep the free space coalesced in the 
center of the page at all times -- so if a record deletion or update creates a "hole", you should 
move records around to keep all of the free space together. Note that this will not change the 
RIDs of your records; when you move a record within a page, you will also keep track of the 
record's new offset in the slot table on the page, and the RID only contains the slot number, not 
the offset itself. 
Note that the API data format above is intended just for passing the data into insertRecord(). This
does not necessarily mean that the internal representation of your record should be the same as 
this format. (It probably shouldn't be. :-)) 
*/
RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    //calculate size of new slot + size of record to add
    struct SlotHeader header;
    int free_bytes = 0;
    int total_used = 0;
    char* data_ptr = (char *) data;
    bool nullBit = false;

    int nullBytes = getActualByteForNullsIndicator(recordDescriptor.size());

    //Initialize and copy over nullFieldIndicator data
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullBytes);
    memset(nullsIndicator, 0, nullBytes);
    memcpy(nullsIndicator, data, sizeof(nullBytes));
    total_used += nullBytes;

    for(size_t i = 0; i<recordDescriptor.size(); i++){
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
            }
        }
    }

    
    struct SlotInfo * directory = (struct SlotInfo *) malloc(1);
    for(size_t i = 0; i < fileHandle.getNumberOfPages(); i++ ){
	    //Seek over to the end of a page and read the header
	    fseek(fileHandle.getfpV2(), -1 * (sizeof(struct SlotHeader)) + (PAGE_SIZE * (i + 1)), SEEK_SET);
	    fread(&header, sizeof(struct SlotHeader), 1, fileHandle.getfpV2());
	    
	    //Initial new variable to store slot directory
	    //cout << "iter: " << i << " Header numSlots:" << header.slotsV2 << "  FreeSpace Offset:" << header.freeSpace <<endl;
	    directory = (struct SlotInfo *) realloc(directory, sizeof(SlotInfo)*(header.slotsV2 + 1));
	    size_t newdirectorySize = (header.slotsV2 + 1)*sizeof(struct SlotInfo);
	    size_t prevdirectorySize = newdirectorySize - sizeof(struct SlotInfo);
	    if(header.slotsV2 != 0 ){
	       fseek(fileHandle.getfpV2(), -1 * (sizeof(struct SlotHeader) + prevdirectorySize) + (PAGE_SIZE * (i + 1)), SEEK_SET);
	       fread(directory, prevdirectorySize, 1, fileHandle.getfpV2());
	    }


        //Subtracting size of the header and the Directory
        free_bytes = PAGE_SIZE - (newdirectorySize + sizeof(struct SlotHeader));
        //cout << "free_bytes: " << free_bytes << endl;
        //cout << "total_used: " << total_used << endl;
        //Subtract the amount of space already taken
        free_bytes -=  ((header.freeSpace - (PAGE_SIZE * i)));

        free_bytes -= total_used;
        //cout << "free_bytes after: " << free_bytes << endl;  

	    if(free_bytes >= 0){
	    	//cout << "Found Page with Avalible Size: " << endl;
	        //cout << "Total_used: " << total_used << endl;
	        header.slotsV2 += 1;
	        //Seek to free space offset and write the data there
	        fseek(fileHandle.getfpV2(), header.freeSpace, SEEK_SET);
	        fwrite(data, total_used, 1, fileHandle.getfpV2());
	        
	        //After updating the header, write this back to the file
	        header.freeSpace += total_used;
	        fseek(fileHandle.getfpV2(), (-1 *sizeof(struct SlotHeader)) + (PAGE_SIZE * (i + 1)), SEEK_SET);
	        fwrite(&header, sizeof(struct SlotHeader), 1, fileHandle.getfpV2());

	        //TAKE arraysize - 1 and make a pair with ending offset and size
	        directory[header.slotsV2 - 1].endOffset = header.freeSpace; 
	        directory[header.slotsV2 - 1].length = total_used; 
	        fseek(fileHandle.getfpV2(), (-1 * (newdirectorySize + sizeof(struct SlotHeader))) + (PAGE_SIZE * (i + 1)), SEEK_SET);
	        fwrite(directory, newdirectorySize, 1, fileHandle.getfpV2());

	        rid.pageNum = i;
	        rid.slotNum = header.slotsV2 - 1;
	        //cout << "Total File Size: " << fileHandle.getNumberOfPages() * PAGE_SIZE << " numPages: " << fileHandle.getNumberOfPages() << endl;
			//cout << "RID (Pagenum, slotNum): " << rid.pageNum << ", " << rid.slotNum << endl;

	        free(directory);
	        free(nullsIndicator); 
	        return 0;
	    }
    }

    //cout << "Page with Avalible Size Not Found, Appending Page. " << endl;

    free(directory);
    free(nullsIndicator);
    //Out of for loop, no adequete free space found append new page
    int numPages = fileHandle.getNumberOfPages();
    fileHandle.appendPage(nullptr);  
    header.slotsV2  = 1;    
    header.freeSpace = PAGE_SIZE*numPages + total_used;

	//cout << "Header numSlots:" << header.slotsV2 << "  FreeSpace Offset:" << header.freeSpace <<endl;

    //Write in the data
    fseek(fileHandle.getfpV2(), header.freeSpace - total_used, SEEK_SET);
    fwrite(data, total_used, 1, fileHandle.getfpV2());

    //Write in the updated header
    fseek(fileHandle.getfpV2(),(-1 * sizeof(struct SlotHeader)), SEEK_END);
    fwrite(&header, sizeof(struct SlotHeader), 1, fileHandle.getfpV2());
    
    //Write in the directory
    struct SlotInfo * directory2 = (struct SlotInfo *) malloc(header.slotsV2);
    size_t directorySize = (header.slotsV2)*sizeof(struct SlotInfo);
    directory2[header.slotsV2 - 1].endOffset = header.freeSpace; 
	directory2[header.slotsV2 - 1].length = total_used; 
	fseek(fileHandle.getfpV2(), -1 * (directorySize + sizeof(struct SlotHeader)), SEEK_END);
	fwrite(directory2, directorySize, 1, fileHandle.getfpV2());

	numPages = fileHandle.getNumberOfPages();

	//cout << "Total File Size: " << numPages * PAGE_SIZE << " numPages: " << numPages << endl;

	rid.pageNum = numPages - 1 ;
	rid.slotNum = header.slotsV2 - 1;
	//cout << "RID (Pagenum, slotNum): " << rid.pageNum << ", " << rid.slotNum << endl;
	free(directory2);
	return 0;
}

/*
Given a record descriptor, read the record identified by the given rid. 
*/

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    struct SlotHeader header;
    size_t page_offset = 0;

    fseek(fileHandle.getfpV2(),(-1 * sizeof(struct SlotHeader)) + (PAGE_SIZE * (rid.pageNum + 1)), SEEK_SET);
    fread(&header, sizeof(struct SlotHeader), 1, fileHandle.getfpV2());

    //cout << "Read Record Header Info:" << header.slotsV2 << " " << header.freeSpace << endl;
    
    size_t directorySize = (header.slotsV2)*sizeof(struct SlotInfo);

    struct SlotInfo directory2;
    fseek(fileHandle.getfpV2(), -1 * (directorySize + sizeof(struct SlotHeader)) + (PAGE_SIZE * (rid.pageNum + 1)) +(sizeof(SlotInfo) * rid.slotNum) , SEEK_SET);
    fread(&directory2, sizeof(struct SlotInfo), 1, fileHandle.getfpV2()); 
    //cout << "readRecord Directory: " << directory2.endOffset <<" "<< directory2.length << endl;

    page_offset = directory2.endOffset - directory2.length ;
    // cout << "page_offset: " << page_offset  <<endl;

    fseek(fileHandle.getfpV2(), page_offset, SEEK_SET);
    fread(data, directory2.length , 1, fileHandle.getfpV2());
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
