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



RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
    //calculate size of new slot + size of record to add
    struct SlotHeader header;
    int free_bytes = 0;
    int total_used = 0;
    int passed_used = 0;
    vector<int> recVec;
    char* data_ptr = (char *) data;
    bool nullBit = false;

    int nullBytes = getActualByteForNullsIndicator(recordDescriptor.size());
    //Initialize and copy over nullFieldIndicator data
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullBytes);
   
    memset(nullsIndicator, 0, nullBytes);
    memcpy(nullsIndicator, data_ptr, nullBytes);

    total_used = nullBytes;
    passed_used = nullBytes;
    int nullIndex = 0;
    cout << "insertRecord1"<<endl;

    for(size_t i = 0; i<recordDescriptor.size(); i++){
        nullIndex = i/8;
        nullBit = nullsIndicator[nullIndex] & (1 << (7 - (i%8)));
        if (recordDescriptor[i].type == TypeInt){
            if (!nullBit){
            	cout << " int: " << i << endl;
	            total_used += INT_SIZE;
	            passed_used += INT_SIZE;
	            recVec.push_back(INT_SIZE);
            }
        }
        else if (recordDescriptor[i].type == TypeReal){
            if (!nullBit){
            	cout << " float: " << i << endl;
	            total_used += INT_SIZE;
	            passed_used += FLOAT_SIZE;
	            recVec.push_back(FLOAT_SIZE);
	        }
        }
        else if (recordDescriptor[i].type == TypeVarChar){
        	if(!nullBit){
	            int varcharsize;
            	cout << " varcharsize: " << i << endl;
	            memcpy(&varcharsize, (data_ptr + passed_used), INT_SIZE);
                total_used += varcharsize;
                passed_used = passed_used + varcharsize + INT_SIZE;
	            recVec.push_back(varcharsize);
            }
        }
    }
    cout << "Total_used" << total_used <<endl;

    //Extra byte to indicate number of fields
    int recArrCount = recVec.size();
    int recArrBytes = (recArrCount) * INT_SIZE;
    int setupBytes = (recArrCount + 1) * INT_SIZE;
   
    cout << "recArrBytes: " << recArrBytes << endl;
    //Adding on the array for managing the fields.
    total_used += setupBytes;

    void * record = malloc(total_used);

    cout << "Total_used" << total_used <<endl;
    cout << "setupBytes" << setupBytes <<endl;


    //Copy over the size of the array
    memcpy((char *)record, &recArrCount, INT_SIZE);

    //Copy over the nullbytes
    memset((char *)record + INT_SIZE, 0, nullBytes);
    memcpy((char *)record + INT_SIZE, nullsIndicator, nullBytes);
   
    int offset = nullBytes;
    int recOffset = nullBytes + setupBytes; 
    for(size_t i = 0; i<recordDescriptor.size(); i++){
        nullIndex = i/8;
        nullBit = nullsIndicator[nullIndex] & (1 << (7 - (i%8)));
        if (recordDescriptor[i].type == TypeInt){
            if (!nullBit){
                int buffer;
                memcpy((char *)record + recOffset, data_ptr += offset, INT_SIZE);
                memcpy(&buffer, (char *)record + recOffset, INT_SIZE);
                cout << "INT " << recOffset << " " << offset;
                //memcpy(record + recOffset ,&buffer, INT_SIZE);
                cout << "INT " << buffer << endl;
   

                recOffset += INT_SIZE;
                offset = INT_SIZE;
                cout << "INT " << recOffset << " " << offset << endl;

            }
        }
        else if (recordDescriptor[i].type == TypeReal){
            if (!nullBit){
                float buffer;

               // recOffset -= FLOAT_SIZE;

                cout << "FLOAT " << recOffset << " " << offset;
                memcpy((char *)record + recOffset, data_ptr += offset, FLOAT_SIZE);
                memcpy(&buffer, (char *)record + recOffset, FLOAT_SIZE);
                cout << "float " << buffer << endl;

                cout << "FLOAT " << recOffset << " " << offset <<endl;
                recOffset += FLOAT_SIZE;
                offset = FLOAT_SIZE;
            }
        }
        else if (recordDescriptor[i].type == TypeVarChar){
            if(!nullBit){
                int varcharsize;
                memcpy(&varcharsize, (data_ptr += offset), INT_SIZE);
                offset = INT_SIZE;
                char * varchar = (char *) malloc((varcharsize));
                memcpy((char *)record + recOffset, data_ptr += offset, varcharsize);
                cout << "varchar " << recOffset << " " << offset;
                recOffset += varcharsize;
                offset = varcharsize;
                cout << "varchar " << recOffset << " " << offset;
                free(varchar);
            }
        }
    }

    struct SlotInfo * directory = (struct SlotInfo *) malloc(1);
    for(size_t i = 0; i < fileHandle.getNumberOfPages(); i++ ){
        //Seek over to the end of a page and read the header
        fseek(fileHandle.getfpV2(), -1 * (sizeof(struct SlotHeader)) + (PAGE_SIZE * (i + 1)), SEEK_SET);
        fread(&header, sizeof(struct SlotHeader), 1, fileHandle.getfpV2());

        //Initial new variable to store slot directory
        cout << "iter: " << i << " Header numSlots:" << header.slotsV2 << "  FreeSpace Offset:" << header.freeSpace <<endl;
        directory = (struct SlotInfo *) realloc(directory, sizeof(SlotInfo)*(header.slotsV2 + 1));
        int newdirectorySize = (header.slotsV2 + 1)*sizeof(struct SlotInfo);
        int prevdirectorySize = newdirectorySize - sizeof(struct SlotInfo);
        if(header.slotsV2 != 0 ){
           fseek(fileHandle.getfpV2(), -1 * (sizeof(struct SlotHeader) + prevdirectorySize) + (PAGE_SIZE * (i + 1)), SEEK_SET);
           fread(directory, prevdirectorySize, 1, fileHandle.getfpV2());
        }

        //Subtracting size of the header and the Directory
        free_bytes = PAGE_SIZE - (newdirectorySize + sizeof(struct SlotHeader));
        //cout << "free_bytes: " << free_bytes << endl;
        //cout << "total_used: " << total_used << endl;
        //Subtract the amount of space already taken
      //  free_bytes -=  ((header.freeSpace - (PAGE_SIZE * i)));
        free_bytes -=  header.freeSpace;

        //cout << "free_bytes after: " << free_bytes << endl;

        if(free_bytes >= 0){
            //cout << "Found Page with Avalible Size: " << endl;
            //cout << "Total_used: " << total_used << endl;
            header.slotsV2 += 1;

            for(size_t l = 0; l < recVec.size(); l++){
                if (l == 0){
                    recVec[l] += header.freeSpace;
                }else{
                    recVec[l] +=  (recVec[l - 1] + recVec[l]);
                }
            }
            
            int recArr[recArrCount];
            std::copy(recVec.begin(), recVec.end(), recArr);

            memcpy((char *)record + INT_SIZE + nullBytes, &recArr[0], recArrBytes);

            //Seek to free space offset and write the data there
			 //header.freeSpace = PAGE_SIZE*numPages + header.freeSpace;
			 int totalFreeOffset = PAGE_SIZE*i + header.freeSpace;
			 cout << "Header numSlots:" << header.slotsV2 << "  FreeSpace Offset:" << header.freeSpace <<endl;
			 //Write in the data
			 fseek(fileHandle.getfpV2(), totalFreeOffset, SEEK_SET);
			 fwrite((char *)record , total_used, 1, fileHandle.getfpV2());

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
            free(record);
            free(nullsIndicator);
            return 0;
        }
    }

    cout << "Page with Avalible Size Not Found, Appending Page. " << endl;
    free(directory);
    free(nullsIndicator);
    //Out of for loop, no adequete free space found append new page
    int numPages = fileHandle.getNumberOfPages();
    fileHandle.appendPage(nullptr);
    header.slotsV2  = 1;

    //Beginning of where records are written.
    //header.freeSpace = PAGE_SIZE*numPages + setupBytes + nullBytes;
    header.freeSpace = setupBytes + nullBytes;
    cout << "Header numSlots:" << header.slotsV2 << "  FreeSpace Offset:" << header.freeSpace <<endl;
    cout << "Total_used" << total_used <<endl;

    for(size_t l = 0; l < recVec.size(); l++){
        if (l == 0){
            recVec[l] += header.freeSpace;
        }else{
            recVec[l] +=  (recVec[l - 1]);
            cout << "recVec" << recVec[l - 1] << " " << recVec[l] << endl;
        }
    }
    int recArr[recArrCount];
    std::copy(recVec.begin(), recVec.end(), recArr);
    for(int i =0; i < recArrCount; i++){
    	cout << i << ": " << recArr[i] << endl;
    }
    
    //Copy array right after int telling us start of the array
    memcpy((char *)record + INT_SIZE + nullBytes, &recArr[0], recArrBytes);
    
    // fseek(fileHandle.getfpV2(), header.freeSpace - total_used, SEEK_SET);
    // fread(&arrSize, INT_SIZE , 1, fileHandle.getfpV2());

    //recStart
   // header.freeSpace = PAGE_SIZE*numPages + total_used;
    header.freeSpace = PAGE_SIZE*numPages + total_used;
    cout << "Header numSlots:" << header.slotsV2 << "  FreeSpace Offset:" << header.freeSpace <<endl;
    //Write in the data
    fseek(fileHandle.getfpV2(), header.freeSpace - total_used, SEEK_SET);
    fwrite((char *)record , total_used, 1, fileHandle.getfpV2());


    header.freeSpace = total_used;
    //Write in the updated header
    fseek(fileHandle.getfpV2(),(-1 * sizeof(struct SlotHeader)), SEEK_END);
    fwrite(&header, sizeof(struct SlotHeader), 1, fileHandle.getfpV2());

    //Write in the directory
    struct SlotInfo * directory2 = (struct SlotInfo *) malloc(sizeof(struct SlotInfo)*header.slotsV2);
    int directorySize = (header.slotsV2)*sizeof(struct SlotInfo);
    directory2[header.slotsV2 - 1].endOffset = header.freeSpace;
    directory2[header.slotsV2 - 1].length = total_used;
    fseek(fileHandle.getfpV2(), -1 * (directorySize + sizeof(struct SlotHeader)), SEEK_END);
    fwrite(directory2, directorySize, 1, fileHandle.getfpV2());

   // int arrSize;
   // fseek(fileHandle.getfpV2(), header.freeSpace - total_used, SEEK_SET);
    //fread(&arrSize, INT_SIZE , 1, fileHandle.getfpV2());
   // cout << "buffer: "<< arrSize << endl;

    numPages = fileHandle.getNumberOfPages();

    //cout << "Total File Size: " << numPages * PAGE_SIZE << " numPages: " << numPages << endl;

    rid.pageNum = numPages - 1 ;
    rid.slotNum = header.slotsV2 - 1;
    cout << "RID (Pagenum, slotNum): " << rid.pageNum << ", " << rid.slotNum << endl;
    free(record);
	free(directory2);
	return 0;
}

/*
Given a record descriptor, read the record identified by the given rid.
*/

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {
    cout << "In readRecord " <<endl;
    struct SlotHeader header;
    int page_offset = 0;
    char* data_ptr = (char *) data;
    int offset = 0;
    int nullBytes = getActualByteForNullsIndicator(recordDescriptor.size());
    bool nullBit = false;

    //Initialize and copy over nullFieldIndicator data
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullBytes);

    fseek(fileHandle.getfpV2(),(-1 * sizeof(struct SlotHeader)) + (PAGE_SIZE * (rid.pageNum + 1)), SEEK_SET);
    fread(&header, sizeof(struct SlotHeader), 1, fileHandle.getfpV2());

    cout << "Read Record Header Info:" << header.slotsV2 << " " << header.freeSpace << endl;

    int directorySize = (header.slotsV2)*sizeof(struct SlotInfo);

    struct SlotInfo directory2;
    fseek(fileHandle.getfpV2(), -1 * (directorySize + sizeof(struct SlotHeader)) + (PAGE_SIZE * (rid.pageNum + 1)) +(sizeof(SlotInfo) * rid.slotNum) , SEEK_SET);
    fread(&directory2, sizeof(struct SlotInfo), 1, fileHandle.getfpV2());
    cout << "readRecord Directory: " << directory2.endOffset <<" "<< directory2.length << endl;

    page_offset = (rid.pageNum * PAGE_SIZE)+ directory2.endOffset - directory2.length  ;

  //  char * buffer = (char *) malloc(directory2.length);
    int arrSize;
    fseek(fileHandle.getfpV2(), page_offset, SEEK_SET);
    fread(&arrSize, INT_SIZE , 1, fileHandle.getfpV2());
    cout << "arrSize: " << arrSize << endl;

    //Get the size of the array
    int recArr[arrSize];
    fseek(fileHandle.getfpV2(), page_offset + INT_SIZE + nullBytes, SEEK_SET);
    fread(&recArr[0], INT_SIZE, arrSize, fileHandle.getfpV2());
    for(int i = 0; i < arrSize; i++){
        cout << i <<": " << recArr[i] << " " << endl;
    }
    

    //Load in Null Indicator
    fseek(fileHandle.getfpV2(), page_offset + INT_SIZE, SEEK_SET);
    fread(nullsIndicator, nullBytes , 1 , fileHandle.getfpV2());
    
    
    fseek(fileHandle.getfpV2(), page_offset + INT_SIZE, SEEK_SET);
    fread(data_ptr, nullBytes , 1 , fileHandle.getfpV2());

    
    page_offset += (INT_SIZE *( 1 + arrSize)) + nullBytes ;
    offset = nullBytes;
    int nonNullCount = 0;
    int attrLen = 0;
    for(size_t i = 0; i<recordDescriptor.size(); i++){
        int nullIndex = i/8;
        nullBit = nullsIndicator[nullIndex] & (1 << (7 - (i%8)));
        if(!nullBit){
            if(i == 0){
            	attrLen = recArr[nonNullCount] - ((INT_SIZE *( 1 + arrSize)) + nullBytes)  ;
            	cout << "attrLen" << attrLen << endl;
            }else{
            	attrLen = recArr[nonNullCount] - recArr[nonNullCount - 1];
            	cout << "attrLen" << attrLen << endl;
            }
        }

        if (recordDescriptor[i].type == TypeInt){
            if (!nullBit){
                cout << "HERE INT"  << i<< endl;
                fseek(fileHandle.getfpV2(), page_offset , SEEK_SET);
                fread(data_ptr + offset, INT_SIZE, 1, fileHandle.getfpV2());
                cout << page_offset << " " << offset <<  endl;
                page_offset += INT_SIZE;
                offset +=INT_SIZE;
                nonNullCount++;
            }
        }
        else if (recordDescriptor[i].type == TypeReal){
            if (!nullBit){
                float fuffer;
                fseek(fileHandle.getfpV2(), page_offset, SEEK_SET);
                fread(data_ptr + offset, FLOAT_SIZE, 1, fileHandle.getfpV2());
                fread(&fuffer, FLOAT_SIZE, 1, fileHandle.getfpV2());
                cout << "HERE FLOAT" << i<<" " << fuffer<< endl;
                cout << page_offset << " " << offset <<  endl;
                page_offset += FLOAT_SIZE;
                offset +=FLOAT_SIZE;
                nonNullCount++;
            }
        }
        else if (recordDescriptor[i].type == TypeVarChar){
            if(!nullBit){

                cout << "HERE varchar" << i<<endl;
               
                fseek(fileHandle.getfpV2(), page_offset, SEEK_SET);
                memcpy(data_ptr + offset, &attrLen, INT_SIZE);
                offset +=INT_SIZE;
                
                fseek(fileHandle.getfpV2(), recArr[nonNullCount]- attrLen, SEEK_SET);
                fread(data_ptr + offset, attrLen, 1, fileHandle.getfpV2());
                cout << page_offset << " " << offset << endl;
                page_offset += attrLen;
                offset += attrLen;
                nonNullCount++;
            }
        }
    }



    free(nullsIndicator);
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
    int offset = nullBytes;
    char* data_ptr = (char *) data;
    bool nullBit = false;
    unsigned char *nullsIndicator = (unsigned char *) malloc(nullBytes);
    //unsigned char *nullsIndicator = (unsigned char *) calloc(1, nullBytes);
    memset(nullsIndicator, 0, nullBytes);
    memcpy(nullsIndicator, data, nullBytes);

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
                // char buffer[varcharsize + 1];
                char * buffer = (char *) malloc((varcharsize+1));
                memcpy(buffer, (data_ptr += offset), varcharsize);
                buffer[varcharsize] ='\0';
                offset = varcharsize;
                cout << recordDescriptor[i].name << " : " << buffer << " ";
                free(buffer);
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
