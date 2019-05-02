#include "rbfm.h"
#include <cmath>
#include <cstdlib>
#include <iostream>
#include <string.h>
#include <iomanip>

RecordBasedFileManager* RecordBasedFileManager::_rbf_manager = NULL;
PagedFileManager *RecordBasedFileManager::_pf_manager = NULL;

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

bool RecordBasedFileManager::compRID(const RID &rid1, const RID &rid2 ){
    if(rid1.slotNum == rid2.slotNum && rid1.pageNum == rid2.pageNum ){
        //cout << "compRID: TRUE " << endl;
        return true;
    }else{
        return false;
    }
}

RC RecordBasedFileManager::createFile(const string &fileName) {
   // Creating a new paged file.
   if (_pf_manager->createFile(fileName))
       return RBFM_CREATE_FAILED;

   // Setting up the first page.
   void * firstPageData = calloc(PAGE_SIZE, 1);
   if (firstPageData == NULL)
       return RBFM_MALLOC_FAILED;
   newRecordBasedPage(firstPageData);
   // Adds the first record based page.

   FileHandle handle;
   if (_pf_manager->openFile(fileName.c_str(), handle))
       return RBFM_OPEN_FAILED;
   if (handle.appendPage(firstPageData))
       return RBFM_APPEND_FAILED;
   _pf_manager->closeFile(handle);

   free(firstPageData);

   return SUCCESS;
}

RC RecordBasedFileManager::destroyFile(const string &fileName) {
	return _pf_manager->destroyFile(fileName);
}

RC RecordBasedFileManager::openFile(const string &fileName, FileHandle &fileHandle) {
   return _pf_manager->openFile(fileName.c_str(), fileHandle);
}

RC RecordBasedFileManager::closeFile(FileHandle &fileHandle) {
   return _pf_manager->closeFile(fileHandle);
}

RC RecordBasedFileManager::insertRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid) {
   // Gets the size of the record.
   unsigned recordSize = getRecordSize(recordDescriptor, data);

   // Cycles through pages looking for enough free space for the new entry.
   void *pageData = malloc(PAGE_SIZE);
   if (pageData == NULL)
       return RBFM_MALLOC_FAILED;
   bool pageFound = false;
   int slotFound = 1;
   unsigned i;
   unsigned numPages = fileHandle.getNumberOfPages();
   for (i = 0; i < numPages; i++)
   {
       if (fileHandle.readPage(i, pageData))
           return RBFM_READ_FAILED;

       // When we find a page with enough space (accounting also for the size that will be added to the slot directory), we stop the loop.
       if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + recordSize)
       {
           pageFound = true;
           break;
       }
   }

   // If we can't find a page with enough space, we create a new one
   if(!pageFound)
   {
       newRecordBasedPage(pageData);
   }

   SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

   // Setting up the return RID.

   unsigned useSlotNum = slotHeader.recordEntriesNumber;

   // for(int j = 0; j < slotHeader.recordEntriesNumber; j++){
   //     SlotDirectoryRecordEntry temp = getSlotDirectoryRecordEntry(pageData, j);
   //     if(temp.offset == 0){
   //         //cout << "hit in yaya" << endl;
   //         useSlotNum = j;
   //         slotFound = 0; // set this to 0 cuz then we aren't adding a slot
   //         break;
   //     }
   // }

   rid.pageNum = i;
   rid.slotNum = useSlotNum;

   // Adding the new record reference in the slot directory.
   SlotDirectoryRecordEntry newRecordEntry;
   newRecordEntry.length = recordSize;
   newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
   setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

   // Updating the slot directory header.
   slotHeader.freeSpaceOffset = newRecordEntry.offset;
   slotHeader.recordEntriesNumber += slotFound;
   setSlotDirectoryHeader(pageData, slotHeader);

   // Adding the record data.
   setRecordAtOffset (pageData, newRecordEntry.offset, recordDescriptor, data);

   // Writing the page to disk.
   if (pageFound)
   {
       if (fileHandle.writePage(i, pageData))
           return RBFM_WRITE_FAILED;
   }
   else
   {
       if (fileHandle.appendPage(pageData))
           return RBFM_APPEND_FAILED;
   }

   free(pageData);
   return SUCCESS;
}

RC RecordBasedFileManager::readRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, void *data) {

   //cout << endl << "hit in read record" << endl;
   // Retrieve the specified page
   void * pageData = malloc(PAGE_SIZE);
   void * orPageData = malloc(PAGE_SIZE);
   if (fileHandle.readPage(rid.pageNum, orPageData))
       return RBFM_READ_FAILED;

   if(rid.slotNum < 0 )
       return RBFM_READ_FAILED;

   SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(orPageData);

   // if(rid.slotNum >= slotHeader.recordEntriesNumber)
   //     return RBFM_SLOT_DN_EXIST;

   //cout << "rid slotNum:" << rid.slotNum << endl;
   //cout << "rid pageNum:" << rid.pageNum << endl;

   //rid forwarding function, also moves pageData pointer to correct page
   RID new_rid = findRID(fileHandle, pageData, rid);

   // if (fileHandle.readPage(new_rid.pageNum, pageData))
   //     return RBFM_READ_FAILED;

   if(new_rid.slotNum < 0)
       return RBFM_READ_FAILED;

   //cout << "rid slotNum:" << new_rid.slotNum << endl;
   //cout << "rid pageNum:" << new_rid.pageNum << endl;

   // Checks if the specific slot id exists in the page
   slotHeader = getSlotDirectoryHeader(pageData);

   // Gets the slot directory record entry data
   SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, new_rid.slotNum);

   // if(recordEntry.offset == 0){
   //     return RBFM_UPDATE_FAILED;
   // }
   if(compRID(rid, new_rid) && (recordEntry.offset == 0)){
       //cout << " Same RID " << endl;
       free(orPageData);
       free(pageData);
       return RBFM_SLOT_DN_EXIST;
   }else if(!compRID(rid, new_rid) && (recordEntry.offset == 0)){
       SlotDirectoryRecordEntry updatedRecordEntry;
       updatedRecordEntry.offset = 0;
       updatedRecordEntry.length = 0;
       setSlotDirectoryRecordEntry(orPageData, rid.slotNum, updatedRecordEntry);
       if (fileHandle.writePage(rid.pageNum, orPageData))
        return RBFM_WRITE_FAILED;
       free(orPageData);
       free(pageData);
       return RBFM_SLOT_DN_EXIST;
   }

   // Retrieve the actual entry data
   getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

   free(orPageData);
   free(pageData);
   return SUCCESS;
}

RC RecordBasedFileManager::printRecord(const vector<Attribute> &recordDescriptor, const void *data) {
   // Parse the null indicator and save it into an array
   int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
   char nullIndicator[nullIndicatorSize];
   memset(nullIndicator, 0, nullIndicatorSize);
   memcpy(nullIndicator, data, nullIndicatorSize);

   // We've read in the null indicator, so we can skip past it now
   unsigned offset = nullIndicatorSize;

   cout << "----" << endl;
   for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
   {
       cout << setw(10) << left << recordDescriptor[i].name << ": ";
       // If the field is null, don't print it
       bool isNull = fieldIsNull(nullIndicator, i);
       if (isNull)
       {
           //cout << "NULL" << endl;
           continue;
       }
       switch (recordDescriptor[i].type)
       {
           case TypeInt:
               uint32_t data_integer;
               memcpy(&data_integer, ((char*) data + offset), INT_SIZE);
               offset += INT_SIZE;

               cout << "" << data_integer << endl;
           break;
           case TypeReal:
               float data_real;
               memcpy(&data_real, ((char*) data + offset), REAL_SIZE);
               offset += REAL_SIZE;

               cout << "" << data_real << endl;
           break;
           case TypeVarChar:
               // First VARCHAR_LENGTH_SIZE bytes describe the varchar length
               uint32_t varcharSize;
               memcpy(&varcharSize, ((char*) data + offset), VARCHAR_LENGTH_SIZE);
               offset += VARCHAR_LENGTH_SIZE;

               // Gets the actual string.
               char *data_string = (char*) malloc(varcharSize + 1);
               if (data_string == NULL)
                   return RBFM_MALLOC_FAILED;
               memcpy(data_string, ((char*) data + offset), varcharSize);

               // Adds the string terminator.
               data_string[varcharSize] = '\0';
               offset += varcharSize;

               cout << data_string << endl;
               free(data_string);
           break;
       }
   }
   cout << "----" << endl;

   return SUCCESS;
}

SlotDirectoryHeader RecordBasedFileManager::getSlotDirectoryHeader(void * page)
{
   // Getting the slot directory header.
   SlotDirectoryHeader slotHeader;
   memcpy (&slotHeader, page, sizeof(SlotDirectoryHeader));
   return slotHeader;
}

void RecordBasedFileManager::setSlotDirectoryHeader(void * page, SlotDirectoryHeader slotHeader)
{
   // Setting the slot directory header.
   memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

SlotDirectoryRecordEntry RecordBasedFileManager::getSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber)
{
   // Getting the slot directory entry data.
   SlotDirectoryRecordEntry recordEntry;
   memcpy  (
           &recordEntry,
           ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
           sizeof(SlotDirectoryRecordEntry)
           );

   return recordEntry;
}

void RecordBasedFileManager::setSlotDirectoryRecordEntry(void * page, unsigned recordEntryNumber, SlotDirectoryRecordEntry recordEntry)
{
   // Setting the slot directory entry data.
   memcpy  (
           ((char*) page + sizeof(SlotDirectoryHeader) + recordEntryNumber * sizeof(SlotDirectoryRecordEntry)),
           &recordEntry,
           sizeof(SlotDirectoryRecordEntry)
           );
}

// Configures a new record based page, and puts it in "page".
void RecordBasedFileManager::newRecordBasedPage(void * page)
{
   memset(page, 0, PAGE_SIZE);
   // Writes the slot directory header.
   SlotDirectoryHeader slotHeader;
   slotHeader.freeSpaceOffset = PAGE_SIZE;
   slotHeader.recordEntriesNumber = 0;
	memcpy (page, &slotHeader, sizeof(SlotDirectoryHeader));
}

unsigned RecordBasedFileManager::getRecordSize(const vector<Attribute> &recordDescriptor, const void *data)
{
   // Read in the null indicator
   int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
   char nullIndicator[nullIndicatorSize];
   memset(nullIndicator, 0, nullIndicatorSize);
   memcpy(nullIndicator, (char*) data, nullIndicatorSize);

   // Offset into *data. Start just after the null indicator
   unsigned offset = nullIndicatorSize;
   // Running count of size. Initialize it to the size of the header
   unsigned size = sizeof (RecordLength) + (recordDescriptor.size()) * sizeof(ColumnOffset) + nullIndicatorSize;

   for (unsigned i = 0; i < (unsigned) recordDescriptor.size(); i++)
   {
       // Skip null fields
       if (fieldIsNull(nullIndicator, i))
           continue;
       switch (recordDescriptor[i].type)
       {
           case TypeInt:
               size += INT_SIZE;
               offset += INT_SIZE;
           break;
           case TypeReal:
               size += REAL_SIZE;
               offset += REAL_SIZE;
           break;
           case TypeVarChar:
               uint32_t varcharSize;
               // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
               memcpy(&varcharSize, (char*) data + offset, VARCHAR_LENGTH_SIZE);
               size += varcharSize;
               offset += varcharSize + VARCHAR_LENGTH_SIZE;
           break;
       }
   }

   return size;
}

// Calculate actual bytes for null-indicator for the given field counts
int RecordBasedFileManager::getNullIndicatorSize(int fieldCount)
{
   return int(ceil((double) fieldCount / CHAR_BIT));
}

bool RecordBasedFileManager::fieldIsNull(char *nullIndicator, int i)
{
   int indicatorIndex = i / CHAR_BIT;
   int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
   return (nullIndicator[indicatorIndex] & indicatorMask) != 0;
}

// Computes the free space of a page (function of the free space pointer and the slot directory size).
unsigned RecordBasedFileManager::getPageFreeSpaceSize(void * page)
{
   SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
   return slotHeader.freeSpaceOffset - slotHeader.recordEntriesNumber * sizeof(SlotDirectoryRecordEntry) - sizeof(SlotDirectoryHeader);
}

// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, void *data)
{
   // Pointer to start of record
   char *start = (char*) page + offset;

   // Allocate space for null indicator.
   int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
   char nullIndicator[nullIndicatorSize];
   memset(nullIndicator, 0, nullIndicatorSize);
   // nullIndicator[nullIndicatorSize+1] = '\0';

   // Get number of columns and size of the null indicator for this record
   RecordLength len = 0;
   memcpy (&len, start, sizeof(RecordLength));
   int recordNullIndicatorSize = getNullIndicatorSize(len);

   // Read in the existing null indicator
   memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

   // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
   for (unsigned i = len; i < recordDescriptor.size(); i++)
   {
       int indicatorIndex = (i+1) / CHAR_BIT;
       int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
       nullIndicator[indicatorIndex] |= indicatorMask;
   }
   // Write out null indicator
   memcpy(data, nullIndicator, nullIndicatorSize);

   // Initialize some offsets
   // rec_offset: points to data in the record. We move this forward as we read data from our record
   unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
   // data_offset: points to our current place in the output data. We move this forward as we write to data.
   unsigned data_offset = nullIndicatorSize;
   // directory_base: points to the start of our directory of indices
   char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

   for (unsigned i = 0; i < recordDescriptor.size(); i++)
   {
       if (fieldIsNull(nullIndicator, i))
           continue;

       // Grab pointer to end of this column
       ColumnOffset endPointer;
       memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

       // rec_offset keeps track of start of column, so end-start = total size
       uint32_t fieldSize = endPointer - rec_offset;

       // Special case for varchar, we must give data the size of varchar first
       if (recordDescriptor[i].type == TypeVarChar)
       {
           memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
           data_offset += VARCHAR_LENGTH_SIZE;
       }
       // Next we copy bytes equal to the size of the field and increase our offsets
       memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
       rec_offset += fieldSize;
       data_offset += fieldSize;
   }
}

void RecordBasedFileManager::setRecordAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const void *data)
{
   // Read in the null indicator
   int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
   char nullIndicator[nullIndicatorSize];
   memset (nullIndicator, 0, nullIndicatorSize);
   memcpy(nullIndicator, (char*) data, nullIndicatorSize);

   // Points to start of record
   char *start = (char*) page + offset;

   // Offset into *data
   unsigned data_offset = nullIndicatorSize;
   // Offset into page header
   unsigned header_offset = 0;

   RecordLength len = recordDescriptor.size();
   memcpy(start + header_offset, &len, sizeof(len));
   header_offset += sizeof(len);

   memcpy(start + header_offset, nullIndicator, nullIndicatorSize);
   header_offset += nullIndicatorSize;

   // Keeps track of the offset of each record
   // Offset is relative to the start of the record and points to the END of a field
   ColumnOffset rec_offset = header_offset + (recordDescriptor.size()) * sizeof(ColumnOffset);

   unsigned i = 0;
   for (i = 0; i < recordDescriptor.size(); i++)
   {
       if (!fieldIsNull(nullIndicator, i))
       {
           // Points to current position in *data
           char *data_start = (char*) data + data_offset;

           // Read in the data for the next column, point rec_offset to end of newly inserted data
           switch (recordDescriptor[i].type)
           {
               case TypeInt:
                   memcpy (start + rec_offset, data_start, INT_SIZE);
                   rec_offset += INT_SIZE;
                   data_offset += INT_SIZE;
               break;
               case TypeReal:
                   memcpy (start + rec_offset, data_start, REAL_SIZE);
                   rec_offset += REAL_SIZE;
                   data_offset += REAL_SIZE;
               break;
               case TypeVarChar:
                   unsigned varcharSize;
                   // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                   memcpy(&varcharSize, data_start, VARCHAR_LENGTH_SIZE);
                   memcpy(start + rec_offset, data_start + VARCHAR_LENGTH_SIZE, varcharSize);
                   // We also have to account for the overhead given by that integer.
                   rec_offset += varcharSize;
                   data_offset += VARCHAR_LENGTH_SIZE + varcharSize;
               break;
           }
       }
       // Copy offset into record header
       // Offset is relative to the start of the record and points to END of field
       memcpy(start + header_offset, &rec_offset, sizeof(ColumnOffset));
       header_offset += sizeof(ColumnOffset);
   }
}

RC RecordBasedFileManager::deleteRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid){
// // Assume the RID does not change after an update
   // find offset & starting memaddress for record
   // memset all memory contents to 0
   // update header
   void *pageDataV = malloc(PAGE_SIZE);
   char *pageData = (char *)pageDataV;

   //cout << "rid slotNum:" << rid.slotNum << endl;
   //cout << "rid pageNum:" << rid.pageNum << endl;

   //rid forwarding function, also moves pageData pointer to correct page
   RID new_rid = findRID(fileHandle, pageData, rid);
   if(rid.slotNum < 0)
       return RBFM_READ_FAILED;

   //cout << "rid slotNum:" << new_rid.slotNum << endl;
   //cout << "rid pageNum:" << new_rid.pageNum << endl;

   if (pageData == NULL)
       return RBFM_MALLOC_FAILED;

   //cout << "hit1" << endl;

   if(fileHandle.readPage(new_rid.pageNum, pageData))
           return RBFM_READ_FAILED;

   //cout << "hit1" << endl;

   SlotDirectoryRecordEntry record = getSlotDirectoryRecordEntry(pageData, new_rid.slotNum);
   SlotDirectoryHeader header = getSlotDirectoryHeader(pageData);

   if(record.offset == 0){
       return RBFM_DELETE_FAILED;
   }

   //cout << "hit1" << endl;

   //Pointer to the "end of the free space"
   //cout << "(offset, length) : (" << record.offset << ", " << record.length << ") " <<  endl;
   char * headir_end = pageData + sizeof(SlotDirectoryHeader) + (sizeof(SlotDirectoryRecordEntry) * header.recordEntriesNumber);
   //cout << "recordEntriesNumber: " << header.recordEntriesNumber << endl;

   uint32_t headir_offset = sizeof(SlotDirectoryHeader) + sizeof(SlotDirectoryRecordEntry) * header.recordEntriesNumber;
   //cout << "headir_offset: " << headir_offset << endl;
   // Pointer to the start of the record to be deleted
   //void * record_start = pageData + record.offset - record.length;
   // Zero out the data at that point
   //memset(record_start, 0, record.length);

   //Create a pointer to contain the data after the deleted record for shifting
   char * to_shift_data = pageData + header.freeSpaceOffset;
   void * records_to_load = malloc(PAGE_SIZE);
   //cout << "headerfs: " << header.freeSpaceOffset << endl;
       //cout << "recordos: " << record.offset << endl;
           //cout << "recordl: " << record.length << endl;
   int to_shift_data_size =  (record.offset) - header.freeSpaceOffset;
   //cout << "to_shift_data_size: " << to_shift_data_size << endl;
   // Copies over the shifted data
   memcpy(records_to_load, to_shift_data, to_shift_data_size);

   //
   uint32_t freeSpace_size = record.offset - headir_offset + record.length;
   //cout << "freeSpace_size: " << freeSpace_size << endl;
   // added to_shift_data_size ask Rebecca(?)
   memset(headir_end, 0, freeSpace_size);
   char * freeSpace_pointer = to_shift_data + record.length;
   memcpy(freeSpace_pointer, records_to_load, to_shift_data_size);

   for(size_t i = 0; i < header.recordEntriesNumber; i++){

       SlotDirectoryRecordEntry upd_entry = getSlotDirectoryRecordEntry(pageData, i);

       if(i == new_rid.slotNum){
           upd_entry.offset = 0;
       }else{
           upd_entry.offset += record.length;
       }
       setSlotDirectoryRecordEntry( pageData, i, upd_entry);
   }
    //cout << "Here"<< endl;

   // move freespaceOffset size of deleted record
   header.freeSpaceOffset += record.length;
   //cout << "After headerfs: " << header.freeSpaceOffset << endl;
   setSlotDirectoryHeader(pageData, header);
    //cout << "Here"<< endl;

    if (fileHandle.writePage(new_rid.pageNum, pageData))
        return RBFM_WRITE_FAILED;
   free(pageDataV);
   free(records_to_load);
   return 0;
}

RID RecordBasedFileManager::findRID(FileHandle &fileHandle, void *pageData, const RID &rid){
   //cout << "hit in findRID" << endl;
   unsigned new_pageNum = rid.pageNum;
   unsigned new_slotNum = rid.slotNum;
   unsigned prev_slotNum = -1;
   unsigned prev_pageNum = 0;
   bool dbl_jump = false;
   RID new_rid;
   void * firstPageData = malloc(PAGE_SIZE);//this points to og page right?

   fileHandle.readPage(rid.pageNum, pageData);
   fileHandle.readPage(rid.pageNum, firstPageData);

   SlotDirectoryRecordEntry first_rec = getSlotDirectoryRecordEntry(firstPageData, rid.slotNum);
   SlotDirectoryRecordEntry record = first_rec;
   //cout << "Offset:" << record.offset << "Length:" << record.length << endl;

   new_rid.slotNum = -1;
   new_rid.pageNum = 0;

   while(record.offset < 0){
       prev_slotNum = new_slotNum;
       prev_pageNum = new_pageNum;
       new_pageNum = (unsigned) record.offset * -1;
       new_slotNum = (unsigned) record.length;
       if(dbl_jump){
           first_rec.offset = (unsigned) record.offset;
           first_rec.length = new_slotNum;

           setSlotDirectoryRecordEntry(firstPageData, rid.slotNum, first_rec);

           if(fileHandle.writePage(rid.pageNum, firstPageData))
                   return new_rid;

           first_rec.offset = 0;
           first_rec.length = 0;
           setSlotDirectoryRecordEntry(pageData, prev_slotNum, first_rec);

           if(fileHandle.writePage(prev_pageNum, pageData))
                   return new_rid;

           // first_rec = getSlotDirectoryRecordEntry(pageData, prev_slotNum);
           // //cout << "first_rec Offset:" << first_rec.offset << "Length:" << first_rec.length << endl;
           //cout << "hit in double jump" << endl;
       }
       if(fileHandle.readPage(new_pageNum, pageData))
           return new_rid; //returns error coded rid
       record = getSlotDirectoryRecordEntry(pageData, new_slotNum);
       //cout << "Offset:" << record.offset << "Length:" << record.length << endl;
       // header = getSlotDirectoryHeader(pageData);

       dbl_jump = true;
   }

   // if(record.offset == 0){
   //     first_rec.offset = 0;
   //     first_rec.length = 0;
   //     setSlotDirectoryRecordEntry(pageData, rid.slotNum, first_rec);
   // }

   new_rid.pageNum = new_pageNum;
   new_rid.slotNum = new_slotNum;

   free(firstPageData);
   return new_rid;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, const RID &rid){

   // malloc new page
   void *pageDataV = malloc(PAGE_SIZE);
   char *pageData = (char *)pageDataV;

   //get to load record size
   uint16_t to_load_recordSize = getRecordSize(recordDescriptor, data);
   // char *to_load_data = (char *) data;

   //error checking
   if (pageData == NULL)
       return RBFM_MALLOC_FAILED;

   //readPage into pageData ptr
   if(fileHandle.readPage(rid.pageNum, pageData))
           return RBFM_READ_FAILED;

   //cout << "old rid slotNum:" << rid.slotNum << endl;
   //cout << "old rid pageNum:" << rid.pageNum << endl;

   //rid forwarding function, also moves pageData pointer to correct page
   RID found_rid = findRID(fileHandle, pageData, rid);
   if(found_rid.slotNum < 0)
       return RBFM_READ_FAILED;

   //cout << "new rid slotNum:" << found_rid.slotNum << endl;
   //cout << "new rid pageNum:" << found_rid.pageNum << endl;

   SlotDirectoryRecordEntry record = getSlotDirectoryRecordEntry(pageData, found_rid.slotNum);
   SlotDirectoryHeader header = getSlotDirectoryHeader(pageData);

   if(record.offset == 0){
       return RBFM_UPDATE_FAILED;
   }

   // void *page2 = malloc(100);

   // SlotDirectoryRecordEntry erase;
   // setSlotDirectoryRecordEntry( page2, found_rid.slotNum, erase );

   void * records_to_load = malloc(PAGE_SIZE);
   char * to_shift_data = pageData + header.freeSpaceOffset;

   char * start_to_upd_record = pageData + record.offset;

   //set contents of record to 0
   memset(start_to_upd_record, 0, record.length);

   if(record.length == to_load_recordSize){
       //straight load it in
       cout << "hit 1 hit 1 hit 1" << endl;
       // memcpy(start_to_upd_record, to_load_data, to_load_recordSize);
       setRecordAtOffset(pageData, record.offset, recordDescriptor, data);
       if (fileHandle.writePage(found_rid.pageNum, pageData))
           return RBFM_WRITE_FAILED;
   }
   else if(record.length > to_load_recordSize){ //can read in record but must coalesce rest of records
       cout << "hit 2 hit 2 hit 2" << endl;
       //load in updated info by adding difference of size to properly align records
       unsigned diff = record.length - to_load_recordSize;
       // memcpy(start_to_upd_record + diff, to_load_data, to_load_recordSize);
       // setRecordAtOffset(pageData + diff, record.offset, recordDescriptor, data);

       //coalesce
       uint32_t to_shift_data_size =  record.offset - header.freeSpaceOffset;

       cout << "record.offset: " << record.offset << endl;
       cout << "header.freeSpaceOffset: " << header.freeSpaceOffset << endl;
       cout << "diff: " << diff << endl;
       cout << "to_shift_data_size: " << to_shift_data_size << endl;

       // load into buffer
       memmove(records_to_load, to_shift_data, to_shift_data_size);
       memset(to_shift_data, 0, to_shift_data_size + record.length);//check what this gets
       memmove(to_shift_data + record.length, records_to_load, to_shift_data_size);

       header.freeSpaceOffset += diff;

       setRecordAtOffset(pageData, header.freeSpaceOffset, recordDescriptor, data);

       //update headers & slots
       for(size_t i = 0; i < header.recordEntriesNumber; i++){

           SlotDirectoryRecordEntry upd_entry = getSlotDirectoryRecordEntry(pageData, i);

           if(i == found_rid.slotNum){ //dont kill rid
               upd_entry.offset = record.offset + diff;
               upd_entry.length = to_load_recordSize;
           }else if(record.offset > upd_entry.offset){
               upd_entry.offset += record.length;
           }else{ //else dont update
               continue;
           }
           // is the second arg right?
           setSlotDirectoryRecordEntry(pageData, i, upd_entry);
       }

       // header.freeSpaceOffset += diff;
       setSlotDirectoryHeader(pageData, header);

       if (fileHandle.writePage(found_rid.pageNum, pageData))
           return RBFM_WRITE_FAILED;
   }
   else{ // else to_load_recordSize is bigger than record.length
       cout << "hit 3 hit 3 hit 3" << endl;
       bool pageFound = false;
       unsigned i;
       unsigned inserted_pageNum;
       unsigned numPages = fileHandle.getNumberOfPages();
       void* pageDataVoid = malloc(PAGE_SIZE);

       //look for page with enough free space
       //cout << "hit0" << endl;

       if(getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + to_load_recordSize){
           cout << "enough freespace on found_rid Page" << endl;
           inserted_pageNum = found_rid.pageNum;
           i = found_rid.pageNum;
           pageFound = true;
           //cout << "found page " << inserted_pageNum << " with enough space" << endl;
       }else{
           for (i = 0; i < numPages; i++){
               //cout << "hit3" << endl;
               if(i == found_rid.pageNum)
                    continue;

               //pageDataVoid = (void*) pageData;
               memset(pageDataVoid, 0, PAGE_SIZE);
               if (fileHandle.readPage(i, pageDataVoid))
                    return RBFM_WRITE_FAILED;

               if (getPageFreeSpaceSize(pageDataVoid) >= sizeof(SlotDirectoryRecordEntry) + to_load_recordSize){
                   //cout << "hit4" << endl;
                   cout << "Page Found at: " << i <<endl;
                   inserted_pageNum = i;
                   pageFound = true;
                   //cout << "found page " << inserted_pageNum << " with enough space" << endl;
                   break;
               }
           }
       }

       // If we can't find a page with enough space, we create a new one
       if(!pageFound)
       {
           // pageDataVoid = calloc(sizeof(char), PAGE_SIZE - sizeof(SlotDirectoryHeader));
           memset(pageDataVoid, 0, PAGE_SIZE);
           //cout << "Num Pages: " << i << endl;
           newRecordBasedPage(pageDataVoid);
           if(fileHandle.appendPage(pageDataVoid)) //may need records_to_load as arg
               return RBFM_APPEND_FAILED;
           i = fileHandle.getNumberOfPages()-1;
           cout << "Num Pages: " << i+1 << endl;
           inserted_pageNum = i;

           cout << "no page found with enough space, appending" << endl;
       }

       if(found_rid.pageNum == i){ // if page found is same page as original record
           // write updated record to page
           cout << "updating greater sized record in original page" << endl;
           header.freeSpaceOffset = header.freeSpaceOffset - to_load_recordSize;
           // memcpy(pageData + header.freeSpaceOffset, data, to_load_recordSize);
           setRecordAtOffset(pageData, header.freeSpaceOffset, recordDescriptor, data);


           to_shift_data = pageData + header.freeSpaceOffset;
           //coalesce
           uint32_t to_shift_data_size = record.offset - header.freeSpaceOffset;
           // load to_shift_data into buffer
           memmove(records_to_load, to_shift_data, to_shift_data_size);
           // clear mem for new memcpy
           memset(to_shift_data, 0, to_shift_data_size + record.length);
           //load buffer into page, coalesced
           memmove(to_shift_data + record.length, records_to_load, to_shift_data_size);


           header.freeSpaceOffset += record.length;

           //update headers & slots
           for(i = 0; i < header.recordEntriesNumber; i++){
               SlotDirectoryRecordEntry upd_entry = getSlotDirectoryRecordEntry(pageData, i);
               if(found_rid.slotNum == i){
                   //cout << "hit in rid.slotNum" << endl;
                   upd_entry.length = to_load_recordSize;
                   upd_entry.offset = header.freeSpaceOffset;
                   setSlotDirectoryRecordEntry(pageData, i, upd_entry);
               }else if(record.offset > upd_entry.offset){
                   upd_entry.offset += record.length;
                   setSlotDirectoryRecordEntry(pageData, i, upd_entry);
               }
           }

           setSlotDirectoryHeader(pageData, header);
           fileHandle.writePage(inserted_pageNum, pageData);
           free(pageDataVoid);

       }else{ //else if is to be moved to another page
           //cout << "moving record to another page" << endl;
           // have page i that has enough free space
           char* pageDataV2 = (char *) pageDataVoid;

           //read contents of page with enough free space
           if(fileHandle.readPage(i, pageDataV2))
                   return RBFM_READ_FAILED;

           // //cout<< "hit 0" << endl;

           // grab header of newly read page
           SlotDirectoryHeader headerV2 = getSlotDirectoryHeader(pageDataV2);

           // //cout<< "hit 1" << endl;

           //set pointer to start of freespace and load in updated record
           unsigned fpOffset = headerV2.freeSpaceOffset - to_load_recordSize;

           // //cout<< "hit 2" << endl;
           //cout<< "freeSpaceOffset: " << headerV2.freeSpaceOffset << endl;
           //cout<< "to_load_recordSize " << to_load_recordSize <<endl;
           //cout<< "fpOffset: " << fpOffset << endl;
           // memcpy(pageDataV2, data, to_load_recordSize);
           setRecordAtOffset(pageDataV2, fpOffset, recordDescriptor, data);

           // //cout<< "hit 3" << endl;

           headerV2.freeSpaceOffset = fpOffset;

           //coalesce
           // double check this below line is right
           uint32_t to_shift_data_size =  record.offset - header.freeSpaceOffset;
           //load to_shift_data into buffer
           memmove(records_to_load, to_shift_data, to_shift_data_size);
           // clear mem for new memcpy
           memset(to_shift_data, 0, to_shift_data_size + record.length);
           //load in now coalecsed data
           memmove(to_shift_data + record.length, records_to_load, to_shift_data_size);

           //update headers & slots of original page
           for(i = 0; i < header.recordEntriesNumber; i++){

               SlotDirectoryRecordEntry upd_entry = getSlotDirectoryRecordEntry(pageData, i);
               if(i == found_rid.slotNum){
                   // kill rid
                   // dont add below line + 1 because we want that indice
                   upd_entry.length = headerV2.recordEntriesNumber;
                   upd_entry.offset = inserted_pageNum * -1;
               }else if(record.offset > upd_entry.offset){
                   upd_entry.offset += record.length;
               }
               setSlotDirectoryRecordEntry(pageData, i, upd_entry);
           }

           // set header for original page
           header.freeSpaceOffset = header.freeSpaceOffset + record.length;
           setSlotDirectoryHeader(pageData, header);
           fileHandle.writePage(found_rid.pageNum, pageData);

           // update information for newly added to page
           // create a new slot for added record
           // create new rid
           RID new_rid;
           new_rid.pageNum = inserted_pageNum;
           new_rid.slotNum = headerV2.recordEntriesNumber;

           //cout << "new_rid.pageNum " << inserted_pageNum << " ";
           //cout << "new_rid.slotNum " << new_rid.slotNum << endl;

           SlotDirectoryRecordEntry new_entry;
           new_entry.offset = headerV2.freeSpaceOffset;
           new_entry.length = to_load_recordSize;
           setSlotDirectoryRecordEntry(pageDataV2, new_rid.slotNum, new_entry);
           headerV2.recordEntriesNumber += 1;
           setSlotDirectoryHeader(pageDataV2, headerV2);
           fileHandle.writePage(inserted_pageNum, pageDataV2);

           if(!pageFound)
               free(pageDataVoid);
       }
   }

   free(pageDataV);
   free(records_to_load);
   return 0;
}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){

   // Retrieve the specified page
   void * pageData = malloc(PAGE_SIZE);

   //cout << "rid slotNum:" << rid.slotNum << endl;
   //cout << "rid pageNum:" << rid.pageNum << endl;

   //rid forwarding function, also moves pageData pointer to correct page
   RID new_rid = findRID(fileHandle, pageData, rid);
   if(rid.slotNum < 0)
       return RBFM_READ_FAILED;

   //cout << "rid slotNum:" << new_rid.slotNum << endl;
   //cout << "rid pageNum:" << new_rid.pageNum << endl;

   if (fileHandle.readPage(new_rid.pageNum, pageData))
       return RBFM_READ_FAILED;

   // Checks if the specific slot id exists in the page
   SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

   if(slotHeader.recordEntriesNumber < new_rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

   // Gets the slot directory record entry data
   SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, new_rid.slotNum);

   // Retrieve the actual entry data
   getRecordAttrAtOffsetWithNull(pageData, recordEntry.offset, recordDescriptor, attributeName, data);



   //TEST PRINT OF RETURNED DATA
   // unsigned i = 0;
   // for (i = 0; i < recordDescriptor.size(); i++){
   //     if(!attributeName.compare(recordDescriptor[i].name)){
	   //      switch (recordDescriptor[i].type)
	   //      {
	   //          case TypeInt:
	   //              int retInt;
	   //              memcpy(&retInt, data, INT_SIZE);
	   //              //cout << "readAttribute INT: " << retInt << endl;

	   //          break;
	   //          case TypeReal:
	   //              float retFlot;
	   //              memcpy(&retFlot, data, REAL_SIZE);
	   //              //cout << "readAttribute float: " << retFlot << endl;


	   //          break;
	   //          case TypeVarChar:
	   //              char retStr[recordDescriptor[i].length];

	   //              //Currently HARDCODED CHANGE FOR DIFFERENT RID HARDCODED FOR UCSCSLUG
	   //              memcpy(&retStr, data, 8);
	   //              retStr[8] = '\0';
	   //              //cout << "readAttribute STR: " << retStr << endl;

	   //          break;
	   //      }
   //     }
   // }
   free(pageData);
   return SUCCESS;
}


// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordAttrAtOffsetWithNull(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const string &attributeName, void *data)
{

  // Pointer to start of record
  char *start = (char*) page + offset;

  // Allocate space for null indicator.
  int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
  char nullIndicator[nullIndicatorSize];
  memset(nullIndicator, 0, nullIndicatorSize);

  int newNullIndicatorSize = 1;
  char newNullIndicator[newNullIndicatorSize];
  memset(newNullIndicator, 0, newNullIndicatorSize);


  // Get number of columns and size of the null indicator for this record
  RecordLength len = 0;
  memcpy (&len, start, sizeof(RecordLength));
  int recordNullIndicatorSize = getNullIndicatorSize(len);

  // Read in the existing null indicator
  memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

  // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
  for (unsigned i = len; i < recordDescriptor.size(); i++)
  {
      int indicatorIndex = (i+1) / CHAR_BIT;
      int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
      nullIndicator[indicatorIndex] |= indicatorMask;
  }

  // Initialize some offsets
  // rec_offset: points to data in the record. We move this forward as we read data from our record
  unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
  // data_offset: points to our current place in the output data. We move this forward as we write to data.
  unsigned data_offset = newNullIndicatorSize;
  // directory_base: points to the start of our directory of indices
  char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

  for (unsigned i = 0; i < recordDescriptor.size(); i++)
  {

      // Grab pointer to end of this column
      ColumnOffset endPointer;
      memcpy(&endPointer, directory_base + (i * sizeof(ColumnOffset)), sizeof(ColumnOffset));

      // rec_offset keeps track of start of column, so end-start = total size
      uint32_t fieldSize = endPointer - rec_offset;
      //cout << "fieldSize: " << fieldSize << endl;

      // If it turns out the attribute names match
      if (!attributeName.compare(recordDescriptor[i].name))
      {
       	//If NULL skip field completely, should result in a null indiccator i.e. 1
    	   	if (fieldIsNull(nullIndicator, i)){
          		newNullIndicator[0] |= 1 << 7;
      	   	}else{
      	   		//Else set up the data
      	   		//If it is a TypeVarChar, remember to put in the sizeof the string
      	   		if(recordDescriptor[i].type == TypeVarChar){
      	   			memcpy((char *)data + data_offset, &fieldSize, INT_SIZE);
      	   			data_offset += INT_SIZE;
           	}

          		memcpy((char *)data + data_offset, start + rec_offset, fieldSize);
   		}
          	memcpy(data, newNullIndicator, newNullIndicatorSize);
          	break;
      }
      // Next we copy bytes equal to the size of the field and increase our offsets
      // memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
      rec_offset += fieldSize;
      //data_offset += fieldSize;
  }
  //No matching attribute name found
}


// Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordAttrAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const string &attributeName, void *data)
{

   // Pointer to start of record
   char *start = (char*) page + offset;

   // Allocate space for null indicator.
   int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
   char nullIndicator[nullIndicatorSize];
   memset(nullIndicator, 0, nullIndicatorSize);

   // Get number of columns and size of the null indicator for this record
   RecordLength len = 0;
   memcpy (&len, start, sizeof(RecordLength));
   int recordNullIndicatorSize = getNullIndicatorSize(len);

   // Read in the existing null indicator
   memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

   // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
   for (unsigned i = len; i < recordDescriptor.size(); i++)
   {
       int indicatorIndex = (i+1) / CHAR_BIT;
       int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
       nullIndicator[indicatorIndex] |= indicatorMask;
   }

   // Initialize some offsets
   // rec_offset: points to data in the record. We move this forward as we read data from our record
   unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
   // data_offset: points to our current place in the output data. We move this forward as we write to data.
   //unsigned data_offset = 0;
   // directory_base: points to the start of our directory of indices
   char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

   for (unsigned i = 0; i < recordDescriptor.size(); i++)
   {
   	//If NULL skip field completely, should result in a null pointer
       if (fieldIsNull(nullIndicator, i))
           continue;

       // Grab pointer to end of this column
       ColumnOffset endPointer;
       memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

       // rec_offset keeps track of start of column, so end-start = total size
       uint32_t fieldSize = endPointer - rec_offset;

       // If it turns out the attribute names match
       if (!attributeName.compare(recordDescriptor[i].name))
       {
           memcpy(data, start + rec_offset, fieldSize);
           break;
       }
       // Next we copy bytes equal to the size of the field and increase our offsets
       // memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
       rec_offset += fieldSize;
       //data_offset += fieldSize;
   }
   //No matching attribute name found
}

// // Scan returns an iterator to allow the caller to go through the results one by one.
// RC RecordBasedFileManager::scan(FileHandle &fileHandle,
//     const vector<Attribute> &recordDescriptor,
//     const string &conditionAttribute,
//     const CompOp compOp,                  // comparision type such as "<" and "="
//     const void *value,                    // used in the comparison
//     const vector<string> &attributeNames, // a list of projected attributes
//     RBFM_ScanIterator &rbfm_ScanIterator);

RC RecordBasedFileManager::scan(
	FileHandle &fileHandle,
	const vector<Attribute> &recordDescriptor,
	const string &conditionAttribute,
	const CompOp compOp, const void *value,
	const vector<string> &attributeNames,
	RBFM_ScanIterator &rbfm_ScanIterator)
{

    bool attrFound = false;
    unsigned i;
    for(i = 0; i < recordDescriptor.size(); i++){
    	//If the condition attribute matchs any of the given attributes, initialize the fields of the scan iterator, else throw an error
       //cout << "condAttr: " << conditionAttribute << endl;
       //cout << "recordDesc: " << recordDescriptor[i].name << endl;
    	if(!(conditionAttribute.compare(recordDescriptor[i].name))){
           //Matching attr name found
           attrFound = true;
           break;
    	}
    }

    //Attr not found, return err code

    rbfm_ScanIterator.initializeScanIterator(
    	fileHandle,
    	recordDescriptor,
    	conditionAttribute,
    	compOp,
    	value,
    	attributeNames,
    	attrFound,
    	i );

    if(!attrFound){
    	return 1;
    }
    if(compOp == NO_OP){
    	return SUCCESS;
    }

    return SUCCESS;

}


RC RBFM_ScanIterator::initializeScanIterator(
 	FileHandle &fileHandle,
	const vector<Attribute> &recordDescriptor,
	const string &conditionAttribute,
	const CompOp compOp,
	const void *value,
	const vector<string> &attributeNames,
	const bool wasNameFound,
 	const unsigned fieldNumber)
{; //Set fields that are directly passed in

	fH = fileHandle;
	recordDesc = recordDescriptor;
	condAttr = conditionAttribute;
	compOper = compOp;
	compValue = value;
	wantedAttrs = attributeNames;
	initScanDone = false;
	rbfm = RecordBasedFileManager::instance();
	attrNameFound = wasNameFound;
	//Initilize RID to start searching
	currRID.slotNum = 0;
	currRID.pageNum = 0;

	fieldNum = fieldNumber;

	return SUCCESS;
};

RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
	 if(!attrNameFound){
        //cout << "attr name not found" << endl;
 		return RBFM_EOF;
 	}

	 //cout << "In Get Next Record " << endl;
 	void *pageData = malloc(PAGE_SIZE);
 	if (pageData == NULL)
   	return RBFM_MALLOC_FAILED;

 	bool matchingFieldFound = false;
	// unsigned recordSize = getRecordSize(recordDesc, data);;
 	unsigned numPages = fH.getNumberOfPages();
 	unsigned j, k;

 	if(initScanDone == true){
 		k = currRID.slotNum + 1;
 	}else{
 		k = currRID.slotNum ;
 		initScanDone = true;
 	}
 	SlotDirectoryHeader header;

 	for (j = currRID.pageNum; j < numPages; j++){
 		//cout << "pageNum: " << j << endl;
    	if (fH.readPage(j, pageData)){
       	return RBFM_READ_FAILED;
        //cout << "after read Page" << endl;
     	}else{
     		header = rbfm->getSlotDirectoryHeader(pageData);
    	}


    	for(; k < header.recordEntriesNumber; k++){
    		//getrecord
 	    	// //cout << "slotNum: " << k << endl;
     		//For each slot retrieve the actual entry
     		SlotDirectoryRecordEntry recordEntry = rbfm->getSlotDirectoryRecordEntry(pageData, k);

        	// if forwarded RID skip, cuz we'll find it eventually
        	if(recordEntry.offset <= 0){
        	    rid.slotNum = k;
        		//cout << "Found True With NULL EQ_OP" << endl;
        		rid.pageNum = j;
        		currRID.slotNum = k;
        		currRID.pageNum = j;
           	continue;
        	}


           //Allocate enough memory for incoming data for the attr and the null indicator  and INT incase
     	    void *returnedFieldData = malloc(recordDesc[fieldNum].length + 1 + INT_SIZE);

     	    //Get the actual attr
           rbfm->getRecordAttrAtOffsetWithNull(pageData, recordEntry.offset, recordDesc, condAttr, returnedFieldData);

           vector<Attribute> one;
           one.push_back(recordDesc[fieldNum]);

           // rbfm->printRecord( one,returnedFieldData);
        	//If NULL skip field completely, should result in a null pointer

           //Initialize the NUll Indicator, should only really be 1 bit i.e. takes up 1 byte
           int nullIndicatorSize = rbfm->getNullIndicatorSize(1);
           char nullIndicator[nullIndicatorSize];
           memset(nullIndicator, 0, nullIndicatorSize);

           memcpy(nullIndicator, returnedFieldData, nullIndicatorSize);

           //If null and EQ_OP we can return the record
           if((rbfm->fieldIsNull(nullIndicator, 1)) ){
           	if(compValue == NULL && compOper == EQ_OP){

        		    rid.slotNum = k;
        	        //cout << "Found True With NULL EQ_OP" << endl;
        		    rid.pageNum = j;
        		    currRID.slotNum = k;
        		    currRID.pageNum = j;
        		    rbfm->getRecordwithGivenAttrsAtOffset(pageData, recordEntry.offset, recordDesc, wantedAttrs, data);
                   free(returnedFieldData);
        		    return SUCCESS;
           	}
           }

           if((rbfm->fieldIsNull(nullIndicator, 1)) ){
           	//if the op was not EQ_OP just continue
           	continue;
           }

           switch (recordDesc[fieldNum].type)
        	{
        	    case TypeInt:
	         	    uint32_t data_integer, compInt;
	         	    memcpy(&data_integer, (char *)returnedFieldData+ nullIndicatorSize, INT_SIZE);
	         	    memcpy(&compInt, compValue, INT_SIZE);
	         	    //cout << "" << data_integer << endl;

        	        matchingFieldFound = compare(data_integer, compInt, compOper);
        	    break;
        	    case TypeReal:

        	        float data_real, compReal;
        	        memcpy(&data_real, (char *)returnedFieldData + nullIndicatorSize, REAL_SIZE);
        	        memcpy(&compReal, compValue, REAL_SIZE);

    	            //cout << "" << data_real << endl;

        	        matchingFieldFound = compare(data_real, compReal, compOper);
        	    break;
        	    case TypeVarChar:

                   //Grab the size of the attribute for the string and grab the string
        	        int varcharSize;
        	        memcpy(&varcharSize, (char *)returnedFieldData + nullIndicatorSize, INT_SIZE);

                   // //cout << "VarCharSize: " << varcharSize << endl;
        	        char *datachar = (char *)malloc(recordDesc[fieldNum].length);
                    memset(datachar, 0 , recordDesc[fieldNum].length);
        	        memcpy(datachar,(char*)returnedFieldData + nullIndicatorSize + INT_SIZE, varcharSize);
        	        datachar[varcharSize + 1] = '\0';
        	        //string dataString(datachar);

                   memcpy(&varcharSize, compValue, INT_SIZE);
                   //char *datachar2 = (char *)malloc(varcharSize + 1);
                   char *datachar2 = (char *)malloc(recordDesc[fieldNum].length);
                   memset(datachar2, 0 , recordDesc[fieldNum].length);
                   memcpy(datachar2,(char*)compValue + INT_SIZE, varcharSize);
                   datachar2[varcharSize + 1] = '\0';
                   //string valueString(datachar2);

        	        // //cout << "Value String: "<< valueString << endl;

                    matchingFieldFound = compare(datachar, datachar2, compOper);
                    free(datachar2);
        	        free(datachar);
        	    break;
        	}
        	if(matchingFieldFound == true){
        		//getrecord
        		currRID.slotNum = k;
        		currRID.pageNum = j;
        	    rid.slotNum = k;
        	    //cout << "Found True" << endl;
        		rid.pageNum = j;
        		rbfm->getRecordwithGivenAttrsAtOffset(pageData, recordEntry.offset, recordDesc, wantedAttrs, data);
                free(pageData);
                free(returnedFieldData);
        		return SUCCESS;
        	}

    free(returnedFieldData);
   	}
   	k = 0;

	}
   //cout << "hit in scan" << endl;
   free(pageData);

	return RBFM_EOF;
};



/* RC RBFM_ScanIterator::getNextRecord(RID &rid, void *data){
    //cout << "In Get Next Record " << endl;
    void *pageData = malloc(PAGE_SIZE);
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;
    bool matchingFieldFound = false;
   // unsigned recordSize = getRecordSize(recordDesc, data);;
    unsigned numPages = fH.getNumberOfPages();
    unsigned j, k;

    if(initScanDone == true){
    	k = currRID.slotNum + 1;
    }else{
    	k = currRID.slotNum ;
    	initScanDone = true;
    }
    SlotDirectoryHeader header;

    for (j = currRID.pageNum; j < numPages; j++){
    	 //cout << "pageNum: " << j << endl;
        if (fH.readPage(j, pageData)){
            return RBFM_READ_FAILED;
        }else{
        	header = rbfm->getSlotDirectoryHeader(pageData);
        }


       for(; k < header.recordEntriesNumber; k++)
       {
       	//getrecord
    	    //cout << "slotNum: " << k << endl;
        	//For each slot retrieve the actual entry
        	SlotDirectoryRecordEntry recordEntry = rbfm->getSlotDirectoryRecordEntry(pageData, k);

           // if forwarded RID skip, cuz we'll find it eventually
           if(recordEntry.offset <= 0){
               continue;
           }

           //Allocate enough memory for incoming data for the attr
        	void *returnedFieldData = malloc(recordDesc[fieldNum].length);

        	//Get the actual attr
           rbfm->getRecordAttrAtOffset(pageData, recordEntry.offset, recordDesc, condAttr, returnedFieldData);


           //Creates a pointer to the start of the record
           char *start = (char*) pageData + recordEntry.offset;

           //Initializie the nullIndicator
           int nullIndicatorSize = rbfm->getNullIndicatorSize(recordDesc.size());
           char nullIndicator[nullIndicatorSize];
           memset(nullIndicator, 0, nullIndicatorSize);

           // Get number of columns and size of the null indicator for this record
           RecordLength len = 0;
           memcpy (&len, start, sizeof(RecordLength));
           int recordNullIndicatorSize = rbfm->getNullIndicatorSize(len);

           // Read in the existing null indicator
           memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);


           //Get the length of the character
           char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

           for (unsigned i = 0; i <= fieldNum; i++)
           {
           	//If NULL skip field completely, should result in a null pointer
               if (rbfm->fieldIsNull(nullIndicator, i))
                   continue;

               unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);

               // Grab pointer to end of this column
               ColumnOffset endPointer;
               memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

               // rec_offset keeps track of start of column, so end-start = total size
               uint32_t fieldSize = endPointer - rec_offset;

               // If it turns out the attribute names match
               if (i == fieldNum)
               {
               	switch (recordDesc[i].type)
               	{
               	    case TypeInt:
               	        uint32_t data_integer, compInt;
               	        memcpy(&data_integer, returnedFieldData, INT_SIZE);
               	        memcpy(&compInt, compValue, INT_SIZE);

               	        //cout << "" << data_integer << endl;
               	        matchingFieldFound = compare(data_integer, compInt, compOper);
               	    break;
               	    case TypeReal:
               	        float data_real, compReal;
               	        memcpy(&data_real, returnedFieldData, REAL_SIZE);
               	        memcpy(&compReal, compValue, REAL_SIZE);

               	        //cout << "" << data_real << endl;
               	        matchingFieldFound = compare(data_real, compReal, compOper);
               	    break;
               	    case TypeVarChar:
               	        // Convert returned data to char*
               	        char *data_string = (char *)malloc(fieldSize);

               	        memcpy(data_string, returnedFieldData, fieldSize);

               	        matchingFieldFound = compare(data_string, (char *)compValue, compOper);


               	        // Adds the string terminator.
               	        //data_string[varcharSize] = '\0';

                           //print should be a little of since it it not a string
               	        //cout << data_string << endl;
               	        free(data_string);
               	    break;
               	}
               	if(matchingFieldFound == true){
               		//getrecord
               		currRID.slotNum = k;
               		currRID.pageNum = j;
               	    rid.slotNum = k;
               	    //cout << "Found True" << endl;
               		rid.pageNum = j;
               		rbfm->getRecordwithGivenAttrsAtOffset(pageData, recordEntry.offset, recordDesc, wantedAttrs, data);
               		return SUCCESS;
               	}

               }
               // Next we copy bytes equal to the size of the field and increase our offsets
               // memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
               rec_offset += fieldSize;
               //data_offset += fieldSize;
           }
      	}
      	k = 0;

	}
	return RBFM_EOF;
};*/


bool RBFM_ScanIterator::compare(const uint32_t &data, const uint32_t &value, CompOp compOp){
	switch(compOp)
	{    case EQ_OP:
		    if(data == value)
		    	return true;
		    else return false;
		 break;
        case LT_OP:      // <
	         if(data < value)
	         	return true;
	         else return false;

        break;
        case LE_OP:      // <=
	         if(data <= value)
	         	return true;
	         else return false;

        break;
        case GT_OP:      // >
	         if(data > value)
	         	return true;
	         else return false;
        break;
        case GE_OP:      // >=
	         if(data >= value)
	         	return true;
	         else return false;
        break;
        case NE_OP:      // !=
	         if(data != value)
	         	return true;
	         else return false;

        break;
        default:
        	return false;
   }
   return false;
};
// bool RBFM_ScanIterator::compare(const string &data, const string &value, CompOp compOp){
//    //cout << "In string compare" <<endl;
//    //cout << "data length: " << data.length() << endl;
//    //cout << "value length: " << value.length() << endl;
//    //cout << "data: " << data << endl;
//    //cout << "value: " << value << endl;
//
// 	//<0	the first character that does not match has a lower value in ptr1 than in ptr2
// 	//0	the contents of both strings are equal
// 	//>0	the first character that does not match has a greater value in ptr1 than in ptr2
// 	int diffvalue = data.compare(value);
// 	switch(compOp)
// 	{    case EQ_OP:
// 		    if(diffvalue == 0){
//                //cout << "true" << endl;
// 		    	return true;
//            }
// 		    else{ //cout << "false" << endl;
//                return false;}
// 		 break;
//         case LT_OP:      // <
// 	         if( diffvalue < 0)
// 	         	return true;
// 	         else return false;
//
//         break;
//         case LE_OP:      // <=
// 	         if(diffvalue <= 0)
// 	         	return true;
// 	         else return false;
//
//         break;
//         case GT_OP:      // >
// 	         if(diffvalue > 0)
// 	         	return true;
// 	         else return false;
//         break;
//         case GE_OP:      // >=
// 	         if(diffvalue >= 0)
// 	         	return true;
// 	         else return false;
//         break;
//         case NE_OP:      // !=
// 	         if(diffvalue != 0)
// 	         	return true;
// 	         else return false;
//
//         break;
//         default:
//         	return false;
//    }
//
//    return false;
//
// };
bool RBFM_ScanIterator::compare(const float &data, const float &value, CompOp compOp){
	switch(compOp)
	{    case EQ_OP:
		    if(data == value)
		    	return true;
		    else return false;
		 break;
        case LT_OP:      // <
	         if(data < value)
	         	return true;
	         else return false;
        break;
        case LE_OP:      // <=
	         if(data <= value)
	         	return true;
	         else return false;

        break;
        case GT_OP:      // >
	         if(data > value)
	         	return true;
	         else return false;
        break;
        case GE_OP:      // >=
	         if(data >= value)
	         	return true;
	         else return false;
        break;
        case NE_OP:      // !=
	         if(data != value)
	         	return true;
	         else return false;

        break;
        default:
        	return false;
   }

   return false;

};
bool RBFM_ScanIterator::compare(const char * data, const char * value, CompOp compOp){
   //cout << "In string compare" <<endl;
   // //cout << "data length: " << data.length() << endl;
   // //cout << "value length: " << value.length() << endl;
   // //cout << "data: " << data << endl;
   // //cout << "value: " << value << endl;

	//<0	the first character that does not match has a lower value in ptr1 than in ptr2
	//0	the contents of both strings are equal
	//>0	the first character that does not match has a greater value in ptr1 than in ptr2
	int diffvalue = strcmp(data,value);
	switch(compOp)
	{    case EQ_OP:
		    if(diffvalue == 0){
               //cout << "true" << endl;
		    	return true;
           }
		    else{ //cout << "false" << endl;
               return false;}
		 break;
        case LT_OP:      // <
	         if( diffvalue < 0)
	         	return true;
	         else return false;

        break;
        case LE_OP:      // <=
	         if(diffvalue <= 0)
	         	return true;
	         else return false;

        break;
        case GT_OP:      // >
	         if(diffvalue > 0)
	         	return true;
	         else return false;
        break;
        case GE_OP:      // >=
	         if(diffvalue >= 0)
	         	return true;
	         else return false;
        break;
        case NE_OP:      // !=
	         if(diffvalue != 0)
	         	return true;
	         else return false;

        break;
        default:
        	return false;
   }

   return false;

};

// / Support header size and null indicator. If size is less than recordDescriptor size, then trailing records are null
void RecordBasedFileManager::getRecordwithGivenAttrsAtOffset(void *page, unsigned offset, const vector<Attribute> &recordDescriptor, const vector<string> &attributeNames, void *data)
{
    // Pointer to start of record
    char *start = (char*) page + offset;

    // Allocate space for null indicator.
    int nullIndicatorSize = getNullIndicatorSize(recordDescriptor.size());
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);

    // Allocate space for an updated null indicator.
    int updatedNullIndicatorSize = getNullIndicatorSize(attributeNames.size());
    char updatedNullIndicator[updatedNullIndicatorSize];
    memset(updatedNullIndicator, 0, updatedNullIndicatorSize);

    // Get number of columns and size of the null indicator for this record
    RecordLength len = 0;
    memcpy (&len, start, sizeof(RecordLength));
    int recordNullIndicatorSize = getNullIndicatorSize(len);

    // Read in the existing null indicator
    memcpy (nullIndicator, start + sizeof(RecordLength), recordNullIndicatorSize);

    // If this new recordDescriptor has had fields added to it, we set all of the new fields to null
    for (unsigned i = len; i < recordDescriptor.size(); i++)
    {
        int indicatorIndex = (i+1) / CHAR_BIT;
        int indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        nullIndicator[indicatorIndex] |= indicatorMask;
    }
    // Write out null indicator
   // memcpy(data, nullIndicator, nullIndicatorSize);

    // Initialize some offsets
    // rec_offset: points to data in the record. We move this forward as we read data from our record
    unsigned rec_offset = sizeof(RecordLength) + recordNullIndicatorSize + len * sizeof(ColumnOffset);
    // data_offset: points to our current place in the output data. We move this forward as we write to data.
    unsigned data_offset = updatedNullIndicatorSize;
    // directory_base: points to the start of our directory of indices
    char *directory_base = start + sizeof(RecordLength) + recordNullIndicatorSize;

    for (unsigned i = 0; i < recordDescriptor.size(); i++)
    {
        if (fieldIsNull(nullIndicator, i))
            continue;

    	ColumnOffset endPointer;
    	memcpy(&endPointer, directory_base + i * sizeof(ColumnOffset), sizeof(ColumnOffset));

    	// rec_offset keeps track of start of column, so end-start = total size
    	uint32_t fieldSize = endPointer - rec_offset;

    	for(unsigned j = 0; j < attributeNames.size(); j++)
    	{
    		if(attributeNames[j].compare(recordDescriptor[i].name) == 0)
    		{
		        if (fieldIsNull(nullIndicator, i)){
		        	int indicatorIndex = j / CHAR_BIT;
                    updatedNullIndicator[indicatorIndex] = updatedNullIndicator[indicatorIndex] << 1;
		            continue;
                }

                int indicatorIndex = j / CHAR_BIT;
                updatedNullIndicator[indicatorIndex] = (updatedNullIndicator[indicatorIndex] << 1) | 1;
		        // Grab pointer to end of this column


		        // Special case for varchar, we must give data the size of varchar first
		        if (recordDescriptor[i].type == TypeVarChar)
		        {
		            memcpy((char*) data + data_offset, &fieldSize, VARCHAR_LENGTH_SIZE);
		            data_offset += VARCHAR_LENGTH_SIZE;
		        }
		        // Next we copy bytes equal to the size of the field and increase our offsets
		        memcpy((char*) data + data_offset, start + rec_offset, fieldSize);
		        data_offset += fieldSize;
        	}
    	}
		rec_offset += fieldSize;
    }
}


/*

bool RecordBasedFileManager::isFowardAddress(void *page, unsigned recordEntryNumber){
   char * start = (char *)page;
   SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
   SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(page, recordEntryNumber);
   if(recordEntry.offset < 0){
       return true;
   }else{
       return false;
   }
}

RC RecordBasedFileManager::returnFowardAddress(void *page, unsigned recordEntryNumber,){
   char * start = (char *)page;
   SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(page);
   SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(page, recordEntryNumber);
   if(recordEntry.offset < 0){
       return true;
   }else{
       return false;
   }
}


//Probably given the new
 RC RecordBasedFileManager::accessFowardAddressPage(){
   char * start = (char *)page;
   SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);
   SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(page, recordEntryNumber);
   if(recordEntry.offset < 0){
       return true;
   }else{
       return false;
   }
}
*/
