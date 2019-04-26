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
    rid.pageNum = i;
    rid.slotNum = slotHeader.recordEntriesNumber;

    // Adding the new record reference in the slot directory.
    SlotDirectoryRecordEntry newRecordEntry;
    newRecordEntry.length = recordSize;
    newRecordEntry.offset = slotHeader.freeSpaceOffset - recordSize;
    setSlotDirectoryRecordEntry(pageData, rid.slotNum, newRecordEntry);

    // Updating the slot directory header.
    slotHeader.freeSpaceOffset = newRecordEntry.offset;
    slotHeader.recordEntriesNumber += 1;
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
    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    if(slotHeader.recordEntriesNumber < rid.slotNum)
        return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // Retrieve the actual entry data
    getRecordAtOffset(pageData, recordEntry.offset, recordDescriptor, data);

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
            cout << "NULL" << endl;
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

    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;

    if(fileHandle.readPage(rid.pageNum, pageData))
            return RBFM_READ_FAILED;

    SlotDirectoryRecordEntry record = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    SlotDirectoryHeader header = getSlotDirectoryHeader(pageData);

    //Pointer to the "end of the free space"
    cout << "(headersize, slotsize) : (" << sizeof(SlotDirectoryHeader) << ", " << sizeof(SlotDirectoryRecordEntry) << ") " <<  endl;
    char * headir_end = pageData + sizeof(SlotDirectoryHeader) + (sizeof(SlotDirectoryRecordEntry) * header.recordEntriesNumber);
    cout << "recordEntriesNumber: " << header.recordEntriesNumber << endl;

    uint32_t headir_offset = sizeof(SlotDirectoryHeader) + sizeof(SlotDirectoryRecordEntry) * header.recordEntriesNumber;
    cout << "headir_offset: " << headir_offset << endl;
    // Pointer to the start of the record to be deleted
    //void * record_start = pageData + record.offset - record.length;
    // Zero out the data at that point
    //memset(record_start, 0, record.length);

    //Create a pointer to contain the data after the deleted record for shifting
    char * to_shift_data = pageData + header.freeSpaceOffset;
    void * records_to_load = malloc(PAGE_SIZE);
    cout << "headerfs: " << header.freeSpaceOffset << endl;
        cout << "recordos: " << record.offset << endl;
            cout << "recordl: " << record.length << endl;
    int to_shift_data_size =  (record.offset) - header.freeSpaceOffset;
    cout << "to_shift_data_size: " << to_shift_data_size << endl;
    // Copies over the shifted data
    memcpy(records_to_load, to_shift_data, to_shift_data_size);

    //
    uint32_t freeSpace_size = record.offset - headir_offset + record.length;
    cout << "freeSpace_size: " << freeSpace_size << endl;
    // added to_shift_data_size ask Rebecca(?)
    memset(headir_end, 0, freeSpace_size);
    char * freeSpace_pointer = to_shift_data + record.length;
    memcpy(freeSpace_pointer, records_to_load, to_shift_data_size);

    for(size_t i = 0; i < header.recordEntriesNumber; i++){

        SlotDirectoryRecordEntry upd_entry = getSlotDirectoryRecordEntry(pageData, i);

        if(i == rid.slotNum){
            upd_entry.offset = 0;
        }else{
            upd_entry.offset += record.length;
        }
        setSlotDirectoryRecordEntry( pageData, i, upd_entry);
    }
     cout << "Here"<< endl;

    // move freespaceOffset size of deleted record
    header.freeSpaceOffset += record.length;
    cout << "After headerfs: " << header.freeSpaceOffset << endl;
    setSlotDirectoryHeader(pageData, header);
     cout << "Here"<< endl;

    fileHandle.writePage(rid.pageNum, pageData);
    free(pageDataV);
    free(records_to_load);
    return 0;
}

RC RecordBasedFileManager::updateRecord(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const void *data, RID &rid){

    // malloc new page
    void *pageDataV = malloc(PAGE_SIZE);
    char *pageData = (char *)pageDataV;

    //get to load record size
    unsigned to_load_recordSize = getRecordSize(recordDescriptor, data);
    char *to_load_data = (char *) data;

    //error checking
    if (pageData == NULL)
        return RBFM_MALLOC_FAILED;

    //readPage into pageData ptr
    if(fileHandle.readPage(rid.pageNum, pageData))
            return RBFM_READ_FAILED;

    SlotDirectoryRecordEntry record = getSlotDirectoryRecordEntry(pageData, rid.slotNum);
    SlotDirectoryHeader header = getSlotDirectoryHeader(pageData);

    void *page2 = malloc(100);

    // SlotDirectoryRecordEntry erase;
    // setSlotDirectoryRecordEntry( page2, rid.slotNum, erase );

    void * records_to_load = malloc(PAGE_SIZE);
    char * to_shift_data = pageData + header.freeSpaceOffset;

    char * start_to_upd_record = pageData + record.offset;

    //set contents of record to 0
    memset(start_to_upd_record, 0, record.length);

    if(record.length == to_load_recordSize){
        //straight load it in
        cout << "hit 1 hit 1 hit 1" << endl;
        memcpy(start_to_upd_record, to_load_data, to_load_recordSize);
        fileHandle.writePage(rid.pageNum, pageData);
    }
    else if(record.length > to_load_recordSize){ //can read in record but must coalesce rest of records
         cout << "hit 2 hit 2 hit 2" << endl;
        //load in updated info by adding difference of size to properly align records
        unsigned diff = record.length - to_load_recordSize;
        memcpy(start_to_upd_record + diff, to_load_data, to_load_recordSize);

        //coalesce
        //subtract diff to make up for change in record size
        uint32_t to_shift_data_size =  (record.offset) - header.freeSpaceOffset - diff;
        // load into buffer
        memcpy(records_to_load, to_shift_data, to_shift_data_size);
        memset(to_shift_data, 0, to_shift_data_size);
        memcpy(to_shift_data + diff, records_to_load, to_shift_data_size);

        //update headers & slots
        for(size_t i = 0; i < header.recordEntriesNumber; i++){

            SlotDirectoryRecordEntry upd_entry = getSlotDirectoryRecordEntry(pageData, i);

            if(i == rid.slotNum){ //dont kill rid
                upd_entry.offset = record.offset + diff; //set to 0 for dead RID
                upd_entry.length = to_load_recordSize;
            }else{
                upd_entry.offset += diff;
            }
            // is the second arg right?
            setSlotDirectoryRecordEntry(pageData, i, upd_entry);
        }

        header.freeSpaceOffset += diff;
        setSlotDirectoryHeader(pageData, header);
    }
    else{ // else to_load_recordSize is bigger than record.length
        cout << "hit 3 hit 3 hit 3" << endl;
        bool pageFound = false;
        unsigned i;
        unsigned inserted_pageNum;
        unsigned numPages = fileHandle.getNumberOfPages();

        //look for page with enough free space
        for (i = 0; i < numPages; i++)
        {
            if (getPageFreeSpaceSize(pageData) >= sizeof(SlotDirectoryRecordEntry) + to_load_recordSize)
            {
                inserted_pageNum = i;
                pageFound = true;
                break;
            }
        }

        // If we can't find a page with enough space, we create a new one
        if(!pageFound)
        {
            //can i pass in nullptr to create a blank appendPage
            if(fileHandle.appendPage(nullptr))
                return RBFM_APPEND_FAILED;
            i += 1;
            inserted_pageNum = i;
        }

        if(rid.pageNum == i){ // if page found is same page as original record
            // write updated record to page
            header.freeSpaceOffset = header.freeSpaceOffset - to_load_recordSize;
            memcpy(pageData + header.freeSpaceOffset, data, to_load_recordSize);

            to_shift_data = pageData + header.freeSpaceOffset;
            //coalesce
            uint32_t to_shift_data_size =  record.offset - header.freeSpaceOffset;
            //load to_shift_data into buffer
            memcpy(records_to_load, to_shift_data, to_shift_data_size);
            // clear mem for new memcpy
            memset(to_shift_data, 0, to_shift_data_size);
            //load in buffer
            memcpy(to_shift_data, records_to_load, to_shift_data_size);

            //update headers & slots
            for(i = 0; i < header.recordEntriesNumber; i++){
                SlotDirectoryRecordEntry upd_entry = getSlotDirectoryRecordEntry(pageData, i);
                if(rid.slotNum == i){
                    upd_entry.length = to_load_recordSize;
                    upd_entry.offset = header.freeSpaceOffset;
                }else{
                    upd_entry.offset += record.length;
                }
                // what should the second arg be
                setSlotDirectoryRecordEntry(pageData, i, upd_entry);
            }

            fileHandle.writePage(inserted_pageNum, pageData);

        }else{ //else if is to be moved to another page

            // have page i that has enough free space
            void* pageDataVoid = malloc(PAGE_SIZE);
            char* pageDataV2 = (char *) pageDataVoid;

            //read contents of page with enough free space
            if(fileHandle.readPage(i, pageDataV2))
                    return RBFM_READ_FAILED;

            // grab header of newly read page
            SlotDirectoryHeader headerV2 = getSlotDirectoryHeader(pageDataV2);

            //set pointer to start of freespace and load in updated record
            pageDataV2 += (headerV2.freeSpaceOffset - to_load_recordSize);
            memcpy(pageDataV2, data, to_load_recordSize);

            headerV2.freeSpaceOffset = headerV2.freeSpaceOffset - to_load_recordSize;

            //coalesce
            // double check this below line is right
            uint32_t to_shift_data_size =  record.offset - header.freeSpaceOffset;
            //load to_shift_data into buffer
            memcpy(records_to_load, to_shift_data, to_shift_data_size);
            // clear mem for new memcpy
            memset(to_shift_data, 0, to_shift_data_size + record.length);
            //
            memcpy(to_shift_data + record.length, records_to_load, to_shift_data_size);

            //update headers & slots of original page
            for(i = 0; i < header.recordEntriesNumber; i++){

                SlotDirectoryRecordEntry upd_entry = getSlotDirectoryRecordEntry(pageData, i);
                if(i == rid.slotNum){
                    // kill rid
                    upd_entry.offset = 0;
                }else{
                    upd_entry.offset += record.length;
                }
                // what should the second arg be
                setSlotDirectoryRecordEntry(pageData, i, upd_entry);
            }

            // set header for original page
            header.freeSpaceOffset = header.freeSpaceOffset + record.length;
            setSlotDirectoryHeader(pageData, header);

            // update information for newly added to page
            // create a new slot for added record
            // create new rid
            rid.pageNum = inserted_pageNum;
            rid.slotNum = headerV2.recordEntriesNumber + 1;

            SlotDirectoryRecordEntry new_entry;
            new_entry.offset = headerV2.freeSpaceOffset;
            new_entry.length = to_load_recordSize;
            setSlotDirectoryRecordEntry(pageDataV2, headerV2.recordEntriesNumber+1, new_entry);
            fileHandle.writePage(inserted_pageNum, pageDataV2);
        }
    }

    free(pageDataV);
    free(records_to_load);
    return 0;

}

RC RecordBasedFileManager::readAttribute(FileHandle &fileHandle, const vector<Attribute> &recordDescriptor, const RID &rid, const string &attributeName, void *data){

    // Retrieve the specified page
    void * pageData = malloc(PAGE_SIZE);
    if (fileHandle.readPage(rid.pageNum, pageData))
        return RBFM_READ_FAILED;

    // Checks if the specific slot id exists in the page
    SlotDirectoryHeader slotHeader = getSlotDirectoryHeader(pageData);

    if(slotHeader.recordEntriesNumber < rid.slotNum)
         return RBFM_SLOT_DN_EXIST;

    // Gets the slot directory record entry data
    SlotDirectoryRecordEntry recordEntry = getSlotDirectoryRecordEntry(pageData, rid.slotNum);

    // Retrieve the actual entry data
    getRecordAttrAtOffset(pageData, recordEntry.offset, recordDescriptor, attributeName, data);

    //TEST PRINT OF RETURNED DATA
    unsigned i = 0;
    for (i = 0; i < recordDescriptor.size(); i++){
        if(!attributeName.compare(recordDescriptor[i].name)){
        switch (recordDescriptor[i].type)
        {
            case TypeInt:
                int retInt;
                memcpy(&retInt, data, INT_SIZE);
                cout << "readAttribute INT: " << retInt << endl;

            break;
            case TypeReal:
                float retFlot;
                memcpy(&retFlot, data, REAL_SIZE);
                cout << "readAttribute float: " << retFlot << endl;


            break;
            case TypeVarChar:
                char retStr[recordDescriptor[i].length];
               //Currently HARDCODED CHANGE FOR DIFFERENT RID
                memcpy(&retStr, data, 8);
                retStr[8] = '\0';
                cout << "readAttribute STR: " << retStr << endl;

            break;
        }
    }
    }
    free(pageData);
    return SUCCESS;
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
    unsigned data_offset = 0;
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
