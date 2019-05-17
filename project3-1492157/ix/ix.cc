
#include "ix.h"

IndexManager* IndexManager::_index_manager = 0;
PagedFileManager *IndexManager::_pf_manager = NULL;

IndexManager* IndexManager::instance()
{
    if(!_index_manager)
        _index_manager = new IndexManager();

    return _index_manager;
}

IndexManager::IndexManager()
{
    _pf_manager = PagedFileManager::instance();
}

IndexManager::~IndexManager()
{
}

void IXFileHandle::IXToFile(FileHandle &fileHandle){

    fileHandle.readPageCounter = ixReadPageCounter;
    fileHandle.writePageCounter = ixWritePageCounter;
    fileHandle.appendPageCounter = ixAppendPageCounter;
    fileHandle.setfd(getfd());
}

void IXFileHandle::fileToIX(FileHandle &fileHandle){
    ixReadPageCounter = fileHandle.readPageCounter;
    ixWritePageCounter = fileHandle.writePageCounter;
    ixAppendPageCounter = fileHandle.appendPageCounter;
    setfd(fileHandle.getfd());
}

void IndexManager::newLeafPage(void * page){
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    NodeHeader nodeHeader;
    nodeHeader.parent = 0;
    nodeHeader.numSlots = 0;
    nodeHeader.isLeaf = true;
    nodeHeader.isRoot = false;
    nodeHeader.freeSpaceOffset = sizeof(NodeHeader) + sizeof(LeafHeader);
    setNodeHeader(page, nodeHeader);

    LeafHeader leafHeader;
    leafHeader.nextPage = 0;
    leafHeader.prevPage = 0;

    setLeafHeader(page, leafHeader);
}

void IndexManager::newNonLeafPage(void * page){
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    NodeHeader nodeHeader;
    nodeHeader.parent = 0;
    nodeHeader.numSlots = 0;
    nodeHeader.isLeaf = false;
    nodeHeader.isRoot = false;
    nodeHeader.freeSpaceOffset = sizeof(NodeHeader);
    setNodeHeader(page, nodeHeader);
}

void IndexManager::setNodeHeader(void* page, NodeHeader nodeHeader){
    memcpy (page, &nodeHeader, sizeof(NodeHeader));
}

void IndexManager::setLeafHeader(void* page, LeafHeader leafHeader){
    memcpy ((char*) page + sizeof(NodeHeader), &leafHeader, sizeof(LeafHeader));
}

NodeHeader IndexManager::getNodeHeader(void * page)
{
    // Getting the slot directory header.
    NodeHeader nodeHeader;
    memcpy (&nodeHeader, page, sizeof(NodeHeader));
    return nodeHeader;
}

LeafHeader IndexManager::getLeafHeader(void * page)
{
    // Getting the slot directory header.
    LeafHeader leafHeader;
    memcpy (&leafHeader, (char*)page + sizeof(NodeHeader), sizeof(LeafHeader));
    return leafHeader;
}

RC IndexManager::createFile(const string &fileName)
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;

    // Setting up the first page.
    void * firstPageData = calloc(PAGE_SIZE, 1);
    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;
    newNonLeafPage(firstPageData);
    NodeHeader nodeHeader = getNodeHeader(firstPageData);
    nodeHeader.isRoot = true;

    setNodeHeader(firstPageData, nodeHeader);

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

RC IndexManager::destroyFile(const string &fileName)
{
    RC rc = _pf_manager->destroyFile(fileName.c_str());
    return rc;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    FileHandle fileHandle;
    ixfileHandle.IXToFile(fileHandle);

    RC rc = _pf_manager->openFile(fileName.c_str(), fileHandle);
    ixfileHandle.fileToIX(fileHandle);
    return rc;
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    FileHandle fileHandle;
    ixfileHandle.IXToFile(fileHandle);

    RC rc = _pf_manager->closeFile(fileHandle);
    ixfileHandle.fileToIX(fileHandle);
    return rc;
}

bool IndexManager::enoughFreeSpaceForDataEntry(void * pageData, const Attribute &attribute, const void *key){
	unsigned dataEntrySize = 0;

    cout << "att type " << attribute.type << endl;
	switch (attribute.type)
	{
	    case TypeInt:
            cout << "Data Entry size: " << dataEntrySize << endl;
	        dataEntrySize = dataEntrySize + INT_SIZE;
            cout << "INT_SIZE" << INT_SIZE << endl;
            cout << "INT_SIZE" << (unsigned) INT_SIZE << endl;
        break;
	    case TypeReal:
	        dataEntrySize += REAL_SIZE;
            cout << "INT_SIZE" << INT_SIZE << endl;
            cout << "INT_SIZE" << (unsigned) INT_SIZE << endl;
        break;
	    case TypeVarChar:
	        uint32_t varcharSize;
	        memcpy(&varcharSize, key, VARCHAR_LENGTH_SIZE);
	        dataEntrySize += VARCHAR_LENGTH_SIZE;
	        dataEntrySize += varcharSize;
            cout << "INT_SIZE" << INT_SIZE << endl;
            cout << "INT_SIZE" << (unsigned) INT_SIZE << endl;
        break;
	    }
    cout << "Data Entry size: " << dataEntrySize << endl;
    dataEntrySize += sizeof(RID);

    cout << "Data Entry size: " << dataEntrySize << endl;

	if (getPageFreeSpaceSize(pageData) >= dataEntrySize){
	    return true;
	}else{
		return false;
	}

}

RC IndexManager::insertDataEntry(void * pageData, const Attribute &attribute,const DataEntry &dataEntry)
{
	char * page = (char *)pageData;

    NodeHeader leafPageNodeHeader = getNodeHeader(page);

    switch (attribute.type)
    {
        case TypeInt:
            memcpy(page + leafPageNodeHeader.freeSpaceOffset, dataEntry.key, INT_SIZE);
            leafPageNodeHeader.freeSpaceOffset += INT_SIZE;
        break;
        case TypeReal:
            memcpy(page + leafPageNodeHeader.freeSpaceOffset, dataEntry.key, REAL_SIZE);
            leafPageNodeHeader.freeSpaceOffset += REAL_SIZE;
        break;
        case TypeVarChar:
            uint32_t varcharSize;
            memcpy(&varcharSize, dataEntry.key, VARCHAR_LENGTH_SIZE);
            memcpy(page + leafPageNodeHeader.freeSpaceOffset, &varcharSize, VARCHAR_LENGTH_SIZE);
            leafPageNodeHeader.freeSpaceOffset += VARCHAR_LENGTH_SIZE;

            memcpy(page + leafPageNodeHeader.freeSpaceOffset, ((char*) dataEntry.key + VARCHAR_LENGTH_SIZE), varcharSize);
            leafPageNodeHeader.freeSpaceOffset += varcharSize;
        break;
    }
    memcpy(page + leafPageNodeHeader.freeSpaceOffset, &dataEntry.rid, sizeof(RID));
    leafPageNodeHeader.freeSpaceOffset += sizeof(RID);

    leafPageNodeHeader.numSlots += 1;

    //Update the Header
    setNodeHeader(page, leafPageNodeHeader);

    return SUCCESS;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void * rootPageData = malloc(PAGE_SIZE);
    char * rootPage = (char *)rootPageData;

    FileHandle fileHandle;
    ixfileHandle.IXToFile(fileHandle);
    if(fileHandle.readPage(0, rootPage))
        return IX_READ_FAILED;

    cout << "hit 1" << endl;

    NodeHeader rootNodeHeader = getNodeHeader(rootPage);
    //Base Case: Root is Empty, first insertion
    if(rootNodeHeader.numSlots == 0){
        //Data Page
        // Create a new page for Data Entries (Leaf Page)
        void * pageData = malloc(PAGE_SIZE);
        char * page = (char *)pageData;

        newLeafPage(page);
        DataEntry dataEntry;

        unsigned size = 0;

        switch (attribute.type)
        {
            case TypeInt:
                size += INT_SIZE;
                int keyd;
                memcpy(&keyd, key, size);
                dataEntry.key = &keyd;
            break;
            case TypeReal:
                size += REAL_SIZE;
                float keyf;
                memcpy(&keyf, key, size);
                dataEntry.key = &keyf;
            break;
            case TypeVarChar:
                int varcharSize;
                memcpy(&varcharSize, key, VARCHAR_LENGTH_SIZE);
                size = varcharSize + VARCHAR_LENGTH_SIZE;
                memcpy(dataEntry.key, key, size);
                // dataEntry.key = &keyd;
            break;
        }

        dataEntry.rid = rid;
        // Check if there is enough room for the new Data Entry and insert into the Leaf
        if(enoughFreeSpaceForDataEntry(page, attribute, dataEntry.key)){
            cout << "hit 2" << endl;
            insertDataEntry(page, attribute, dataEntry);
        }

        cout << "hit 3" << endl;
        // Update Header of this New Leaf Page so that the parent is ROOT
        NodeHeader dataNodeHeader = getNodeHeader(page);
        cout << "hit 4" << endl;
        dataNodeHeader.parent = 0;
        setNodeHeader(page, dataNodeHeader);
        cout << "hit 5" << endl;

        // Take this New Leaf Page and Finally add it to the actual file
        if (fileHandle.appendPage(page))
            return RBFM_APPEND_FAILED;

        // Updating ROOT Index Page with correspong Index entry
        IndexEntry indexEntry;
        indexEntry.key = &key;
        indexEntry.rightChild = 1;
        cout << "hit 6" << endl;

        if(enoughFreeSpaceForIndexEntry(rootPage, attribute, key)){
            insertIndexEntry(rootPage, attribute, indexEntry);
        }

        cout << "hit 7" << endl;

        if (fileHandle.writePage(0, page)){
            return RBFM_APPEND_FAILED;
        }

        free(pageData);
    }

    ixfileHandle.fileToIX(fileHandle);

    free(rootPageData);
    return SUCCESS;
}

bool IndexManager::enoughFreeSpaceForIndexEntry(void * pageData, const Attribute &attribute, const void *key){
	int indexEntrySize = 0;
	switch (attribute.type)
	{
	    case TypeInt:
	        indexEntrySize += INT_SIZE;
        break;
	    case TypeReal:
	        indexEntrySize += REAL_SIZE;
        break;
	    case TypeVarChar:
	        uint32_t varcharSize;
	        memcpy(&varcharSize, key, VARCHAR_LENGTH_SIZE);
	        indexEntrySize += VARCHAR_LENGTH_SIZE;
	        indexEntrySize += varcharSize;
        break;
	    }
	indexEntrySize += sizeof(uint32_t);

	if (getPageFreeSpaceSize(pageData) >= indexEntrySize){
	    return true;
	}else{
		return false;
	}
}

RC IndexManager::insertIndexEntry(void * pageData,const Attribute &attribute,const IndexEntry &indexEntry)
{
	char * page = (char *)pageData;

    NodeHeader nonLeafPageNodeHeader = getNodeHeader(page);

    switch (attribute.type)
    {
        case TypeInt:
            memcpy(page + nonLeafPageNodeHeader.freeSpaceOffset, indexEntry.key, INT_SIZE);
            nonLeafPageNodeHeader.freeSpaceOffset += INT_SIZE;
        break;
        case TypeReal:
            memcpy(page + nonLeafPageNodeHeader.freeSpaceOffset, indexEntry.key, REAL_SIZE);
            nonLeafPageNodeHeader.freeSpaceOffset += REAL_SIZE;
        break;
        case TypeVarChar:
            uint32_t varcharSize;
            memcpy(&varcharSize, indexEntry.key, VARCHAR_LENGTH_SIZE);
            memcpy(page + nonLeafPageNodeHeader.freeSpaceOffset, &varcharSize, VARCHAR_LENGTH_SIZE);
            nonLeafPageNodeHeader.freeSpaceOffset += VARCHAR_LENGTH_SIZE;

            memcpy(page + nonLeafPageNodeHeader.freeSpaceOffset, ((char*) indexEntry.key + VARCHAR_LENGTH_SIZE), varcharSize);
            nonLeafPageNodeHeader.freeSpaceOffset += varcharSize;
        break;
    }
    memcpy(page + nonLeafPageNodeHeader.freeSpaceOffset, &indexEntry.rightChild, sizeof(uint32_t));
    nonLeafPageNodeHeader.freeSpaceOffset += sizeof(uint32_t);

    nonLeafPageNodeHeader.numSlots += 1;

    //Update the Header
    setNodeHeader(page, nonLeafPageNodeHeader);

    return SUCCESS;
}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    return -1;
}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return -1;
}

RC IndexManager::recurBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, unsigned pageNum){
    return -1;
    // void * pageData = malloc(PAGE_SIZE);
    // char * page = (char *) pageData;
    //
    // FileHandle fileHandle;
    // ixfileHandle.IXToFile(fileHandle);
    //
    // if(fileHandle.readPage(pageNum, page))
    //     return IX_READ_FAILED;
    //
    // NodeHeader nodeHeader = getNodeHeader(page);
    // for(unsigned i = 0; i < nodeHeader.numSlots; i++){
    //     //Get the left child
    //     // if(!nodeHeader.isLeaf){
    //     //     //Some get indexEntryFunction
    //     //     // Extract the left Child
    //     //     // offset += size of the left key entry
    //     //     // PRINT KEY
    //     //     // PRINT 'children : ['
    //     //     // recurBtree(left child);
    //     // }else{
    //     //     //PRINT ALL DATA ENTRIES ON LEAF PAGE
    //     // }
    //
    // }

    // Some get indexEntryFunction
    // Extract the right Child
    // offset += size of the right key entry
    // if(!nodeHeader.isLeaf){
    //     // print leafPage helper function
    //     recurBtree(right child);
    // }

}

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) {
    // visit rootPage
    // traverse left sub Tree
    // traverse the right subtree
    void * pageData = malloc(PAGE_SIZE);
    char * page = (char *) pageData;

    FileHandle fileHandle;
    ixfileHandle.IXToFile(fileHandle);

    fileHandle.readPage(1, page);
    //     return IX_READ_FAILED;

    NodeHeader nodeHeader = getNodeHeader(page);

    printLeafHelper(page, attribute);



    // for(unsigned i = 0; i < nodeHeader.numSlots; i++){
    //     printBtree()
    //
    // }


}



void IndexManager::printLeafHelper(void * pageData, const Attribute &attribute){

    char * page = (char *) pageData;
    NodeHeader nodeHeader = getNodeHeader(page);

    unsigned offset = sizeof(NodeHeader) + sizeof(LeafHeader);
    cout << "{\"keys\":  [";
    switch(attribute.type)
    {
        case TypeInt: {
            map<int, vector<RID>> intMapVecRid;
            for(unsigned i = 0; i < nodeHeader.numSlots; i++){
                int keyInt;
                memcpy(&keyInt, page + offset, INT_SIZE);
                offset += INT_SIZE;
                // cout << "keyInt: " << keyInt << endl;

                RID rid;
                memcpy(&rid, page + offset, sizeof(RID));
                offset += sizeof(RID);

                std::vector<RID> ridVec;
                ridVec.push_back(rid);
                if(!intMapVecRid.insert({keyInt, ridVec}).second){
                    // duplicate handling
                    intMapVecRid[keyInt].push_back(rid);
                }

            }
        // go throuhg map and print.
            for(auto it = intMapVecRid.begin(); it != intMapVecRid.end(); ++it){
                cout << "\""<< it->first << ":["  <<endl;
                for(unsigned j = 0; j < it->second.size(); j++  ){
                    cout << "(" << it->second[j].pageNum << "," << it->second[j].slotNum << ")";
                    if(j + 1 != it->second.size()){
                        cout << ",";
                    }
                }
                cout << "]\"";
                if(std::next(it, 1) != intMapVecRid.end()){
                    cout << ",";
                }
            }
            cout << "},";
            break;
        }
        case TypeReal:{
            map<float, vector<RID>> floatMapVecRid;
            for(unsigned i = 0; i < nodeHeader.numSlots; i++){
                float keyFloat;
                memcpy(&keyFloat, page + offset, REAL_SIZE);
                offset += REAL_SIZE;

                RID rid;
                memcpy(&rid, page + offset, sizeof(RID));
                offset += sizeof(RID);

                std::vector<RID> ridVec;
                ridVec.push_back(rid);
                if(!floatMapVecRid.insert({keyFloat, ridVec}).second){
                    // duplicate handling
                    floatMapVecRid[keyFloat].push_back(rid);
                }
            }
            for(auto it = floatMapVecRid.begin(); it != floatMapVecRid.end(); ++it){
                cout << "\""<< it->first << ":["  <<endl;
                for(unsigned j = 0; j < it->second.size(); j++  ){
                    cout << "(" << it->second[j].pageNum << "," << it->second[j].slotNum << ")";
                    if(j + 1 != it->second.size()){
                        cout << ",";
                    }
                }
                cout << "]\"";
                if(std::next(it, 1) != floatMapVecRid.end()){
                    cout << ",";
                }
            }
            cout << "},";

            break;
        }
        case TypeVarChar:{

            uint32_t varcharSize;
            map<char*, vector<RID>> charMapVecRid;
            for(unsigned i = 0; i < nodeHeader.numSlots; i++){

                memcpy(&varcharSize, page + offset, VARCHAR_LENGTH_SIZE);
                offset+= VARCHAR_LENGTH_SIZE;

                char * keyVarChar = (char *) calloc(1, varcharSize + 1);
                memcpy(keyVarChar, page + offset, varcharSize);

                offset += varcharSize;
                keyVarChar[varcharSize + 1] = '\0';

                RID rid;
                memcpy(&rid, page + offset, sizeof(RID));
                offset += sizeof(RID);

                std::vector<RID> ridVec;
                ridVec.push_back(rid);
                if(!charMapVecRid.insert({keyVarChar, ridVec}).second){
                    // duplicate handling
                    charMapVecRid[keyVarChar].push_back(rid);
                }
            }
            for(auto it = charMapVecRid.begin(); it != charMapVecRid.end(); ++it){
                cout << "\""<< it->first << ":["  <<endl;
                for(unsigned j = 0; j < it->second.size(); j++  ){
                    cout << "(" << it->second[j].pageNum << "," << it->second[j].slotNum << ")";
                    if(j + 1 != it->second.size()){
                        cout << ",";
                    }
                }
                cout << "]\"";
                if(std::next(it, 1) != charMapVecRid.end()){
                    cout << ",";
                }
            }
            cout << "},";

            break;
        }

    }


}


// void IndexManager::printNonLeafHelper(void * page){
//
// }

IX_ScanIterator::IX_ScanIterator()
{
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{
    return -1;
}

RC IX_ScanIterator::close()
{
    return -1;
}


IXFileHandle::IXFileHandle()
{
    ixReadPageCounter = 0;
    ixWritePageCounter = 0;
    ixAppendPageCounter = 0;
}

IXFileHandle::~IXFileHandle()
{
}

RC IXFileHandle::collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount)
{
    readPageCount   = ixReadPageCounter;
    writePageCount  = ixWritePageCounter;
    appendPageCount = ixAppendPageCounter;
    return SUCCESS;
}

void IXFileHandle::setfd(FILE *fd)
{
    _fd = fd;
}

FILE *IXFileHandle::getfd()
{
    return _fd;
}

unsigned IndexManager::getPageFreeSpaceSize(void * page)
{
    NodeHeader nodeHeader = getNodeHeader(page);
    return PAGE_SIZE - nodeHeader.freeSpaceOffset;
}
