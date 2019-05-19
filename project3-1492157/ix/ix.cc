
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

void IndexManager::initializeIndexFileHeaderPage(void * page){
    memset(page, 0 ,PAGE_SIZE);
    // Page Zero Is this Header, page one is initiailly Root
    struct IndexFileHeader indexFileHeader = {1};

    memcpy(page , &indexFileHeader, sizeof(IndexFileHeader));
}


void IndexManager::newLeafPage(void * page){
	memset(page, 0 ,PAGE_SIZE);
    // Writes the slot directory header.
    uint32_t freeSpaceOffset = sizeof(NodeHeader) + sizeof(LeafHeader);
    struct NodeHeader nodeHeader = {0, 0, true, false, freeSpaceOffset};
    setNodeHeader(page, nodeHeader);

    struct LeafHeader leafHeader = {0, 0};
    setLeafHeader(page, leafHeader);
}

void IndexManager::newNonLeafPage(void * page){
	memset(page, 0 ,PAGE_SIZE);
    // Writes the slot directory header.
    uint32_t freeSpaceOffset = sizeof(NodeHeader);
    struct NodeHeader nodeHeader = {0, 0, false, false, freeSpaceOffset};
    setNodeHeader(page, nodeHeader);
}

//Check ONLY WRITE TO PAGE ZERO
void IndexManager::setIndexFileHeader(void * page, IndexFileHeader indexFileHeader){
    
    memcpy (page, &indexFileHeader, sizeof(IndexFileHeader));
}

void IndexManager::setNodeHeader(void* page, NodeHeader nodeHeader){
    memcpy (page, &nodeHeader, sizeof(NodeHeader));
}

void IndexManager::setLeafHeader(void* page, LeafHeader leafHeader){
    memcpy ((char*) page + sizeof(NodeHeader), &leafHeader, sizeof(LeafHeader));
}

NodeHeader IndexManager::getNodeHeader(void * page)
{

    NodeHeader nodeHeader;
    memcpy (&nodeHeader, page, sizeof(NodeHeader));
    return nodeHeader;
}

LeafHeader IndexManager::getLeafHeader(void * page)
{
    LeafHeader leafHeader;
    memcpy (&leafHeader, (char*)page + sizeof(NodeHeader), sizeof(LeafHeader));
    return leafHeader;
}

IndexFileHeader IndexManager::getIndexFileHeader(void * page)
{
    IndexFileHeader indexFileHeader;
    memcpy (&indexFileHeader, page, sizeof(IndexFileHeader));
    return indexFileHeader;
}


void IndexManager::setSlotEntry(uint32_t slotNum, SlotEntry slotEntry, void * page){

    char * pageP = (char *) page;
    unsigned slotOffset = PAGE_SIZE - (slotNum * sizeof(slotEntry));

    memcpy(pageP + slotOffset, &slotEntry, sizeof(SlotEntry));
}

SlotEntry IndexManager::getSlotEntry(uint32_t slotNum, void * page){

    char * pageP = (char *) page;
    unsigned slotOffset = PAGE_SIZE - ((slotNum + 1)* sizeof(SlotEntry));

    SlotEntry slotEntry;
    memcpy(&slotEntry, pageP + slotOffset, sizeof(SlotEntry));

    return slotEntry;
}


RC IndexManager::createFile(const string &fileName)
{
    // Creating a new paged file.
    if (_pf_manager->createFile(fileName))
        return RBFM_CREATE_FAILED;


    // Setting up the first page.
    void * firstPageData = malloc(PAGE_SIZE);

    if (firstPageData == NULL)
        return RBFM_MALLOC_FAILED;

    FileHandle handle;
    if (_pf_manager->openFile(fileName.c_str(), handle))
        return RBFM_OPEN_FAILED;

    initializeIndexFileHeaderPage(firstPageData);

    // Page 0 is now the header page for the index file
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;


    //Creating the first nonleaf oage - ROOT
    newNonLeafPage(firstPageData);
    NodeHeader nodeHeader = getNodeHeader(firstPageData);
    nodeHeader.isRoot = true;

    setNodeHeader(firstPageData, nodeHeader);

    // Adds the first root nonleaf page.
    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;

    _pf_manager->closeFile(handle);

    free(firstPageData);
    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return _pf_manager->destroyFile(fileName.c_str());
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return _pf_manager->openFile(fileName.c_str(), ixfileHandle.fh);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return _pf_manager->closeFile(ixfileHandle.fh);
}

bool IndexManager::enoughFreeSpaceForDataEntry(void * pageData, const Attribute &attribute, const void *key){
	unsigned dataEntrySize = 0;

	switch (attribute.type)
	{
	    case TypeInt:
	        dataEntrySize += INT_SIZE;
        break;
	    case TypeReal:
	        dataEntrySize += REAL_SIZE;
        break;
	    case TypeVarChar:
	        uint32_t varcharSize;
	        memcpy(&varcharSize, key, VARCHAR_LENGTH_SIZE);
	        dataEntrySize += VARCHAR_LENGTH_SIZE;
	        dataEntrySize += varcharSize;
        break;
        }

    dataEntrySize += sizeof(SlotEntry);
    dataEntrySize += sizeof(RID);

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
    //Offset page by size of headers
    page += leafPageNodeHeader.freeSpaceOffset;
    uint32_t length = 0;

    switch (attribute.type)
    {
        case TypeInt:
            cout << "dataEntry.key: " << dataEntry.key << endl;
            int qump;
            memcpy(&qump, dataEntry.key, INT_SIZE);
            cout << "quemp " << qump << endl;
            memcpy(page + length, dataEntry.key, INT_SIZE);
            length += INT_SIZE;
        break;
        case TypeReal:
            memcpy(page + length, dataEntry.key, REAL_SIZE);
            length += REAL_SIZE;
        break;
        case TypeVarChar:
            uint32_t varcharSize;
            memcpy(&varcharSize, dataEntry.key, VARCHAR_LENGTH_SIZE);
            memcpy(page + length, &varcharSize, VARCHAR_LENGTH_SIZE);
            length += VARCHAR_LENGTH_SIZE;

            memcpy(page + length, ((char*) dataEntry.key + VARCHAR_LENGTH_SIZE), varcharSize);
            length += varcharSize;
        break;
    }
    //Writing the RID to the page
    memcpy(page + length, &dataEntry.rid, sizeof(RID));
    length += sizeof(RID);

    //Inserting a slot for the new entry

    uint32_t offset = leafPageNodeHeader.freeSpaceOffset;
    struct SlotEntry slotEntry = {length, offset};

    setSlotEntry(leafPageNodeHeader.numSlots, slotEntry, pageData);
    leafPageNodeHeader.numSlots += 1;

    //Updating header variables
    leafPageNodeHeader.freeSpaceOffset += length;

    //Update the Header
    setNodeHeader(pageData, leafPageNodeHeader);

    return SUCCESS;
 //      return -1;
}

void IndexManager::getKeyd(const Attribute &attribute, void * retKey, const void * key){
    int size = 0;
    switch (attribute.type)
    {
        case TypeInt:
            int keyd;
            memcpy(&keyd, key, INT_SIZE);
            cout << "keyd:" << keyd << endl;
            memcpy(retKey, key, INT_SIZE);
        break;
        case TypeReal:
            size += REAL_SIZE;
            float keyf;
            memcpy(&keyf, key, REAL_SIZE);
            retKey = &keyf;
        break;
        case TypeVarChar:
            int varcharSize;
            memcpy(&varcharSize, key, VARCHAR_LENGTH_SIZE);

            char * ya = (char*) malloc(varcharSize);

            memcpy(retKey, ya, varcharSize);
        break;
    }
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void * indexFileHeaderPage = malloc(PAGE_SIZE);
   
    void * rootPageData = malloc(PAGE_SIZE);
    char * rootPage = (char *)rootPageData;

    //Grab the Header Page
    if(ixfileHandle.readPage(0, indexFileHeaderPage))
        return IX_READ_FAILED;

    IndexFileHeader indexFileHeader = getIndexFileHeader(indexFileHeaderPage);
    
    cout << "rootPageId: " << indexFileHeader.rootPageId << endl;
    //Using the Header Page information, grab the appropriate ROOT Page
    if(ixfileHandle.readPage(indexFileHeader.rootPageId, rootPage))
        return IX_READ_FAILED;
   
    NodeHeader rootNodeHeader = getNodeHeader(rootPage);

     cout << "numSlots: " << rootNodeHeader.numSlots << endl;
    //Base Case: Root is Empty, first insertion
    if(rootNodeHeader.numSlots == 0){
        //Data Page
        // Create a new page for Data Entries (Leaf Page)
        void * pageData = malloc(PAGE_SIZE);
        char * page = (char *)pageData;

        newLeafPage(page);
        DataEntry dataEntry;
        void * tempkey = nullptr;

        unsigned size = 0;

        dataEntry = {tempkey, rid};
        dataEntry.key = malloc(PAGE_SIZE);

        getKeyd(attribute, dataEntry.key, key);
        // dataEntry.key = tempkey;

        cout << "tempkey" << tempkey << endl;
        cout << "hit 1" << endl;

        // dataEntry.rid = rid;
        // Check if there is enough room for the new Data Entry and insert into the Leaf
        cout << "dataEntry.key " << dataEntry.key << endl;
        if(enoughFreeSpaceForDataEntry(page, attribute, dataEntry.key)){
            insertDataEntry(page, attribute, dataEntry);
        }

        cout << "hit 2" << endl;

        // Update Header of this New Leaf Page so that the parent is ROOT
        NodeHeader dataNodeHeader = getNodeHeader(page);
        dataNodeHeader.parent = indexFileHeader.rootPageId;
        setNodeHeader(page, dataNodeHeader);

        cout << "DataNodeParent: " << dataNodeHeader.parent << endl;
        cout << "DataNodeNumSlots: " << dataNodeHeader.numSlots << endl;

        // Take this New Leaf Page and Finally add it to the actual file
        if (ixfileHandle.appendPage(page))
            return RBFM_APPEND_FAILED;

        // Updating ROOT Index Page with correspong Index entry
        cout << "Key: " << key << endl;;
        IndexEntry indexEntry;
        tempkey = nullptr;
        //indexEntry.rightChild = 1;
        indexEntry = {tempkey, 2};
        indexEntry.key = malloc(PAGE_SIZE);

        getKeyd(attribute, indexEntry.key, key);
        //indexEntry.key = tempkey;

        if(enoughFreeSpaceForIndexEntry(rootPage, attribute, key)){
            insertIndexEntry(rootPage, attribute, indexEntry);
        }

        if (ixfileHandle.writePage(indexFileHeader.rootPageId, rootPage)){
            return RBFM_APPEND_FAILED;
        }

        free(pageData);
    }

    free(rootPageData);
    return SUCCESS;

    //return -1;
}

bool IndexManager::enoughFreeSpaceForIndexEntry(void * pageData, const Attribute &attribute, const void *key){

	unsigned indexEntrySize = 0;
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
    indexEntrySize += sizeof(SlotEntry);
	indexEntrySize += sizeof(uint32_t);

	if (getPageFreeSpaceSize(pageData) >= indexEntrySize){
	    return true;
	}else{
		return false;
	}
	//return false;
}

RC IndexManager::insertIndexEntry(void * pageData, const Attribute &attribute,const IndexEntry &indexEntry)
{
	char * page = (char *)pageData;

    NodeHeader nonLeafPageNodeHeader = getNodeHeader(page);
    page += nonLeafPageNodeHeader.freeSpaceOffset;

    uint32_t length = 0;

    switch (attribute.type)
    {
        case TypeInt:
            int keyd;
            memcpy(&keyd, indexEntry.key, INT_SIZE);
            // cout << "keyd: " << keyd << endl;;
            memcpy(page + length, &keyd, INT_SIZE);
            length += INT_SIZE;
        break;
        case TypeReal:
            memcpy(page + length, indexEntry.key, REAL_SIZE);
            length += REAL_SIZE;
        break;
        case TypeVarChar:
            uint32_t varcharSize;
            memcpy(&varcharSize, indexEntry.key, VARCHAR_LENGTH_SIZE);
            memcpy(page + length, &varcharSize, VARCHAR_LENGTH_SIZE);
            length += VARCHAR_LENGTH_SIZE;

            memcpy(page + nonLeafPageNodeHeader.freeSpaceOffset, ((char*) indexEntry.key + VARCHAR_LENGTH_SIZE), varcharSize);
            length += varcharSize;
        break;
    }

    memcpy(page + length, &indexEntry.rightChild, sizeof(uint32_t));
    length += sizeof(uint32_t);

    uint32_t offset = nonLeafPageNodeHeader.freeSpaceOffset;
    struct SlotEntry slotEntry = {length, offset};

    nonLeafPageNodeHeader.numSlots += 1;
    setSlotEntry(nonLeafPageNodeHeader.numSlots, slotEntry, pageData);

    //Updating header variables
    nonLeafPageNodeHeader.freeSpaceOffset += length;

    //Update the Header
    setNodeHeader(pageData, nonLeafPageNodeHeader);

    return SUCCESS;

   // return -1;
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

    // if(ixfileHandle.readPage(pageNum, page))
    //     return IX_READ_FAILED;

    // NodeHeader nodeHeader = getNodeHeader(page);
    // for(unsigned i = 0; i < nodeHeader.numSlots; i++){
        //Get the left child
        // if(!nodeHeader.isLeaf){
        //     //Some get indexEntryFunction
        //     // Extract the left Child
        //     // offset += size of the left key entry
        //     // PRINT KEY
        //     // PRINT 'children : ['
        //     // recurBtree(left child);
        // }else{
        //     //PRINT ALL DATA ENTRIES ON LEAF PAGE
        // }

//    }

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

    ixfileHandle.readPage(1, page);
    printNonLeafHelper(page, attribute);

    ixfileHandle.readPage(2, page);
    //     return IX_READ_FAILED;

    NodeHeader nodeHeader = getNodeHeader(page);

    printLeafHelper(page, attribute);

    free(pageData);



    // for(unsigned i = 0; i < nodeHeader.numSlots; i++){
    //     printBtree();

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
                SlotEntry SlotEntry = getSlotEntry(i, pageData);
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
                cout << "\""<< it->first << ":[" ;
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
            cout << "}," << endl;

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


void IndexManager::printNonLeafHelper(void * pageData, const Attribute &attribute){

    char * page = (char *) pageData;
    NodeHeader nodeHeader = getNodeHeader(page);

    unsigned offset = sizeof(NodeHeader);
    cout << "{\"keys\":  [";
    switch(attribute.type)
    {
        case TypeInt: {
            // map<int, uint32_t> intMapVecRid;

            for(unsigned i = 0; i < nodeHeader.numSlots; i++){
                int keyInt;
                memcpy(&keyInt, page + offset, INT_SIZE);
                offset += INT_SIZE;
                // cout << "keyInt: " << keyInt << endl;

                uint32_t pageNum;
                memcpy(&pageNum, page + offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);

                cout << "\"" << keyInt << "\"" << pageNum << ",";

                // IndexEntry indexEntry;
                // memcpy(&indexEntry, page + offset, sizeof(IndexEntry));
                // offset += sizeof(IndexEntry);
                // cout << "\"" << indexEntry.key << "\"" << ",";
            }

            cout << "]," << endl;
        // go throuhg map and print.
            // for(auto it = intMapVecRid.begin(); it != intMapVecRid.end(); ++it){
            //     cout << "\""<< it->first << ":[" ;
            //     for(unsigned j = 0; j < it->second.size(); j++  ){
            //         cout << "(" << it->second[j].pageNum << "," << it->second[j].slotNum << ")";
            //         if(j + 1 != it->second.size()){
            //             cout << ",";
            //         }
            //     }
            //     cout << "]\"";
            //     if(std::next(it, 1) != intMapVecRid.end()){
            //         cout << ",";
            //     }
            // }
            // cout << "},";
            break;
        }
        // case TypeReal:{
        //     map<float, vector<RID>> floatMapVecRid;
        //     for(unsigned i = 0; i < nodeHeader.numSlots; i++){
        //         float keyFloat;
        //         memcpy(&keyFloat, page + offset, REAL_SIZE);
        //         offset += REAL_SIZE;
        //
        //         RID rid;
        //         memcpy(&rid, page + offset, sizeof(RID));
        //         offset += sizeof(RID);
        //
        //         std::vector<RID> ridVec;
        //         ridVec.push_back(rid);
        //         if(!floatMapVecRid.insert({keyFloat, ridVec}).second){
        //             // duplicate handling
        //             floatMapVecRid[keyFloat].push_back(rid);
        //         }
        //     }
        //     for(auto it = floatMapVecRid.begin(); it != floatMapVecRid.end(); ++it){
        //         cout << "\""<< it->first << ":["  <<endl;
        //         for(unsigned j = 0; j < it->second.size(); j++  ){
        //             cout << "(" << it->second[j].pageNum << "," << it->second[j].slotNum << ")";
        //             if(j + 1 != it->second.size()){
        //                 cout << ",";
        //             }
        //         }
        //         cout << "]\"";
        //         if(std::next(it, 1) != floatMapVecRid.end()){
        //             cout << ",";
        //         }
        //     }
        //     cout << "}," << endl;
        //
        //     break;
        // }
        // case TypeVarChar:{
        //
        //     uint32_t varcharSize;
        //     map<char*, vector<RID>> charMapVecRid;
        //     for(unsigned i = 0; i < nodeHeader.numSlots; i++){
        //
        //         memcpy(&varcharSize, page + offset, VARCHAR_LENGTH_SIZE);
        //         offset+= VARCHAR_LENGTH_SIZE;
        //
        //         char * keyVarChar = (char *) calloc(1, varcharSize + 1);
        //         memcpy(keyVarChar, page + offset, varcharSize);
        //
        //         offset += varcharSize;
        //         keyVarChar[varcharSize + 1] = '\0';
        //
        //         RID rid;
        //         memcpy(&rid, page + offset, sizeof(RID));
        //         offset += sizeof(RID);
        //
        //         std::vector<RID> ridVec;
        //         ridVec.push_back(rid);
        //         if(!charMapVecRid.insert({keyVarChar, ridVec}).second){
        //             // duplicate handling
        //             charMapVecRid[keyVarChar].push_back(rid);
        //         }
        //     }
        //     for(auto it = charMapVecRid.begin(); it != charMapVecRid.end(); ++it){
        //         cout << "\""<< it->first << ":["  <<endl;
        //         for(unsigned j = 0; j < it->second.size(); j++  ){
        //             cout << "(" << it->second[j].pageNum << "," << it->second[j].slotNum << ")";
        //             if(j + 1 != it->second.size()){
        //                 cout << ",";
        //             }
        //         }
        //         cout << "]\"";
        //         if(std::next(it, 1) != charMapVecRid.end()){
        //             cout << ",";
        //         }
        //     }
        //     cout << "},";
        //
        //     break;
        // }

    }
}

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

RC IXFileHandle::readPage(PageNum pageNum, void *data){
	ixReadPageCounter++;
	return fh.readPage(pageNum, data);

}

RC IXFileHandle::writePage(PageNum pageNum, const void *data){
	ixWritePageCounter++;
	return fh.writePage(pageNum, data);
}

RC IXFileHandle::appendPage(const void *data){
	ixAppendPageCounter++;
	return fh.appendPage(data);

}

unsigned IndexManager::getPageFreeSpaceSize(void * page)
{
    NodeHeader nodeHeader = getNodeHeader(page);
    return PAGE_SIZE - nodeHeader.freeSpaceOffset;
}
