
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


void IndexManager::newLeafPage(void * page,const uint32_t &pageID){
	memset(page, 0 ,PAGE_SIZE);
    // Writes the slot directory header.
    uint32_t freeSpaceOffset = sizeof(NodeHeader) + sizeof(LeafHeader);
    struct NodeHeader nodeHeader = {pageID, 0, 0, true, false, freeSpaceOffset};
    setNodeHeader(page, nodeHeader);

    struct LeafHeader leafHeader = {0, 0};
    setLeafHeader(page, leafHeader);
}

void IndexManager::newNonLeafPage(void * page, const uint32_t &pageID){
	memset(page, 0 ,PAGE_SIZE);
    // Writes the slot directory header.
    uint32_t freeSpaceOffset = sizeof(NodeHeader);
    struct NodeHeader nodeHeader = {pageID, 0, 0, false, false, freeSpaceOffset};
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
    unsigned slotOffset = PAGE_SIZE - ((slotNum + 1) * sizeof(slotEntry));

    memcpy(pageP + slotOffset, &slotEntry, sizeof(SlotEntry));
}

SlotEntry IndexManager::getSlotEntry(uint32_t slotNum, void * page){

    char * pageP = (char *) page;
    unsigned slotOffset = PAGE_SIZE - ((slotNum + 1)* sizeof(SlotEntry));

    SlotEntry slotEntry;
    memcpy(&slotEntry, pageP + slotOffset, sizeof(SlotEntry));

    return slotEntry;
}

RC IndexManager::deleteSlotEntry(uint32_t slotNum, uint32_t totalSlots, void * page){

    char * pageP = (char *) page;
    unsigned slotOffset = PAGE_SIZE - ((slotNum + 1)* sizeof(SlotEntry));
    cout << "slotOffset: " << slotOffset << endl;
    unsigned lastSlotOffset = PAGE_SIZE - ((totalSlots )* sizeof(SlotEntry));
    cout << "lastSlotOffset: " << lastSlotOffset << endl;
    unsigned updatedSlotOffset = lastSlotOffset + sizeof(SlotEntry);
    cout << "updatedSlotOffset: " << updatedSlotOffset << endl;


    unsigned slotDirectoryLength = slotOffset - lastSlotOffset;
    cout << "slotDirectoryLength: " << slotDirectoryLength << endl;

    void * buffer = malloc(slotDirectoryLength);
    memcpy(buffer, pageP + lastSlotOffset, slotDirectoryLength);


    cout << "Hahahha" << endl;
    memset(pageP + lastSlotOffset, 0, slotDirectoryLength + sizeof(SlotEntry));
    memcpy(pageP + updatedSlotOffset, buffer ,slotDirectoryLength);

    free(buffer);

    return SUCCESS;
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
    newNonLeafPage(firstPageData, 1);
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
}

// RC IndexManager::insertDataEntry(void * pageData, const Attribute &attribute,const DataEntry &dataEntry, const uint32_t &slotNum)
// {
// 	char * page = (char *)pageData;
// 	SlotEntry slotEntryToInsert = {0, 0};
// 	SlotEntry slotEntryToMove = {0, 0};
// 	SlotEntry lastSlot = {0, 0};
// 	SlotEntry slotEntry = {0, 0};

// 	NodeHeader nodeHeader = getNodeHeader(page);
// 	uint32_t totalSlots = nodeHeader.numSlots;

// 	//Find the length of the Entire Entry
// 	uint32_t newEntryLength = sizeof(RID);
// 	uint32_t newEntryKeyLength = 0;

// 	switch (attribute.type)
// 	{
// 	    case TypeInt:
// 	    	newEntryKeyLength += INT_SIZE;
// 	        newEntryLength += INT_SIZE;
// 	    break;
// 	    case TypeReal:
// 	    	newEntryKeyLength += REAL_SIZE;
// 	        newEntryLength += REAL_SIZE;
// 	    break;
// 	    case TypeVarChar:
// 	        uint32_t varcharSize;
// 	        memcpy(&varcharSize, dataEntry.key, VARCHAR_LENGTH_SIZE);
// 	    	newEntryKeyLength += VARCHAR_LENGTH_SIZE;
// 	    	newEntryKeyLength += varcharSize;
// 	        newEntryLength += VARCHAR_LENGTH_SIZE;
// 	        newEntryLength += varcharSize;
// 	    break;
// 	}

// 	slotEntryToInsert.length = newEntryLength;

// 	//You are appending at the END
// 	if(slotNum == totalSlots ){
// 		slotEntryToInsert.offset = nodeHeader.freeSpaceOffset;
// 		setSlotEntry(totalSlots, slotEntryToInsert, page);
// 		memcpy(page + slotEntryToInsert.offset, dataEntry.key, newEntryLength - sizeof(RID));
// 		memcpy(page + slotEntryToInsert.offset + newEntryKeyLength , &dataEntry.rid, sizeof(RID));
// 	}else{
// 		slotEntryToMove = getSlotEntry(slotNum, page);
// 		lastSlot = getSlotEntry(totalSlots - 1, page);
// 		uint32_t dataToMoveLength = (lastSlot.offset + lastSlot.length) - (slotEntryToMove.offset);
// 		slotEntryToInsert.offset = slotEntryToMove.offset;
// 		//Copy over the index Entries
// 		memcpy((page + slotEntryToMove.offset + newEntryLength), (page + slotEntryToMove.offset), dataToMoveLength);

// 		//Copy over the new Index Entry
// 		memcpy(page + slotEntryToInsert.offset, dataEntry.key, newEntryKeyLength);
// 		memcpy(page + slotEntryToInsert.offset + newEntryKeyLength , &dataEntry.rid, sizeof(RID));

// 		for(uint32_t j = slotNum; j < nodeHeader.numSlots; j++){
// 		    slotEntry = getSlotEntry(j, pageData);
// 		    slotEntry.offset += newEntryLength;
// 		    setSlotEntry(j + 1, slotEntry, pageData);
// 		}

// 		setSlotEntry(totalSlots, slotEntryToInsert, page);


// 	}

// 	//SETTING THE NODE HEADER
// 	nodeHeader.numSlots += 1;

// 	//Updating header variables
// 	nodeHeader.freeSpaceOffset += newEntryLength;

// 	//Update the Header
// 	setNodeHeader(pageData, nodeHeader);

// 	return SUCCESS;
// }


void IndexManager::getDataEntry(uint32_t slotNum, void * page, DataEntry &dataEntry){
	SlotEntry slotEntry = getSlotEntry(slotNum, page);
	char * pageP = (char *)page + slotEntry.offset;

	unsigned keyLength = slotEntry.length - sizeof(RID);
	int wow;

	memcpy(&wow, pageP , keyLength);

	//cout << "Wow: " << wow << endl;
	memcpy(dataEntry.key, pageP , keyLength);
	memcpy(&dataEntry.rid, pageP + keyLength, sizeof(RID));
}

RC IndexManager::deleteDataEntry(uint32_t slotNum, uint32_t totalSlots, void * page){
	char * pageP = (char *)page;
	NodeHeader nodeHeader = getNodeHeader(page);


	SlotEntry slotEntry = getSlotEntry(slotNum, page);
	SlotEntry lastSlotEntry = getSlotEntry(totalSlots - 1, page);

	uint32_t occupiedLength = (lastSlotEntry.offset + lastSlotEntry.length) - (slotEntry.offset + slotEntry.length);

	void * buffer = malloc(occupiedLength);
	memcpy(buffer, pageP + slotEntry.offset + slotEntry.length, occupiedLength);

	memset(pageP + slotEntry.offset, 0, occupiedLength + slotEntry.length);
	memcpy(pageP + slotEntry.offset, buffer ,occupiedLength);

	nodeHeader.numSlots = nodeHeader.numSlots - 1;
	setNodeHeader(page, nodeHeader);

	free(buffer);
	return SUCCESS;
}


void IndexManager::getKeyd(const Attribute &attribute, void * retKey, const void * key){
    switch (attribute.type)
    {
        case TypeInt:
            // int keyd;
            // memcpy(&keyd, key, INT_SIZE);
            // cout << "keyd:" << keyd << endl;
            memcpy(retKey, key, INT_SIZE);
        break;
        case TypeReal:
            memcpy(retKey, key, REAL_SIZE);
        break;
        case TypeVarChar:
            int varcharSize;
            memcpy(&varcharSize, key, VARCHAR_LENGTH_SIZE);

            char * ya = (char*) malloc(varcharSize);

            memcpy(retKey, ya, varcharSize);
        break;
    }
}

//Under the assumption we are given some preallocated page
void IndexManager::splitPages(IXFileHandle &ixfileHandle, const Attribute &attribute, void * originalPage, void * newPage, uint32_t &retMiddleSlot, IndexEntry &overFlowIndexEntry){
	NodeHeader originalPageNodeHeader = getNodeHeader(originalPage);
	NodeHeader newPageNodeHeader = getNodeHeader(newPage);

	uint32_t totalSlots =  originalPageNodeHeader.numSlots;
	uint32_t halfSlotNum = (totalSlots / 2) - 1;
	if(totalSlots % 2 != 0 ){
		halfSlotNum += 1;
	}
	SlotEntry lastSlot = getSlotEntry(totalSlots - 1, originalPage);
	SlotEntry halfSlot = getSlotEntry(halfSlotNum, originalPage);
    retMiddleSlot = halfSlotNum; //is this right
	uint32_t lengthOfNewPage = (lastSlot.offset + lastSlot.length) - (halfSlot.offset + halfSlot.length);

    //i moved this up here
    //Writes the actual entries to the new page
	memcpy((char*) newPage + newPageNodeHeader.freeSpaceOffset, (char*) originalPage + halfSlot.offset + halfSlot.length, lengthOfNewPage);

    uint32_t tempFreeSpace = newPageNodeHeader.freeSpaceOffset;
    newPageNodeHeader.freeSpaceOffset += lengthOfNewPage;

	//Update the second half of the slots and write them to the split page
	uint32_t newPageSlotCount = 0;
	for(uint32_t i = halfSlotNum + 1; i < totalSlots; i++){
        cout << "i:" << i << endl;
		SlotEntry slotEntry = getSlotEntry(i, originalPage);
		slotEntry.offset = tempFreeSpace;
        tempFreeSpace += slotEntry.length;

        cout << "hit 3 in splitPages" << endl;

		//Zero out original slot
		memset((char *)originalPage + (PAGE_SIZE - (i + 1) * sizeof(SlotEntry)), 0, sizeof(SlotEntry));
        originalPageNodeHeader.numSlots -= 1;
		setSlotEntry(newPageSlotCount, slotEntry, newPage); //this should be new page right
		newPageSlotCount += 1;
	}

    //0 out index to be pushed up
    memset((char*)originalPage + halfSlot.offset + halfSlot.length, 0, lengthOfNewPage);
    originalPageNodeHeader.numSlots -= 1;
    memset((char *)originalPage + (PAGE_SIZE - (halfSlotNum + 1) * sizeof(SlotEntry)), 0, sizeof(SlotEntry));

    printNonLeafHelper(originalPage, attribute);

    cout << "hit 4 in splitPages" << endl;

	IndexEntry currMiddleEntry = {NULL, 0};
	currMiddleEntry.key = malloc(attribute.length);

    // this is the slot to be pushed up
    // this isn't getting a value for currMiddleEntry.key
    // only rightChild
	getIndexEntry(ixfileHandle, attribute, currMiddleEntry, (char*) originalPage, halfSlot);
    cout << "currMiddleEntry.rightChild" << currMiddleEntry.rightChild << endl;

    memset((char*) originalPage + halfSlot.offset, 0, halfSlot.length);
    uint32_t removedLength = halfSlot.length;

    cout << "hit 5 in splitPages" << endl;

    newPageNodeHeader.parent = originalPageNodeHeader.parent;
	newPageNodeHeader.numSlots = totalSlots - halfSlotNum - 1;
	setNodeHeader(newPage, newPageNodeHeader);

    originalPageNodeHeader.freeSpaceOffset -= (lengthOfNewPage + removedLength);
	originalPageNodeHeader.numSlots = halfSlotNum;
	setNodeHeader(originalPage, originalPageNodeHeader);

    // if to be isnerted key is less than halfSlot key then insert into original page
    // else insert into newPage
    // do comparisons in switch case

    int currMiddleKey = 0;
    int overFlowKey   = 0;

	switch (attribute.type)
	{
	    case TypeInt:

	        memcpy(&currMiddleKey, currMiddleEntry.key, INT_SIZE);
            currMiddleKey = 200;
            cout << "hit 7 in splitPages currMiddleEntrykey:" << currMiddleKey << endl;
	        memcpy(&overFlowKey, overFlowIndexEntry.key, INT_SIZE);
            cout << "hit 7 in splitPages overFlowIndexEntry.key:" << overFlowKey << endl;

            uint32_t slotNum = 0;
            int temp;
            memcpy(&temp, overFlowIndexEntry.key, sizeof(int));
            cout << "temp:" << temp << endl;
            if(overFlowKey <= currMiddleKey){
                // prep sets our slotNum right?
                prepInsertEntry(originalPage, attribute, overFlowIndexEntry.key, slotNum);
                cout << "hit 8 in splitPages slotNum:" << slotNum << endl;
                insertIndexEntry(originalPage, attribute, overFlowIndexEntry, slotNum);
            }else{
                cout << "hit 9 in splitPages" << endl;
                prepInsertEntry(newPage, attribute, overFlowIndexEntry.key, slotNum);
                cout << "hit 8 in splitPages slotNum:" << slotNum << endl;
                insertIndexEntry(newPage, attribute, overFlowIndexEntry, slotNum);
            }

	        // cout << "keyd:" << keyd << endl;
	        // memcpy(retKey, key, INT_SIZE);
	    break;
	    // case TypeReal:
	    //     // memcpy(retKey, key, REAL_SIZE);
	    // break;
	    // case TypeVarChar:
	    //     int varcharSize;
	    //     memcpy(&varcharSize, key, VARCHAR_LENGTH_SIZE);
        //
	    //     char * ya = (char*) malloc(varcharSize);
        //
	    //     memcpy(retKey, ya, varcharSize);
	    // break;
	}

	// newPageNodeHeader.freeSpaceOffset += lengthOfNewPage;

    cout << "newPage in splitPage" << endl;

    printNonLeafHelper(newPage, attribute);

    cout << "hit 11 in splitPages" << endl;
    getKeyd(attribute, overFlowIndexEntry.key, &currMiddleKey);
    overFlowIndexEntry.rightChild = ixfileHandle.fh.getNumberOfPages();
    cout << "hit 12 in splitPages" << endl;
    free(currMiddleEntry.key);
}


//ONLY USE ON INDEX PAGES
RC IndexManager::recurPush(IXFileHandle &ixfileHandle, void * originalPage, const uint32_t &originalPageNum ,IndexEntry &overFlowIndexEntry){
	//Grab the original page and attempt to check if a split if needed
	// NodeHeader originalPageNodeHeader = getNodeHeader(originalPage);
	// void * newSplitPage = malloc(PAGE_SIZE);

	// uint32_t totalSlots = originalPageNodeHeader.numSlots;
	// uint32_t halfSlot = totalSlots / 2;
	// uint32_t retMiddleSlotNum = 0;

	// if(enoughFreeSpaceForIndexEntry(originalPage, attribute, key)){
	// 	uint32_t slotNumToInsertAt;
	// 	prepInsertEntry(originalPage, attribute, overFlowIndexEntry.key, slotNumToInsertAt);
	//     insertIndexEntry(originalPage, attribute, overFlowIndexEntry, slotNumToInsertAt);
	// 	return SUCCESS;
	// }else {
	// 	cout << "THE INDEX ENTRY IS FULL NOT IMPLEMENTED YET"
	// 	return -1;

		//Initialize the new page for the split
		// newNonLeafPage(newSplitPage, ixfileHandle.fh.getNumberOfPages());

		// splitPages(originalPage, newSplitPage, retMiddleSlotNum);

		// if (ixfileHandle.writePage(originalPageNum, newSplitPage))
		//     return RBFM_APPEND_FAILED;

		// if (ixfileHandle.appendPage(newSplitPage))
		//     return RBFM_APPEND_FAILED;


		// //FINISHED APPENDING AND SPLITING PAGE, NOW HAN

		// //In case where we have basically hit Root and need to some special handling to split root and create a new root
		// if(originalPageNodeHeader.isRoot){
		// 	cout << "ROOT IS FULL " << endl;
		// 	return -1;
		// }else{
		// 	return -1;
		// }
		// In case where we should recursive call the split/recurpush function
	//}
	return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	void * indexFileHeaderPage = malloc(PAGE_SIZE);
	void * rootPageData = malloc(PAGE_SIZE);
	char * rootPage = (char *)rootPageData;

	DataEntry dataEntry;
	dataEntry = {nullptr, rid};
	dataEntry.key = malloc(attribute.length + VARCHAR_LENGTH_SIZE);

	//Prepare the dataKey to insert
	getKeyd(attribute, dataEntry.key, key);

	IndexEntry indexEntry;
	indexEntry = {nullptr, 0};
	indexEntry.key = malloc(attribute.length + VARCHAR_LENGTH_SIZE);
	getKeyd(attribute, indexEntry.key, key);

	//Grab the Header Page
	if(ixfileHandle.readPage(0, indexFileHeaderPage))
	    return IX_READ_FAILED;

	IndexFileHeader indexFileHeader = getIndexFileHeader(indexFileHeaderPage);

	//Using the Header Page information, grab the appropriate ROOT Page
	if(ixfileHandle.readPage(indexFileHeader.rootPageId, rootPage))
	    return IX_READ_FAILED;

	NodeHeader rootNodeHeader = getNodeHeader(rootPage);

	if(rootNodeHeader.numSlots == 0){
		//Data Page

		// Create a new page for Data Entries (Leaf Page)
		void * pageData = malloc(PAGE_SIZE);
		char * page = (char *) pageData;
		newLeafPage(page, 2);

		// Check if there is enough room for the new Data Entry and insert into the Leaf
		if(enoughFreeSpaceForDataEntry(page, attribute, dataEntry.key)){
			uint32_t prepNum;
			//prepInsertEntry(page, attribute, dataEntry.key, prepNum);
			//insertDataEntry(page, attribute, dataEntry, prepNum);
			insertDataEntry(page, attribute, dataEntry);
		}

		// Update Header of this New Leaf Page so that the parent is ROOT
		NodeHeader dataNodeHeader = getNodeHeader(page);
		dataNodeHeader.parent = indexFileHeader.rootPageId;
		setNodeHeader(page, dataNodeHeader);

		cout << "DataNodeParent: " << dataNodeHeader.parent << endl;
		cout << "DataNodeNumSlots: " << dataNodeHeader.numSlots << endl;

		// Take this New Leaf Page and Finally add it to the actual file
		if (ixfileHandle.appendPage(page))
		    return RBFM_APPEND_FAILED;

		printLeafHelper(page, attribute);



		//INSERT NONLEAF
		indexEntry.rightChild = 2;
		getKeyd(attribute, indexEntry.key, key);

		if(enoughFreeSpaceForIndexEntry(rootPage, attribute, key)){
			uint32_t slotNumToInsertAt;
			prepInsertEntry(rootPage, attribute, key, slotNumToInsertAt);
		    insertIndexEntry(rootPage, attribute, indexEntry, slotNumToInsertAt);
		}else{
            cout << "not enough space in IndexPage" << endl;
            // void * newPage = malloc(PAGE_SIZE);
            // uint32_t retMiddleSlot = 0;
            // IndexEntry overFlowIndexEntry = {nullptr, 0};
            // splitPages(ixfileHandle, attribute, rootPage, newPage, retMiddleSlot, overFlowIndexEntry);
            // // recurBtree(ixfileHandle, attribute, pageNum);
        }

		if (ixfileHandle.writePage(indexFileHeader.rootPageId, rootPage)){
		    return RBFM_WRITE_FAILED;
		}

		printNonLeafHelper(rootPage, attribute);

		free(rootPageData);
		free(indexFileHeaderPage);
		free(dataEntry.key);
		free(indexEntry.key);
		free(pageData);
		return SUCCESS;
	}else{
		void * pageData = malloc(PAGE_SIZE);
		char * page = (char *)pageData;
		bool foundLeafPage;
		uint32_t slotHolder;

		getKeyd(attribute, dataEntry.key, key);

		uint32_t retPageID;

		foundLeafPage = traverseTree(ixfileHandle, attribute, dataEntry.key, page, retPageID, slotHolder);
		cout << "slotHolder : " << slotHolder << endl;
		cout << "retPageID : " << retPageID << endl;

		void * dataPageToInsertIn = malloc(PAGE_SIZE);
		//THIS WILL EITHER GIVE YOU A PAGE THAT ALREADY EXISTS OR A NEW MALLOCED PAGE
		if(!enoughFreeSpaceForDataEntry(page, attribute, dataEntry.key) && !foundLeafPage){
		    cout << "Did not find an available leaf page" << endl;
		    void  * newLeafPageData = malloc(PAGE_SIZE);
	    	LeafHeader leafHeader = getLeafHeader(page);
	    	NodeHeader leafNodeHeader = getNodeHeader(page);

	    	//Initialize new Leaf Page and grab headers
		    newLeafPage(newLeafPageData, (uint32_t) ixfileHandle.fh.getNumberOfPages());
	    	NodeHeader newLeafNodeHeader = getNodeHeader(newLeafPageData);
	    	LeafHeader newleafHeader = getLeafHeader(newLeafPageData);

	    	//Updating the full page's header information
	    	leafHeader.nextPage = (uint32_t) ixfileHandle.fh.getNumberOfPages();
	    	setLeafHeader(page, leafHeader);

	    	//Updating the newLeafPage's LeafHeader
		    newleafHeader.prevPage = leafNodeHeader.pageNum;
		    newleafHeader.nextPage = 0;
	    	setLeafHeader(newLeafPageData, newleafHeader);

	    	//Updating the newLeafPage's NodeHeader
	    	newLeafNodeHeader.pageNum = (uint32_t) ixfileHandle.fh.getNumberOfPages();;
	    	newLeafNodeHeader.parent = leafNodeHeader.parent;
	    	setNodeHeader(newLeafPageData, newLeafNodeHeader );

		    //IXFileHandle *ixfh = &ixfileHandle;
		    cout << "indexEntry.rightChild" << indexEntry.rightChild << endl;

		    memcpy(dataPageToInsertIn, newLeafPageData, PAGE_SIZE);

		    if (ixfileHandle.appendPage(newLeafPageData))
		        return RBFM_APPEND_FAILED;

		    if (ixfileHandle.writePage(leafNodeHeader.pageNum, page))
		        return RBFM_APPEND_FAILED;

		    free(newLeafPageData);
		}else if(!foundLeafPage){
			cout << "In here no page found" << endl;
			//If we are here we could not find a data page and instead were given the parent page
			// Should always push up an entry

			if (ixfileHandle.readPage(retPageID, page))
			    return RBFM_WRITE_FAILED;

        	NodeHeader IndexPageNodeHeader = getNodeHeader(page);

        	//Initilize and grab the first headers of the new leaf pages
    	    void  * newLeafPageData = malloc(PAGE_SIZE);
    	    char  * newLeafPageP = (char *)newLeafPageData;
    	    newLeafPage(newLeafPageData, (uint32_t) ixfileHandle.fh.getNumberOfPages());
        	LeafHeader newleafHeader = getLeafHeader(newLeafPageData);
        	NodeHeader newLeafNodeHeader = getNodeHeader(newLeafPageData);

        	//Updating the newLeafPage's LeafHeader
        	//Grab the Index page's Last Slot
        	cout << "slotNumPrev: " << IndexPageNodeHeader.numSlots << endl;
        	uint32_t slotNumPrev = IndexPageNodeHeader.numSlots - 1 ;
        	cout << "slotNumPrev: " << slotNumPrev << endl;
        	IndexEntry lastIndexEntry;
        	lastIndexEntry.key = malloc(attribute.length + VARCHAR_LENGTH_SIZE);
        	SlotEntry lastSlotEntry = getSlotEntry(slotNumPrev, page);
        	getIndexEntry(ixfileHandle, attribute, lastIndexEntry, page, lastSlotEntry);

        	void * prevDataPage = malloc(PAGE_SIZE);
        	cout << "lastIndexEntry.rightChild: " << lastIndexEntry.rightChild;
        	if (ixfileHandle.readPage(lastIndexEntry.rightChild, prevDataPage))
        	    return RBFM_APPEND_FAILED;

        	NodeHeader prevDataNodeHeader = getNodeHeader(prevDataPage);
        	LeafHeader prevDataLeafHeader = getLeafHeader(prevDataPage);
        	prevDataLeafHeader.nextPage =  (uint32_t) ixfileHandle.fh.getNumberOfPages();
        	setLeafHeader(prevDataPage, prevDataLeafHeader);

    	    newleafHeader.prevPage = lastIndexEntry.rightChild;
    	    newleafHeader.nextPage = 0;
        	setLeafHeader(newLeafPageData, newleafHeader);

        	//Updating the newLeafPage's NodeHeader
        	newLeafNodeHeader.pageNum = (uint32_t) ixfileHandle.fh.getNumberOfPages();
        	newLeafNodeHeader.parent = IndexPageNodeHeader.pageNum;
        	setNodeHeader(newLeafPageData, newLeafNodeHeader );

        	//uint32_t prepNum2;
        	//prepInsertEntry(newLeafPageP, attribute, dataEntry.key, prepNum2);
        	//insertDataEntry(newLeafPageP, attribute, dataEntry, prepNum2);
        	insertDataEntry(newLeafPageP, attribute, dataEntry);
        	indexEntry.rightChild = newLeafNodeHeader.pageNum;
        	cout <<"No page found: rightChild: " << newLeafNodeHeader.pageNum << endl;
        	prepInsertEntry(page, attribute, key, slotNumPrev);
        	cout << "slotNumPrev: " << slotNumPrev << endl;
        	insertIndexEntry(page, attribute, indexEntry, slotNumPrev);

        	if (ixfileHandle.appendPage(newLeafPageData))
        	    return RBFM_APPEND_FAILED;

        	if (ixfileHandle.writePage(lastIndexEntry.rightChild, prevDataPage))
        	    return RBFM_APPEND_FAILED;

        	if (ixfileHandle.writePage(retPageID, page))
        	    return RBFM_APPEND_FAILED;

		}else{
			//We have found a place where we can insert without an issue.
		    cout << "Found leafPage from traverseTree: " << retPageID << endl;

        	//uint32_t prepNum3;
        	//prepInsertEntry(page, attribute, dataEntry.key, prepNum3);
        	//insertDataEntry(page, attribute, dataEntry,prepNum3);
        	insertDataEntry(page, attribute, dataEntry);

        	if (ixfileHandle.writePage(retPageID, page))
        	    return RBFM_WRITE_FAILED;

        	return SUCCESS;

		}

		//Else either we have to append some new page and must now perform the split.

		if(enoughFreeSpaceForDataEntry(page, attribute, dataEntry.key)){
			//uint32_t prepNum3;
			//prepInsertEntry(page, attribute, dataEntry.key, prepNum3);
		    //insertDataEntry(page, attribute, dataEntry, prepNum3);
		    insertDataEntry(page, attribute, dataEntry);

		    //RECUR INSERT INDEX ENTRY FUNTION
		}else{
		    cout << "HIT in not enough space in DataEnry" << endl;
		    return -1;
		    //Grab the current found leaf page and update it's next page to a newly appended page.
		    // LeafHeader leafHeader = getLeafHeader(page);
		    // leafHeader.nextPage = (uint32_t) ixfileHandle.fh.getNumberOfPages();
		    // setLeafHeader(page, leafHeader);

		    // // Create the new Leaf Page and set it's
		    // memset(page, 0, PAGE_SIZE);
		    // newLeafPage(page, leafHeader.nextPage);
		    // LeafHeader newleafHeader = getLeafHeader(page);
		    // newleafHeader.prevPage = leafHeader.nextPage;
		    // setLeafHeader(page, leafHeader);

		    // make new page have prevPage of current Page

		    //PERFORM THE SPLIT ON THE THE DATA ENTRIES, then perform the recursive index split if nessesary
		}



		free(rootPageData);
		free(indexFileHeaderPage);
		free(dataEntry.key);
		free(indexEntry.key);
		free(pageData);
		return SUCCESS;


	}

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
}

void IndexManager::prepInsertEntry(void* pageData, const Attribute &attribute, const void * value, uint32_t &slotNum){
	char * page = (char *)pageData;
	uint32_t offset = 0;
	bool inserted = false;

	NodeHeader nodeHeader = getNodeHeader(page);
    cout << "num slots in prepInsert" << nodeHeader.numSlots << endl;

	SlotEntry slotEntry;

	switch (attribute.type)
	{
	    case TypeInt:
	        int toInsertKey, curKey;
	        memcpy(&toInsertKey, value, INT_SIZE);

	        for(uint32_t i = 0; i < nodeHeader.numSlots; i++){
	            slotEntry = getSlotEntry(i, page);
	            offset = slotEntry.offset;
	            memcpy(&curKey, page + offset, INT_SIZE);

	            if(curKey > toInsertKey){
                    slotNum = i;
	                return;
	            }
	        }
	        slotNum = nodeHeader.numSlots;
	        break;
	}
}


RC IndexManager::insertIndexEntry(void * pageData, const Attribute &attribute, const IndexEntry &indexEntry, const uint32_t &slotNum)
{
	char * page = (char *)pageData;
	SlotEntry slotEntryToInsert = {0, 0};
	SlotEntry slotEntryToMove = {0, 0};
	SlotEntry lastSlot = {0, 0};
	SlotEntry slotEntry = {0, 0};

	NodeHeader nodeHeader = getNodeHeader(page);
	uint32_t totalSlots = nodeHeader.numSlots;

	//Find the length of the Entire Entry
	uint32_t newEntryLength = sizeof(uint32_t);
	uint32_t newEntryKeyLength = 0;

	switch (attribute.type)
	{
	    case TypeInt:
	    	newEntryKeyLength += INT_SIZE;
	        newEntryLength += INT_SIZE;
	    break;
	    case TypeReal:
	    	newEntryKeyLength += REAL_SIZE;
	        newEntryLength += REAL_SIZE;
	    break;
	    case TypeVarChar:
	        uint32_t varcharSize;
	        memcpy(&varcharSize, indexEntry.key, VARCHAR_LENGTH_SIZE);
	    	newEntryKeyLength += VARCHAR_LENGTH_SIZE;
	    	newEntryKeyLength += varcharSize;
	        newEntryLength += VARCHAR_LENGTH_SIZE;
	        newEntryLength += varcharSize;
	    break;
	}

	slotEntryToInsert.length = newEntryLength;

	//You are appending at the END
	if(slotNum == totalSlots ){
		slotEntryToInsert.offset = nodeHeader.freeSpaceOffset;
		setSlotEntry(totalSlots, slotEntryToInsert, page);
		memcpy(page + slotEntryToInsert.offset, indexEntry.key, newEntryLength - sizeof(uint32_t));
		memcpy(page + slotEntryToInsert.offset + newEntryKeyLength , &indexEntry.rightChild, sizeof(uint32_t));
	}else{
		slotEntryToMove = getSlotEntry(slotNum, page);
		lastSlot = getSlotEntry(totalSlots - 1, page);
		uint32_t dataToMoveLength = (lastSlot.offset + lastSlot.length) - (slotEntryToMove.offset);
		slotEntryToInsert.offset = slotEntryToMove.offset;
		//Copy over the index Entries
		memcpy((page + slotEntryToMove.offset + newEntryLength), (page + slotEntryToMove.offset), dataToMoveLength);

		//Copy over the new Index Entry
		memcpy(page + slotEntryToInsert.offset, indexEntry.key, newEntryKeyLength);
		memcpy(page + slotEntryToInsert.offset + newEntryKeyLength , &indexEntry.rightChild, sizeof(uint32_t));

		for(uint32_t j = slotNum; j < nodeHeader.numSlots; j++){
		    slotEntry = getSlotEntry(j, pageData);
		    slotEntry.offset += newEntryLength;
		    setSlotEntry(j + 1, slotEntry, pageData);
		}

		setSlotEntry(totalSlots, slotEntryToInsert, page);


	}

	//SETTING THE NODE HEADER
	nodeHeader.numSlots += 1;

	//Updating header variables
	nodeHeader.freeSpaceOffset += newEntryLength;

	//Update the Header
	setNodeHeader(pageData, nodeHeader);

	return SUCCESS;
}

void IndexManager::getIndexEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, IndexEntry &indexEntry, char * pageData, SlotEntry dirtySlot){
    char * page = pageData;
    // NodeHeader nonLeafPageNodeHeader = getNodeHeader(page);

    page += dirtySlot.offset;
    int offset = 0;

    switch (attribute.type)
    {
        case TypeInt:
            // memcpy(&indexEntry.key, page, INT_SIZE);
            offset = INT_SIZE;
            memcpy(&indexEntry.rightChild, page + offset, sizeof(uint32_t));
        break;
        case TypeReal:
            offset = REAL_SIZE;
            memcpy(&indexEntry.rightChild, page + offset, sizeof(uint32_t));

    }
}

void IndexManager::getFullIndexEntry(uint32_t slotNum, void * page, IndexEntry &indexEntry){
    SlotEntry slotEntry = getSlotEntry(slotNum, page);

	char * pageP = (char *)page + slotEntry.offset;

    unsigned keyLength = slotEntry.length - sizeof(uint32_t);

 	int wow;
	memcpy(&wow, pageP, keyLength);
	//cout << "Woe: " << wow <<endl;
	memcpy(indexEntry.key, pageP, keyLength);
	memcpy(&indexEntry.rightChild, pageP + keyLength, sizeof(uint32_t));

}


RC IndexManager::deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
	void * retPage = malloc(PAGE_SIZE);

	uint32_t pageNum;
	uint32_t slotNum;
	DataEntry dataEntry = {NULL, 0};
	dataEntry.key = malloc(attribute.length + VARCHAR_LENGTH_SIZE);
	bool entryFound = false;
	SlotEntry foundEntry;
	uint32_t foundSlotNum = 0;
	uint32_t totalOriginalSlots = 0;


    if(traverseTree(ixfileHandle, attribute, key, retPage, pageNum, slotNum)){
    	NodeHeader nodeHeader = getNodeHeader(retPage);
    	totalOriginalSlots = nodeHeader.numSlots;
	    switch (attribute.type)
	    {
	        case TypeInt:
	        	cout << "In type int for delete " << endl;
	        	for(uint32_t i = 0; i < nodeHeader.numSlots; i ++){
	            	int32_t dataInt;
	            	int32_t keyInt;
	            	getDataEntry(i, retPage, dataEntry);
	            	memcpy(&dataInt, dataEntry.key, INT_SIZE);
	            	getKeyd(attribute, &keyInt, key);
	            	if(keyInt == dataInt && rid.pageNum == dataEntry.rid.pageNum && rid.slotNum == dataEntry.rid.slotNum){
	            		deleteDataEntry(i, nodeHeader.numSlots, retPage);
	            		//ixfileHandle.writePage(pageNum, retPage);
	            		entryFound = true;
	            		foundEntry = getSlotEntry(i, retPage);
	            		foundSlotNum = i;

	            		// free(dataEntry.key);
	            		// free(retPage);
	            		// return SUCCESS;
	            	}
	            	if(entryFound){
	            		SlotEntry slotEntry = getSlotEntry(i, retPage);
	            		slotEntry.offset = slotEntry.offset - foundEntry.length;
	            		setSlotEntry(i, slotEntry, retPage);
	            	}


	        	}
        		free(dataEntry.key);

        		if(entryFound){
        			cout << "Entry Found updating slot Directory" << endl;
        			deleteSlotEntry(foundSlotNum ,totalOriginalSlots, retPage);
	            	ixfileHandle.writePage(pageNum, retPage);
        			free(retPage);
	            	return SUCCESS;
        		}else{
        			free(retPage);
	        		return IX_DELETE_FAILED;
        		}


	        break;
	        case TypeReal:
	    		return IX_DELETE_FAILED;

	        break;
	        case TypeVarChar:
	        	return IX_DELETE_FAILED;
	        break;
	    }

    }else{
		free(dataEntry.key);
    	free(retPage);
    	return IX_DELETE_FAILED;
    }

}


RC IndexManager::scan(IXFileHandle &ixfileHandle,
        const Attribute &attribute,
        const void      *lowKey,
        const void      *highKey,
        bool			lowKeyInclusive,
        bool        	highKeyInclusive,
        IX_ScanIterator &ix_ScanIterator)
{
    return ix_ScanIterator.scanInit(ixfileHandle, attribute, lowKey, highKey, lowKeyInclusive, highKeyInclusive);
}



RC IndexManager::recurBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, unsigned pageNum){
    void * pageData = malloc(PAGE_SIZE);
    char * page = (char *) pageData;

    if(ixfileHandle.fh.readPage(pageNum, page))
        return IX_READ_FAILED;

    NodeHeader nodeHeader = getNodeHeader(page);

    IndexEntry indexEntry;

    if(nodeHeader.isLeaf){
        printLeafHelper(page, attribute);
        free(pageData);
        return 0;
    }else{
        printNonLeafHelper(page, attribute);
    }

    for(unsigned i = 0; i < nodeHeader.numSlots; i++){
        // cout << "array[i]: " << array[i] << endl;
        // SlotEntry dirtySlot = getSlotEntry(i, page);

        SlotEntry dirtySlot = getSlotEntry(i, page);

        indexEntry = {nullptr, 0};

        indexEntry.key = malloc(attribute.length + VARCHAR_LENGTH_SIZE);
        getIndexEntry(ixfileHandle, attribute, indexEntry, page, dirtySlot);

        recurBtree(ixfileHandle, attribute, indexEntry.rightChild);

        free(indexEntry.key);
        // array2.push_back(indexEntry.rightChild);
        // cout << "rightChild" << indexEntry.rightChild << endl;
    }
    free(pageData);
        //     //Some get indexEntryFunction
        //     // Extract the left Child
        //     // offset += size of the left key entry
        //     // PRINT KEY
        //     // PRINT 'children : ['
        //     // recurBtree(left child);
        // }else{
        //     //PRINT ALL DATA ENTRIES ON LEAF PAGE
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
    char * page = (char *) pageData; //point to base

    if (ixfileHandle.fh.readPage(0, page))
            return ;

    IndexFileHeader header = getIndexFileHeader(page);

    uint32_t rootPageId = header.rootPageId;

    memset(page, 0, PAGE_SIZE);

    if(ixfileHandle.fh.readPage(rootPageId, page))
         return ;

    NodeHeader nodeHeader = getNodeHeader(page);

    printNonLeafHelper(page, attribute);

    IndexEntry indexEntry = {nullptr, 0};
    indexEntry.key = malloc(attribute.length + VARCHAR_LENGTH_SIZE);

    std::vector< uint32_t > array;

    for(unsigned i = 0; i < nodeHeader.numSlots; i++){

        //Get the left child
        SlotEntry dirtySlot = getSlotEntry(i, page);
        // may need to malloc for indexEntry.key
        getIndexEntry(ixfileHandle, attribute, indexEntry, page, dirtySlot);

        // call recurBtree
        recurBtree(ixfileHandle, attribute,indexEntry.rightChild);

        // array.push_back(indexEntry.rightChild);
        // cout << "IndexEntry: " << indexEntry.rightChild << endl;
    }


    cout << "]}" << endl;

    memset(page, 0, PAGE_SIZE);

    std::vector< uint32_t > array2;
    free(indexEntry.key);
    free(pageData);
}

void IndexManager::printLeafHelper(void * pageData, const Attribute &attribute){

    char * page = (char *) pageData;
    NodeHeader nodeHeader = getNodeHeader(page);

    unsigned offset = sizeof(NodeHeader) + sizeof(LeafHeader);
    cout << "\t{\"keys\":  [";
    // cout << "\"children\": [";
    switch(attribute.type)
    {
        case TypeInt: {
            map<int, vector<RID>> intMapVecRid;
            for(unsigned i = 0; i < nodeHeader.numSlots; i++){
                // SlotEntry SlotEntry = getSlotEntry(i, pageData);
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
            cout << "]},\n";
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
                    cout << "]},\n";
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
    cout << "attribute.type: " << attribute.type << endl;
    cout << "{\"keys\":  [";
    switch(attribute.type)
    {
        case TypeInt: {

            for(unsigned i = 0; i < nodeHeader.numSlots; i++){
                int keyInt;
                memcpy(&keyInt, page + offset, INT_SIZE);
                offset += INT_SIZE;
                // cout << "keyInt: " << keyInt << endl;

                uint32_t pageNum;
                memcpy(&pageNum, page + offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);

                if(i == 0){
                    cout << "\"" << keyInt << "\"";
                }

                cout << ","<< "\"" << keyInt << "\"";
            }

            cout << "]," << endl;
            cout << "\"children\": [" << endl;
            break;
        }
        case TypeReal:{
            for(unsigned i = 0; i < nodeHeader.numSlots; i++){
                float keyFloat;
                memcpy(&keyFloat, page + offset, REAL_SIZE);
                offset += REAL_SIZE;

                uint32_t pageNum;
                memcpy(&pageNum, page + offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);

                if(i == 0){
                    cout << "\"" << keyFloat << "\"";
                }

                cout << ","<< "\"" << keyFloat << "\"";
            }
            cout << "]," << endl;
            cout << "\"children\": [" << endl;
            break;
        }
        case TypeVarChar:{
            uint32_t varcharSize;

            for(unsigned i = 0; i < nodeHeader.numSlots; i++){

                memcpy(&varcharSize, page + offset, VARCHAR_LENGTH_SIZE);
                offset += VARCHAR_LENGTH_SIZE;

                char * keyVarChar = (char *) calloc(1, varcharSize + 1);
                memcpy(keyVarChar, page + offset, varcharSize);

                offset += varcharSize;
                keyVarChar[varcharSize + 1] = '\0';

                uint32_t pageNum;
                memcpy(&pageNum, page + offset, sizeof(uint32_t));
                offset += sizeof(uint32_t);

                // need to print
                cout << "hit in printNonLeafHelper varchar *********" << endl;

            }
            cout << "]," << endl;
            cout << "\"children\": [" << endl;
            break;
        }
    }
}

IX_ScanIterator::IX_ScanIterator()
: currPage(0), currSlot(0), totalPage(0), totalSlot(0)
{
   ixm = IndexManager::instance();
}

IX_ScanIterator::~IX_ScanIterator()
{
}

RC IX_ScanIterator::getNextEntry(RID &rid, void *key)
{

		//TEST FOR TREE TRAVERSAL
/* 	int value = 100;
 	uint32_t pageID;

 	cout << "Case: Value Smallest" << endl;
	ixm->traverseTree(ixfileHandle, attribute, &value, pageData, pageID);
 	cout << "PageID: " << pageID <<  endl;

 	value = 200;
 	cout << "Case Value ==" << endl;
	ixm->traverseTree(ixfileHandle, attribute, &value, pageData, pageID);
 	cout << "PageID: " << pageID <<  endl;

 	value = 300;
	cout << "Case Value larger" << endl;
	ixm->traverseTree(ixfileHandle, attribute, &value, pageData, pageID);
 	cout << "PageID: " << pageID <<  endl;
 	return IX_EOF;
*/
		//END OF TEST

	NodeHeader nodeHeader = ixm->getNodeHeader(pageData);
	totalSlot = nodeHeader.numSlots;

	cout << "Going into NextSlots" << endl;
    cout << "currPage: " << currPage << endl;
    cout << "currSlot: " << currSlot << endl;

    cout << "totalPage: " << totalPage << endl;
    cout << "totalSlot: " <<  totalSlot << endl;


	RC rc = getNextSlot();

	if (rc == IX_EOF){
		cout << "Returning an EOF" << endl;
	    return rc;
	}

	DataEntry dataEntry = {NULL, 0};
	dataEntry.key = malloc(attribute.length + VARCHAR_LENGTH_SIZE);
	ixm->getDataEntry(currSlot, pageData, dataEntry);
	rid = dataEntry.rid;
	ixm -> getKeyd(attribute, key, dataEntry.key);
	cout << "RID: " << dataEntry.rid.pageNum  << " , " << dataEntry.rid.slotNum << endl;
	currSlot++;

	free(dataEntry.key);

	return SUCCESS;
}

RC IX_ScanIterator::close()
{
    free(pageData);
    return SUCCESS;
}

// Initialize the scanIterator with all necessary state
RC IX_ScanIterator::scanInit(
	IXFileHandle &ixfh,
	const Attribute &attr,
    const void      *lKey,
    const void      *hKey,
    bool			lKeyInclusive,
    bool        	hKeyInclusive)
{
    // Start at page 0 slot 0

    currPage = 0;
    currSlot = 0;
    totalPage = 0;
    totalSlot = 0;
    lowKey = NULL;
    highKey = NULL;
    // Store the variables passed in to
    ixfileHandle = &ixfh;
    attribute = attr;
    lowKeyInclusive = lKeyInclusive;
    highKeyInclusive = hKeyInclusive;

  	if(ixfh.getfd() == NULL){
  		cout << "Failed Scan" << endl;
  		return IX_SCAN_FAILED;
  	}


    // Keep a buffer to hold the current page
    pageData = malloc(PAGE_SIZE);

    if(lKey != NULL){
    	cout << "lkey notNull" << endl;
    	lowKey = malloc(attribute.length + VARCHAR_LENGTH_SIZE);
    	ixm->getKeyd(attribute, lowKey, lKey);
    }
    if(hKey != NULL){
    	cout << "hkey notNull" << endl;
    	highKey = malloc(attribute.length + VARCHAR_LENGTH_SIZE);
    	ixm->getKeyd(attribute, highKey, hKey);

    }

     uint32_t rootPageId = 0;

    // Get total number of pages
    totalPage = ixfileHandle->fh.getNumberOfPages();
    cout << "totalPage: " << totalPage << endl;
    // If the think consists of more that just a header page and a root page, i.e. we actually have data entries
    if (totalPage > 2)
    {
    	//If yes, grab the PageHeader page
        if (ixfileHandle->readPage(0, pageData))
            return RBFM_READ_FAILED;

   		 IndexFileHeader indexFileHeader = ixm->getIndexFileHeader(pageData);

   		 rootPageId = indexFileHeader.rootPageId;

   		 memset(pageData, 0 , PAGE_SIZE);

   		//If yes, grab the root page
   		if (ixfileHandle->readPage(rootPageId, pageData))
   		    return RBFM_READ_FAILED;
    }
    else{
        return IX_EOF;
    }

    //Once we get the root page, shoudl traverse the nodes to find the start of actual data pages
    //NEED THE HELPER FUNCTION

    // Get number of slots on first page
    NodeHeader header = ixm->getNodeHeader(pageData);
    totalSlot = header.numSlots;
    currPage = rootPageId;


    // Might want to check the two range values and the compop to find if a comparison is needed
    switch (attribute.type)
    {
        case TypeInt:
        	cout << "Scan Init: IntType" << endl;
            int intlowKey;
            int inthighKey;

            if(highKey != NULL){
            	memcpy(&inthighKey, highKey, INT_SIZE);
 		       	cout << "highKey: " << inthighKey << endl;
            }

        	if(lowKey != NULL){
            	memcpy(&intlowKey, lowKey, INT_SIZE);
        		cout << "lowKey: " << intlowKey << endl;
        	}

            if( highKey != NULL && lowKey!= NULL && (inthighKey == intlowKey) && !(lowKeyInclusive && highKeyInclusive) ){
            	return IX_EOF;
            }

        break;
        case TypeReal:
	        int reallowKey;
	        int realhighKey;
	        memcpy(&reallowKey, lowKey, INT_SIZE);
	        memcpy(&realhighKey, highKey, INT_SIZE);
	        if((realhighKey == reallowKey) && !(lowKeyInclusive && highKeyInclusive) ){
	        	return IX_EOF;
	        }

        break;
        case TypeVarChar:
            uint32_t varcharSize;
            memcpy(&varcharSize, lowKey, VARCHAR_LENGTH_SIZE);
            char * lowVarChar = (char *) calloc(1, varcharSize + 1);
            memcpy(lowVarChar, (char *) lowKey + VARCHAR_LENGTH_SIZE, varcharSize);
            lowVarChar[varcharSize + 1] = '\0';

            memcpy(&varcharSize, highKey, VARCHAR_LENGTH_SIZE);
            char * highVarChar = (char *) calloc(1, varcharSize + 1);
            memcpy(highVarChar, (char *) highKey + VARCHAR_LENGTH_SIZE, varcharSize);
            highVarChar[varcharSize + 1] = '\0';

            if(!(strcmp(lowVarChar, highVarChar)) && !(lowKeyInclusive && highKeyInclusive) ){
            	return IX_EOF;
            }

        break;
    }

    if(!(ixm->traverseTree(ixfh, attribute, lowKey, pageData, currPage, currSlot))){
     	return IX_EOF;
    }

    currSlot = 0;

    cout << "return on scanInit" << endl;

    return SUCCESS;
}



RC IX_ScanIterator::getNextSlot()
{
    // If we're done with the current page, or we've read the last page
    if (currSlot >= totalSlot)
    {
    	cout << "Changing Pages" << endl;
        // Reinitialize the current slot and increment page number
        currSlot = 0;

        // Otherwise get next page ready
        RC rc = getNextPage();
        if (rc)
            return rc;
    }else{
	    if (!checkScanCondition())
	    {
	    	cout << "Changing Slots" << endl;
	    	cout << "currSlot: " << currSlot << endl;
	        ++currSlot;
	        // If not, try next slot
	        return getNextSlot();
	    }

    }

}

RC IX_ScanIterator::getNextPage()
{
    // Read in page
    if (ixfileHandle->readPage(currPage, pageData))
        return RBFM_READ_FAILED;

    LeafHeader leafHeader = ixm->getLeafHeader(pageData);

    //If next is equal to zero, we've reach the end of all possible pages to traverse
    if(leafHeader.nextPage == 0){
    	cout << "The next page is 0" << endl;
    	return IX_EOF;
    }

    if (ixfileHandle->readPage(leafHeader.nextPage, pageData))
        return RBFM_READ_FAILED;

    // Update slot total
    NodeHeader leafPageNodeHeader = ixm->getNodeHeader(pageData);
    currPage = leafHeader.nextPage;
    totalSlot = leafPageNodeHeader.numSlots;

    return SUCCESS;
}

bool IX_ScanIterator::checkScanCondition()
{
    bool result = false;

    DataEntry dataEntry;
    dataEntry = {NULL, 0};
    dataEntry.key = malloc(VARCHAR_LENGTH_SIZE + attribute.length);


    //Initialize the Data entry
    ixm->getDataEntry(currSlot, pageData, dataEntry);

    void *data = malloc(VARCHAR_LENGTH_SIZE + attribute.length);

    //Isolate the actual Key
    ixm->getKeyd(attribute, data, dataEntry.key);
    cout << "In Check Scan Condition " << endl;


    // Might want to check the two range values and the compop to find if a comparison is needed
    switch (attribute.type)
    {
        case TypeInt:
    		cout << "Case TypeInt " << endl;
            int32_t keyInt;
            memcpy(&keyInt, data, INT_SIZE);
            result = checkScanCondition(keyInt);

        break;
        case TypeReal:
    		cout << "Case TypeReal " << endl;
	        float keyReal;
	        memcpy(&keyReal, data, REAL_SIZE);
	        result = checkScanCondition(keyReal);

        break;
        case TypeVarChar:
    		cout << "Case TypeVarChar " << endl;
            uint32_t varcharSize;
            memcpy(&varcharSize, data, VARCHAR_LENGTH_SIZE);

            char keyVarChar[varcharSize + 1];
            memcpy(keyVarChar, (char *) data + VARCHAR_LENGTH_SIZE, varcharSize);
            keyVarChar[varcharSize + 1] = '\0';
            result = checkScanCondition(keyVarChar);
        break;
    }

    free(dataEntry.key);
    free (data);
    return result;
    // Allocate enough memory to hold attribute and 1 byte null indicator

}

bool IX_ScanIterator::checkScanCondition(int keyInt)
{
    int32_t lowintValue;
    int32_t highintValue;

    cout << "Int Check Scan Condition" << endl;
    //Both Null
    if(lowKey == NULL && highKey == NULL){
    	return true;
    }
	 //One Null Low
    else if(lowKey == NULL && highKey != NULL){
    	memcpy (&highintValue, highKey, INT_SIZE);
    	if(highKeyInclusive){
    		return (keyInt <= highintValue);
    	}else{
    		return (keyInt < highintValue);
    	}
    }

    //One Null High
   	else if(lowKey != NULL && highKey == NULL){
    	memcpy (&lowintValue, lowKey, INT_SIZE);
   		if(lowKeyInclusive){
   			return (lowintValue <= keyInt);
   		}else{
   			return (lowintValue < keyInt);
   		}
   	}
   	// Both values fullfill a field
   	else{
   		bool ret = false;
    	memcpy (&lowintValue, lowKey, INT_SIZE);
    	memcpy (&highintValue, highKey, INT_SIZE);
   		if(lowKeyInclusive){
   			ret = (lowintValue <= keyInt);
   		}else{
   			ret = (lowintValue < keyInt);
   		}

   		if(highKeyInclusive){
    		return (keyInt <= highintValue);
    	}else{
    		return (keyInt < highintValue);
    	}

    	return ret;

   	}

}

bool IX_ScanIterator::checkScanCondition(float keyReal)
{
    return false;
}

bool IX_ScanIterator::checkScanCondition(char *keyVarChar)
{
    return false;
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
	// cout <<"Read count : " <<  ixReadPageCounter << endl;
	return fh.readPage(pageNum, data);

}

RC IXFileHandle::writePage(PageNum pageNum, const void *data){
	ixWritePageCounter++;
	// cout <<"Write count : " <<  ixWritePageCounter << endl;
	return fh.writePage(pageNum, data);
}

RC IXFileHandle::appendPage(const void *data){
	ixAppendPageCounter++;
	// cout <<"append count : " <<  ixAppendPageCounter << endl;
	return fh.appendPage(data);

}

void IXFileHandle::setfd(FILE *fd){
	fh.setfd(fd);
}

FILE * IXFileHandle::getfd(){
	return fh.getfd();
}

unsigned IndexManager::getPageFreeSpaceSize(void * page)
{
    NodeHeader nodeHeader = getNodeHeader(page);
    return PAGE_SIZE - nodeHeader.freeSpaceOffset;
}

//Function that finds the correct data page to access the leaf, returns True Of it could find a Leaf Page, returns false otherwise
bool IndexManager::traverseTree(IXFileHandle &ixfh, const Attribute &attribute, const void * value, void * retPage, uint32_t &pageNum, uint32_t &slotNum){
	//Grab the Header Page
	//Grab the root Page
	//Check the number of Slots
	//For each slot, check the passed in values agaisnt the key
	//If match, follow down a layer
	//Repeat Until LEAF PAGE is Found

	char * indexFileHeaderPage = (char *)malloc(PAGE_SIZE);

	if(ixfh.readPage(0, indexFileHeaderPage))
	    return false;

	IndexFileHeader indexFileHeader = getIndexFileHeader(indexFileHeaderPage);

	char * page = (char *)malloc(PAGE_SIZE);

	pageNum = indexFileHeader.rootPageId;

	if(ixfh.readPage(indexFileHeader.rootPageId, page))
	    return false;

	printNonLeafHelper(page, attribute);
	uint32_t currSlot = 0;
	uint32_t totalSlot = 0;
	bool keyValBool = false;
	bool keyValEq = false;
	bool startedTraversing = false;
	slotNum = currSlot;

	IndexEntry indexEntry = {NULL, 0};
	indexEntry.key = malloc(attribute.length + VARCHAR_LENGTH_SIZE);

	NodeHeader nodeHeader = getNodeHeader(page);
	SlotEntry slotEntry;

	totalSlot = nodeHeader.numSlots;

	cout << "totalSlot: " <<  totalSlot << endl;



	//At root
	//First Entry in a page always points to the left child
	//If there are more than 1 entry, grab the first two to become the
	// " Left and right" Child
	//If there is only one, grab it as the lieft child and mark the right child as
	//non existant with a zero
	//If the are no entries in the page for some reason, there are no children to search.
	if(totalSlot > 0){
		getFullIndexEntry(0, page, indexEntry);
		cout << "IndexRightChild: " << indexEntry.rightChild << endl;
	}else{
	    free(indexFileHeaderPage);
	    free(page);
		free(indexEntry.key);
		return false;
	}

	//Traverse a non-leaf page for all it's entries until you can  find a correct path to get to the
	//Keep traverseing if the value is larger than the current slot being examined
	switch (attribute.type)
	{
	    case TypeInt:

	    	cout << "In traverseTree: TypeInt" << endl;
	    	int keyInt;
	    	int valueInt;

			if(indexEntry.rightChild != 0){
    			memcpy(&keyInt, indexEntry.key, INT_SIZE);
	    		cout << "KeyInt: " << keyInt;
    		}

			while(currSlot < totalSlot && !(nodeHeader.isLeaf)){

				cout << "Iteration: " << currSlot << endl;
				cout << "IndexRightChild: " << indexEntry.rightChild << endl;

				slotEntry = getSlotEntry(currSlot,page);

				if(totalSlot > 0){
					getFullIndexEntry(currSlot, page, indexEntry);
				}


				if(indexEntry.rightChild != 0){
		    		cout << "KeyInt: " << keyInt;
	    			memcpy(&keyInt, indexEntry.key, INT_SIZE);
	    		}

				cout << "SlotEntry.lengt: "<< slotEntry.length << endl;
				cout << "SlotEntry.offset: "<< slotEntry.offset << endl;

				if(value == NULL){
					keyValBool = false;
					keyValEq = true;
				}else{
	    			memcpy(&valueInt, value, INT_SIZE);
					keyValBool = (keyInt < valueInt);
	 			   	cout << "valueInt: " << valueInt << endl;
				}

				if(keyValBool){
					// If the keyIny < Value increment all the things
					currSlot+=1;
					slotNum+=1;

					cout << "Incrementing: " << endl;
				}else{

					cout << "In k >= v: " << endl;
					//Have decided that page 0 is a header page of the file, if we are pointing to this for some reason, there is likely no valid nonleaf or leaf page associated with this part.
					if(indexEntry.rightChild == 0){
						cout << "Child is empty" << endl;
						return false;
					}else{

						cout << "Child Found" << endl;
						pageNum = indexEntry.rightChild;
						if(ixfh.readPage(indexEntry.rightChild, page))
						    return false;

						nodeHeader = getNodeHeader(page);
						totalSlot = nodeHeader.numSlots;
						cout << totalSlot << endl;
						if(!nodeHeader.isLeaf){
							cout << "Child is a nonleaf "<< endl;
							if(totalSlot > 0){
								cout << "Child entries is 1" << endl;
								getFullIndexEntry(0, page, indexEntry);
								slotNum = currSlot = 0;
							}else{
								//Weird Case
								return false;
							}
						}
					}

				}
			}

			cout << "RetPage: " << pageNum << endl;
			cout << "slotNum: " << slotNum << endl;

			//If we ran out of the Page Without finding a Leaf Page return false
			if(!(nodeHeader.isLeaf)){
				cout << "Did not find a leaf!";
				cout << pageNum << endl;
				memcpy(retPage, page, PAGE_SIZE);
			    free(indexFileHeaderPage);
			    free(page);
				free(indexEntry.key);
				return false;
			}else{
				cout << "Did find a leaf!";
				cout << pageNum << endl;
				slotNum = currSlot;
				currSlot = 0;
				memcpy(retPage, page, PAGE_SIZE);
			    free(indexFileHeaderPage);
			    free(page);
				free(indexEntry.key);
				return true;
			}

        break;
	    case TypeReal:
	    	cout << "in traverse tree TypeReal" << endl;
	    	return false;

        break;
	    case TypeVarChar:
	    	cout << "in traverse tree TypeVarChar" << endl;
	    	return false;
        break;
    }

    free(indexFileHeaderPage);
    free(page);
	free(indexEntry.key);
}
