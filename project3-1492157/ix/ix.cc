
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

RC IndexManager::insertDataEntry(void * pageData, const Attribute &attribute, const void *key, const RID &rid)
{   int indexSize = 0;
    switch (attribute.type)
    {
        case TypeInt:
            indexSize += INT_SIZE;
        case TypeReal:
            indexSize += REAL_SIZE;
        case TypeVarChar:
            uint32_t varcharSize;
            memcpy(&varcharSize, key, VARCHAR_LENGTH_SIZE);
            indexSize += VARCHAR_LENGTH_SIZE;
            indexSize += varcharSize;
    }
    indexSize += sizeof(uint32_t);
    char * page = (char *)pageData;
    if (!(getPageFreeSpaceSize(page) >= indexSize)){
         return IX_INSERT_FAILED;
    }

    NodeHeader leafPageNodeHeader = getNodeHeader(page);

    switch (attribute.type)
    {
        case TypeInt:
            memcpy(page + leafPageNodeHeader.freeSpaceOffset, key, INT_SIZE);
            leafPageNodeHeader.freeSpaceOffset += INT_SIZE;
        case TypeReal:
            memcpy(page + leafPageNodeHeader.freeSpaceOffset, key, REAL_SIZE);
            leafPageNodeHeader.freeSpaceOffset += REAL_SIZE;
        case TypeVarChar:
            uint32_t varcharSize;
            memcpy(&varcharSize, key, VARCHAR_LENGTH_SIZE);
            memcpy(page + leafPageNodeHeader.freeSpaceOffset, &varcharSize, VARCHAR_LENGTH_SIZE);
            leafPageNodeHeader.freeSpaceOffset += VARCHAR_LENGTH_SIZE;
            memcpy(page + leafPageNodeHeader.freeSpaceOffset, ((char*) key + VARCHAR_LENGTH_SIZE), varcharSize);
            leafPageNodeHeader.freeSpaceOffset += varcharSize;
    }
    memcpy(page + leafPageNodeHeader.freeSpaceOffset, &rid, sizeof(RID));
    leafPageNodeHeader.freeSpaceOffset += sizeof(RID);
    return SUCCESS;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
    void * pageData = malloc(PAGE_SIZE);
    char * page = (char *)pageData;

    FileHandle fileHandle;
    ixfileHandle.IXToFile(fileHandle);
    if(fileHandle.readPage(0, page))
        return IX_READ_FAILED;

    NodeHeader nodeHeader = getNodeHeader(page);
    if(nodeHeader.numSlots == 0){
        // Index Page
        

        //Data Page
        newLeafPage(page);
        NodeHeader leafPageNodeHeader = getNodeHeader(page);
        leafPageNodeHeader.numSlots += 1;
        DataEntry dataEntry;


    }

    ixfileHandle.fileToIX(fileHandle);

    return -1;
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

void IndexManager::printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute) const {
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
