
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

void IndexManager::newLeafPage(void * page){
    memset(page, 0, PAGE_SIZE);
    // Writes the slot directory header.
    NodeHeader nodeHeader;
    nodeHeader.parent = 0;
    nodeHeader.numSlots = 0;
    nodeHeader.isLeaf = true;
    nodeHeader.isRoot = false;
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

    newLeafPage(firstPageData);

    if (handle.appendPage(firstPageData))
        return RBFM_APPEND_FAILED;

    _pf_manager->closeFile(handle);

    free(firstPageData);

    return SUCCESS;
}

RC IndexManager::destroyFile(const string &fileName)
{
    return -1;
}

RC IndexManager::openFile(const string &fileName, IXFileHandle &ixfileHandle)
{
    return _pf_manager->openFile(fileName.c_str(), ixfileHandle);
}

RC IndexManager::closeFile(IXFileHandle &ixfileHandle)
{
    return -1;
}

RC IndexManager::insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid)
{
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
