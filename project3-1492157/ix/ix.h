#ifndef _ix_h_
#define _ix_h_

#include <vector>
#include <string>
#include <map>
#include <cstring>

#include "../rbf/rbfm.h"

# define IX_EOF (-1)  // end of the index scan
# define IX_READ_FAILED 1
# define IX_INSERT_FAILED 2

class IX_ScanIterator;
class IXFileHandle;

typedef struct IndexFileHeader{
    uint32_t rootPageId;
}IndexFileHeader;

typedef struct DataEntry{
    void* key;
    RID rid;
}DataEntry;

typedef struct NodeHeader{
    uint32_t numSlots;
    uint32_t parent;
    bool isLeaf;
    bool isRoot;
    uint32_t freeSpaceOffset;
}NodeHeader;

typedef struct LeafHeader{
    uint32_t nextPage;
    uint32_t prevPage;
}LeafHeader;

typedef struct IndexEntry{
    void* key;
    uint32_t rightChild;
} IndexEntry;

typedef struct SlotEntry{
    uint32_t length;
    uint32_t offset;
} SlotEntry;

class IndexManager {

    public:
        static IndexManager* instance();

        // Create an index file.
        RC createFile(const string &fileName);

        // Delete an index file.
        RC destroyFile(const string &fileName);

        // Open an index and return an ixfileHandle.
        RC openFile(const string &fileName, IXFileHandle &ixfileHandle);

        // Close an ixfileHandle for an index.
        RC closeFile(IXFileHandle &ixfileHandle);

        // Insert an entry into the given index that is indicated by the given ixfileHandle.
        RC insertEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Delete an entry from the given index that is indicated by the given ixfileHandle.
        RC deleteEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, const void *key, const RID &rid);

        // Initialize and IX_ScanIterator to support a range search
        RC scan(IXFileHandle &ixfileHandle,
                const Attribute &attribute,
                const void *lowKey,
                const void *highKey,
                bool lowKeyInclusive,
                bool highKeyInclusive,
                IX_ScanIterator &ix_ScanIterator);

        // Print the B+ tree in pre-order (in a JSON record format)
        void printBtree(IXFileHandle &ixfileHandle, const Attribute &attribute);

    protected:
        IndexManager();
        ~IndexManager();

    private:
        static IndexManager *_index_manager;
        static PagedFileManager *_pf_manager;

        void initializeIndexFileHeaderPage(void * page);
        void setIndexFileHeader(void * page, IndexFileHeader indexFileHeader);
        IndexFileHeader getIndexFileHeader(void * page);

        void setNodeHeader(void* page, NodeHeader nodeHeader);
        NodeHeader getNodeHeader(void * page);

        void newNonLeafPage(void * page);

        void newLeafPage(void * page);
        void setLeafHeader(void* page, LeafHeader leafHeader);
        LeafHeader getLeafHeader(void * page);

        void setSlotEntry(uint32_t slotNum, SlotEntry slotEntry, void * page);
        SlotEntry getSlotEntry(uint32_t slotNum, void * page);


        RC insertDataEntry(void * pageData, const Attribute &attribute, const DataEntry &dataEntry);
        bool enoughFreeSpaceForDataEntry(void * pageData, const Attribute &attribute, const void *key);

        RC insertIndexEntry(void * pageData,const Attribute &attribute,const IndexEntry &indexEntry);
        bool enoughFreeSpaceForIndexEntry(void * pageData, const Attribute &attribute, const void *key);

        RC recurBtree(IXFileHandle &ixfileHandle, const Attribute &attribute, unsigned pageNum);
        void printLeafHelper(void * pageData, const Attribute &attribute);
        void printNonLeafHelper(void * pageData, const Attribute &attribute);

        unsigned getPageFreeSpaceSize(void * page);

        void getKeyd(const Attribute &attribute, void * retKey, const void * key);

        void getIndexEntry(IXFileHandle &ixfileHandle, const Attribute &attribute, IndexEntry &indexEntry, char * pageData, SlotEntry dirtySlot);
};


class IX_ScanIterator {
    public:

		// Constructor
        IX_ScanIterator();

        // Destructor
        ~IX_ScanIterator();

        // Get next matching entry
        RC getNextEntry(RID &rid, void *key);

        // Terminate index scan
        RC close();
};



class IXFileHandle {
    public:

    // variables to keep counter for each operation
    unsigned ixReadPageCounter;
    unsigned ixWritePageCounter;
    unsigned ixAppendPageCounter;

    // Constructor
    IXFileHandle();

    // Destructor
    ~IXFileHandle();

    RC readPage(PageNum pageNum, void *data);                           // Get a specific page
    RC writePage(PageNum pageNum, const void *data);                    // Write a specific page
    RC appendPage(const void *data);                                    // Append a specific page
    unsigned getNumberOfPages();
	// Put the current counter values of associated PF FileHandles into variables
	RC collectCounterValues(unsigned &readPageCount, unsigned &writePageCount, unsigned &appendPageCount);

    friend class FileHandle;
    friend class IndexManager;
    friend class PagedFileManager;

    private:
    FileHandle fh;

};

#endif
