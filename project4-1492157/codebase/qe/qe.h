#ifndef _qe_h_
#define _qe_h_

#include <vector>
#include <cstring>

#include "../rbf/rbfm.h"
#include "../rm/rm.h"
#include "../ix/ix.h"

#define QE_EOF (-1)  // end of the index scan

using namespace std;

typedef enum{ MIN=0, MAX, COUNT, SUM, AVG } AggregateOp;

// The following functions use the following
// format for the passed data.
//    For INT and REAL: use 4 bytes
//    For VARCHAR: use 4 bytes for the length followed by the characters

struct Value {
    AttrType type;          // type of value
    void     *data;         // value
};


struct Condition {
    string  lhsAttr;        // left-hand side attribute
    CompOp  op;             // comparison operator
    bool    bRhsIsAttr;     // TRUE if right-hand side is an attribute and not a value; FALSE, otherwise.
    string  rhsAttr;        // right-hand side attribute if bRhsIsAttr = TRUE
    Value   rhsValue;       // right-hand side value if bRhsIsAttr = FALSE
};


class Iterator {
    // All the relational operators and access methods are iterators.
    public:
        virtual RC getNextTuple(void *data) = 0;
        virtual void getAttributes(vector<Attribute> &attrs) const = 0;
        virtual void setIterator(Condition condition) = 0;
        virtual RC setNE_OP(Condition condition) = 0;
        virtual ~Iterator() {};
};


class TableScan : public Iterator
{
    // A wrapper inheriting Iterator over RM_ScanIterator
    public:
        RelationManager &rm;
        RM_ScanIterator *iter;
        string tableName;
        vector<Attribute> attrs;
        vector<string> attrNames;
        RID rid;

        TableScan(RelationManager &rm, const string &tableName, const char *alias = NULL):rm(rm)
        {
        	//Set members
        	this->tableName = tableName;

            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Get Attribute Names from RM
            unsigned i;
            for(i = 0; i < attrs.size(); ++i)
            {
                // convert to char *
                attrNames.push_back(attrs.at(i).name);
            }

            // Call RM scan to get an iterator
            iter = new RM_ScanIterator();
            rm.scan(tableName, "", NO_OP, NULL, attrNames, *iter);

            // Set alias
            if(alias) this->tableName = alias;
        };

        // Start a new iterator given the new compOp and value
        void setIterator(Condition condition)
        {   
            iter->close();
            delete iter;
            iter = new RM_ScanIterator();

            string delimiter = ".";
			string parsedAttributeName = condition.lhsAttr.substr(condition.lhsAttr.find(delimiter) + 1, condition.lhsAttr.length());

            cout << "parsedAttributeName: " << parsedAttributeName << endl;
            rm.scan(tableName, parsedAttributeName, condition.op, condition.rhsValue.data, attrNames, *iter);
        };

        RC getNextTuple(void *data)
        {
            return iter->getNextTuple(rid, data);
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };


        RC setNE_OP(Condition condition){

        	return QE_EOF;

        }

        ~TableScan()
        {
        	iter->close();
        };
};


class IndexScan : public Iterator
{
    // A wrapper inheriting Iterator over IX_IndexScan
    public:
        RelationManager &rm;
        RM_IndexScanIterator *iter;
        string tableName;
        string attrName;
        vector<Attribute> attrs;
        char key[PAGE_SIZE];
        RID rid;

        IndexScan(RelationManager &rm, const string &tableName, const string &attrName, const char *alias = NULL):rm(rm)
        {
        	// Set members
        	this->tableName = tableName;
        	this->attrName = attrName;


            // Get Attributes from RM
            rm.getAttributes(tableName, attrs);

            // Call rm indexScan to get iterator

            cout << "Here " << endl;
            iter = new RM_IndexScanIterator();
            cout << rm.getIndexFileName(tableName, attrName) << endl;
            rm.indexScan(tableName, attrName, NULL, NULL, true, true, *iter);

            // Set alias
            if(alias) this->tableName = alias;

            cout << "Done " << endl;
        };

        // Start a new iterator given the new key range
        // void setIterator(void* lowKey,
        //                  void* highKey,
        //                  bool lowKeyInclusive,
        //                  bool highKeyInclusive)
        // {
        //     iter->close();
        //     delete iter;
        //     iter = new RM_IndexScanIterator();
        //     rm.indexScan(tableName, attrName, lowKey, highKey, lowKeyInclusive,
        //                    highKeyInclusive, *iter);
        // };

        void setIterator(Condition condition)
        {
            iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();

   //          string delimiter = ".";
			// string parsedAttributeName = condition.lhsAttr.substr(condition.lhsAttr.find(delimiter) + 1, condition.lhsAttr.length());

            switch(condition.op){
                case EQ_OP:
			    rm.indexScan(tableName, attrName, condition.rhsValue.data, condition.rhsValue.data, true,
			                   true, *iter);  
                break; 
                case LT_OP: 
			    rm.indexScan(tableName, attrName, NULL, condition.rhsValue.data, true,
			                   false, *iter);

                break;
                case GT_OP:
                 rm.indexScan(tableName, attrName, condition.rhsValue.data, NULL, false,
                            true, *iter);


                break; 
                case LE_OP:
                rm.indexScan(tableName, attrName, NULL, condition.rhsValue.data, true,
                           true, *iter);

                break; 
                case GE_OP:
                cout << "In GE_OP" << endl;
                rm.indexScan(tableName, attrName, condition.rhsValue.data, NULL, true,
                          true, *iter);
                cout <<" Scan ok" << endl;
                break; 
                case NE_OP:
                rm.indexScan(tableName, attrName, NULL, condition.rhsValue.data, true,
                            false, *iter); 

                break; 
                case NO_OP:
                rm.indexScan(tableName, attrName, NULL, NULL, true,
                            true, *iter); 

                break; 
            }


           // rm.indexScan(tableName, parsedAttributeName, lowKey, highKey, lowKeyInclusive,
           //                highKeyInclusive, *iter);
        };

        RC getNextTuple(void *data)
        {   
            int rc = iter->getNextEntry(rid, key);
            if(rc == 0)
            {
                rc = rm.readTuple(tableName.c_str(), rid, data);
            }
            return rc;
        };

        void getAttributes(vector<Attribute> &attrs) const
        {
            attrs.clear();
            attrs = this->attrs;
            unsigned i;

            // For attribute in vector<Attribute>, name it as rel.attr
            for(i = 0; i < attrs.size(); ++i)
            {
                string tmp = tableName;
                tmp += ".";
                tmp += attrs.at(i).name;
                attrs.at(i).name = tmp;
            }
        };

        RC setNE_OP(Condition condition){
        	iter->close();
            delete iter;
            iter = new RM_IndexScanIterator();
            string delimiter = ".";
			string parsedAttributeName = condition.lhsAttr.substr(condition.lhsAttr.find(delimiter) + 1, condition.lhsAttr.length());
        	rm.indexScan(tableName, parsedAttributeName, condition.rhsValue.data, NULL, false,
        	           true, *iter);

        	return SUCCESS;

        }

        ~IndexScan()
        {
            iter->close();
        };
};


class Filter : public Iterator {
    // Filter operator
    public:
        Filter(Iterator *input,               // Iterator of input R
               const Condition &condition     // Selection condition
        );
        ~Filter(){};
        Condition condition;
        Iterator* iter;
        vector <Attribute> attrs;
        bool NE_OPFirstRun = false;

        void setIterator(Condition condition);
        RC getNextTuple(void *data);
        bool compOpCases(int ogComp, int toComp, CompOp op);
        bool compOpCases(float ogComp, float toComp, CompOp op);
        bool compValue(void * buffer, Value rhsValue, CompOp op);
         RC setNE_OP(Condition condition){return -1;};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};


class Project : public Iterator {
    // Projection operator
    public:
        Project(Iterator *input,                    // Iterator of input R
              const vector<string> &attrNames);;   // vector containing attribute names
        ~Project(){};

        Condition condition;
        Iterator* iter;
        vector<string> attrNames;
        vector <Attribute> attrs;

        RC getNextTuple(void *data);
	     RC setNE_OP(Condition condition){return -1;};
        void setIterator(Condition condition){};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};


class INLJoin : public Iterator {
    // Index nested-loop join operator
    public:
        INLJoin(Iterator *leftIn,           // Iterator of input R
               IndexScan *rightIn,          // IndexScan Iterator of input S
               const Condition &condition   // Join condition
        );
        ~INLJoin(){};

        Condition condition;
        Iterator* iter;
        vector <Attribute> attrs;

        RC getNextTuple(void *data);
	     RC setNE_OP(Condition condition){return -1;};
	    void setIterator(Condition condition){};
        // For attribute in vector<Attribute>, name it as rel.attr
        void getAttributes(vector<Attribute> &attrs) const;
};



#endif
