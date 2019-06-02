
#include "qe.h"

// ------------------- filter interface -------------------

Filter::Filter(Iterator* input, const Condition &condition)
{
    // This filter class is initialized by an input iterator and a selection 
    // condition. It filters the tuples from the input iterator by applying the 
    // filter predicate condition on them. For simplicity, we assume this filter 
    // only has a single selection condition. The schema of the returned tuples 
    // should be the same as the input tuples from the iterator.

    iter = input;
    this->condition = condition;

    attrs.clear();
    input->getAttributes(attrs);
}

RC Filter::getNextTuple(void *data) 
{
    // RC rc;
    // // recur through the tuples

    // while (rc = getNextTuple(data) == 1 )
    // {
    //     // get each of the tuples
    //     // compare the tuples with the condition
    //     // use that tuple
    // }

    return QE_EOF;
}

void Filter::getAttributes(vector<Attribute> &attrs) const 
{
	attrs.clear();
    attrs = this->attrs;
}


//------------------- project interface -------------------

Project::Project(Iterator *input, const vector<string> &attrNames)
{
    // This project class takes an iterator and a vector of attribute names as input.
    // It projects out the values of the attributes in the attrNames. The schema of
    // the returned tuples should be the attributes in attrNames, in the order of attributes in the vector.

    iter = input; 
    this->attrNames = attrNames;

}

RC Project::getNextTuple(void *data)
{
    return QE_EOF;
}

void Project::getAttributes(vector<Attribute> &attrs) const
{
    
}


// ------------------- index nested loop interface -------------------

INLJoin::INLJoin(
        Iterator *leftIn,           // Iterator of input R
        IndexScan *rightIn,          // IndexScan Iterator of input S
        const Condition &condition   // Join condition
        )
    {
        // The INLJoin iterator takes two iterators as input. The leftIn iterator
        // works as the outer relation, and the rightIn iterator is the inner relation.
        // The rightIn is an object of IndexScan Iterator. Again, we have already implemented 
        //the IndexScan class for you, which is a wrapper on RM_IndexScanIterator. The 
        // returned schema should be the attributes of tuples from leftIn concatenated with the
        // attributes of tuples from rightIn. You don't need to remove any duplicate attributes.


    }

RC INLJoin::getNextTuple(void *data)
{
    return QE_EOF;
}

void INLJoin::getAttributes(vector<Attribute> &attrs) const
{

}


