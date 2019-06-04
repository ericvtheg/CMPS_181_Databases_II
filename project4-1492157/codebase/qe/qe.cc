
#include "qe.h"

// ------------------- filter interface -------------------

bool Filter::compOpCases(int ogComp, int toComp, CompOp op){
    switch(op){
        case EQ_OP: return ogComp == toComp;
        case LT_OP: return ogComp > toComp;
        case GT_OP: return ogComp < toComp;
        case LE_OP: return ogComp >= toComp;
        case GE_OP: return ogComp <= toComp;
        case NE_OP: return ogComp != toComp;
        default: return false;

    }
}

bool Filter::compOpCases(float ogComp, float toComp, CompOp op){
    switch(op){
        case EQ_OP: return ogComp == toComp;
        case LT_OP: return ogComp > toComp;
        case GT_OP: return ogComp < toComp;
        case LE_OP: return ogComp >= toComp;
        case GE_OP: return ogComp <= toComp;
        case NE_OP: return ogComp != toComp;
        default: return false;
    }
}

// {
// //   int cmp = strcmp(recordString, valueStr);
  
//   switch (compOp)
//   {
//     case EQ_OP: return cmp == 0;
//     case LT_OP: return cmp <  0;
//     case GT_OP: return cmp >  0;
//     case LE_OP: return cmp <= 0;
//     case GE_OP: return cmp >= 0;
//     case NE_OP: return cmp != 0;
//         // Should never happen
//         default: return false;
//     }
// }

bool Filter::compValue(void * buffer, Value rhsValue, CompOp op){
    int ogCompInt = 0;
    int toCompInt = 0;
    float ogCompFloat = 0;
    float toCompFloat = 0;
    switch(rhsValue.type){
        case TypeInt: 
            memcpy(&ogCompInt, rhsValue.data, sizeof(int));
            memcpy(&toCompInt, buffer, sizeof(int));
            return compOpCases(ogCompInt, toCompInt, op);
        case TypeReal:
            memcpy(&ogCompFloat, rhsValue.data, sizeof(float));
            memcpy(&toCompFloat, buffer, sizeof(float));
            return compOpCases(ogCompFloat, toCompFloat, op);
        case TypeVarChar:
        // deal with
        // would our value pointer hold the size of the varchar as well?
        // im guessing yes
            return false;
        
        default: return false;
    }
}

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

    void *data = malloc(PAGE_SIZE);

    // do we call get next Tuple from here
}

RC Filter::getNextTuple(void *data) 
{
    bool nullBit = false;
    bool satisfied = false;
    RC rc;
    int offset;
    void * buffer = malloc(PAGE_SIZE);
    int compInt = 0;
    float compFloat = 0;
    char* compChar = "";


   //load in value into void pointer
//    getCompValue(buffer, condition.rhsValue);

    // recur through the tuples
    while ((rc = getNextTuple(data)) == 1 )
    {
    //   our code
      offset = 1;
      satisfied = false;
      


    // for all attributes in tuple
      for(int i =0; attrs.size(); i++){
        nullBit = *(unsigned char *)((char *)data) & (1 << (7-i));
        // what to do for nullbit case

        
        if(condition.bRhsIsAttr){
        //   if (switchCases(condition.op, condition.rhsValue, attrs.)){  //(attrs[i].name == condition.rhsAttr){ 
        //       satisfied = true;
        //     // attribute that we care about and want to print  
        //   }
        }
        else //bRhsISAttr is False
          if(attrs[i].name == condition.lhsAttr){
            if(attrs[i].type == TypeVarChar){
              // need to load in 4 byte variable first
            }else if(attrs[i].type == TypeReal){
              //buffer now holds value for corresponding attr
              memcpy(buffer, (char*)data+offset, attrs[i].length);
            }else{
              int compInt;
              memcpy(buffer, (char*)data+offset, attrs[i].length);
            }

            // check to see if rhsValue.data matches comparison condition and value
            satisfied = compValue(buffer, condition.rhsValue, condition.op);
        }
        if(attrs[i].type == TypeVarChar){
          offset += 4;
        //   offset += sizeofVarChar
        }else{
          offset += attrs[i].length;
        }
        
                
        // even if isn't attribute we want to comp still memcpy into buffer so we can "keep original tuple schema"


   
    }
      if(satisfied){ // if tuple satisfied criteria return it
        // how do we return a bunch of tuples?
        // should we just be printing each one individually?
      }
    }

    free(buffer);
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


