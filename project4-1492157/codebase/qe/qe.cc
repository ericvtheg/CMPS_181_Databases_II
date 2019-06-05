
#include "qe.h"

// ------------------- filter interface -------------------

bool Iterator::compOpCases(int ogComp, int toComp, CompOp op){
    switch(op){
        case EQ_OP: return ogComp == toComp;
        case LT_OP: return ogComp < toComp;
        case GT_OP: return ogComp > toComp;
        case LE_OP: return ogComp <= toComp;
        case GE_OP: return ogComp >= toComp;
        case NE_OP: return ogComp != toComp;
        case NO_OP: return true;
        default: return false;

    }
}

bool Iterator::compOpCases(float ogComp, float toComp, CompOp op){
    switch(op){
        case EQ_OP: return ogComp == toComp;
        case LT_OP: return ogComp < toComp;
        case GT_OP: return ogComp > toComp;
        case LE_OP: return ogComp <= toComp;
        case GE_OP: return ogComp >= toComp;
        case NE_OP: return ogComp != toComp;
        case NO_OP: return true;
        default: return false;
    }
}

bool Iterator::compOpCases(char * ogComp, char * toComp, CompOp op){
    int cmp = strcmp(ogComp, toComp);
    switch (op)
    {
        case EQ_OP: return cmp == 0;
        case LT_OP: return cmp <  0;
        case GT_OP: return cmp >  0;
        case LE_OP: return cmp <= 0;
        case GE_OP: return cmp >= 0;
        case NE_OP: return cmp != 0;
        case NO_OP: return true;
        default: return false;
    }
    
}

bool Iterator::compValue(void * ogCompPointer, void * toCompPointer, Condition condition)
{
    bool result = false;

    if(condition.op == NO_OP ){
        return true;
    }

    switch(condition.rhsValue.type){
        case TypeInt:
            int32_t ogCompInt;
            int32_t toCompInt;
            memcpy(&ogCompInt, ogCompPointer, INT_SIZE);
            memcpy(&toCompInt, toCompPointer, INT_SIZE);
            cout << "ogCompInt: " << ogCompInt << endl;
            cout << "toCompInt: " << toCompInt << endl;
            result = compOpCases(ogCompInt, toCompInt, condition.op);

        break;
        case TypeReal:
            float ogCompFloat;
            float toCompFloat;
            memcpy(&ogCompFloat, ogCompPointer, REAL_SIZE);
            memcpy(&toCompFloat, toCompPointer, REAL_SIZE);
            result = compOpCases(ogCompFloat, toCompFloat, condition.op);

        break;
        case TypeVarChar:
            uint32_t varcharSize = 0;
            memcpy(&varcharSize, ogCompPointer, INT_SIZE);
            char ogCompChar [varcharSize + 1];
            memcpy(&ogCompChar, (char *)ogCompPointer + VARCHAR_LENGTH_SIZE, varcharSize);
            ogCompChar[varcharSize + 1] = '\0';

            memcpy(&varcharSize, toCompPointer, INT_SIZE);
            char toCompChar [varcharSize + 1];
            memcpy(&toCompChar, (char *)toCompPointer + VARCHAR_LENGTH_SIZE, varcharSize);
            toCompChar[varcharSize + 1] = '\0';
            
            result = compOpCases(ogCompChar, toCompChar, condition.op);

        break;
    }

    return result;

}

bool Iterator::prepAttributeValue(Condition condition, vector<Attribute> attrVec, void * data, void * retValue){
    int nullIndicatorSize =  int(ceil((double) attrVec.size() / CHAR_BIT));
    char nullIndicator[nullIndicatorSize];
    memset(nullIndicator, 0, nullIndicatorSize);
    memcpy(nullIndicator, (char*) data, nullIndicatorSize);

    int indicatorIndex;
    int indicatorMask;
    bool fieldIsNull;

    unsigned data_offset = nullIndicatorSize;
 
    string delimiter = ".";
    // string parsedAttributeName = condition.lhsAttr.substr(condition.lhsAttr.find(delimiter) + 1, condition.lhsAttr.length());
    // //bool attrFoundinVec = false;
    // cout <<"parsedAttributeName: " <<  parsedAttributeName << endl;
    cout <<"vectorSize: " <<  attrVec.size() << endl;

    for (unsigned i = 0; i < (unsigned) attrVec.size(); i++)
    {
        cout << "attName: " << attrVec[i].name << endl;
        indicatorIndex = i / CHAR_BIT;
        indicatorMask  = 1 << (CHAR_BIT - 1 - (i % CHAR_BIT));
        fieldIsNull = (nullIndicator[indicatorIndex] & indicatorMask) != 0;
        if (fieldIsNull == true){
            cout << "Field Is Null" << endl;
            if(attrVec[i].name.compare(condition.lhsAttr) == 0){
                cout << "Desired Field is Null" << endl;
                return false;
            }
            continue;
        }
        switch (attrVec[i].type)
        {
            case TypeInt:
                if(attrVec[i].name.compare(condition.lhsAttr) == 0){
                    memcpy(retValue,(char *)data + data_offset, INT_SIZE);
                    return true;
                }
                data_offset += INT_SIZE;
            break;
            case TypeReal:
                if(attrVec[i].name.compare(condition.lhsAttr) == 0){
                    memcpy(retValue,(char *)data + data_offset, REAL_SIZE);
                    return true;
                }
                data_offset += REAL_SIZE;
            break;
            case TypeVarChar:
                uint32_t varcharSize;
                if(attrVec[i].name.compare(condition.lhsAttr) == 0){
                    memcpy(&varcharSize, (char*) data + data_offset, VARCHAR_LENGTH_SIZE);
                    memcpy(retValue,(char *)data + data_offset, VARCHAR_LENGTH_SIZE);
                    memcpy((char *)retValue + VARCHAR_LENGTH_SIZE,(char *)data + data_offset + VARCHAR_LENGTH_SIZE, varcharSize);
                    return true;
                }
                // We have to get the size of the VarChar field by reading the integer that precedes the string value itself
                data_offset += varcharSize;
                data_offset += varcharSize + VARCHAR_LENGTH_SIZE;
            break;
        }
    }

    cout << "how did you make it our here?" << endl;
    return false;

}


// bool Filter::compValue(void * buffer, Value rhsValue, CompOp op){
//     int ogCompInt = 0;
//     int toCompInt = 0;
//     float ogCompFloat = 0;
//     float toCompFloat = 0;
//     switch(rhsValue.type){
//         case TypeInt: 
//             memcpy(&ogCompInt, rhsValue.data, sizeof(int));
//             memcpy(&toCompInt, buffer, sizeof(int));
//             return compOpCases(ogCompInt, toCompInt, op);
//         case TypeReal:
//             memcpy(&ogCompFloat, rhsValue.data, sizeof(float));
//             memcpy(&toCompFloat, buffer, sizeof(float));
//             return compOpCases(ogCompFloat, toCompFloat, op);
//         case TypeVarChar:
//         // deal with
//         // would our value pointer hold the size of the varchar as well?
//         // im guessing yes
//             return false;
        
//         default: return false;
//     }
// }

Filter::Filter(Iterator* input, const Condition &condition)
{
    // This filter class is initialized by an input iterator and a selection 
    // condition. It filters the tuples from the input iterator by applying the 
    // filter predicate condition on them. For simplicity, we assume this filter 
    // only has a single selection condition. The schema of the returned tuples 
    // should be the same as the input tuples from the iterator.

    this->iter = input;
    this->condition = condition;
    this->attrs.clear();
    input->getAttributes(this->attrs);

    //void *data = malloc(PAGE_SIZE);

    // do we call get next Tuple from here
}

RC Filter::getNextTuple(void *data) 
{
	RC rc;
    //RC rc = iter->getNextTuple(data);
    void * retValue = malloc(PAGE_SIZE);
    //RC nullValueRC = SUCCESS;

    while(true){
        rc = iter->getNextTuple(data);

        if(rc == QE_EOF) return QE_EOF;
        else if(prepAttributeValue(condition, attrs, data, retValue ) == false){
            cout << "Value was null" << endl;
            continue;
        }
        else if (compValue(retValue, condition.rhsValue.data , condition))
        return SUCCESS;

    }


    free(retValue);
    // bool nullBit = false;
    // bool satisfied = false;
    // RC rc;
    // int offset;
    // void * buffer = malloc(PAGE_SIZE);
    // int compInt = 0;
    // float compFloat = 0;
    // char* compChar = "";


   //load in value into void pointer
//    getCompValue(buffer, condition.rhsValue);

    // recur through the tuples
    // while ((rc = getNextTuple(data)) == 1 )
    // {
    // //   our code
    //   offset = 1;
    //   satisfied = false;
      


    // // for all attributes in tuple
    //   for(int i =0; attrs.size(); i++){
    //     nullBit = *(unsigned char *)((char *)data) & (1 << (7-i));
    //     // what to do for nullbit case

        
    //     if(condition.bRhsIsAttr){
    //     //   if (switchCases(condition.op, condition.rhsValue, attrs.)){  //(attrs[i].name == condition.rhsAttr){ 
    //     //       satisfied = true;
    //     //     // attribute that we care about and want to print  
    //     //   }
    //     }
    //     else //bRhsISAttr is False
    //       if(attrs[i].name == condition.lhsAttr){
    //         if(attrs[i].type == TypeVarChar){
    //           // need to load in 4 byte variable first
    //         }else if(attrs[i].type == TypeReal){
    //           //buffer now holds value for corresponding attr
    //           memcpy(buffer, (char*)data+offset, attrs[i].length);
    //         }else{
    //           int compInt;
    //           memcpy(buffer, (char*)data+offset, attrs[i].length);
    //         }

    //         // check to see if rhsValue.data matches comparison condition and value
    //         satisfied = compValue(buffer, condition.rhsValue, condition.op);
    //     }
    //     if(attrs[i].type == TypeVarChar){
    //       offset += 4;
    //     //   offset += sizeofVarChar
    //     }else{
    //       offset += attrs[i].length;
    //     }
        
                
    //     // even if isn't attribute we want to comp still memcpy into buffer so we can "keep original tuple schema"


   
    // }
    //   if(satisfied){ // if tuple satisfied criteria return it
    //     // how do we return a bunch of tuples?
    //     // should we just be printing each one individually?
    //   }
    // }

    // free(buffer);
    // return QE_EOF;
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


