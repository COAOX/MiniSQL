#include <iostream>
#include "RecordManager.h"
#include <cstring>
#include "API.h"
int RecordManager::tableCreate(string table){
    string tableFileName = tableFileNameGet(table);  
    FILE *file;
    file = fopen(tableFileName.c_str(), "w+");
    if (file == NULL){
        return 0;
    }
    fclose(file);
    return 1;
}
int RecordManager::tableDrop(string table){
    string tableFileName = tableFileNameGet(table);
    bm.deleteFileNode(tableFileName.c_str());
    if (remove(tableFileName.c_str())){
        return 0;
    }
    return 1;
}
int RecordManager::indexCreate(string indexName){
    string indexFileName = indexFileNameGet(indexName);
    
    FILE *file;
    file = fopen(indexFileName.c_str(), "w+");
    if (file == NULL){
        return 0;
    }
    fclose(file);
    return 1;
}
int RecordManager::indexDrop(string indexName){
    string indexFileName = indexFileNameGet(indexName);
    bm.deleteFileNode(indexFileName.c_str());
    if (remove(indexFileName.c_str())){
        return 0;
    }
    return 1;
}

int RecordManager::recordInsert(string table,char* record, int recordSize){
    fileTmp = bm.getFile(tableFileNameGet(table).c_str());
    blockTmp = bm.getBlockHead(fileTmp);
    while (true){
        if (blockTmp == NULL){
            return -1;
        }
        if (bm.getUsingSize(*blockTmp) <= bm.getBlockSize() - recordSize){
                    char* addressBegin;
            addressBegin = bm.getContent(*blockTmp) + bm.getUsingSize(*blockTmp);
            memcpy(addressBegin, record, recordSize);
            bm.setUsingSize(*blockTmp, bm.getUsingSize(*blockTmp) + recordSize);
            bm.setDirty(*blockTmp);
            return blockTmp->offsetNum;
        }
        else{
            blockTmp = bm.getNextBlock(fileTmp, blockTmp);
        }
    }
    
    return -1;
}
int RecordManager::recordAllShow(string table, vector<string>* attributeNameVector,  vector<Condition>* conditionVector){
    fileTmp = bm.getFile(tableFileNameGet(table).c_str());
    blockTmp = bm.getBlockHead(fileTmp);
    int count = 0;
    while (true){
        if (blockTmp == NULL){
            return -1;
        }
        if (blockTmp->ifbottom){
            int recordBlockNum = recordBlockShow(table,attributeNameVector, conditionVector, blockTmp);
            count += recordBlockNum;
            return count;
        }
        else{
            int recordBlockNum = recordBlockShow(table, attributeNameVector, conditionVector, blockTmp);
            count += recordBlockNum;
            blockTmp = bm.getNextBlock(fileTmp, blockTmp);
        }
    }
    
    return -1;
}
int RecordManager::recordBlockShow(string table, vector<string>* attributeNameVector, vector<Condition>* conditionVector, int blockOffset){
    fileTmp = bm.getFile(tableFileNameGet(table).c_str());
    blockNode* block = bm.getBlockByOffset(fileTmp, blockOffset);
    if (block == NULL){
        return -1;
    }
    else{
        return  recordBlockShow(table, attributeNameVector, conditionVector, block);
    }
}
int RecordManager::recordBlockShow(string table, vector<string>* attributeNameVector, vector<Condition>* conditionVector, blockNode* block){
    if (block == NULL){
        return -1;
    }
    
    int count = 0;
    
    char* recordBegin = bm.getContent(*block);
    vector<Attribute> attributeVector;
    int recordSize = api->recordSizeGet(table);

    api->attributeGet(table, &attributeVector);
    char* blockBegin = bm.getContent(*block);
    size_t usingSize = bm.getUsingSize(*block);
    
    while (recordBegin - blockBegin  < usingSize){
        if(recordConditionFit(recordBegin, recordSize, &attributeVector, conditionVector)){
            count ++;
            recordPrint(recordBegin, recordSize, &attributeVector, attributeNameVector);
            printf("\n");
        }
        
        recordBegin += recordSize;
    }
    
    return count;
}
int RecordManager::recordAllFind(string table, vector<Condition>* conditionVector){
    fileTmp = bm.getFile(tableFileNameGet(table).c_str());
    blockTmp = bm.getBlockHead(fileTmp);
    int count = 0;
    while (true){
        if (blockTmp == NULL){
            return -1;
        }
        if (blockTmp->ifbottom){
            int recordBlockNum = recordBlockFind(table, conditionVector, blockTmp);
            count += recordBlockNum;
            return count;
        }
        else{
            int recordBlockNum = recordBlockFind(table, conditionVector, blockTmp);
            count += recordBlockNum;
            blockTmp = bm.getNextBlock(fileTmp, blockTmp);
        }
    }
    
    return -1;
}
int RecordManager::recordBlockFind(string table, vector<Condition>* conditionVector, blockNode* block){
    if (block == NULL){
        return -1;
    }
    int count = 0;
    
    char* recordBegin = bm.getContent(*block);
    char* contentBegin = bm.getContent(*block);
    vector<Attribute> attributeVector;
    int recordSize = api->recordSizeGet(table);
    int len = bm.getUsingSize(*block);

    api->attributeGet(table, &attributeVector);
    
    while (recordBegin - contentBegin  < len){
            if(recordConditionFit(recordBegin, recordSize, &attributeVector, conditionVector)){
            count++;
        }
        
        recordBegin += recordSize;
        
    }
    
    return count;
}
int RecordManager::recordAllDelete(string table, vector<Condition>* conditionVector){
    fileTmp = bm.getFile(tableFileNameGet(table).c_str());
    blockTmp = bm.getBlockHead(fileTmp);

    int count = 0;
    while (true){
        if (blockTmp == NULL){
            return -1;
        }
        if (blockTmp->ifbottom){
            int recordBlockNum = recordBlockDelete(table, conditionVector, blockTmp);
            count += recordBlockNum;
            return count;
        }
        else{
            int recordBlockNum = recordBlockDelete(table, conditionVector, blockTmp);
            count += recordBlockNum;
            blockTmp = bm.getNextBlock(fileTmp, blockTmp);
        }
    }
    
    return -1;
}
int RecordManager::recordBlockDelete(string table,  vector<Condition>* conditionVector, int blockOffset){
    fileTmp = bm.getFile(tableFileNameGet(table).c_str());
    blockNode* block = bm.getBlockByOffset(fileTmp, blockOffset);
    if (block == NULL){
        return -1;
    }
    else{
        return  recordBlockDelete(table, conditionVector, block);
    }
}

int RecordManager::recordBlockDelete(string table,  vector<Condition>* conditionVector, blockNode* block){
    if (block == NULL){
        return -1;
    }
    int count = 0;
    
    char* recordBegin = bm.getContent(*block);
    vector<Attribute> attributeVector;
    int recordSize = api->recordSizeGet(table);
    
    api->attributeGet(table, &attributeVector);
    
    while (recordBegin - bm.getContent(*block) < bm.getUsingSize(*block)){
            if(recordConditionFit(recordBegin, recordSize, &attributeVector, conditionVector)){
            count ++;
            
            api->recordIndexDelete(recordBegin, recordSize, &attributeVector, block->offsetNum);
            int i = 0;
            for (i = 0; i + recordSize + recordBegin - bm.getContent(*block) < bm.getUsingSize(*block); i++){
                recordBegin[i] = recordBegin[i + recordSize];
            }
            memset(recordBegin + i, 0, recordSize);
            bm.setUsingSize(*block, bm.getUsingSize(*block) - recordSize);
            bm.setDirty(*block);
        }
        else{
            recordBegin += recordSize;
        }
    }
    
    return count;
}

int RecordManager::indexRecordAllAlreadyInsert(string table,string indexName){
    fileTmp = bm.getFile(tableFileNameGet(table).c_str());
    blockTmp = bm.getBlockHead(fileTmp);
    int count = 0;
    while (true){
        if (blockTmp == NULL){
            return -1;
        }
        if (blockTmp->ifbottom){
            int recordBlockNum = indexRecordBlockAlreadyInsert(table, indexName, blockTmp);
            count += recordBlockNum;
            return count;
        }
        else{
            int recordBlockNum = indexRecordBlockAlreadyInsert(table, indexName, blockTmp);
            count += recordBlockNum;
            blockTmp = bm.getNextBlock(fileTmp, blockTmp);
        }
    }
    return -1;
}
int RecordManager::indexRecordBlockAlreadyInsert(string table,string indexName,  blockNode* block){
    if(block == NULL){
        return -1;
    }
    int count = 0;
    
    char* recordBegin = bm.getContent(*block);
    vector<Attribute> attributeVector;
    int recordSize = api->recordSizeGet(table);
    
    api->attributeGet(table, &attributeVector);
    
    int type;
    int typeSize;
    char * contentBegin;
    
    while (recordBegin - bm.getContent(*block)  < bm.getUsingSize(*block)){
        contentBegin = recordBegin;
        for (int i = 0; i < attributeVector.size(); i++){
            type = attributeVector[i].type;
            typeSize = api->typeSizeGet(type);
            
            if (attributeVector[i].index == indexName){
                api->indexInsert(indexName, contentBegin, type, block->offsetNum);
                count++;
            }
            
            contentBegin += typeSize;
        }
        recordBegin += recordSize;
    }
    
    return count;
}
bool RecordManager::recordConditionFit(char* recordBegin,int recordSize, vector<Attribute>* attributeVector,vector<Condition>* conditionVector){
    if (conditionVector == NULL) {
        return true;
    }
    int type;
    string attributeName;
    int typeSize;
    char content[255];
    
    char *contentBegin = recordBegin;
    for(int i = 0; i < attributeVector->size(); i++){
        type = (*attributeVector)[i].type;
        attributeName = (*attributeVector)[i].name;
        typeSize = api->typeSizeGet(type);
        
        memset(content, 0, 255);
        memcpy(content, contentBegin, typeSize);
        for(int j = 0,nn = (*conditionVector).size(); j < nn; j++){
            if ((*conditionVector)[j].attributeName == attributeName){
                if(!contentConditionFit(content, type, &(*conditionVector)[j])){
                    return false;
                }
            }
        }

        contentBegin += typeSize;
    }
    return true;
}
void RecordManager::recordPrint(char* recordBegin, int recordSize, vector<Attribute>* attributeVector, vector<string> *attributeNameVector){
    int type;
    string attributeName;
    int typeSize;
    char content[255];
    
    char *contentBegin = recordBegin;
    for(int i = 0; i < attributeVector->size(); i++){
        type = (*attributeVector)[i].type;
        typeSize = api->typeSizeGet(type);
        
        memset(content, 0, 255);
        
        memcpy(content, contentBegin, typeSize);

        for(int j = 0; j < (*attributeNameVector).size(); j++){
            if ((*attributeNameVector)[j] == (*attributeVector)[i].name){
                contentPrint(content, type);
                break;
            }
        }
        
        contentBegin += typeSize;
    }
}
void RecordManager::contentPrint(char * content, int type){
    if (type == Attribute::TYPE_INT){
        int tmp = *((int *) content);
        printf("%d ", tmp);
    }
    else if (type == Attribute::TYPE_FLOAT){
        float tmp = *((float *) content);
        printf("%f ", tmp);
    }
    else{
        string tmp = content;
        printf("%s ", tmp.c_str());
    }
}
bool RecordManager::contentConditionFit(char* content,int type,Condition* condition){
    if (type == Attribute::TYPE_INT){
        int tmp = *((int *) content);
        return condition->ifRight(tmp);
    }
    else if (type == Attribute::TYPE_FLOAT){
        float tmp = *((float *) content);
        return condition->ifRight(tmp);
    }
    else{
        return condition->ifRight(content);
    }
    return true;
}
string RecordManager::indexFileNameGet(string indexName){
    string tmp = "";
    return "INDEX_FILE_"+indexName;
}
string RecordManager::tableFileNameGet(string table){
    string tmp = "";
    return tmp + "TABLE_FILE_" + table;
}
