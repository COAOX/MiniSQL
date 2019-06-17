#include "CatalogManager.h"
#include "BufferManager.h"
#include "IndexInfo.h"
#include <iostream>
#include <vector>
#include <string>
#include <cstring>
#include <sstream>
#include "Attribute.h"
#define UNKNOWN_FILE 8
#define TABLE_FILE 9
#define INDEX_FILE 10

CatalogManager::CatalogManager() {
    fileTmp = NULL;
    blockTmp = NULL;
}
CatalogManager::~CatalogManager() {}

int CatalogManager::dropTable(string tableName){
    bm.deleteFileNode(tableName.c_str());
    if (remove(tableName.c_str())){
        return 0;
    }
    return 1;
}
int CatalogManager::getIndexType(string indexName){
    fileTmp = bm.getFile("Indexs");
    blockTmp = bm.getBlockHead(fileTmp);
    if (blockTmp ){
        char* addressBegin;
        addressBegin = bm.getContent(*blockTmp);
        IndexInfo * i = (IndexInfo *)malloc(sizeof(IndexInfo));
		
        int nn;
        for(int j = 0,nn = (bm.getUsingSize(*blockTmp)/sizeof(IndexInfo)) ;j<nn;j++){
			memcpy(i, addressBegin, sizeof(IndexInfo));
            if(i->indexName==indexName){
				int res = i->type;
				free(i);
                return res;
            }
			addressBegin += sizeof(IndexInfo);
        }
		free(i);
        return -2;
    }

    return -2;
}

int CatalogManager::getAllIndex(vector<IndexInfo> * indexNames){
    fileTmp = bm.getFile("Indexs");
    blockTmp = bm.getBlockHead(fileTmp);
    if (blockTmp ){
        char* addressBegin;
        addressBegin = bm.getContent(*blockTmp);
        IndexInfo * i = (IndexInfo *)addressBegin;
        int nn;
        for(int j = 0,nn = (bm.getUsingSize(*blockTmp)/sizeof(IndexInfo)) ;j<nn;j++){
            indexNames->push_back((*i));
            i ++;
        }
    }

    return 1;
}
int CatalogManager::addIndex(string indexName,string tableName,string Attribute,int type,char * addressBegin){
    fileTmp = bm.getFile("Indexs");
    blockTmp = bm.getBlockHead(fileTmp);
    IndexInfo i(indexName,tableName,Attribute,type);
	int res = 0;
    while (true){
        if (blockTmp == NULL){
            return 0;
        }
        if (bm.getUsingSize(*blockTmp) <= bm.getBlockSize() - sizeof(IndexInfo)){
            addressBegin = bm.getContent(*blockTmp) + bm.getUsingSize(*blockTmp);
            memcpy(addressBegin, &i, sizeof(IndexInfo));
            bm.setUsingSize(*blockTmp, bm.getUsingSize(*blockTmp) + sizeof(IndexInfo));
            bm.setDirty(*blockTmp);


            res =  this->setIndexOnAttribute(tableName,Attribute,indexName);

			break;
        }
        else{
            blockTmp = bm.getNextBlock(fileTmp, blockTmp);
        }
    }

    return res;
}
int CatalogManager::findTable(string tableName){
    FILE *fp;
    fp = fopen(tableName.c_str(), "r");
    if (fp == NULL){
        return 0;
    }
    else{
        fclose(fp);
        return TABLE_FILE;
    }

}
int CatalogManager::findIndex(string fileName){
    fileTmp = bm.getFile("Indexs");
    blockTmp = bm.getBlockHead(fileTmp);
    if (blockTmp ){
        char* addressBegin;
        addressBegin = bm.getContent(*blockTmp);
        IndexInfo * i = (IndexInfo *)addressBegin;
        int flag = UNKNOWN_FILE;
        int nn;
        for(int j = 0,nn = (bm.getUsingSize(*blockTmp)/sizeof(IndexInfo)) ;j<nn;j++){
            if((*i).indexName==fileName){
                flag = INDEX_FILE;
                break;
            }
            i ++;
        }
        return flag;
    }

    return 0;
}
int CatalogManager::dropIndex(string indexName){
    fileTmp = bm.getFile("Indexs");
    blockTmp = bm.getBlockHead(fileTmp);
    if (blockTmp){
        char* addressBegin;
        addressBegin = bm.getContent(*blockTmp);
        IndexInfo * i = (IndexInfo *)addressBegin;
        int j = 0;
        int nn;
        for(j = 0,nn =(bm.getUsingSize(*blockTmp)/sizeof(IndexInfo)) ;j<nn;j++){
            if((*i).indexName==indexName){
                break;
            }
            i ++;
        }
        this->revokeIndexOnAttribute((*i).tableName,(*i).Attribute,(*i).indexName);
        for(;j<(bm.getUsingSize(*blockTmp)/sizeof(IndexInfo)-1);j++){
            (*i) = *(i + sizeof(IndexInfo));
            i ++;
        }
        bm.setUsingSize(*blockTmp, bm.getUsingSize(*blockTmp) - sizeof(IndexInfo));
        bm.setDirty(*blockTmp);

        return 1;
    }

    return 0;
}
int CatalogManager::revokeIndexOnAttribute(string tableName,string indexName,string AttributeName){
    fileTmp = bm.getFile(tableName.c_str());
    blockTmp = bm.getBlockHead(fileTmp);

    if (blockTmp){
        char* addressBegin = bm.getContent(*blockTmp) ;
        addressBegin += (1+sizeof(int));
        int size = *addressBegin;
        addressBegin++;
        Attribute *a = (Attribute *)addressBegin;
        int i;
        for(i =0;i<size;i++){
            if(a->name == AttributeName){
                if(a->index == indexName){
                    a->index = "";
                    bm.setDirty(*blockTmp);
                }
                else{
                    cout<<"revoke unknown indexName: "<<indexName<<" on "<<tableName<<"!"<<endl;
                    cout<<"Attribute: "<<AttributeName<<" on table "<<tableName<<" has indexName: "<<a->index<<"!"<<endl;
                }
                break;
            }
            a ++;
        }
        if(i<size)
            return 1;
        else
            return 0;
    }
    return 0;
}
int CatalogManager::indexNameListGet(string tableName, vector<string>* indexNameVector){
    fileTmp = bm.getFile("Indexs");
    blockTmp = bm.getBlockHead(fileTmp);
    if (blockTmp ){
        char* addressBegin;
        addressBegin = bm.getContent(*blockTmp);
        IndexInfo * i = (IndexInfo *)addressBegin;
        int nn;
        for(int j = 0,nn = (bm.getUsingSize(*blockTmp)/sizeof(IndexInfo)) ;j<nn;j++){
            if((*i).tableName==tableName){
                (*indexNameVector).push_back((*i).indexName);
            }
            i ++;
        }
        return 1;
    }

    return 0;
}

int CatalogManager::deleteValue(string tableName, int deleteNum){
    fileTmp = bm.getFile(tableName.c_str());
    blockTmp = bm.getBlockHead(fileTmp);

    if (blockTmp){

        char* addressBegin = bm.getContent(*blockTmp) ;
        int * recordNum = (int*)addressBegin;
        if((*recordNum) <deleteNum){
            cout<<"error in CatalogManager::deleteValue"<<endl;
            return 0;
        }
        else
            (*recordNum) -= deleteNum;

        bm.setDirty(*blockTmp);
        return *recordNum;
    }
    return 0;
}
int CatalogManager::insertRecord(string tableName, int recordNum){
    fileTmp = bm.getFile(tableName.c_str());
    blockTmp = bm.getBlockHead(fileTmp);

    if (blockTmp){

        char* addressBegin = bm.getContent(*blockTmp) ;
        int * originalRecordNum = (int*)addressBegin;
        *originalRecordNum += recordNum;
        bm.setDirty(*blockTmp);
        return *originalRecordNum;
    }
    return 0;
}

int CatalogManager::getRecordNum(string tableName){
    fileTmp = bm.getFile(tableName.c_str());
    blockTmp = bm.getBlockHead(fileTmp);

    if (blockTmp){
        char* addressBegin = bm.getContent(*blockTmp) ;
        int * recordNum = (int*)addressBegin;
        return *recordNum;
    }
    return 0;
}

int CatalogManager::addTable(string tableName, vector<Attribute>* attributeVector, string primaryKeyName = "",int primaryKeyLocation = 0){
    FILE *fp;
    fp = fopen(tableName.c_str(), "w+");
    if (fp == NULL){
        return 0;
    }
    fclose(fp);
    fileTmp = bm.getFile(tableName.c_str());
    blockTmp = bm.getBlockHead(fileTmp);

    if (blockTmp ){
        char* addressBegin = bm.getContent(*blockTmp) ;
        int * size = (int*)addressBegin;
        *size = 0;
        addressBegin += sizeof(int);
        *addressBegin = primaryKeyLocation;
        addressBegin++;
        *addressBegin = (*attributeVector).size();
        addressBegin++;
        int nn;
        for(int i= 0,nn = (*attributeVector).size();i<nn;i++){
            memcpy(addressBegin, &((*attributeVector)[i]), sizeof(Attribute));
            addressBegin += sizeof(Attribute);
        }
        bm.setUsingSize(*blockTmp, bm.getUsingSize(*blockTmp) + (*attributeVector).size()*sizeof(Attribute)+2+sizeof(int));
        bm.setDirty(*blockTmp);
        return 1;
    }
    return 0;
}
int CatalogManager::setIndexOnAttribute(string tableName,string AttributeName,string indexName){
    fileTmp = bm.getFile(tableName.c_str());
    blockTmp = bm.getBlockHead(fileTmp);

    if (blockTmp){
        char* addressBegin = bm.getContent(*blockTmp) ;
        addressBegin += 1+sizeof(int);
        int size = *addressBegin;
        addressBegin++;
        Attribute *a = (Attribute *)addressBegin;
        int i;
        for(i =0;i<size;i++){
            if(a->name == AttributeName){
                a->index = indexName;
                bm.setDirty(*blockTmp);
                break;
            }
            a ++;
        }
        if(i<size)
            return 1;
        else
            return 0;
    }
    return 0;
}
int CatalogManager::attributeGet(string tableName, vector<Attribute>* attributeVector){
    fileTmp = bm.getFile(tableName.c_str());
    blockTmp = bm.getBlockHead(fileTmp);
    if (blockTmp){
        char* addressBegin = bm.getContent(*blockTmp) ;
        addressBegin += 1+sizeof(int);
        int size = *addressBegin;
        addressBegin++;
        Attribute *a = (Attribute *)malloc(sizeof(Attribute));
        
        for(int i =0;i<size;i++){
			memcpy(a, addressBegin, sizeof(Attribute));
            attributeVector->push_back(*a);
            addressBegin+=sizeof(Attribute);
        }

        return 1;
    }
    return 0;
}

int CatalogManager::calcuteLenth(string tableName){
    fileTmp = bm.getFile(tableName.c_str());
    blockTmp = bm.getBlockHead(fileTmp);
    if (blockTmp){
        int singleRecordSize =  0;
        char* addressBegin = bm.getContent(*blockTmp) ;
        addressBegin += 1+sizeof(int);
        int size = *addressBegin;
        addressBegin++;
        Attribute *a = (Attribute *)addressBegin;
        for(int i =0;i<size;i++){
            if((*a).type==-1){
                singleRecordSize += sizeof(float);
            }
            else if((*a).type == 0){
                singleRecordSize += sizeof(int);
            }
            else if((*a).type>0){
                singleRecordSize += (*a).type * sizeof(char);
            }
            else{
                cout<<"Catalog data damaged!"<<endl;
                return 0;
            }
            a ++;
        }

        return singleRecordSize;
    }
    return 0;
}

int CatalogManager::calcuteLenth2(int type){
    if (type == Attribute::TYPE_INT) {
        return sizeof(int);
    }
    else if (type == Attribute::TYPE_FLOAT){
        return sizeof(float);
    }
    else{
        return (int)(type* sizeof(char));
    }
}

void CatalogManager::recordStringGet(string tableName, vector<string>* recordContent, char* recordResult, vector<Attribute>* attributeVector){
    char * contentBegin = recordResult;
    for(int i = 0; i < (*attributeVector).size(); i++){
        Attribute attribute = (*attributeVector)[i];
        string content = (*recordContent)[i];
        int type = attribute.type;
        int typeSize = calcuteLenth2(type);
        stringstream ss;
        ss << content;
        if (type == Attribute::TYPE_INT){
            int intTmp;
            ss >> intTmp;
            memcpy(contentBegin, ((char*)&intTmp), typeSize);
        }
        else if (type == Attribute::TYPE_FLOAT){
            float floatTmp;
            ss >> floatTmp;
            memcpy(contentBegin, ((char*)&floatTmp), typeSize);
        }
        else{
            memcpy(contentBegin, content.c_str(), typeSize);
        }

        contentBegin += typeSize;
    }
    return ;
}
void CatalogManager::recordStringGet(string tableName, vector<string>* recordContent, char* recordResult){
    vector<Attribute> attributeVector;
    attributeGet(tableName, &attributeVector);
    char * contentBegin = recordResult;

    for(int i = 0; i < attributeVector.size(); i++){
        Attribute attribute = attributeVector[i];
        string content = (*recordContent)[i];
        int type = attribute.type;
        int typeSize = calcuteLenth2(type);
        stringstream ss;
        ss << content;
        if (type == Attribute::TYPE_INT){
            int intTmp;
            ss >> intTmp;
            memcpy(contentBegin, ((char*)&intTmp), typeSize);
        }
        else if (type == Attribute::TYPE_FLOAT){
            float floatTmp;
            ss >> floatTmp;
            memcpy(contentBegin, ((char*)&floatTmp), typeSize);
        }
        else{
            memcpy(contentBegin, content.c_str(), typeSize);
        }

        contentBegin += typeSize;
    }
    return ;
}
