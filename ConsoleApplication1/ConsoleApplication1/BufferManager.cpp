#include "BufferManager.h"
#include "Minisql.h"
#include <stdlib.h>
#include <string>
#include <cstring>
#include <queue>

BufferManager::BufferManager(){
    totalBlock = 0;
    totalFile = 0;
    fileHead = NULL;
    for (int i = 0; i < MAX_FILE_NUM; i ++){
        filePool[i].fileName = (char*)malloc(MAX_FILE_NAME*sizeof(char));
        if(!filePool[i].fileName) errorPrint(1,NULL);
        
        initFile(filePool[i]);
    }
    for (int i = 0; i < MAX_BLOCK_NUM; i ++) {
        blockPool[i].content = (char*)malloc(BLOCK_SIZE*sizeof(char));
        if(!blockPool[i].content) errorPrint(1,NULL);
        blockPool[i].fileName = (char*)malloc(MAX_FILE_NAME*sizeof(char));
        if(!blockPool[i].fileName) errorPrint(1,NULL);
        initBlock(blockPool[i]);
    }
}

BufferManager::~BufferManager(){
    writtenBackToDiskAll();
    for (int i = 0; i < MAX_FILE_NUM; i ++){
        delete [] filePool[i].fileName;
    }
    for (int i = 0; i < MAX_BLOCK_NUM; i ++){
        delete [] blockPool[i].content;
    }
}
void BufferManager::initFile(fileNode &file){
    memset(file.fileName,0,MAX_FILE_NAME);
    file.nextFile = NULL;
    file.preFile = NULL;
    file.blockHead = NULL;
    file.pin = false;  
}
void BufferManager::initBlock(blockNode &block){
    size_t init_usage = 0;
    memset(block.content,0,BLOCK_SIZE);
    memcpy(block.content, (char*)&init_usage, sizeof(size_t));
    memset(block.fileName,0,MAX_FILE_NAME);
    block.using_size = sizeof(size_t);
    block.dirty = false;
    block.nextBlock = NULL;
    block.preBlock = NULL;
    block.offsetNum = -1;
    block.pin = false;
    block.reference = false;
    block.ifbottom = false;
    
}
fileNode* BufferManager::getFile(const char * fileName, bool if_pin){
    blockNode * btmp = NULL;
    fileNode * ftmp = NULL;
    if(fileHead != NULL)
        for(ftmp = fileHead;ftmp != NULL;ftmp = ftmp->nextFile)
            if(!strcmp(fileName, ftmp->fileName)){ftmp->pin = if_pin;return ftmp;}
    if(totalFile == 0){
        ftmp = &filePool[totalFile];
        totalFile ++;
        fileHead = ftmp;
    }
    else if(totalFile < MAX_FILE_NUM){
        ftmp = &filePool[totalFile];
        filePool[totalFile-1].nextFile = ftmp;
        ftmp->preFile = &filePool[totalFile-1];
        totalFile ++;
    }
    else{
        ftmp = fileHead;
        while(ftmp->pin){
            if(ftmp -> nextFile)ftmp = ftmp->nextFile;
            else errorPrint(2,NULL);
        }
        for(btmp = ftmp->blockHead;btmp != NULL;btmp = btmp->nextBlock){
            if(btmp->preBlock){
                initBlock(*(btmp->preBlock));
                totalBlock--;
            }
            writtenBackToDisk(btmp->fileName,btmp);
        }
        initFile(*ftmp);
    }
    if(strlen(fileName) + 1 > MAX_FILE_NAME)errorPrint(3,NULL);
    strncpy(ftmp->fileName, fileName,MAX_FILE_NAME);
    setPin(*ftmp, if_pin);
    return ftmp;
}

blockNode* BufferManager::getBlock(fileNode * file,blockNode *position, bool if_pin){
    const char * fileName = file->fileName;
    blockNode * btmp = NULL;
    if(totalBlock == 0){
        btmp = &blockPool[0];
        totalBlock ++;
    }
    else if(totalBlock < MAX_BLOCK_NUM){
        for(int i = 0 ;i < MAX_BLOCK_NUM;i ++){
            if(blockPool[i].offsetNum == -1){
                btmp = &blockPool[i];
                totalBlock ++;
                break;
            }
            else continue;
        }
    }
    else{
        int i = replacedBlock;
        while (true){
            i ++;
            if(i >= totalBlock) i = 0;
            if(!blockPool[i].pin){
                if(blockPool[i].reference == true)
                    blockPool[i].reference = false;
                else{
                    btmp = &blockPool[i];
                    if(btmp->nextBlock) btmp -> nextBlock -> preBlock = btmp -> preBlock;
                    if(btmp->preBlock) btmp -> preBlock -> nextBlock = btmp -> nextBlock;
                    if(file->blockHead == btmp) file->blockHead = btmp->nextBlock;
                    replacedBlock = i;
                    writtenBackToDisk(btmp->fileName, btmp);
                    initBlock(*btmp);
                    break;
                }
            }
            else continue;
        }
    }
    if(position != NULL && position->nextBlock == NULL){
        btmp -> preBlock = position;
        position -> nextBlock = btmp;
        btmp -> offsetNum = position -> offsetNum + 1;
    }
    else if (position !=NULL && position->nextBlock != NULL){
        btmp->preBlock=position;
        btmp->nextBlock=position->nextBlock;
        position->nextBlock->preBlock=btmp;
        position->nextBlock=btmp;
        btmp -> offsetNum = position -> offsetNum + 1;
    }
    else{
        btmp -> offsetNum = 0;
        if(file->blockHead){
            file->blockHead -> preBlock = btmp;
            btmp->nextBlock = file->blockHead;
        }
        file->blockHead = btmp;
    }
    setPin(*btmp, if_pin);
    if(strlen(fileName) + 1 > MAX_FILE_NAME)errorPrint(3,NULL);
    strncpy(btmp->fileName, fileName, MAX_FILE_NAME);
    
    //read the file content to the block
    FILE * fileHandle;
    if((fileHandle = fopen(fileName, "ab+")) != NULL){
        if(fseek(fileHandle, btmp->offsetNum*BLOCK_SIZE, 0) == 0){
            if(fread(btmp->content, 1, BLOCK_SIZE, fileHandle)==0)
                btmp->ifbottom = true;
            btmp ->using_size = getUsingSize(btmp);
        }
        else errorPrint(4,fileName);
        fclose(fileHandle);
    }
    else errorPrint(4,fileName);
    return btmp;
}
void BufferManager::writtenBackToDisk(const char* fileName,blockNode* block){
    if(!block->dirty) {
        return;
    }
    else{
        FILE * fileHandle = NULL;
        if((fileHandle = fopen(fileName, "rb+")) != NULL){
            if(fseek(fileHandle, block->offsetNum*BLOCK_SIZE, 0) == 0){
                if(fwrite(block->content, block->using_size+sizeof(size_t), 1, fileHandle) != 1) errorPrint(5,fileName);
            }
            else errorPrint(6,fileName);
            fclose(fileHandle);
        }
        else errorPrint(7,fileName);
    }
}
void BufferManager::writtenBackToDiskAll(){
    blockNode *btmp = NULL;
    fileNode *ftmp = NULL;
    if(fileHead){
        for(ftmp = fileHead;ftmp != NULL;ftmp = ftmp ->nextFile){
            if(ftmp->blockHead){
                for(btmp = ftmp->blockHead;btmp != NULL;btmp = btmp->nextBlock){
                    if(btmp->preBlock)initBlock(*(btmp -> preBlock));
                    writtenBackToDisk(btmp->fileName, btmp);
                }
            }
        }
    }
}
blockNode* BufferManager::getNextBlock(fileNode* file,blockNode* block){
    if(block->nextBlock == NULL){
        if(block->ifbottom) block->ifbottom = false;
        return getBlock(file, block);
    }
    else{
        if(block->offsetNum == block->nextBlock->offsetNum - 1){
            return block->nextBlock;
        }
        else {
            return getBlock(file, block);
        }
    }
}
blockNode* BufferManager::getBlockHead(fileNode* file){
    blockNode* btmp = NULL;
    if(file->blockHead != NULL){
        if(file->blockHead->offsetNum == 0){
            btmp = file->blockHead;
        }
        else{
            btmp = getBlock(file, NULL);
        }
    }
    else{
        btmp = getBlock(file,NULL);
    }
    return btmp;
}
blockNode* BufferManager::getBlockByOffset(fileNode* file, int offsetNumber){
    blockNode* btmp = NULL;
    if(offsetNumber == 0) return getBlockHead(file);
    else
    {
        btmp = getBlockHead(file);
        while( offsetNumber > 0){
            btmp = getNextBlock(file, btmp);
            offsetNumber --;
        }
        return btmp;
    }
}
void BufferManager::deleteFileNode(const char * fileName){
    fileNode* ftmp = getFile(fileName);
    blockNode* btmp = getBlockHead(ftmp);
    queue<blockNode*> blockQ;
    while (true) {
        if(btmp == NULL) return;
        blockQ.push(btmp);
        if(btmp->ifbottom) break;
        btmp = getNextBlock(ftmp,btmp);
    }
    totalBlock -= blockQ.size();
    while(!blockQ.empty()){
        initBlock(*blockQ.back());
        blockQ.pop();
    }
    if(ftmp->preFile) ftmp->preFile->nextFile = ftmp->nextFile;
    if(ftmp->nextFile) ftmp->nextFile->preFile = ftmp->preFile;
    if(fileHead == ftmp) fileHead = ftmp->nextFile;
    initFile(*ftmp);
    totalFile--;
}


void BufferManager::setPin(blockNode &block,bool pin){
    block.pin = pin;
    if(!pin)
        block.reference = true;
}

void BufferManager::setPin(fileNode &file,bool pin){
    file.pin = pin;
}

void BufferManager::setDirty(blockNode &block){
    block.dirty = true;
}

void BufferManager::clean_dirty(blockNode &block){
    block.dirty = false;
}


size_t BufferManager::getUsingSize(blockNode* block){
    return *(size_t*)block->content;
}

void BufferManager::setUsingSize(blockNode & block,size_t usage){
    block.using_size = usage;
    memcpy(block.content,(char*)&usage,sizeof(size_t));
}

size_t BufferManager::getUsingSize(blockNode & block){
    return block.using_size;
}

char* BufferManager::getContent(blockNode& block){
    char* tmp = block.content;
    return tmp + sizeof(size_t);
}

void BufferManager::errorPrint(int type,const char* fileName){
    switch(type){
        case 1:
            printf("Can not allocate memory\n");
            exit (1);
        break;
        case 2:
            printf("There are no enough file node in the pool!");
            exit(2);
        break;
        case 3:
            printf("File name too long, enter a name no more than %d\n",MAX_FILE_NAME);
            exit(3);
        break;
        case 4:
            printf("Problem seeking the file %s in reading",fileName);
            exit(1);
        break;
        case 5:
            printf("Problem writing the file %s in writtenBackToDisking",fileName);
            exit(1);
        break;
        case 6: 
            printf("Problem seeking the file %s in writtenBackToDisking",fileName);
            exit(1);
        break;
        case 7:
            printf("Problem opening the file %s in writtenBackToDisking",fileName);
            exit(1);
        break;
        default:
        break;
    }
}



