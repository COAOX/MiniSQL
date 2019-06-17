#ifndef __Minisql__BufferManager__
#define __Minisql__BufferManager__
#include "Minisql.h"
#include <stdio.h>

static int replacedBlock = -1;


class BufferManager
{
    private:
        fileNode *fileHead;
        fileNode filePool[MAX_FILE_NUM];
        blockNode blockPool[MAX_BLOCK_NUM];
        int totalBlock;
        int totalFile;
        void initBlock(blockNode & block);
        void initFile(fileNode & file);
        blockNode* getBlock(fileNode * file,blockNode* position,bool if_pin = false);
        void writtenBackToDiskAll();
        void writtenBackToDisk(const char* fileName,blockNode* block);
        void clean_dirty(blockNode &block);
        size_t getUsingSize(blockNode* block);
        static const int BLOCK_SIZE = 4096;

    public:
        BufferManager();
        ~BufferManager();
        void deleteFileNode(const char * fileName);
        fileNode* getFile(const char* fileName,bool if_pin = false);
        void setDirty(blockNode & block);
        void setPin(blockNode & block,bool pin);
        void setPin(fileNode & file,bool pin);
        void setUsingSize(blockNode & block,size_t usage);
        void errorPrint(int type,const char* fileName);
        size_t getUsingSize(blockNode & block);
        char* getContent(blockNode& block);
        static int getBlockSize()
        {
            return BLOCK_SIZE - sizeof(size_t);
        }

    
        blockNode* getNextBlock(fileNode * file,blockNode* block);
        blockNode* getBlockHead(fileNode* file);
        blockNode* getBlockByOffset(fileNode* file, int offestNumber);

};

#endif 


