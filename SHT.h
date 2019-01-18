//
// Created by carolosjk on 18/1/2019.
//

#ifndef SHT_H
#define SHT_H

#include "HT.h"

typedef struct{
    int fileDesc;
    char* attrName;
    int attrLength;
    long int numBuckets;
    char* fileName;
}SHT_info;

typedef struct{
    Record record;
    int blockId;
}SecondaryRecord;

typedef struct{     //A cut down version of SecondaryRecord that needs less space at the disk.
    int id;
    char name[15];
    int blockId;
}MinimalSecondaryRecord;

typedef struct{
    int counter;        //Number of records in the block
    MinimalSecondaryRecord record[MAX_RECORDS];       //An array of records
    int nextBlock;      //"Pointer" to the next block(using the id)
}SecondaryBlock;

typedef struct{     //The first block in the file.
    char* attrName;
    int attrLength;
    long int numBuckets;
    char* fileName;
    int hash_table[BUCKETS];
} SecondaryBlock0;

int createNewSecondaryBlock(const int fileDesc){     //Returns the new block id or -1 upon failure

    SecondaryBlock newBlock;
    void* blockData = malloc(BLOCK_SIZE);
    newBlock.counter = 0;
    newBlock.nextBlock = -1;

    if (BF_AllocateBlock(fileDesc) < 0){
        BF_PrintError("Error with BF_AllocateBlock\n");
        return -1;
    }
    int number = BF_GetBlockCounter(fileDesc);
    if (number < 0 ){
        BF_PrintError("Error with BF_GetBlockCounter\n");
        return -1;
    }
    if (BF_ReadBlock(fileDesc,number-1, &blockData) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return -1;
    }
    memcpy(blockData, &newBlock, sizeof(SecondaryBlock));
    if(BF_WriteBlock(fileDesc,number-1) < 0){

        BF_PrintError("Error with write\n");
        return -1;
    }

    return  number - 1;
}


int SHT_CreateSecondaryIndex(char* sfileName, char* attrName, int attrLength, int buckets, char* fileName){

    int number;
    number = BF_CreateFile(sfileName);
    if (number < 0) {
        BF_PrintError("Error with BF_CreateFile\n");
        return -1;
    }
    int fileDesc = BF_OpenFile(sfileName);
    if (fileDesc < 0) {
        BF_PrintError("Error with BF_OpenFile\n");
        return -1;
    }


    SecondaryBlock0 block0;


    block0.attrLength = attrLength;
    block0.attrName =  malloc(strlen(attrName)+1);
    strcpy(block0.attrName,attrName);
    block0.numBuckets = buckets;
    block0.fileName =  malloc(strlen(fileName)+1);
    strcpy(block0.fileName,fileName);

    if (BF_AllocateBlock(fileDesc) < 0){
        BF_PrintError("Error with BF_AllocateBlock\n");
        return -1;
    }

    void* blockData = malloc(BLOCK_SIZE);


    if (BF_ReadBlock(fileDesc,0, &blockData) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return -1;
    }

    int nameSize = strlen(block0.attrName)+1;  // attrName size
    memcpy(blockData , block0.attrName, nameSize);
    memcpy(blockData + nameSize, &(block0.attrLength), sizeof(int));
    memcpy(blockData + sizeof(int) + nameSize, &(block0.numBuckets), sizeof(long int));
    int fileNameSize = strlen(block0.fileName)+1;  // fileName size
    memcpy(blockData + sizeof(int) + nameSize + sizeof(long int), fileName , fileNameSize);

    for (int i =0; i<block0.numBuckets; i++) block0.hash_table[i] = -1;
    memcpy(blockData + sizeof(int) + sizeof(long int) + nameSize + fileNameSize,
            &(block0.hash_table), sizeof(block0.hash_table));

    if(BF_WriteBlock(fileDesc, 0) < 0){

        BF_PrintError("Error with write\n");
        return -1;
    }

    //Now we must synchronise the secondary index with the primary index.



    if(BF_CloseFile(fileDesc) < 0){

        BF_PrintError("Error with closing file\n");
        return -1;
    }
    return 0;

}

SHT_info* SHT_OpenSecondaryIndex(char* sfileName){

    int fileDesc = BF_OpenFile(sfileName);
    if (fileDesc < 0) {
        BF_PrintError("Error with BF_OpenFile\n");
        return NULL;
    }


    SHT_info* info = malloc(sizeof(SHT_info));
    void* blockData = malloc(BLOCK_SIZE);


    if (BF_ReadBlock(fileDesc,0,&blockData) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return NULL;
    }
    info->fileDesc = fileDesc;
    int nameSize = strlen((char*)blockData) + 1; // Finding the size of attrName
    info->attrName = (char*) malloc(nameSize);
    memcpy(info->attrName ,  blockData , nameSize);
    memcpy(&(info->attrLength) ,  blockData + nameSize , sizeof(int));
    memcpy(&(info->numBuckets) ,  blockData + sizeof(int) + nameSize, sizeof(long int));
    int fileNameSize = strlen((char*)blockData + sizeof(int) + nameSize + sizeof(long int)) + 1; // Finding the size of fileName
    info->fileName = (char*) malloc(fileNameSize);
    memcpy(info->fileName ,  blockData + sizeof(int) + nameSize + sizeof(long int), fileNameSize);

//    if(BF_CloseFile(fileDesc) < 0){
//
//        BF_PrintError("Error with closing file\n");
//        return NULL;
//    }

    return info;
};

int SHT_CloseSecondaryIndex(SHT_info* header_info){

    if(BF_CloseFile(header_info->fileDesc) < 0){
        BF_PrintError("Error with closing file\n");
        return -1;
    }
    free(header_info);
    return 0;

};

int SHT_SecondaryInsertEntry(SHT_info header_info, SecondaryRecord record){

    int bucket = hash_char(record.record.name,header_info.numBuckets);    // The bucket in which the record must be inserted
    int hash_table[header_info.numBuckets];

    void* blockData0 = malloc(BLOCK_SIZE);

    if (BF_ReadBlock(header_info.fileDesc,0, &blockData0) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return -1;
    }
    int nameSize = strlen(header_info.attrName) + 1;
    int fileNameSize = strlen(header_info.fileName) + 1;
    memcpy(&hash_table[0] ,  blockData0 + sizeof(int) + fileNameSize + nameSize + sizeof(long int),sizeof(hash_table));

    //If the bucket that we must insert the record in, has no blocks, then we allocate a new block
    //and put the id of the block to hash_table[bucket]
    if(hash_table[bucket] == -1){

        int newBlockId = createNewSecondaryBlock(header_info.fileDesc);
//        printf("%d\n",newBlockId);
        if (newBlockId < 0 ){
            BF_PrintError("Error with BF_GetBlockCounter\n");
            return -1;
        }
        hash_table[bucket] = newBlockId;
        memcpy(blockData0 + fileNameSize + sizeof(int) + sizeof(long int) + nameSize,&hash_table[0],
               sizeof(hash_table));
        if(BF_WriteBlock(header_info.fileDesc, 0) < 0){

            BF_PrintError("Error with write\n");
            return -1;
        }
    }

    SecondaryBlock block;
    void* blockData = malloc(BLOCK_SIZE);

    //Reading the first block of the bucket.
    if (BF_ReadBlock(header_info.fileDesc,hash_table[bucket], &blockData) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return -1;
    }
    int blockId = hash_table[bucket];
    memcpy(&block, blockData, sizeof(SecondaryBlock));

    while(block.counter == MAX_RECORDS){    //Block is full of Records, we move to the next one.
        if(block.nextBlock == -1){  //There is no next block
            int newBlockId = createNewSecondaryBlock(header_info.fileDesc);  //Creating new block
//            printf("%d\n",newBlockId);
            if (newBlockId < 0 ){
                BF_PrintError("Error with BF_GetBlockCounter\n");
                return -1;
            }
            block.nextBlock = newBlockId;     // Setting the nextBlock pointer to the id of the new block created
            memcpy(blockData, &block, sizeof(SecondaryBlock));       //Turning the current block to byteArray

            if(BF_WriteBlock(header_info.fileDesc, blockId) < 0){     //Writing the current block back to the file

                BF_PrintError("Error with write\n");
                return -1;
            }

            if (BF_ReadBlock(header_info.fileDesc,newBlockId, &blockData) < 0){     //Reading the newly created block
                BF_PrintError("Error with BF_ReadBlock\n");
                return -1;
            }
            memcpy(&block, blockData, sizeof(SecondaryBlock));

        }else{   // block.nextBlock != -1
            blockId = block.nextBlock;
            if (BF_ReadBlock(header_info.fileDesc,block.nextBlock, &blockData) < 0){
                BF_PrintError("Error with BF_ReadBlock\n");
                return -1;
            }
            memcpy(&block, blockData, sizeof(SecondaryBlock));
        }

    }       //while(block.counter == MAX_RECORDS)...end

    //Putting the into we need into a MinimalSecondaryRecord
    MinimalSecondaryRecord minimalRecord;
    minimalRecord.id = record.record.id;
    strcpy(minimalRecord.name ,record.record.name);
    minimalRecord.blockId = record.blockId;


    block.record[block.counter] = minimalRecord;       // Inserting the new record to the block
    block.counter++;

    memcpy(blockData, &block, sizeof(SecondaryBlock));       //Turning the block to byteArray

    if(BF_WriteBlock(header_info.fileDesc, blockId) < 0){     //Writing the block back to the file

        BF_PrintError("Error with write\n");
        return -1;
    }
//    printf("%d\n",blockId);
    return 0;
};

int SHT_SecondaryGetAllEntries(SHT_info header_info_sht, HT_info header_info_ht, void* value){

    char* key = (char*) value;
    int bucket = hash_char(key,header_info_sht.numBuckets);    // The bucket in which the record must be inserted
    int hash_table[header_info_sht.numBuckets];

    void* blockData = malloc(BLOCK_SIZE);

    if (BF_ReadBlock(header_info_sht.fileDesc,0, &blockData) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return -1;
    }
    int nameSize = strlen(header_info_sht.attrName) + 1;
    int fileNameSize = strlen(header_info_sht.fileName) + 1;
    memcpy(&hash_table[0] ,  blockData + sizeof(int) + fileNameSize + nameSize + sizeof(long int),sizeof(hash_table));

    SecondaryBlock block;

    if (hash_table[bucket] == -1) return -1; // No blocks in the bucket

    int nextBlock = hash_table[bucket];
    int blocksRead = 0;
    int foundAtLeastOneRecord = -1;     //A value to check if at least one record with the correct key was found
    while(nextBlock != -1) {
        //Reading a block
        if (BF_ReadBlock(header_info_sht.fileDesc, nextBlock, &blockData) < 0) {
            BF_PrintError("Error with BF_ReadBlock\n");
            return -1;
        }
        blocksRead++;   //Increasing the blocksRead counter
        memcpy(&block, blockData, sizeof(SecondaryBlock));

        //Searching for a record with the correct key.
        for (int i = 0; i < block.counter; i++) {
            if (strcmp(block.record[i].name, key) == 0) {      //Found a record with the correct key value.
                Record record;
                record = HT_GetRecordFromKey(header_info_ht.fileDesc,block.record[i].blockId,(void*)&block.record[i].id);
                if(record.id != DELETED_RECORD_ID) {
                    printRecordInfo(record);
                    foundAtLeastOneRecord = 0;
                }
            }
        }
        nextBlock = block.nextBlock;
    }
    if(foundAtLeastOneRecord == 0) {
        return blocksRead;
    }
    else return -1;

};




#endif //SHT_H
