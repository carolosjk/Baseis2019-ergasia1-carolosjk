//
// Created by karolos on 13/1/2019.
//



#ifndef BF_HT_H_
#define BF_HT_H_

#define MAX_RECORDS 6
#define BUCKETS 10
#define INITIAL_VALUE 5381
#define DELETED_RECORD_ID -99999

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <limits.h>
#include "BF.h"


int hash_int(int x, long int buckets){
    return ((3*x + 14544) % 78901) % buckets;
}

int hash_char(char* x, long int buckets){
    int h = INITIAL_VALUE;
    int length = strlen(x);
    for(int i = 0; i < length; i++){
        h = ((h*33) + x[i]) % buckets;
    }
    return h;
}

typedef struct{
    int id;
    char name[15];
    char surname[20];
    char address[40];
}Record;

//void printRecordInfo(Record record){
//    printf("Record:\n");
//    printf("Id: %d\n", record.id);
//    printf("Name: %s\n", record.name);
//    printf("Surname: %s\n",record.surname);
//    printf("Address: %s\n", record.address);
//}

void printRecordInfo(Record record){
    printf("Record: (%d, %s, %s, %s)\n",record.id,record.name,record.surname,record.address);
}

typedef struct{
    int counter;        //Number of records in the block
    Record record[MAX_RECORDS];       //An array of records
    int nextBlock;      //"Pointer" to the next block(using the id)
}Block;


typedef struct{
    int fileDesc;
    char attrType;
    char* attrName;
    int attrLength;
    long int numBuckets;
} HT_info;

typedef struct{     //The first block in the file.
    char attrType;
    char* attrName;
    int attrLength;
    long int numBuckets;
    int hash_table[BUCKETS];
} Block0;


int createNewBlock(const int fileDesc){     //Returns the new block id or -1 upon failure

    Block newBlock;
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
    memcpy(blockData, &newBlock, sizeof(Block));
    if(BF_WriteBlock(fileDesc,number-1) < 0){

        BF_PrintError("Error with write\n");
        return -1;
    }

    return  number - 1;
}


int HT_CreateIndex(char* fileName, char attrType, char* attrName, int attrLength, int buckets){

    int number;
    number = BF_CreateFile(fileName);
    if (number < 0) {
        BF_PrintError("Error with BF_CreateFile\n");
        return -1;
    }
    int fileDesc = BF_OpenFile(fileName);
    if (fileDesc < 0) {
        BF_PrintError("Error with BF_OpenFile\n");
        return -1;
    }


    Block0 block0;


    block0.attrLength = attrLength;
    block0.attrName =  malloc(strlen(attrName)+1);
    strcpy(block0.attrName,attrName);
    block0.attrType = attrType;
    block0.numBuckets = buckets;

    if (BF_AllocateBlock(fileDesc) < 0){
        BF_PrintError("Error with BF_AllocateBlock\n");
        return -1;
    }

    void* blockData = malloc(BLOCK_SIZE);


    if (BF_ReadBlock(fileDesc,0, &blockData) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return -1;
    }

    memcpy(blockData, &(block0.attrType), sizeof(char));
    int nameSize = strlen(block0.attrName)+1;  // attrName size
    printf("%d\n",nameSize);
    memcpy(blockData + sizeof(char), block0.attrName, nameSize);
    memcpy(blockData + sizeof(char) + nameSize, &(block0.attrLength), sizeof(int));
    memcpy(blockData + sizeof(char) + sizeof(int) + nameSize, &(block0.numBuckets), sizeof(long int));

    for (int i =0; i<block0.numBuckets; i++) block0.hash_table[i] = -1;
    memcpy(blockData + sizeof(char) + sizeof(int) + sizeof(long int) + nameSize,&(block0.hash_table),
            sizeof(block0.hash_table));

    if(BF_WriteBlock(fileDesc, 0) < 0){

        BF_PrintError("Error with write\n");
        return -1;
    }

    if(BF_CloseFile(fileDesc) < 0){

        BF_PrintError("Error with closing file\n");
        return -1;
    }
    return 0;
}

HT_info* HT_OpenIndex(char* fileName){

    int fileDesc = BF_OpenFile(fileName);
    if (fileDesc < 0) {
        BF_PrintError("Error with BF_OpenFile\n");
        return NULL;
    }


    HT_info* info = malloc(sizeof(HT_info));
    void* blockData = malloc(BLOCK_SIZE);


    if (BF_ReadBlock(fileDesc,0,&blockData) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return NULL;
    }
    info->fileDesc = fileDesc;
    memcpy(&(info->attrType) , blockData , sizeof(char));
    int nameSize = strlen((char*)blockData + sizeof(char)) + 1; // Finding the size of attrName
    info->attrName = (char*) malloc(nameSize);
    memcpy(info->attrName ,  blockData + sizeof(char) , nameSize);
    memcpy(&(info->attrLength) ,  blockData + sizeof(char) + nameSize , sizeof(int));
    memcpy(&(info->numBuckets) ,  blockData + sizeof(int) + sizeof(char) + nameSize, sizeof(long int));

//    if(BF_CloseFile(fileDesc) < 0){
//
//        BF_PrintError("Error with closing file\n");
//        return NULL;
//    }

    return info;
};

int HT_CloseIndex(HT_info* header_info){

    if(BF_CloseFile(header_info->fileDesc) < 0){
        BF_PrintError("Error with closing file\n");
        return -1;
    }
    free(header_info);
    return 0;

};

int HT_InsertEntry(HT_info header_info, Record record){

    int bucket = hash_int(record.id,header_info.numBuckets);    // The bucket in which the record must be inserted
//    printf("%d\n",bucket);
    int blockId;        // The id of the block in which the record was inserted
    int hash_table[header_info.numBuckets];

    void* blockData0 = malloc(BLOCK_SIZE);

    if (BF_ReadBlock(header_info.fileDesc,0, &blockData0) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return -1;
    }
    int nameSize = strlen(header_info.attrName) + 1;
    memcpy(&hash_table[0] ,  blockData0 + sizeof(int) + sizeof(char) + nameSize + sizeof(long int),sizeof(hash_table));

    //If the bucket that we must insert the record in, has no blocks, then we allocate a new block
    //and put the id of the block to hash_table[bucket]
    if(hash_table[bucket] == -1){

        int newBlockId = createNewBlock(header_info.fileDesc);
//        printf("%d\n",newBlockId);
        if (newBlockId < 0 ){
            BF_PrintError("Error with BF_GetBlockCounter\n");
            return -1;
        }
        hash_table[bucket] = newBlockId;
        memcpy(blockData0 + sizeof(char) + sizeof(int) + sizeof(long int) + nameSize,&hash_table[0],
               sizeof(hash_table));
        if(BF_WriteBlock(header_info.fileDesc, 0) < 0){

            BF_PrintError("Error with write\n");
            return -1;
        }
    }

    Block block;
    void* blockData = malloc(BLOCK_SIZE);

    //Reading the first block of the bucket.
    if (BF_ReadBlock(header_info.fileDesc,hash_table[bucket], &blockData) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return -1;
    }
    blockId = hash_table[bucket];
    memcpy(&block, blockData, sizeof(Block));

    while(block.counter == MAX_RECORDS){    //Block is full of Records, we move to the next one.
        if(block.nextBlock == -1){  //There is no next block
            int newBlockId = createNewBlock(header_info.fileDesc);  //Creating new block
//            printf("%d\n",newBlockId);
            if (newBlockId < 0 ){
                BF_PrintError("Error with BF_GetBlockCounter\n");
                return -1;
            }
            block.nextBlock = newBlockId;     // Setting the nextBlock pointer to the id of the new block created
            memcpy(blockData, &block, sizeof(Block));       //Turning the current block to byteArray

            if(BF_WriteBlock(header_info.fileDesc, blockId) < 0){     //Writing the current block back to the file

                BF_PrintError("Error with write\n");
                return -1;
            }

            if (BF_ReadBlock(header_info.fileDesc,newBlockId, &blockData) < 0){     //Reading the newly created block
                BF_PrintError("Error with BF_ReadBlock\n");
                return -1;
            }
            memcpy(&block, blockData, sizeof(Block));
            blockId = newBlockId;

        }else{   // block.nextBlock != -1
            blockId = block.nextBlock;
             if (BF_ReadBlock(header_info.fileDesc,block.nextBlock, &blockData) < 0){
                BF_PrintError("Error with BF_ReadBlock\n");
                return -1;
            }
            memcpy(&block, blockData, sizeof(Block));
        }

    }       //while(block.counter == MAX_RECORDS)...end

    block.record[block.counter] = record;       // Inserting the new record to the block
    block.counter++;

    memcpy(blockData, &block, sizeof(Block));       //Turning the block to byteArray

    if(BF_WriteBlock(header_info.fileDesc, blockId) < 0){     //Writing the block back to the file

        BF_PrintError("Error with write\n");
        return -1;
    }
//    printf("%d\n",blockId);
    return blockId;

};

int HT_DeleteEntry(HT_info header_info, void* value){

    int key = *((int*) value);
    int bucket = hash_int(key,header_info.numBuckets);    // The bucket in which we'll search for the record
//    printf("%d\n",bucket);
    int hash_table[header_info.numBuckets];

    void* blockData = malloc(BLOCK_SIZE);

    if (BF_ReadBlock(header_info.fileDesc,0, &blockData) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return -1;
    }
    int nameSize = strlen(header_info.attrName) + 1;
    memcpy(&hash_table[0] ,  blockData + sizeof(int) + sizeof(char) + nameSize + sizeof(long int),sizeof(hash_table));

    Block block;

    if (hash_table[bucket] == -1) return -1; // No blocks in the bucket

    int nextBlock = hash_table[bucket];
    int found = -1;
    while(nextBlock != -1 && found == -1) {
        //Reading a block
        if (BF_ReadBlock(header_info.fileDesc, nextBlock, &blockData) < 0) {
            BF_PrintError("Error with BF_ReadBlock\n");
            return -1;
        }
        memcpy(&block, blockData, sizeof(Block));

        //Searching for the record in the block.
        for(int i =0; i< block.counter; i++){

            if(block.record[i].id == key){      //Found the correct record
                block.record[i].id = DELETED_RECORD_ID;
                found = 0;
                memcpy(blockData, &block, sizeof(Block));

                if(BF_WriteBlock(header_info.fileDesc, nextBlock) < 0){     //Writing the block back to the file

                    BF_PrintError("Error with write\n");
                    return -1;
                }
                break;
            }
        }
        nextBlock = block.nextBlock;
    }
    return found;

};

int HT_GetAllEntries(HT_info header_info, void* value){

    int key = *((int*) value);
    int bucket = hash_int(key,header_info.numBuckets);    // The bucket in which we'll search for the record
    int hash_table[header_info.numBuckets];

    void* blockData = malloc(BLOCK_SIZE);

    if (BF_ReadBlock(header_info.fileDesc,0, &blockData) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return -1;
    }
    int nameSize = strlen(header_info.attrName) + 1;
    memcpy(&hash_table[0] ,  blockData + sizeof(int) + sizeof(char) + nameSize + sizeof(long int),sizeof(hash_table));

    Block block;

    if (hash_table[bucket] == -1) return -1; // No blocks in the bucket

    int nextBlock = hash_table[bucket];
    int blocksRead = 0;
    int foundAtLeastOneRecord = -1;     //A value to check if at least one record with the correct key was found
    while(nextBlock != -1) {
        //Reading a block
        if (BF_ReadBlock(header_info.fileDesc, nextBlock, &blockData) < 0) {
            BF_PrintError("Error with BF_ReadBlock\n");
            return -1;
        }
        blocksRead++;   //Increasing the blocksRead counter
        memcpy(&block, blockData, sizeof(Block));

        //Searching for a record with the correct key.
        for (int i = 0; i < block.counter; i++) {
            if (block.record[i].id == key) {      //Found a record with the correct key value.
                printRecordInfo(block.record[i]);
                foundAtLeastOneRecord = 0;
            }
        }
        nextBlock = block.nextBlock;
    }
    if(foundAtLeastOneRecord == 0) return blocksRead;
    else return -1;

};

// Finds a record in the primary index from it's id and returns it.
Record HT_GetRecordFromKey(int fileDesc, int blockId, void* value){

    int key = *((int*) value);

    void* blockData = malloc(BLOCK_SIZE);

    Block block;

    if (BF_ReadBlock(fileDesc, blockId, &blockData) < 0) {
        BF_PrintError("Error with BF_ReadBlock\n");
        Record record;
        record.id = DELETED_RECORD_ID;
        return record;      //We return a record with a DELETED_RECORD_ID
    }
    memcpy(&block, blockData, sizeof(Block));
    //Searching for a record with the correct key.
    for (int i = 0; i < block.counter; i++) {
        if (block.record[i].id == key) {      //Found the correct record
            return block.record[i];
        }
    }
    //If we reach this point then there was an error or the record was deleted from the primary index.

    Record record;
    record.id = DELETED_RECORD_ID;
    return record;      //We return a record with a DELETED_RECORD_ID

};


int HashStatistics(char* filename){

    HT_info* info = malloc(sizeof(HT_info));
    info = HT_OpenIndex(filename);  //Getting the HT_info from the file
    if(info == NULL){
        printf("Error with HT_OpenIndex\n");
        return -1;
    }

    //Getting the hash_table from the first block
    void* blockData = malloc(BLOCK_SIZE);
    int hash_table[info->numBuckets];
    if (BF_ReadBlock(info->fileDesc,0, &blockData) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return -1;
    }
    int nameSize = strlen(info->attrName) + 1;
    memcpy(&hash_table[0] ,  blockData + sizeof(int) + sizeof(char) + nameSize + sizeof(long int),sizeof(hash_table));

    Block block;
    int buckets = info->numBuckets;
    int numberOfBlocks = 0;     //Total number of blocks within ALL buckets
    int numberOfRecords = 0;      //Total number of records within ALL buckets
    int bucketsWithOverflowBlocks = 0;      // The number of buckets that have at least one overflow block.
    int minRecords = INT_MAX;       //The minimum number of records within a bucket
    int maxRecords = 0;         //The maximum number of records within a bucket

    for (int i =0; i<buckets; i++) {
        int blocksInBucket = 0;
        int recordsInBucket = 0;
        int nextBlock = hash_table[i];

        while (nextBlock != -1) {
            //Reading a block
            if (BF_ReadBlock(info->fileDesc, nextBlock, &blockData) < 0) {
                BF_PrintError("Error with BF_ReadBlock\n");
                return -1;
            }
            memcpy(&block, blockData, sizeof(Block));

            blocksInBucket++;   //Increasing

            //Searching for not-deleted records.
            for (int j = 0; j < block.counter; j++) {
                if (block.record[j].id != DELETED_RECORD_ID) {      //Record is not deleted
                    recordsInBucket++;
                }
            }
            nextBlock = block.nextBlock;
        }
        numberOfBlocks += blocksInBucket;
        numberOfRecords += recordsInBucket;
        if (recordsInBucket > maxRecords) maxRecords = recordsInBucket;
        if (recordsInBucket < minRecords) minRecords = recordsInBucket;
        if (blocksInBucket > 1) bucketsWithOverflowBlocks++;
        if(blocksInBucket != 0) {
            printf("Bucket %d has %d overflow Blocks\n", i+1, blocksInBucket-1);
        }
        else printf("Bucket %d has 0 overflow Blocks\n", i);
    }
    printf("Buckets that have at least one overflow block: %d\n",bucketsWithOverflowBlocks);
    printf("Total blocks in the file: %d\n", numberOfBlocks);
    if (numberOfRecords == 0) minRecords = 0;   // 0 records in total
    printf("The minimum number of records in a bucket is: %d\n",minRecords);
    printf("The maximum number of records in a bucket is: %d\n",maxRecords);
    if(buckets != 0){
    printf("The average number of records in a bucket is: %f\n",((double)((double)numberOfRecords/(double)buckets)));
    printf("The average number of blocks in a bucket is: %f\n",((double)((double)numberOfBlocks/(double)buckets)));
    }
}

#endif /*BF_HT_H*/
