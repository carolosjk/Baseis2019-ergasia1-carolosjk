//
// Created by carolosjk on 18/1/2019.
//

#ifndef BASEIS2019_ERGASIA1_CAROLOSJK_HASHSTATISTICS_H
#define BASEIS2019_ERGASIA1_CAROLOSJK_HASHSTATISTICS_H

#include "SHT.h"

int HashStatisticsSHT(char* filename){

    SHT_info* info = malloc(sizeof(SHT_info));
    info = SHT_OpenSecondaryIndex(filename);  //Getting the SHT_info from the file
    if(info == NULL){
        printf("Error with SHT_OpenIndex\n");
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
    int fileNameSize = strlen(info->fileName) + 1;
    memcpy(&hash_table[0] ,  blockData + sizeof(int) + fileNameSize + nameSize + sizeof(long int),sizeof(hash_table));

    SecondaryBlock block;
    int buckets = info->numBuckets;
    int numberOfBlocks = 0;     //Total number of blocks within ALL buckets
    int numberOfRecords = 0;      //Total number of records within ALL buckets
    int bucketsWithOverflowBlocks = 0;      // The number of buckets that have at least one overflow block.
    int minRecords = INT_MAX;       //The minimum number of records within a bucket
    int maxRecords = 0;         //The maximum number of records within a bucket

    for (int i = 0; i<buckets; i++) {
        int blocksInBucket = 0;
        int recordsInBucket = 0;
        int nextBlock = hash_table[i];

        while (nextBlock != -1) {
            //Reading a block
            if (BF_ReadBlock(info->fileDesc, nextBlock, &blockData) < 0) {
                BF_PrintError("Error with BF_ReadBlock\n");
                return -1;
            }
            memcpy(&block, blockData, sizeof(SecondaryBlock));

            blocksInBucket++;   //Increasing

            //Searching for not-deleted records.
            for (int j = 0; j < block.counter; j++) {
                if (strcmp(block.record[j].name, DELETED_RECORD_NAME)!=0) {      //Record is not deleted
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
    SHT_CloseSecondaryIndex(info);
    return 0;
}

int HashStatisticsHT(char* filename){

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
    HT_CloseIndex(info);
    return 0;
}

int HashStatistics(char* filename){
    char ht_or_sh = 'h' ;// If 'h' we return the ht statistics if 's' the sht
    HT_info* info = malloc(sizeof(HT_info));
    info = HT_OpenIndex(filename);  //Getting the HT_info from the file
    if(info == NULL){
        ht_or_sh = 's';
    }
    void* blockData = malloc(BLOCK_SIZE);

    if (BF_ReadBlock(info->fileDesc,0, &blockData) < 0){
        ht_or_sh = 's';
    }
    if (info->numBuckets <=0) return HashStatisticsSHT(filename);
    int hash_table[info->numBuckets];

    if(info->attrLength > 100) ht_or_sh = 's';

    int nameSize = strlen(info->attrName) + 1;
    memcpy(&hash_table[0] ,  blockData + sizeof(int) + sizeof(char) + nameSize + sizeof(long int),sizeof(hash_table));

    for (int i =0; i < info->numBuckets; i++){
        if (hash_table[i] < -1 || hash_table[i] == 0 || hash_table[i] > 100000) ht_or_sh = 's';
    }

    HT_CloseIndex(info);
    if(ht_or_sh == 's') return HashStatisticsSHT(filename);
    else  return HashStatisticsHT(filename);
}

#endif //BASEIS2019_ERGASIA1_CAROLOSJK_HASHSTATISTICS_H
