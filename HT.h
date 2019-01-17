//
// Created by karolos on 13/1/2019.
//



#ifndef BF_HT_H
#define BF_HT_H

#define MAX_RECORDS 6
#define BUCKETS 10
#define  INITIAL_VALUE 5381

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include "BF.h"


int hash_int(int x, long int buckets){
    return ((5*x + 12) % 78901) % buckets;
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

unsigned char* blockToByteArray(Block block){

    unsigned char* byteArray = malloc(BLOCK_SIZE);
    byteArray[0] = 1;
    byteArray[1] = block.counter;
    for(int i=0; i < block.counter;i++){
        memcpy(&(byteArray[2+i* sizeof(Record)]),&(block.record[i]), sizeof(Record));
    }


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
    Block0 block0;


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

    int bucket = hash_int(record.id,header_info.numBuckets);
    int hash_table[header_info.numBuckets];

    void* blockData0 = malloc(BLOCK_SIZE);

    if (BF_ReadBlock(header_info.fileDesc,0, &blockData0) < 0){
        BF_PrintError("Error with BF_ReadBlock\n");
        return -1;
    }
    int nameSize = strlen(header_info.attrName) + 1;
    memcpy(&hash_table[0] ,  blockData0 + sizeof(int) + sizeof(char) + nameSize + sizeof(long int),sizeof(hash_table));

    Block block;
    void* blockData = malloc(BLOCK_SIZE);
    If
};

int HT_DeleteEntry(HT_info header_info, void* value){};

int HT_GetAllEntries(HT_info header_info, void* value){};

#endif /*BF_HT_H*/
