//
// Created by karolos on 13/1/2019.
//



#ifndef BF_HT_H
#define BF_HT_H

#define MAX_RECORDS 6
#define BUCKETS 10

#include <string.h>
#include <stdlib.h>
#include "BF.h"


int hash_function(int x){
    return ((5*x + 12) % 78901) % BUCKETS;

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
    HT_info info;       //The HT_Info struct
    int hash_table[BUCKETS];
} Block0;



int HT_CreateIndex(char* fileName, char attrType, char* attrName, int attrLength, int buckets){

    int number;
    number = BF_CreateFile(fileName);
//    if (number != 0) {
//        BF_PrintError(number);
//    }
    int fileDesc = BF_OpenFile(fileName);


    HT_info info;

    info.fileDesc = fileDesc;
    info.attrLength = attrLength;
    info.attrName = (char*) malloc((size_t)attrLength);
    strcpy(info.attrName,attrName);
    info.attrType = attrType;
    info.numBuckets = buckets;

    if (BF_AllocateBlock(fileDesc) < 0){
        BF_PrintError("Error with BF_AllocateBlock");
        return -1;
    }

    void* blockData;

    if (BF_ReadBlock(fileDesc,0, &blockData) < 0){
        BF_PrintError("Error with BF_ReadBlock");
        return -1;
    }

    Block0 block0;
    block0.id = 0;
    block0.info = info;
    for (int i =0; i<BUCKETS; i++) block0.hash_table[i] = -1;


    memcpy(blockData, &block0, sizeof(Block0));

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

HT_info* HT_OpenIndex(char* fileName){};

int HT_CloseIndex(HT_info* header_info){};

int HT_InsertEntry(HT_info header_info, Record record){};

int HT_DeleteEntry(HT_info header_info, void* value){};

int HT_GetAllEntries(HT_info header_info, void* value){};

#endif /*BF_HT_H*/