//
//  Full Name:  Giampouonka Kanellakos Karolow
//  AM:         1115201600030
//

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdlib.h>
#include "SHT.h"
#include "HashStatistics.h"



int main(char argc,char** argv) {

    FILE *fptr;
    char* file = "records15K.txt";

    BF_Init();  //Initialise the BF layer

    //Some parameters
    char *fileName = "ht_file";
    char attrType = 'i';
    char *attrName = "id";
    int attrLength = 4;
    int buckets = 20;
    char *sfileName = "sht_file";
    char *sAttrName = "name";
    int sAttrLength = 15;
    int sBuckets = 10;

    //Creating primary index
    if (HT_CreateIndex(fileName,attrType,attrName,attrLength,buckets) == -1){
        printf("Error when creating primary index.\n");
        return -1;
    }

    HT_info* info = malloc(sizeof(HT_info));

    //Opening the primary index
    info = HT_OpenIndex(fileName);
    if(info==NULL || info->attrType!=attrType || strcmp(info->attrName,attrName)!=0){
        printf("Error when opening primary index.\n");
        return -1;
    }

    //Creating secondary index
    if (SHT_CreateSecondaryIndex(sfileName,sAttrName,sAttrLength,sBuckets,fileName) == -1){
        printf("Error when creating secondary index.\n");
        return -1;
    }

    SHT_info* sInfo = malloc(sizeof(SHT_info));

    //Opening the primary index
    sInfo = SHT_OpenSecondaryIndex(sfileName);
    if(sInfo==NULL ||  strcmp(sInfo->attrName,sAttrName)!=0){
        printf("Error when opening secondary index.\n");
        return -1;
    }

    //Inserting records to the indexes

    Record record;
    SecondaryRecord sRecord;

    if ((fptr = fopen(file, "r")) == NULL)
    {
        printf("Error! opening file\n");
        return -1;
    }
    char* line;
    size_t len = 0;


    while (getline(&line,&len,fptr) != -1) {
        sscanf(line, "%*c%d%*c%*c%[^ \t\n\r\v\f\"]%*c%*c%*c%[^ \t\n\r\v\f\"]%*c%*c%*c%[^ \t\n\r\v\f\"]",
                &record.id,record.name,record.surname,record.address);

        sRecord.blockId = HT_InsertEntry(*info,record);
        if (sRecord.blockId == -1){
            printf("Error when inserting entry to primary index.\n");
            return -1;
        }
        sRecord.record = record;

        if(SHT_SecondaryInsertEntry(*sInfo,sRecord) == -1){
            printf("Error when inserting entry to secondary index.\n");
            return -1;
        }
    }

    fclose(fptr);

    //Searching for records with id from 0 to 1000, ending in 5 (i.e. 5, x5, xx5)

    for (int i = 5; i < 1000; i += 10){
        HT_GetAllEntries(*info, (void*) &i);
    }

    //Searching for records with name ending in 26  up to 10000(i.e. name_26, name_126, ... , name_1326)

    char* name = malloc(20);
    for (int i = 26; i < 10000; i += 100){
        sprintf(name,"name_%d",i);
        SHT_SecondaryGetAllEntries(*sInfo, *info, (void*) name);
    }
    free(name);

    //Closing the indexes
    int htCloseError = HT_CloseIndex(info);
    int shtCloseError = SHT_CloseSecondaryIndex(sInfo);

    if(htCloseError != 0){
        printf("Error when closing the primary index\n");
        return -1;
    }

    if(shtCloseError != 0){
        printf("Error when closing the secondary index\n");
        return -1;
    }

// The hash statistics
    printf("Primary index statistics:\n");
    HashStatistics(fileName);
    printf("Secondary index statistics\n");
    HashStatistics(sfileName);
    return 0;

}