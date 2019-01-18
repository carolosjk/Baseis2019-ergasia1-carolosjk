//
// Created by carolosjk on 18/1/2019.
//

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <stdlib.h>
#include "SHT.h"
#include "HashStatistics.h"


int main(char argc,char** argv) {
    /*
    How many records to use for the  test.
    */
    int testRecordsNumber = atoi(argv[1]);
    /*
    The proportion for the deletes.
    */
    int testDeleteRecords = (int) testRecordsNumber * atof(argv[2]);
    /*
    Init the BF layer.
    */
    BF_Init();
    /*
    Index parameters.
    */
    char *fileName = "primary.index";
    char attrType = 'i';
    char *attrName = "id";
    int attrLength = 4;
    int buckets = 10;
    char *sfileName = "secondary.index";
    char sAttrType = 'c';
    char *sAttrName = "name";
    int sAttrLength = 15;
    int sBuckets = 10;


    HT_CreateIndex(fileName,attrType,attrName,attrLength,buckets);


}