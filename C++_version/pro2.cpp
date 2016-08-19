#include <stdio.h>
#include <stdlib.h>
#include <string.h>


/**
 * Function: check
 * ------------------------------------------------------
 * Read a pair of trip_fare file and trip_data files,
 * check whether they matched line by line
 * parameters:
 *            *filename_fare: the name of the trip_fare file
 *            *filename_data: the name of the trip_data file
 *            *unequal: whether each row in the two files matched(0 means match)
 */
void check(char **filename_fare, char **filename_data, int *unequal){
	
    //open two files
    FILE *file_fare = fopen(*filename_fare, "r");
    FILE *file_data = fopen(*filename_data, "r");


    // If one or two files can not be opened, print the information and return
    if(NULL == file_fare || NULL == file_data)
    {
        fprintf(stderr, "Cannot open file");
        return;
    }

    
    //malloc a space to store each line of a file
    size_t buffer_size = 256;

    char* buffer1 = (char*)malloc(buffer_size);
    char* buffer2 = (char*)malloc(buffer_size);

    bzero(buffer1, buffer_size);
    bzero(buffer2, buffer_size);

    //The first line is the header, we do not need them
    getline(&buffer1, &buffer_size, file_fare);
    getline(&buffer2, &buffer_size, file_data);

    //four columns are used to see if they match
    char *medallion1 = NULL;
    char *hack_license1 = NULL;
    char *vendor_id1 = NULL;
    char *pickup_datatime1 = NULL;

    char *medallion2 = NULL;
    char *hack_license2 = NULL;
    char *vendor_id2 = NULL;
    char *pickup_datatime2 = NULL;


    int nl = 0;

    // read each line of the two files
    while(-1 != getline(&buffer1, &buffer_size, file_fare) && -1 != getline(&buffer2, &buffer_size, file_data))
    {
    	//from the line in trip_fare file, get value of medallion, hack_license, vendor_id1 and pickup time
        char *buffer4 = buffer1;
    	medallion1 = strsep(&buffer4, ",");
        hack_license1 = strsep(&buffer4, ",");
        vendor_id1 = strsep(&buffer4, ",");
        pickup_datatime1 = strsep(&buffer4, ",");

        //from the line in trip_data file, get value of medallion, hack_license, vendor_id1 and pickup time
        char *buffer3 = buffer2;
        medallion2 = strsep(&buffer3, ",");
        hack_license2 = strsep(&buffer3,",");
        vendor_id2 = strsep(&buffer3,",");
        pickup_datatime2 = strsep(&buffer3,",");
        pickup_datatime2 = strsep(&buffer3,",");
        pickup_datatime2 = strsep(&buffer3,",");

		//check whether they are same in two files
        if(strcmp(medallion1, medallion2) == 0 && strcmp(hack_license1, hack_license2) == 0 && strcmp(vendor_id1,vendor_id2) == 0 && strcmp(pickup_datatime1, pickup_datatime2) == 0){
            unequal[nl] = 0;
        }else{
            unequal[nl] = 1;
        }
        nl = nl + 1;

        bzero(buffer1, buffer_size);
        bzero(buffer2, buffer_size);

    }

	free(buffer1);
	free(buffer2);

	fflush(stdout);

    fclose(file_fare);
    fclose(file_data);
}

/*

int main(){
    char* filename1    = "fare_1.csv";
    char* filename2    = "data_1.csv";

    int value[] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
    check(&filename1, &filename2, value);

    for(int i = 0; i<9; i++){
        printf("%d\t", value[i]);
    }

    return 0;
}
*/
