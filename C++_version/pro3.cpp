#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/**
 * Function: getXY
 * ------------------------------------------------------
 * Read a pair of trip_fare file and trip_data files,
 * get trip_time from trip_data file, surcharge and total amount less the tolls amount from trip_fare file
 * parameters:
 *            **filename_fare: the name of the trip_fare file
 *            **filename_data: the name of the trip_data file
 *            *trip_time: trip time from trip_data file
 *            *surcharge: surcharge from trip_fare file
 *            *fee: the column of total amount less the tolls
 */
void getXY(char **filename_fare, char **filename_data, double *trip_time, double *surcharge, double *fee){
	FILE *file_fare = fopen(*filename_fare, "r");
    FILE *file_data = fopen(*filename_data, "r");


    // If one or two files can not be opened, print the information and return
    if(NULL == file_fare || NULL == file_data)
    {
        fprintf(stderr, "Cannot open file");
        return;
    }

   
    //malloc a space to store each line of a file    size_t buffer_size = 256;
    size_t buffer_size = 256;
    char* buffer1 = (char*)malloc(buffer_size);
    char* buffer2 = (char*)malloc(buffer_size);

    bzero(buffer1, buffer_size);
    bzero(buffer2, buffer_size);

    getline(&buffer1, &buffer_size, file_fare);
    getline(&buffer2, &buffer_size, file_data);

    char *tripTime = NULL; //store trip_time
    double total_amount = 0; //total amount from fare file
    double tolls_amount = 0; //toll amount from fare file


    int nl = 0;

    // read each line of the two files
    while(-1 != getline(&buffer1, &buffer_size, file_fare) && -1 != getline(&buffer2, &buffer_size, file_data))
    {
    	//get total amount - toll amount from fare file
        char *result = NULL;
        char *buffer3 = buffer1;

        for(int j = 0; j<7; j++){
                result = strsep(&buffer3, ",");
        }
        surcharge[nl] = atof(result);
        result = strsep(&buffer3, ",");
        result = strsep(&buffer3, ",");
        result = strsep(&buffer3, ",");
        tolls_amount = atof(result);
        
 
        result = strsep(&buffer3,"\n");
        total_amount = atof(result);
        fee[nl] = total_amount - tolls_amount;


        //get trip_time from data file
        char *buffer4 = buffer2;
        for(int j = 0; j<9; j++){
                tripTime = strsep(&buffer4, ",");
        }
        trip_time[nl] = atof(tripTime);

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

    double value1[] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
    double value2[] = {0, 0, 0, 0, 0, 0, 0, 0, 0};
    double value3[] = {0,0,0,0,0,0,0,0,0};
    getXY(&filename1, &filename2, value1, value2,value3);

    for(int i = 0; i<9; i++){
        printf("%f\t", value1[i]);
    }
    printf("\n");
    for(int i = 0; i<9; i++){
        printf("%f\t", value2[i]);
    }
    printf("\n");
    for(int i = 0; i<9; i++){
        printf("%f\t", value3[i]);
    }
    return 0;
}

*/
