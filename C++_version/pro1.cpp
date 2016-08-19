#include <stdio.h>
#include <stdlib.h>
#include <string.h>


/**
 * Function: getFee
 * ------------------------------------------------------
 * Read the trip_fare file and get the value of total amount less the tolls
 * parameters:
 *            *filename: the name of the trip_fare file
 *            *fee: the column of total amount less the tolls
 */
void getFee(char **filename, double *fee){
	FILE *file = fopen(*filename, "r");

    // If the file can not be opened, print the information and return.
    if(NULL == file)
    {
        fprintf(stderr, "Cannot open file: %s\n", *filename);
        return;
    }
    
    // malloc a space to store each line of a file
    size_t buffer_size = 256;
    char* buffer = (char*)malloc(buffer_size);
    bzero(buffer, buffer_size);
    
    //the first line is the header, we do not need it
    getline(&buffer, &buffer_size, file);
    bzero(buffer, buffer_size);

    double total_amount = 0; //the last column-total amount
    double tolls_amount = 0; //the value of tolls amount
    int nl = 0; //the number of each line we will read

    // read each line
    char delims[]   = ",";
    char delims2[]  = "\n";
    while(-1 != getline(&buffer, &buffer_size, file))
    {
    	char *result    = NULL;
        char *buffer2   = buffer;

        //the first 10 columns are separated by ,
    	for(int j = 0; j<10; j++){
    		result = strsep(&buffer2, delims);
    	}
    	tolls_amount = atof(result);
        
        //The last column is ended with line break
        result       = strsep(&buffer2,delims2);
        total_amount = atof(result);
        
        //record total amount minus tolls amount in fee
        fee[nl] = total_amount - tolls_amount;
        nl      = nl + 1;
        
        bzero(buffer, buffer_size);


    }
    
    fflush(stdout);
    fclose(file);
    free(buffer);
    
}

/*

int main ()
{
	char filename[] = "1.csv";

	int numLines = 10;

	double fee[9] = {0, 0, 0, 0, 0, 0, 0, 0, 0};

	getFee(filename, fee, &numLines);

	for(int i=0; i< (numLines - 1); i++){
		printf("%f\n", fee[i]);
	}

	return 0;
}
*/

