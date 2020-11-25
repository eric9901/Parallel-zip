/*
 * NOTE TO STUDENTS: Before you do anything else, please
 * provide your group information here.
 *
 * Group No.: 23 (Join a project group in Canvas)
 * First member's full name: Kwok Man Hei
 * First member's email address: mhkwok22-c@my.cityu.edu.hk
 * Second member's full name: Lie Ka Kit
 * Second member's email address: kklie2-c@my.cityu.edu.hk
 * Third member's full name: 
 * Third member's email address: 
 */

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include<string.h>
#include <sys/mman.h> 
#include <pthread.h>
#include <sys/stat.h> 
#include <sys/sysinfo.h>
#include <unistd.h>
#include <dirent.h>


//Set global valuable 
int requirepages; //the require page for the output
int quenehead=0;  //The circular queue head
int quenetail =0; //The circular queue tail
#define quenecapacity 10 //Since we cannot make the static array 
//so we define the circular quene to current size
int quenesize =0; //The capacitor of circular queue 
int threads; // no of thread
int psize; //page size
int nfiles; //number of file
int Check =0; //boolean for program run
pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER, filelock=PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t empty = PTHREAD_COND_INITIALIZER, fill = PTHREAD_COND_INITIALIZER;
int* numpagefile;

//the struct type for output
struct output 
{	
	char* data; //output charactrer
	int* count; //each character count
	int size;//length of the char array and count
}*out;

//A buffer is used to store the information after reading file
struct buffer 
{
    char* address; //the content start address
    int filenum; 
    int pagenum; 
    int lastpsize; 
}buf[quenecapacity]; // a queue of defalut 10 capacity

// The data for munmap which increase the efficiently of input file
struct fd
{	
	char* addr;
	int size;
}*files;

//The Function of Quene
//We decide to put the index of quenehead to the quene
void put(struct buffer b)
{
  	buf[quenehead] = b; //This step is to enqueue the buf
  	quenehead = (quenehead + 1) % quenecapacity;
  	quenesize++;
}

//Since we set the quene capacity is 10
//This step is to remove the quene tail index
struct buffer get()
{
  	struct buffer b = buf[quenetail]; 
	quenetail = (quenetail + 1) % quenecapacity;
  	quenesize=quenesize-1;
  	return b;
}

//Producer part
// read the file content and put it into buffer
void readF(char* fName,int fIndex)
{
        char* map;
		int file;
		struct stat sb;
        file = open(fName, O_RDONLY);
		int pinfile=0; //page in file
		int lastpsize=0; //last page size
		//check for fail read the file
		if(file == -1){
			exit(1);
		}

		//check for empty file
		if(fstat(file,&sb) == -1){ 
			close(file);
			exit(1);
		}
		
		//get the page number in file
		pinfile=(sb.st_size/psize);
		if(((double)sb.st_size/psize)>pinfile)
		{ 
			pinfile+=1;
			lastpsize=sb.st_size%psize;
		}
		else
		{ 
			lastpsize=psize;
		}
		requirepages+=pinfile;
		numpagefile[fIndex]=pinfile;
		//map the file to virtual memory
		map = mmap(NULL, sb.st_size, PROT_READ, MAP_SHARED, file, 0); 													  
		if (map == MAP_FAILED) 
		{ 
			close(file);	
			exit(0);
    		}	
    	

		for(int j=0;j<pinfile;j++)
		{    
			pthread_mutex_lock(&lock);
			while(quenesize ==quenecapacity) // wait until the queue is not full
			{
			    pthread_cond_broadcast(&fill); 
				pthread_cond_wait(&empty,&lock);
			}
			pthread_mutex_unlock(&lock);
			struct buffer temp; //template for buffer
			if(j==pinfile-1)  
			{ 
				temp.lastpsize =lastpsize;
			}
			else
			{
				temp.lastpsize=psize;
			}
			temp.address=map;
			temp.pagenum=j;
			temp.filenum=fIndex;
			map=map+psize; 
		
			pthread_mutex_lock(&lock);
			put(temp);  //put the temp into queue
			pthread_mutex_unlock(&lock); 
			pthread_cond_signal(&fill);// sent the signal that a new buffer are into the queue
			close(file);
		}
}
// read the file under directory
size_t read_dir(const char * dir_path,char *** file_list)
{
        struct dirent *dir; 
		DIR *d = opendir(dir_path);
        size_t count=0;
		if (d==NULL){return 0;}
		*file_list=NULL;
		char dir_path_bar[100]; //path name of the of the read file
        strcpy(dir_path_bar,dir_path);
        strcat(dir_path_bar,"/"); 
        //count the number of file under directory
	    while ((dir = readdir(d)) != NULL) 
        { 
		    if (!strcmp (dir->d_name, "."))//skip the first 2 dot
            	continue;
         	if (!strcmp (dir->d_name, ".."))    
            	continue;
         	
		    count++;
	    }

		rewinddir(d);// rewind the directory address  
		 
		*file_list= calloc(count,sizeof(char*)); // allocalloc the memory to the stirng array
		count=0;
		
		while ((dir = readdir(d)) != NULL) 
        { 
		 
		    if (!strcmp (dir->d_name, "."))
            	continue;
        	if (!strcmp (dir->d_name, ".."))    
            	continue;	
            //combine the file name and the path
		    char* entry_path=calloc(1000,sizeof(char));
         	strcpy(entry_path,dir_path_bar);
         	strcat(entry_path,dir->d_name);
		    //store the file position
		   (*file_list)[count]=strdup(entry_path);
             
		  count++;
	    }    
		closedir(d);
		 
      return count;
}

//main of the producer
//It used to open and read the file
//and divide the file content to page and send the data to consumer
void* producer(void *arg)
{
    //We need to get the file first
	char** fNames = (char **)arg;
    size_t dir_file_NumCount =0;

	//Second, we need to open those files and read the file
    for(int i=0; i<nfiles; i++)
    {
	    char **files;
		size_t dir_File_Num=0;
		if((dir_File_Num=read_dir(fNames[i],&files))!=0)
		{  
			for(int j=0;j<dir_File_Num;j++)
			readF(files[j],i+dir_file_NumCount+j);
			dir_file_NumCount+=dir_File_Num;
		}
		else{
			readF(fNames[i],i+dir_file_NumCount);
		    }
	}
    Check=1;//The mapping are finish
	pthread_cond_broadcast(&fill);
	//wake up the consumer and pass to them
	return 0;
}

//Consumer part

//It used to compresses the buffer object
struct output RLECompress(struct buffer temp)
{
	struct output compressed;
	compressed.count=malloc(temp.lastpsize*sizeof(int));
	char* tempString=malloc(temp.lastpsize);
	int countIndex=0;

	for(int i=0;i<temp.lastpsize;i++)
	{
		tempString[countIndex]=temp.address[i];
		compressed.count[countIndex]=1;
		while(i+1<temp.lastpsize && tempString[countIndex]==temp.address[i+1])
		{
			compressed.count[countIndex]++;
			i++;
		}
		countIndex++;
	}
	compressed.size=countIndex;
	compressed.data=realloc(tempString,countIndex);
	return compressed;
}

//This is for calculates the file postion and relative page position
//To increase efficiency because we do not need to wait previous compression
int calPosition(struct buffer temp)
{
	int position=0;

	for(int i=0;i<temp.filenum;i++)
	{
		position+= numpagefile [i];
	}

	position+=temp.pagenum;
	return position;
}

//main of the consumer
//It aims to compress the data input and save the result
void *consumer(){
	do{
		pthread_mutex_lock(&lock);
		while(quenesize ==0 && Check==0)
		{
		    pthread_cond_signal(&empty);
			pthread_cond_wait(&fill,&lock); 
			//call the producer to filling the quene
		}
		//if there is no more thing in the quene and finish mapping in producer
		if(Check ==1 && quenesize ==0){ 
			pthread_mutex_unlock(&lock);
			return NULL;
		}
		struct buffer temp=get();
		if(Check ==0){
		    pthread_cond_signal(&empty);
		}	
		pthread_mutex_unlock(&lock);
		
		int position=calPosition(temp);
		out[position]=RLECompress(temp);
	}while(!(Check ==1 && quenesize ==0));
	return NULL;
}
// sum the character count if the last character is same as the first character of the next page
void sumCharacter (int i)
{

		if(i< requirepages -1)
		{
			if(out[i].data[out[i].size-1]==out[i+1].data[0])
			{ 
				out[i+1].count[0]+=out[i].count[out[i].size-1];
				out[i].size--;
			}
		}
}
void printOutput()
{
	for(int i=0;i< requirepages;i++){
		sumCharacter (i);
		for(int j=0;j<out[i].size;j++){
			fwrite(&out[i].count[j],sizeof(int),1,stdout);
			fwrite(&out[i].data[j],sizeof(char),1,stdout);
		}
	}

}



int main(int argc, char* argv[]){
	//To see whether is less than two arg
	if(argc<2){
		printf("pzip: file1 [file2 ...]\n");
		exit(1);
	}
	
	psize = 10000000; //default page size
	nfiles=argc-1; 
	threads =get_nprocs(); //get the maximun number of thread should create
	numpagefile =malloc(sizeof(int)*nfiles);
    out=malloc(sizeof(struct output)* 512000*2); //maximun out put size
	pthread_t pid,cid[threads];
	pthread_create(&pid, NULL, producer, argv+1); 
    //This step is to creat consumer thread to compress for all the page per files
	for (int i = 0; i < threads; i++) {
        pthread_create(&cid[i], NULL, consumer, NULL);
    }

	//Wait all the step finish
    for (int i = 0; i < threads; i++) {
        pthread_join(cid[i], NULL);
    }
    pthread_join(pid,NULL);
	printOutput();

	return 0;
}
