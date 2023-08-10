#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdlib.h> 
#include <string.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdbool.h>
#include <limits.h>
#include <pthread.h>
#include <dirent.h>
#include <openssl/md5.h>


#define BUFSIZE 1024
#define SOCKETERROR (-1)
#define SERVER_BACKLOG 100
#define THREAD_POOL_SIZE 20

pthread_t thread_pool[THREAD_POOL_SIZE];
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t mutex_print = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t condition_var = PTHREAD_COND_INITIALIZER;

static pthread_mutex_t mutexes[100];
static pthread_mutex_t privateidmutex[100];

typedef struct sockaddr_in SA_IN;
typedef struct sockaddr SA;

//prototype
void calculate_md5sum(char *filename);
void *handle_connection(void* p_client_socket);
long int findSize(FILE *fp);
int check(int exp,const char *msg);
void * thread_function(void *arg);
void * client_function(void *arg);
void * depth0download(void *arg);
void * depth1download(void *arg);
void write_file(int sockfd,const char* fileaddr);
void send_file(FILE *fp, int sockfd);
void * resultoutput_function(void *arg);
int setup_server(short port,int backlog);
int accept_new_connection(int server_socket);
//CHANGED         
int accept_new_connection(int server_socket);
int checkIfFileExists(const char* filename);
int min(int num1, int num2);
int checkcompleted_hop1 = 0;
int checkcompleted_depth1 = 0;
int array_neigh_sockets[100];
int depth0downloadcount = 0;
int depth1downloadcount = 0;
int connectedto = 0;

struct node {
    struct node* next;
    int *client_socket;
};
typedef struct node node_t;

void enqueue(int *client_socket);
int* dequeue();

node_t* head = NULL;
node_t* tail = NULL;


char download_directory_addr[100];
char directory_addr[100];
int files_received[50][50];
int index_depth0files[100];
int indices[100];
int indices2[100];
int neighbor_private_id[50];
int port_neighbours_depth1[100];
int private_id_depth0[100];
int private_id_depth2[100];
int lowestuniqueid[100];
int round2completecheck=0; 

struct serverinfo{
    int neighbourid, port, index;
};

int SERVERPORT,ID,private_ID,num_neighbors,numfiles,numfilesfordepth2,num_clientfiles,num_neighbors_depth1,num_clientfiles_hop1,round2start=-1;    
int neighbor_id[100],neighbor_port[100],neighbor_port_depth1[1000];
char files_required[100][50];
char files_requireddepth1[100][50];
char files_owned[100][50];
int num_owned=0;
bool foundatdepth0[50];
int num_neighbors_depth1=0;

struct stat st = {0};

int main(int argc,char **argv)
{   
    strcpy(directory_addr,argv[2]);   
    strcpy(download_directory_addr,directory_addr);
    char downloadaddr[100] = "Downloaded/";
    strcat(download_directory_addr,downloadaddr);
    if (stat(download_directory_addr, &st) == -1) {
        mkdir(download_directory_addr, 0777);
    }
    DIR *d;
    struct dirent *dir;
    d = opendir(argv[2]);
    if (d)
    {   
        while ((dir = readdir(d)) != NULL)
        {   if(!((strcmp(dir->d_name,".")==0)||(strcmp(dir->d_name,"..")==0))){
                strcpy(files_owned[num_owned],dir->d_name);
                num_owned++;
            }
        }
        
        closedir(d);
    }
    
    
    FILE * fp;
    char line[1000];
    size_t len = 0;
    ssize_t read;
    int line_number=0;
    int file_number=0;

    fp = fopen(argv[1], "r");
   
    if (fp == NULL)
        exit(EXIT_FAILURE);

    while(fgets(line, sizeof(line), fp) != NULL){
        if (line_number==0){
            char *p;
            p = strtok(line, " ");
            if(p)
            {
                ID=atoi(p);
            }

            p = strtok(NULL, " ");
            if(p)
                SERVERPORT=atoi(p);
            p = strtok(NULL, " ");
            if(p)
                private_ID=atoi(p);
            line_number++;    
        }
        else if(line_number==1){
            num_neighbors=atoi(line);
            line_number++;
        }
        else if(line_number==2){
            char *p;
            
            p = strtok(line, " ");

            for(int i=0;i<num_neighbors;i++){
                if(p)
                neighbor_id[i]=atoi(p);

                p = strtok(NULL, " ");

                if(p)
                neighbor_port[i]=atoi(p);

                p = strtok(NULL, " ");                        
                
            }
            line_number++;
               
        }
        else if(line_number==3){
            numfiles=atoi(line);
            line_number++;
        }
        else {

            while(line[strlen(line)-1] == '\n'||line[strlen(line)-1] == '\r'){
                line[strlen(line)-1] = '\0';
            }
            
            if (file_number<numfiles){
                strcpy(files_required[file_number],line);
            }    

            file_number++;
            line_number++;
        }

    }

    fclose(fp);
    
    char temp[50];

    //bubble sort
    for(int i=0; i<num_owned; i++){
      for(int j=0; j<num_owned-1-i; j++){
        if(strcmp(files_owned[j], files_owned[j+1]) > 0){
          strcpy(temp, files_owned[j]);
          strcpy(files_owned[j], files_owned[j+1]);
          strcpy(files_owned[j+1], temp);
        }
      }
    }

    int temp1;
    int temp2;
    
    //Sort array using the Bubble Sort algorithm
    for(int i=0; i<num_neighbors; i++){
      for(int j=0; j<num_neighbors-1-i; j++){
        if(neighbor_id[j]> neighbor_id[j+1]){
          //swap array[j] and array[j+1]
          temp1=neighbor_id[j];
          temp2=neighbor_port[j];          
          neighbor_id[j]=neighbor_id[j+1];
          neighbor_port[j]=neighbor_port[j+1];
          neighbor_id[j+1]=temp1;
          neighbor_port[j+1]=temp2;
        }
      }
    }

    bzero(temp,50);


    for (int i=0;i<num_owned;i++){
        printf("%s\n",files_owned[i]);
    }

    for (int i=0;i<num_neighbors;i++){
        array_neigh_sockets[i]=-1;
        pthread_mutex_init(&mutexes[i], NULL);
        pthread_mutex_init(&privateidmutex[i], NULL);
        pthread_mutex_lock(&mutexes[i]);
        pthread_mutex_lock(&privateidmutex[i]);
    }

    int CLIENT_THREAD_POOL_SIZE = num_neighbors;
    pthread_t client_thread_pool[CLIENT_THREAD_POOL_SIZE];

    pthread_mutex_lock(&mutex);
    for(int i = 0; i < 50; i++){
        for(int j = 0; j < 50; j++){
            files_received[i][j] = -1;
        }
    }
    pthread_mutex_unlock(&mutex);

    pthread_t resultcheckingthread;
    pthread_create(&resultcheckingthread,NULL,resultoutput_function,NULL);

    struct serverinfo info[num_neighbors];
    for(int i = 0; i < num_neighbors; i++){
        info[i].neighbourid = neighbor_id[i];
        info[i].port = neighbor_port[i];
        info[i].index = i;
    }


    for(int i = 0; i < CLIENT_THREAD_POOL_SIZE; i++){
        pthread_t t;
        pthread_create(&t,NULL,client_function,&info[i]);
        
    }
    for(int i = 0; i < numfiles; i++){
        private_id_depth0[i]=0;
        index_depth0files[i] = -1;
    }
        int server_socket=setup_server(SERVERPORT, SERVER_BACKLOG);    
    

    while(true){
        
        int client_socket = accept_new_connection(server_socket);

        int *pclient = malloc(sizeof(int));
        *pclient = client_socket;
        pthread_mutex_lock(&mutex);
        enqueue(pclient);
        pthread_cond_signal(&condition_var);
        pthread_mutex_unlock(&mutex);
    } //WHILE

    return EXIT_SUCCESS;
}


int setup_server(short port, int backlog){
    int server_socket, client_socket, addr_size;
    SA_IN server_addr;

    for ( int  i=0 ; i < THREAD_POOL_SIZE; i++){
        pthread_create(&thread_pool[i],NULL,thread_function,NULL);
    }

    check((server_socket = socket(AF_INET, SOCK_STREAM,0)),
     "Failed to create socket");


    //initialize the address struct
    server_addr.sin_family= AF_INET;
    server_addr.sin_addr.s_addr=INADDR_ANY;
    server_addr.sin_port= htons(port);

    check(bind(server_socket,(SA*)&server_addr, sizeof(server_addr)),
    "Bind Failed!");

    check(listen(server_socket,backlog),
    "Listen Failed!");
    return server_socket;
}



int accept_new_connection(int server_socket){
    int addr_size= sizeof(SA_IN);
    int client_socket;
    SA_IN client_addr;
    check(client_socket=accept(server_socket,
                                (SA*)&client_addr,
                                (socklen_t*)&addr_size),
                                "accept failed");
    return client_socket;
}

int check(int exp,const char *msg){
    if (exp==SOCKETERROR){
        perror(msg);
        exit(1);
    }
    return exp;

}

void * thread_function(void *arg){
    while(true){
        int *pclient;
        pthread_mutex_lock(&mutex);
        if((pclient = dequeue()) == NULL){
            pthread_cond_wait(&condition_var,&mutex);
            pclient = dequeue();
        }
        pthread_mutex_unlock(&mutex);
        if(pclient != NULL){
            handle_connection(pclient);
        }
    }
}

void * handle_connection (void *p_client_socket){

    int valread;
    char buffer[100] = {0};
    int client_socket = *((int*)p_client_socket);
    char private_ID_str[100];
    int client_private_id;
    
    free(p_client_socket);
    sprintf(private_ID_str, "%d", private_ID);
    
    send(client_socket , private_ID_str , 100 , 0 );
    valread = read( client_socket, buffer, 100);


    client_private_id =atoi(buffer);
    bzero(buffer,100);

    while(true){

        valread = read( client_socket, buffer, 100);
        if(valread>0){     
        }            
        int depth = atoi(buffer);
        if(depth == 2){
            char file_address[100];
            bzero(file_address,100);
            bzero(buffer,100);
            valread = read( client_socket, buffer, 100);
            strcat(file_address, directory_addr);
            strcat(file_address, buffer);
            FILE *file;
            file = fopen(file_address, "rb");
            send_file(file,client_socket);
            fclose(file);
        }
        else if(depth == 1){            
            valread = read( client_socket, buffer, 100);
            num_clientfiles_hop1 =atoi(buffer);
            
            char files_requireddepth1_hop1[100][50];
            int port_of_file[num_clientfiles_hop1];
            int files_received_depth1_hop1[num_clientfiles_hop1];
            for(int i = 0; i < num_clientfiles_hop1; i++){
                valread = read( client_socket, buffer, 100);
                strcpy(files_requireddepth1_hop1[i],buffer); 
            }

            for(int i = 0; i < num_clientfiles_hop1; i++){
                files_received_depth1_hop1[i] = -1;
                port_of_file[i]= -1 ;
            }

            if(num_clientfiles_hop1 != 0){
                for(int j = 0; j < num_neighbors; j++){                    
                    pthread_mutex_lock(&privateidmutex[j]);
                    if(neighbor_private_id[j]==client_private_id){                       
                        pthread_mutex_unlock(&privateidmutex[j]);
                        continue;     
                        }
                    pthread_mutex_unlock(&privateidmutex[j]);
                    pthread_mutex_lock(&mutexes[j]);
                    int socket_neighbour=array_neigh_sockets[j];
                    char depthtry[100];
                    sprintf(depthtry, "%d", 0);
                    send( socket_neighbour, depthtry , 100 , 0 ); 
                    char digit[100];
                    sprintf(digit, "%d", num_clientfiles_hop1);
                    send(socket_neighbour , digit , 100 , 0 );    
                    for(int i=0;i<num_clientfiles_hop1;i++){
                        send(socket_neighbour , files_requireddepth1_hop1[i] , 100 , 0 );

                        valread = read( socket_neighbour, buffer, 100);             
                        
                        if(strcmp(buffer,"1")==0){
                            pthread_mutex_lock(&mutex);
                            if(files_received_depth1_hop1[i]==-1){
                                files_received_depth1_hop1[i]=neighbor_private_id[j];
                                port_of_file[i]=neighbor_port[j];
                            }
                            else{
                                if(files_received_depth1_hop1[i]>neighbor_private_id[j]){ 
                                    files_received_depth1_hop1[i]=neighbor_private_id[j];
                                    port_of_file[i]=neighbor_port[j];
                                }
                            }
                            pthread_mutex_unlock(&mutex);
                        }
                        else if(strcmp(buffer,"0")==0){
                            continue;
                        }
                    }
                    pthread_mutex_unlock(&mutexes[j]);
                
                }
            }

            char tobesent[100] = "";
            pthread_mutex_lock(&mutex_print);
            pthread_mutex_unlock(&mutex_print);

            for(int i = 0;i<num_clientfiles_hop1;i++){
                sprintf(tobesent, "%d", files_received_depth1_hop1[i]);
                send(client_socket , tobesent, 100 , 0 );
                bzero(tobesent,100);
                sprintf(tobesent, "%d", port_of_file[i]);                    
                send(client_socket , tobesent, 100 , 0 );                
            }           
        }
        else if(depth==0){
            char checkfile[100] = "foo.png";
            valread = read( client_socket, buffer, 100);
            num_clientfiles =atoi(buffer);
            bzero(buffer,100);
            for(int i=0;i<num_clientfiles;i++){
                char file_address[100]="";
                char filename[100]="";

                valread = read( client_socket, buffer, 100);
                pthread_mutex_lock(&mutex_print);
                pthread_mutex_unlock(&mutex_print);
                strcat(file_address, directory_addr);
        //        printf("%s1\n",file_address);
                strcat(file_address, buffer);
        //        printf("%s2\n",file_address);
                strcpy(filename,buffer);    
                //validity check
                if (checkIfFileExists(filename)==1){
                        send(client_socket , "1" , 100 , 0 );
                }
                else{
                        send(client_socket , "0" , 100 , 0 );
                    }
            }
        }
    }

    close(client_socket);
    return NULL;

}


void * client_function(void *arg){

    //****DEPTH 0 SEARCH START****
    int valread;
    char buffer[100] = {0};

    struct serverinfo* portinfo = (struct serverinfo * ) arg;
    int sockfd, connfd;
    struct sockaddr_in servaddr, cli;
    struct serverinfo info = *portinfo;
    int portnum = info.port;
    char digit[100];

    // socket create and verification
    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
    servaddr.sin_port = htons(portnum);

    
    while(true){
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            printf("Client socket creation failed...\n");
            exit(0);
        }
        
        sleep(1);
        if (connect(sockfd, (SA*)&servaddr, sizeof(servaddr)) == 0) {
            array_neigh_sockets[info.index]=sockfd;
            break;        
        }
        close(sockfd);
    }

    char depthtry[100];
    
    sprintf(depthtry, "%d", private_ID);
    send(sockfd , depthtry , 100 , 0 );       
    bzero(depthtry,100);

    sprintf(depthtry, "%d", 0);
    send(sockfd , depthtry , 100 , 0 );    
    pthread_mutex_lock(&mutex_print);
    pthread_mutex_unlock(&mutex_print); 
    valread = read( sockfd, buffer, 100);
    pthread_mutex_lock(&mutex_print);
    pthread_mutex_unlock(&mutex_print);
    //CHANGED
    neighbor_private_id[info.index]=atoi(buffer);
    pthread_mutex_lock(&mutex_print);
    //printf("Connected to %d with unique-ID %d on port %d\n",info.neighbourid,neighbor_private_id[info.index],info.port);
    pthread_mutex_lock(&mutex);
    connectedto++;
    pthread_mutex_unlock(&mutex);
    pthread_mutex_unlock(&mutex_print);
    pthread_mutex_unlock(&privateidmutex[info.index]);
    // printf("Unlocked 1\n");
    pthread_mutex_unlock(&mutexes[info.index]);

    pthread_mutex_lock(&mutexes[info.index]);
    sprintf(digit, "%d", numfiles);
//    printf("%s\n","Reached2c");
        
    send(sockfd , digit , 100 , 0 );    

    //sending names of files
    for(int i=0;i<numfiles;i++){
        send(sockfd , files_required[i] , 100 , 0 );
        valread = read( sockfd, buffer, 100);
        if(strcmp(buffer,"1")==0){
            pthread_mutex_lock(&mutex);
            if(private_id_depth0[i]==0 || private_id_depth0[i] > neighbor_private_id[info.index]){
                private_id_depth0[i] = neighbor_private_id[info.index];
                index_depth0files[i] = info.index;
            } 
            pthread_mutex_unlock(&mutex);
            files_received[i][info.index]=1;
        }
        else if(strcmp(buffer,"0")==0){
            files_received[i][info.index]=0;
        }
        bzero(buffer,100);

    }

    pthread_mutex_unlock(&mutexes[info.index]);


    
////    printf("Reached5\n");
        
    while(round2start==(-1)){
        continue;
    }

    if(round2start==0){
    
        return NULL;    
    }
  
    pthread_mutex_lock(&mutexes[info.index]);
    // printf("Locked 3\n");
    bzero(depthtry,100);
    sprintf(depthtry, "%d", 1);
        
    send(sockfd , depthtry , 100 , 0 );

    sprintf(digit, "%d", numfilesfordepth2);
        
    send(sockfd , digit , 100 , 0 );    

    for(int i=0;i<numfilesfordepth2;i++){
        send(sockfd , files_requireddepth1[i] , 100 , 0 );
    }

    for(int i=0;i<numfilesfordepth2;i++){
        valread = read( sockfd, buffer, 100);

        if(strcmp(buffer,"-1")==0){            
            valread = read( sockfd, buffer, 100);    
    
            continue;
        }
        else {
            pthread_mutex_lock(&mutex);                
            if(private_id_depth2[i]==0 || private_id_depth2[i] > atoi(buffer)  ){
                private_id_depth2[i]=atoi(buffer);
                bzero(buffer,100);
                valread = read( sockfd, buffer, 100);
                port_neighbours_depth1[i]=atoi(buffer);                
                bzero(buffer,100);                                        
            }
            else {
                valread = read( sockfd, buffer, 100);                        
            }
            pthread_mutex_unlock(&mutex);
        }
    }


    pthread_mutex_unlock(&mutexes[info.index]);
    

    pthread_mutex_lock(&mutex);
    round2completecheck++;
    pthread_mutex_unlock(&mutex);

    return NULL;
}

int checkIfFileExists(const char* filename){
    
    for (int i=0;i<num_owned;i++){
        if(strcmp(filename,files_owned[i])==0){
            return 1;
        }        
    }
    return 0;

    return 0;
    
}


void * resultoutput_function(void *arg){
    while(connectedto!=num_neighbors){
        continue;
    }

    for(int i = 0; i < num_neighbors; i++){
        printf("Connected to %d with unique-ID %d on port %d\n",neighbor_id[i],neighbor_private_id[i],neighbor_port[i]);
    }

    int checkedcount = 0;
    int required = num_neighbors*numfiles;
    int index;
    int depth[numfiles];

    for(int k = 0; k < numfiles; k++){
        lowestuniqueid[k] = 0;
        depth[k]=0;
    }

    while(checkedcount != required){

        checkedcount=0;
        for(int i = 0; i < num_neighbors; i++){
            for(int j = 0; j < numfiles; j++){

                if(files_received[j][i] != -1){
                    checkedcount++;
                    if(files_received[j][i] == 1){
                        lowestuniqueid[j] = 2147483647;
                        depth[j]=1;
                    }
                }
            }
        }

    }

    for(int k = 0; k < numfiles; k++){
        foundatdepth0[k] = 0;
        for(int i = 0; i < num_neighbors;i++){
            if(files_received[k][i]==1){
                foundatdepth0[k] = 1;
                lowestuniqueid[k] = min(lowestuniqueid[k],neighbor_private_id[i]);
            }        
        }
    }

    numfilesfordepth2 = 0;
    
    for(int i = 0; i < numfiles; i++){
        if(foundatdepth0[i] == 0){
            strcpy(files_requireddepth1[numfilesfordepth2],files_required[i]);
            numfilesfordepth2++;
            
        }        
    }

    if(numfilesfordepth2 == 0){
        // printf("****NO DEPTH 2 FILES\n***");
        for(int i = 0; i < numfiles;i++){
            indices[i] = i;
            pthread_t t;
            pthread_create(&t,NULL,depth0download,&indices[i]);
        }

        while(depth0downloadcount!=numfiles){
            continue;
        }
        for(int i = 0; i < numfiles;i++){
            pthread_mutex_lock(&mutex_print);
            printf("Found %s at %d with MD5 ",files_required[i],lowestuniqueid[i]);
            pthread_mutex_unlock(&mutex_print);
            calculate_md5sum(files_required[i]);
            printf(" at depth %d\n", 1);
        }
        return NULL;
    }

        
    if(numfilesfordepth2 != 0){
        for(int i=0;i<numfilesfordepth2;i++){
            port_neighbours_depth1[i]=-1;
            private_id_depth2[i]=0;
        }
        round2start=1;

    }
    else{
        round2start=0;
        return NULL;
        
    }

    while(true){
        if(round2completecheck==num_neighbors)break;
    }
    
    int filesfounddepth1 = 0;
    for(int i = 0; i<numfilesfordepth2; i++){
        if(private_id_depth2[i] != 0){
            indices2[i] = i;
            pthread_t t;
            pthread_create(&t,NULL,depth1download,&indices2[i]);
            filesfounddepth1++;            
        }     
    } 
    for(int i = 0; i < numfiles;i++){
            if(foundatdepth0[i]==1){
                indices[i] = i;
                pthread_t t;
                pthread_create(&t,NULL,depth0download,&indices[i]);
            }      
        }
    int filesfounddepth0 = numfiles - numfilesfordepth2;

    while(depth0downloadcount!=filesfounddepth0 || depth1downloadcount!=filesfounddepth1){
        continue;
    }
    int indexdepth2=0;
    for(int i = 0; i<numfiles; i++){
        if(foundatdepth0[i]==1){
            pthread_mutex_lock(&mutex_print);
            printf("Found %s at %d with MD5 ",files_required[i],lowestuniqueid[i]);
            calculate_md5sum(files_required[i]);
            printf(" at depth %d\n", 1);
            pthread_mutex_unlock(&mutex_print);
        }
        else{
            if(private_id_depth2[indexdepth2]!=0){
                pthread_mutex_lock(&mutex_print);
                printf("Found %s at %d with MD5 ",files_requireddepth1[indexdepth2],private_id_depth2[indexdepth2]);
                calculate_md5sum(files_requireddepth1[indexdepth2]);
                printf(" at depth %d\n", 2);
                pthread_mutex_unlock(&mutex_print);
            }
            else{
                pthread_mutex_lock(&mutex_print);
                printf("Found %s at %d with MD5 %d at depth %d\n",files_requireddepth1[indexdepth2],private_id_depth2[indexdepth2],0, 0);
                pthread_mutex_unlock(&mutex_print);
            }
            indexdepth2++;
        }
    }
    return NULL;
}


int min(int num1, int num2) 
{
    return (num1 > num2 ) ? num2 : num1;
}

void * depth0download(void *arg){
    int fileindex = *((int *) arg);
    int neighborindex = index_depth0files[fileindex];
    
    pthread_mutex_lock(&mutexes[neighborindex]);

    int neighborsocket = array_neigh_sockets[neighborindex];
    char depthtry[100];

    sprintf(depthtry, "%d", 2);
        
    send(neighborsocket , depthtry , 100 , 0 );
    send(neighborsocket , files_required[fileindex] , 100 , 0 );

    char file_download_addr[200];
    bzero(file_download_addr,200);
    strcpy(file_download_addr,download_directory_addr);
    strcat(file_download_addr,files_required[fileindex]);
    write_file(neighborsocket,file_download_addr);

    pthread_mutex_unlock(&mutexes[neighborindex]);
    pthread_mutex_lock(&mutex);
    depth0downloadcount++;    
    pthread_mutex_unlock(&mutex);
    return NULL;
}

void * depth1download(void *arg){
    int valread;
    char buffer[100];
    int fileindex = *((int *) arg);
    int portnum = port_neighbours_depth1[fileindex];
    int sockfd, connfd;
    struct sockaddr_in servaddr, cli;
    char digit[100];

    bzero(&servaddr, sizeof(servaddr));
    servaddr.sin_family = AF_INET;
    servaddr.sin_addr.s_addr = inet_addr("127.0.0.1");
    servaddr.sin_port = htons(portnum);

    while(true){
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd == -1) {
            printf("Client socket creation failed...\n");
            exit(0);
        }
        
        sleep(1);
        if (connect(sockfd, (SA*)&servaddr, sizeof(servaddr)) == 0) {
            break;        
        }
        close(sockfd);
    }

    char depthtry[100];
    sprintf(depthtry, "%d", private_ID);
    send(sockfd , depthtry , 100 , 0 );       
    bzero(depthtry,100);

    sprintf(depthtry, "%d", 2);
    send(sockfd , depthtry , 100 , 0 );    
   
    valread = read( sockfd, buffer, 100);
    send(sockfd , files_requireddepth1[fileindex] , 100 , 0 );
    char file_download_addr[200];
    bzero(file_download_addr,200);
    strcpy(file_download_addr,download_directory_addr);
    strcat(file_download_addr,files_requireddepth1[fileindex]);
    write_file(sockfd,file_download_addr);
    pthread_mutex_lock(&mutex);
    depth1downloadcount++;    
    pthread_mutex_unlock(&mutex);
    return NULL;  
}


void write_file(int sockfd,const char* fileaddr){
  int n;
  FILE *file;
  char buffer[1024];
  char *eptr;
  long int filesize;
  file = fopen(fileaddr, "wb");
  n = recv(sockfd,buffer,1024,0);
  filesize = strtol(buffer,&eptr,10);
  long int checksize = filesize;
  long int bytereceived = 0;
  bzero(buffer, 1024);
  pthread_mutex_lock(&mutex_print);
  while (bytereceived<filesize) {
    n = recv(sockfd, buffer, 1024, 0);
    if(checksize >= 1024){
        fwrite(buffer,1,n,file);
    }
    else{
        fwrite(buffer,1,checksize,file);
    }
    fflush(file);
    bzero(buffer, 1024);
    bytereceived = bytereceived + n;
    checksize = checksize - n;
  }
pthread_mutex_unlock(&mutex_print);
  fclose(file);
  return;
}

void send_file(FILE *fp, int sockfd){
  int n;
  char data[1024] = {0};
  long int filesize = findSize(fp);
  sprintf(data,"%ld",filesize);
  send(sockfd, data, 1024, 0);
  bzero(data, 1024);
  fseek(fp,0,SEEK_SET);
  while(fread(data, 1,sizeof(data), fp) > 0) {
    if (send(sockfd, data, 1024, 0) == -1) {
      perror("[-]Error in sending file.");
      exit(1);
    }
    bzero(data, 1024);
  }
}

long int findSize(FILE *fp)
{   
    fseek(fp, 0L, SEEK_END);
  
    // calculating the size of the file
    long int res = ftell(fp);
    return res;
}
  
void calculate_md5sum(char *filename)
{
  //open file for calculating md5sum
    unsigned char c[MD5_DIGEST_LENGTH];
    char fileadressmd5[200];
    bzero(fileadressmd5,200);
    int i;
    char file_download_addr[200];
    bzero(file_download_addr,200);
    strcpy(file_download_addr,download_directory_addr);
    strcpy(fileadressmd5, file_download_addr);
    strcat(fileadressmd5, filename);
    FILE *inFile = fopen (fileadressmd5, "rb");
    MD5_CTX mdContext;
    int bytes;
    unsigned char data[1024];

    if (inFile == NULL) {
        // printf("File %s not found",fileadressmd5);
        printf ("%d", 0);
        return;
    }
    MD5_Init (&mdContext);
    while ((bytes = fread (data, 1, 1024, inFile)) != 0)
        MD5_Update (&mdContext, data, bytes);
    MD5_Final (c,&mdContext);
    // printf("%s",fileadressmd5);
    for(i = 0; i < MD5_DIGEST_LENGTH; i++) printf("%02x", c[i]);
    bzero(c,MD5_DIGEST_LENGTH);
    fclose (inFile);
    return;

  return;
}

void enqueue(int * client_socket){
    node_t *newnode = malloc(sizeof(node_t));
    newnode->client_socket = client_socket;
    newnode->next = NULL;
    if (tail == NULL) {
        head = newnode;
    }
    else {
        tail->next = newnode;
    }
    tail = newnode;
}

int * dequeue(){
    if(head == NULL){
        return NULL;
    }
    else{
        int *result = head->client_socket;
        node_t *temp = head;
        head = head->next;
        if (head == NULL) {
            tail = NULL;
        }
        free(temp);
        return result;
    }
}
