#include <stdio.h>
#include <stdlib.h> 
#include <string.h>
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <stdbool.h>
#include <limits.h>
#include <pthread.h>
#include <dirent.h>

#define BUFSIZE 1024
#define SOCKETERROR (-1)
#define SERVER_BACKLOG 100
#define THREAD_POOL_SIZE 20

pthread_t thread_pool[THREAD_POOL_SIZE];
pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t condition_var = PTHREAD_COND_INITIALIZER;

typedef struct sockaddr_in SA_IN;
typedef struct sockaddr SA;

void *handle_connection(void* p_client_socket);
int check(int exp,const char *msg);
void * thread_function(void *arg);
void * client_function(void *arg);
void * resultoutput_function(void *arg);
int setup_server(short port,int backlog);
int accept_new_connection(int server_socket);
int accept_new_connection(int server_socket);
int checkIfFileExists(const char* filename);
int min(int num1, int num2);

char directory_addr[100];
int files_received[50][50];
int neighbor_private_id[50];
int connectedto = 0;

struct serverinfo{
    int neighbourid, port, index;
};

struct node {
    struct node* next;
    int *client_socket;
};
typedef struct node node_t;

void enqueue(int *client_socket);
int* dequeue();
node_t* head = NULL;
node_t* tail = NULL;


int SERVERPORT,ID,private_ID,num_neighbors,numfiles;
int neighbor_id[100],neighbor_port[100];
char files_required[100][50];
char files_owned[100][50];
int num_owned=0;

int main(int argc,char **argv)
{   strcpy(directory_addr,argv[2]);   
    
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


    for(int i=0; i<num_owned; i++){
      for(int j=0; j<num_owned-1-i; j++){
        if(strcmp(files_owned[j], files_owned[j+1]) > 0){
          strcpy(temp, files_owned[j]);
          strcpy(files_owned[j], files_owned[j+1]);
          strcpy(files_owned[j+1], temp);
        }
      }
    }

    for (int i=0;i<num_owned;i++){
        printf("%s\n",files_owned[i]);
    }

   

    bzero(temp,50);

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
    int CLIENT_THREAD_POOL_SIZE = num_neighbors;
    pthread_t client_thread_pool[CLIENT_THREAD_POOL_SIZE];

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

        int server_socket=setup_server(SERVERPORT, SERVER_BACKLOG);

    

    while(true){
        
        int client_socket = accept_new_connection(server_socket);

        int *pclient = malloc(sizeof(int));
        *pclient = client_socket;
        pthread_mutex_lock(&mutex);
        enqueue(pclient);
        pthread_cond_signal(&condition_var);
        pthread_mutex_unlock(&mutex);
    } 
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
    int num_clientfiles;    
    char private_ID_str[100];

    free(p_client_socket);
    sprintf(private_ID_str, "%d", private_ID);

    send(client_socket , private_ID_str , 100 , 0 );

    close(client_socket);
    return NULL;

}


void * client_function(void *arg){
    int valread;
    char buffer[100] = {0};

    struct serverinfo* portinfo = (struct serverinfo * ) arg;
    int sockfd, connfd;
    struct sockaddr_in servaddr, cli;
    struct serverinfo info = *portinfo;
    int portnum = info.port;
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
   
    valread = read( sockfd, buffer, 100);

    neighbor_private_id[info.index]=atoi(buffer);
    //printf("Connected to %d with unique-ID %d on port %d\n",info.neighbourid,neighbor_private_id[info.index],info.port);
    pthread_mutex_lock(&mutex);
    connectedto++;
    pthread_mutex_unlock(&mutex);
    close(sockfd);
    return NULL;
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

void * resultoutput_function(void *arg){
    while(connectedto!=num_neighbors){
        continue;
    }

    for(int i = 0; i < num_neighbors; i++){
        printf("Connected to %d with unique-ID %d on port %d\n",neighbor_id[i],neighbor_private_id[i],neighbor_port[i]);
    }
    return NULL;
}