/*
 * Copyright (C) 2014 Ki Suh Lee (kslee@cs.cornell.edu)
 * 
 * TCP Proxy skeleton.
 * 
 */
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <signal.h>
#include <pthread.h>
#include <semaphore.h>
#include <fcntl.h>
#include <sys/stat.h>
#include "list.h"

#define MAX_LISTEN_BACKLOG 5
#define MAX_ADDR_NAME	32
#define ONE_K	1024

/* Data that can pile up to be sent before reading must stop temporarily */
#define MAX_CONN_BACKLOG	(8*ONE_K)
#define GRACE_CONN_BACKLOG	(MAX_CONN_BACKLOG / 2)

/* Watermarks for number of active connections. Lower to 2 for testing */
#define MAX_CONN_HIGH_WATERMARK	(256)
#define MAX_CONN_LOW_WATERMARK	(MAX_CONN_HIGH_WATERMARK - 1)

#define MAX_THREAD_NUM 4 	

struct sockaddr_in remote_addr; /* The address of the target server */
#define BUF_SIZE 8192

struct client{
   int client_fd;
   int server_fd;
   char buf_cs[BUF_SIZE]; /* Buffer for client to server */
   char buf_sc[BUF_SIZE]; /* Buffer for server to client */
   int read_endcs;
   int write_endcs;
   int read_endsc;
   int write_endsc;
   int tid;
   int clientEOF;
   int serverEOF;
};
struct client conn[MAX_CONN_HIGH_WATERMARK];

sem_t is_thread_free1;
sem_t is_thread_free2;
sem_t is_thread_free3;
sem_t is_thread_free4;
sem_t max_conn;
pthread_mutex_t mutex;

void transfer(int *client_fd, int *server_fd, int *read_endcs, int *write_endcs,
			int *read_endsc, int *write_endsc, fd_set *r, fd_set *w,int i){
	int ret = 0;
	int buf_len = 0;
	if(*client_fd >0){
		/* Client is ready to write - bufcs*/
		if(FD_ISSET(*client_fd,r)){
			buf_len = BUF_SIZE-*read_endcs;
			ret = recv(*client_fd,conn[i].buf_cs+*read_endcs,buf_len,0);
			if(ret < 0){
				close(*client_fd);
				close(*server_fd);
				*client_fd = -1;
				*server_fd = -1;
			} else if(ret == 0){
				conn[i].clientEOF = 1;
			}else {
				*read_endcs = *read_endcs+ret;
			}
		}
		
		/* Client is ready to read - bufsc */
		if(FD_ISSET(*client_fd,w)){
			buf_len = *read_endsc-*write_endsc;
			ret = send(*client_fd,conn[i].buf_sc+*write_endsc,buf_len,0);
			if(ret<0){
				close(*client_fd);
				close(*server_fd);
				*client_fd = -1;
				*server_fd = -1;
			} else {
				*write_endsc = *write_endsc + ret;
			}	
			
		}
	}
	
	if(*server_fd >0){
		/* Server is ready to read -bufsc*/
		if(FD_ISSET(*server_fd,r)){
			buf_len = BUF_SIZE-*read_endsc;
			ret = recv(*server_fd,conn[i].buf_sc+*read_endsc,buf_len,0);
			if(ret < 0){
				close(*server_fd);
				*server_fd = -1;
				close(*client_fd);
				*client_fd = -1;
			} else if(ret == 0){
				close(*server_fd);
				*server_fd = -1;
				conn[i].serverEOF = 1;		
			} else{
				*read_endsc = *read_endsc +ret;
			}
		}	
		
		/* Server is ready to write - bufcs*/
		if(FD_ISSET(*server_fd,w)){
			buf_len = *read_endcs-*write_endcs;
			ret = send(*server_fd,conn[i].buf_cs+*write_endcs,buf_len,0);
			if(ret<0){
				close(*server_fd);
				*server_fd = -1;
			} else {
				*write_endcs = *write_endcs + ret;
			}
		}
	}
	
}

void handler(int tid){
	
	while(1){	
                
		pthread_mutex_lock(&mutex); 
		int newfd =0;
		unsigned int ret;
		int counter =0;
		fd_set r,w;
		struct timeval tv;
		//10 Second timeout
		//tv.tv_sec = 10;
        	//tv.tv_usec = 0;
		int j=0;
		FD_ZERO(&r);
		FD_ZERO(&w);
		

		for(j=0;j<MAX_CONN_HIGH_WATERMARK;j++){
			if(tid==conn[j].tid){
			
			conn[j].clientEOF = 0;
			conn[j].serverEOF = 0;
			// Client - ready to write data - buf is empty 
			if(conn[j].client_fd > 0 && conn[j].read_endcs < BUF_SIZE){
				FD_SET(conn[j].client_fd,&r);
			}
			// Client - ready to receive data - buf is non-empty
			if(conn[j].client_fd > 0 && conn[j].read_endsc - conn[j].write_endsc > 0){
				FD_SET(conn[j].client_fd,&w);
			}
			// Server - ready to write data - (buf is empty)
			if(conn[j].server_fd > 0 && conn[j].read_endsc < BUF_SIZE){
				FD_SET(conn[j].server_fd,&r);
			}
			// Server - ready to read - write into it (buf non-empty)
			if(conn[j].server_fd > 0 && conn[j].read_endcs-conn[j].write_endcs > 0){
				FD_SET(conn[j].server_fd,&w);
			}
			if (conn[j].client_fd > conn[j].server_fd) 
				newfd = conn[j].client_fd;
       			else if (conn[j].client_fd < conn[j].server_fd)
        			newfd = conn[j].server_fd;
			}
		}
                pthread_mutex_unlock(&mutex); 
		ret = select(newfd + 1, &r, &w, NULL, NULL);
		if(ret == -1 && errno == EINTR)
		    continue;
		if(ret == -1){
		     break;
	        }
                pthread_mutex_lock(&mutex); 

		for(j=0;j<MAX_CONN_HIGH_WATERMARK;j++){
			if(tid==conn[j].tid){
			
			if(conn[j].client_fd > 0 || conn[j].server_fd >0){
				transfer(&conn[j].client_fd,&conn[j].server_fd,&conn[j].read_endcs,&conn[j].write_endcs,&conn[j].read_endsc,&conn[j].write_endsc,&r,&w,j);
				//Buffer is full, reset 
				if(conn[j].read_endcs == conn[j].write_endcs){
					conn[j].read_endcs = conn[j].write_endcs = 0;
				}
			
				if(conn[j].read_endcs ==0 && conn[j].write_endcs ==0 && conn[j].clientEOF == 1){
					shutdown(conn[j].server_fd,O_WRONLY);
				}
				
				if(conn[j].read_endsc == conn[j].write_endsc){
					conn[j].read_endsc = conn[j].write_endsc = 0;
				}
		
				if(conn[j].read_endsc ==0 && conn[j].write_endsc ==0 && conn[j].serverEOF == 1){
					close(conn[j].client_fd);
					conn[j].client_fd = -1;
				}
			   }
			}
		}
		for(j=0;j<MAX_CONN_HIGH_WATERMARK;j++){
			if(tid==conn[j].tid){
				if(conn[j].client_fd > 0 && conn[j].server_fd >0){
					counter++;
				}
			}
		}
		for(j=0;j<MAX_CONN_HIGH_WATERMARK;j++){
			if(tid==conn[j].tid && conn[j].client_fd < 0 && conn[j].server_fd <0){
				sem_post(&max_conn);
				break;
			}
		}
		if(counter == 0){
                   pthread_mutex_unlock(&mutex);
		   break;
		}
                pthread_mutex_unlock(&mutex);
	}
}
void *thread_handler(void *arg) {
	int* tid;
	tid = (int*) arg ;
	while(1) {
		if(*tid == 0){
			sem_wait(&is_thread_free1);
		} else if(*tid == 1){
			sem_wait(&is_thread_free2);
		} else if(*tid == 2){
			sem_wait(&is_thread_free3);
		} else{
			sem_wait(&is_thread_free4);
		}
		handler(*tid);
	}
}


void __loop(int proxy_fd)
{
	struct sockaddr_in client_addr;
	socklen_t addr_size;
	struct client *client;
	int cur_thread=0;
	pthread_t *thread;
	char client_hname[MAX_ADDR_NAME+1];
	char server_hname[MAX_ADDR_NAME+1];
        int client_fd = -1;
	int server_fd = -1;
	int j = 0,i = 0;
	int my_thread_id[MAX_THREAD_NUM];

	for(j=0;j<MAX_CONN_HIGH_WATERMARK;j++){
		conn[j].client_fd = -1;
		conn[j].server_fd = -1;
   		conn[j].read_endcs = 0;
		conn[j].write_endcs = 0;
   		conn[j].read_endsc = 0;
   		conn[j].write_endsc = 0;
	}
	for(j=0;j<MAX_CONN_HIGH_WATERMARK;j++){
		conn[j].tid = j%MAX_THREAD_NUM;
	}
	sem_init(&is_thread_free1,0,0);
	sem_init(&is_thread_free2,0,0);
	sem_init(&is_thread_free3,0,0);
	sem_init(&is_thread_free4,0,0);
	sem_init(&max_conn,0,MAX_CONN_HIGH_WATERMARK);

	
	thread = (pthread_t *) malloc(MAX_THREAD_NUM * sizeof(*thread));
	for (i = 0; i < MAX_THREAD_NUM; i++)
	{	   
           my_thread_id[i] = i;
	   if(pthread_create(&thread[0], NULL, &thread_handler,&my_thread_id[i]) !=0){
	   	printf("Thread creation error\n");
		exit(1);
	   }
	}
	while(1) {
		
		sem_wait(&max_conn);
		
		memset(&client_addr, 0, sizeof(struct sockaddr_in));
		addr_size = sizeof(client_addr);
		client_fd = accept(proxy_fd, (struct sockaddr *)&client_addr,
				&addr_size);
		if(client_fd == -1) {
			fprintf(stderr, "accept error %s\n", strerror(errno));
			continue;
		}

		// For debugging purpose
		if (getpeername(client_fd, (struct sockaddr *) &client_addr, &addr_size) < 0) {
			fprintf(stderr, "getpeername error %s\n", strerror(errno));
		}
		
		strncpy(client_hname, inet_ntoa(client_addr.sin_addr), MAX_ADDR_NAME);
		strncpy(server_hname, inet_ntoa(remote_addr.sin_addr), MAX_ADDR_NAME);

		// TODO: Disable following printf before submission
		/*printf("Connection proxied: %s:%d --> %s:%d\n",
				client_hname, ntohs(client_addr.sin_port),
				server_hname, ntohs(remote_addr.sin_port));
		*/

		// Connect to the server
		if ((server_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
			fprintf(stderr, "socket error %s\n", strerror(errno));
			close(client_fd);
			continue;
		}
	
		if (connect(server_fd, (struct sockaddr *) &remote_addr, 
			sizeof(struct sockaddr_in)) <0) {
			if (errno != EINPROGRESS) {
				fprintf(stderr, "connect error %s\n", strerror(errno));
				close(client_fd);
				close(server_fd);
				continue;
			}		
		}
		
		pthread_mutex_lock(&mutex);
		for(j=0;j<MAX_CONN_HIGH_WATERMARK;j++){
			if(conn[j].client_fd <0 && conn[j].server_fd <0){
				break;
			}
		}
		
		conn[j].client_fd = client_fd;
		conn[j].server_fd = server_fd;
		conn[j].read_endcs= conn[j].write_endcs =0;
		conn[j].read_endsc= conn[j].write_endsc =0;
		pthread_mutex_unlock(&mutex);
			
		if(j%MAX_THREAD_NUM == 0){
			sem_post(&is_thread_free1);
		}else if(j%MAX_THREAD_NUM ==1){
			sem_post(&is_thread_free2);
		} else if(j%MAX_THREAD_NUM ==2){
			sem_post(&is_thread_free3);
		} else{
			sem_post(&is_thread_free4);
		}
	}

	exit(1);
}

int main(int argc, char **argv)
{
	char *remote_name;
	struct sockaddr_in proxy_addr;
	unsigned short local_port, remote_port;
	struct hostent *h;
	int arg_idx = 1, proxy_fd;
	
	if (argc != 4)
	{
		fprintf(stderr, "Usage %s <remote-target> <remote-target-port> "
			"<local-port>\n", argv[0]);
		exit(1);
	}

	remote_name = argv[arg_idx++];
	remote_port = atoi(argv[arg_idx++]);
	local_port = atoi(argv[arg_idx++]);
	

	/* Lookup server name and establish control connection */
	if ((h = gethostbyname(remote_name)) == NULL) {
		fprintf(stderr, "gethostbyname(%s) failed %s\n", remote_name, 
			strerror(errno));
		exit(1);
	}
	memset(&remote_addr, 0, sizeof(struct sockaddr_in));
	remote_addr.sin_family = AF_INET;
	memcpy(&remote_addr.sin_addr.s_addr, h->h_addr_list[0], sizeof(in_addr_t));
	remote_addr.sin_port = htons(remote_port);
	
	/* open up the TCP socket the proxy listens on */
	if ((proxy_fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP)) < 0) {
		fprintf(stderr, "socket error %s\n", strerror(errno));
		exit(1);
	}
	/* bind the socket to all local addresses */
	memset(&proxy_addr, 0, sizeof(struct sockaddr_in));
	proxy_addr.sin_family = AF_INET;
	proxy_addr.sin_addr.s_addr = INADDR_ANY; /* bind to all local addresses */
	proxy_addr.sin_port = htons(local_port);
	if (bind(proxy_fd, (struct sockaddr *) &proxy_addr, 
			sizeof(proxy_addr)) < 0) {
		fprintf(stderr, "bind error %s\n", strerror(errno));
		exit(1);
	}

	listen(proxy_fd, MAX_LISTEN_BACKLOG);

	__loop(proxy_fd);

	return 0;
}
