/*
* File: server.c
* Author: Sergio Gil Luque
* Version: 1.4
* Date: 10-15-15
*/

#include<stdio.h>
#include<string.h>   
#include<sys/socket.h>
#include<arpa/inet.h>
#include<pthread.h>
#include<stdlib.h>
#include <unistd.h>

#include "uthash.h" // For Hashtable implementation

#define MAX_SERVERS 8

int port = 0;
char *ip;



/* Hash table struct */

typedef struct keyStruct {
	char key[20];
	char value[1000];
	UT_hash_handle hh;
} keyStruct_t;


/* This is a double level hash table. First level -> Files ; Second level -> Peers that contain that file */
typedef struct fileStruct {
  char name[50];
  struct fileStruct *sub;
  int val;
  UT_hash_handle hh;
} fileStruct_t;


keyStruct_t *registeredKeys = NULL;


/* Look for key in the hashtable */
char *getValue (char* key) 
	{ 

    	struct keyStruct *s;
    	HASH_FIND_STR (registeredKeys, key, s );  /* s: output pointer */
    
	if (s == NULL)
	{
		return NULL;
	}
	else
	{
		return s->value;
	}
}

/* Insert new key in hash table  */
int insertValue (char* key, char* value) /* TODO: Handle errors */
{
	printf("Registering key %s with value %s\n", key, value);

	if (getValue(key) != NULL)
	{
		printf("*Error: Key %s has been already registered and it must remain unique\n", key);
		return -1;
	}
	struct keyStruct *newValue = malloc (sizeof (struct keyStruct));
	strncpy(newValue -> key, key, strlen(key));
	strncpy(newValue -> value, value, strlen(value));
	HASH_ADD_STR(registeredKeys, key, newValue);

	return 0;
}


/* Deletes a key from the hash table*/
int deleteKey (char* key) 
{
    	struct keyStruct *s;

    	HASH_FIND_STR (registeredKeys, key, s );  /* s: output pointer */
    
	if (s == NULL)
	{
		return -1;
	}
	else
	{
		HASH_DEL(registeredKeys, s);
		free(s);
		return 0;
	}
}



/* Handles incoming connections */
void *connection_handler(void *socket_desc)
{
	char *res = malloc(1024 * sizeof(char));
   	char *requestReceived = malloc(1024 * sizeof(char));
	char *header = malloc(1024 * sizeof(char));
	char *key = malloc(1024 * sizeof(char));
	char *finalKey;
	char *message = malloc (1024 * sizeof(char));

    	char *serverReply = malloc(1024 * sizeof(char));
    	int i;
    	int sock = *(int*)socket_desc;
     
	while (1)
	{
	    	if (read (sock, requestReceived , 1024) == 0)
		{
			printf("*Error: Connection lost\n");
			return (void *) -1;
		}

		strncpy(header, requestReceived, 3);
		strncpy(key, &requestReceived[4], 20);
	
		if (strlen(key) < 24)
		{
			finalKey = strtok(key, " ");
		}
		else
		{
			finalKey = key;
		}

		// Incoming header can be PUT, GET or DEL

		/* Put call */
		if (strcmp(header,"PUT")==0) 
		{
			strncpy(message, &requestReceived[24], 1000);

			if (insertValue(finalKey, message) < 0)
			{
				printf("*Error registering new key");
				strncpy(serverReply, "ERR ", 4);
				write(sock, serverReply, 1024);
			}
			else
			{
				printf("New key %s has been registered\n", finalKey);
				strncpy(serverReply, "OK  ", 4);
				write(sock, serverReply, 1024);
			}
		}

		/* Get call */
		else if (strcmp(header,"GET") == 0) 
		{
			printf("get branch. getting value %s\n", key);
			res = getValue(finalKey);
			printf("res is %s\n", res);
			if (res == NULL)
			{
				printf("*Error obtaining key");
				strncpy(serverReply, "ERR ", 4);
				for (i = 4; i < 1024; i++)
				{
					strncat(serverReply, " ", 1);
				}
				write(sock, serverReply, 1024);
				// return null
			}
			else
			{
				/* Create the reply that will be sent back to the peer */
				strncpy(serverReply, "OK  ", 4);
				strncat(serverReply, "                    ", 20);
				strncat(serverReply, res, strlen(res)); /* We skip the key part of the message */
				/*for (i = 24 + strlen(res); i < 1024; i++)
				{
					strncat(serverReply, " ", 1);
				}*/
				write(sock, serverReply, 1024);
			}
		}
	
		/* Delete call */
		else if (strcmp(header,"DEL")==0)
		{
			if (deleteKey(finalKey) < 0)
			{
				printf("*Error deleting key %s\n", finalKey);
				strncpy(serverReply, "ERR ", 4);
				write(sock, serverReply, 1024);
			}
			else
			{
				printf("Key %s has been deleted\n", finalKey);
				strncpy(serverReply,"OK  ", 4);
				write(sock, serverReply, 1024);
			}
		}

		else 
		{
			printf ("*Error: Bad request\n");

			strncpy("ERR ", serverReply, 4);
			write(sock, serverReply, 1024);
		
	    		//Free the socket pointer
	    		free(socket_desc);
	     
	    		return (void *) -1;
		}

	}
}



void *incoming_connections_handler (void* data)
{
     
    printf ("\t+Initializing incoming connection handler...\n");
     
    int IN_socket_desc , IN_new_socket , c , *IN_new_sock;
    struct sockaddr_in IN_server , client;
     
    //Create socket
    IN_socket_desc = socket(AF_INET , SOCK_STREAM , 0);

    if (IN_socket_desc == -1)
    {
        perror("*Error creating socket");
    }
     
    //Prepare the sockaddr_in structure
    IN_server.sin_family = AF_INET;
    IN_server.sin_addr.s_addr = INADDR_ANY;
    IN_server.sin_port = htons( port );
     
    //Bind
    if( bind(IN_socket_desc,(struct sockaddr *) &IN_server , sizeof(IN_server)) < 0)
    {
        perror("*Error binding");
        return (void *) 1;
    }
     
    //Listen
    listen(IN_socket_desc , 1000); // The server can handle 1000 simulteaneous connections
     
    //Accept an incoming connection
    printf("\t+Waiting for incoming connections in port %i...\n", port);
    c = sizeof(struct sockaddr_in);

    while( (IN_new_socket = accept(IN_socket_desc, (struct sockaddr *)&client, (socklen_t*)&c)) )
    {
        puts(">New connection accepted");
         
        pthread_t sniffer_thread;
        IN_new_sock = malloc(1);
        *IN_new_sock = IN_new_socket;
         
        if( pthread_create( &sniffer_thread , NULL ,  connection_handler , (void*) IN_new_sock) < 0)
        {
            perror("*Error creating thread for incoming request");
	    free(IN_new_sock);
            return (void *) 1;
        }
    }
     
    if (IN_new_socket<0)
    {
        perror("*Error accepting new request");
        return (void *) 1;
    }
     
    return (void *) 0;
}


int loadConfigFile (char* configFilePath, int configNumber)
{

	/* Open file */
	FILE *fp;
	fp = fopen(configFilePath, "r");
	char line[512];
	int count = 0;

    	if (fp != NULL)
    	{
	
		/*Reads the line that correspond to this server configuration */
		printf ("File %s opened\n", configFilePath);
	
		do
		{
			fgets (line, sizeof(line), fp);
			count++;
		} 
		while (count < configNumber);

		if (strlen(line) != 0)
		{
			ip = strtok(line,":");
			port = atoi(strtok(NULL,":"));
			printf("\t+Selecting configuration with ip %s and port %i\n", ip, port);		
			fclose(fp);	
		}
		else
		{
			printf("\n*Error when looking for server configuration\n");
			fclose (fp);
			return -1;
		}

		
	}

	else
	{
		printf("\n*Error when opening configuration file %s\n", configFilePath);
		perror("");
		return -1;
	}

	return 0;
}



int main(int argc , char *argv[])
{
	int res = 0;

	if (argc != 3)
	{
		printf("Usage: %s CONFIG_FILE_PATH CONF_OPTION\n", argv[0]);
		return 0;
	}
	if (atoi(argv[2]) > MAX_SERVERS)
	{
		printf("The maximum number of servers are set to %i\n", MAX_SERVERS);
		return 0;
	}

	/* INITIALIZATION */
	printf("\n\nInitializing server... \n");
	printf("\t+Accessing config file... ");
	int configNumber = atoi(argv[2]);

	// Accessing configuration file and loading internal values
	res = loadConfigFile(argv[1], configNumber); 

	if (res != 0)
	{
		printf ("*Error when accessing config file");
		return -1;
	}

	printf("\t+Setting up... ");

	/* We create the thread that will handle the incoming connections */
	pthread_t incoming_connections_thread;
	if ( pthread_create (&incoming_connections_thread, NULL, incoming_connections_handler, NULL) < 0)
	{
		printf ("ERROR\n");
		perror("*Error when creating listening thread");
		return 1;
	}
	printf("OK\n");
	printf("\t+Server ready\n");

	//Now join the thread, so that we dont terminate before the thread
	pthread_join (incoming_connections_thread, NULL);
	
	return 0;
}



