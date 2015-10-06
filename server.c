/*
* File: server.c
* Author: Sergio Gil Luque
* Version: 1.1
* Date: 10-03-15
*/

#include<stdio.h>
#include<string.h>   
#include<sys/socket.h>
#include<arpa/inet.h>
#include<pthread.h>
#include<stdlib.h>
#include <unistd.h>

#include "uthash.h" // For Hashtable implementation
#include "jsmn.h"   // For pasring json files (The config file)

#define PORT 5000

char *configFile = "./configFile.txt";
int port = 0;
char *ip;

//char *port = "0000";



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


//fileStruct_t *registeredFiles = NULL;

keyStruct_t *registeredKeys = NULL;


/* Look for key in the hashtable */
char *getValue (char* key) { /* TODO: HANDLE ERRORS. RETURNS VALUE FROM KEY, OTHERWISE NULL OR EMPTY STRING */

    	struct keyStruct *s;

    	HASH_FIND_STR (registeredKeys, key, s );  /* s: output pointer */
    
	return s->value;
}

int insertValue (char* key, char* value) /* TODO: Handle errors */
{
	struct keyStruct *newValue = malloc (sizeof (struct keyStruct));
	strncpy(newValue -> key, key, strlen(key));
	strncpy(newValue -> value, value, strlen(value));
	HASH_ADD_STR(registeredKeys, key, newValue);

	return 0;
}


int deleteKey (char* key)
{



	return 0;
}



/* Look for file in the hashtable */
/*struct fileStruct *find_file(char* fileName) {

    struct fileStruct *s;

    HASH_FIND_STR (registeredFiles, fileName, s ); 
    return s;
}*/

/*Look for the peer provided inside a file*/
/*struct fileStruct *findPeerInFile(char* peerName, char* fileName) {

    	struct fileStruct *s;
    	struct fileStruct *i;

    	HASH_FIND_STR (registeredFiles, fileName, s ); 

	for(i=s->sub; i != NULL; i=(struct fileStruct*)(i->hh.next)) 
	{
		if (strcmp(i->name, peerName)==0)
		{
			printf("Peer %s already registered file %s. Skipping...\n", peerName, fileName);
			return i;
		}
	}

    	return NULL;
}*/


/* Look if a file is registered */
/*char* searchFile (char *fileName)
{
	struct fileStruct *s;
	struct fileStruct *i;
	char *reply = malloc(2000 * sizeof(char));
	char valStr[10];
	s = find_file(fileName);

	if (s == NULL)
	{
		printf ("ERROR: FILE NOT FOUND");
		strncpy(reply, "ERROR", 5);
	}

	else
	{
		strncpy(reply, "OK", 2);
		for(i=s->sub; i != NULL; i=(struct fileStruct*)(i->hh.next)) 
		{
			snprintf(valStr, sizeof(i->val) + 1, "%i", i->val);
			strncat(reply, " ", 1);
			strncat(reply, i->name, sizeof(i->name));
			strncat(reply, ":", 1);
			strncat(reply, valStr, sizeof(valStr) + 1);
			strncat(reply, " ", 1);
		}
		return reply;
	}	
	return reply;
}*/

/* Only for debugging purposes */
/*void print_users() 
{
	struct fileStruct *s;
	struct fileStruct *i;

	for(s=registeredFiles; s != NULL; s=(struct fileStruct*)(s->hh.next)) 
	{
		for(i=s->sub; i != NULL; i=(struct fileStruct*)(i->hh.next)) 
		{
	        	printf("file %s: peer %s, and port %i\n", s->name, i->name, i->val);
		}
	}
}*/


/* Add a file into the hashtable. If the file already exists, it will only add the peer in the second level hashtable */

/*int addFile (char* newFileName, char *newPeer, int peerPort) {

   	// Find file in hashTable. If it doesnt exist, create new entry and go deeper. If exists, just go deeper
    	struct fileStruct *aux;
	struct fileStruct *aux2;

    	aux = find_file(newFileName); //Checking if the file already exists in the hashtable
	if (aux == NULL)
	{ */
		/* Adding file in first level of the hash table */
		/*aux = malloc(sizeof(struct fileStruct));
    		strcpy(aux->name, newFileName);
    		aux->sub = NULL;
		aux->val = 0;
    		HASH_ADD_STR( registeredFiles, name, aux );*/

		/* Adding peer in second level of hashtable */
		/*fileStruct_t *s = malloc(sizeof(*s));
		strcpy(s->name, newPeer);
		s->sub = NULL;
		s->val = peerPort;
		HASH_ADD_STR(aux->sub, name, s);
	}
	else
	{
		aux2 = findPeerInFile(newPeer, newFileName); //Checking if the peer has already registered that file before
		if (aux2 == NULL)
		{
			fileStruct_t *s = malloc(sizeof(*s));
			strcpy(s->name, newPeer);
			s->sub = NULL;
			s->val = peerPort;
			HASH_ADD_STR(aux->sub, name, s);
		}
	}

	return 0;
}*/




//* NOTHING ABOVE *//





void *connection_handler(void *socket_desc)
{
	char *res;

   	char *requestReceived = malloc(1024 * sizeof(char));
	char *header = malloc(4 * sizeof(char));
	char *key = malloc(20 * sizeof(char));
	char *message = malloc (1000 * sizeof(char));

    	char *serverReply = malloc(1024 * sizeof(char));
    	
    	int sock = *(int*)socket_desc;
     
	
    	read (sock, requestReceived , 1024);
    	printf ("A request has been received by the server: %s\n", requestReceived);

	strncpy(header, requestReceived, 4);
	strncpy(key, &requestReceived[4], 20);

	//message = strtok(request_received ," ");

	// Code can be PUT, GET or DEL

	if (strcmp(header,"PUT")==0) /* TODO */
	{
		strncpy(message, &requestReceived[24], 1000);

		if (insertValue(key, message) < 0)
		{
			printf("*Error registering new key");
			// return false
		}
		else
		{
			printf("New key %s has been registered\n", key);
			// return true
		}
	}

	else if (strcmp(header,"GET")==0) /* TODO */
	{
		res = getValue(key);

		if (strlen(res) == 0)
		{
			printf("*Error obtaining key");
			// return null
		}
		else
		{
			/* Create the reply that will be sent back to the peer */
			strncpy("OK  ", serverReply, 4);
			strncpy(res, &serverReply[24], strlen(res)); /* We skip the key part of the message */
			write(sock, serverReply, strlen(serverReply));
		}
	}
	else if (strcmp(header,"DEL")==0) /* TODO */
	{
		if (deleteKey(key) < 0)
		{
			printf("*Error deleting key %s\n", key);
			// return false boolean
		}
		else
		{
			printf("Key %s has been deleted\n", key);
			// return true boolean
		}
	}

	/*if (strcmp(message,"REGISTRY")==0) 
	{	
		printf("Registry request\n");
		char *fileName = strtok(NULL, " ");
		char *peerAndPortCouple = strtok(NULL, " ");
		char *peerID = strtok (peerAndPortCouple, ":");
		int port = atoi (strtok (NULL, ":"));
		if ( addFile(fileName, peerID, port) == 0) 
		{
			printf ("SUCCESS\n");
			//print_users(); //For debug purposes
			serverReply = "OK";
			write(sock, serverReply, sizeof(serverReply));
		}
		else
		{
			printf ("*Error: The file was not registered\n");
			serverReply = "ERROR FILE_NOT_REGISTERED";
			write(sock, serverReply, sizeof(serverReply));
		}
     		
	} 

	else if (strcmp(message,"SEARCH")==0)   
	{
		printf("Search request\n");
		char *fileName = strtok(NULL, " ");
		serverReply = searchFile(fileName);
		if ( strcmp (serverReply, "ERROR") != 0) 
		{
			printf ("SUCCESS\n");
			// printf ("Search call returned: %s\n", serverReply); //For debug purposes
			write(sock, serverReply, strlen(serverReply));
		}
		else
		{
			printf ("*Error: File %s was not found\n", fileName);
			serverReply = "ERROR FILE_NOT_FOUND";
			write(sock, serverReply, sizeof(serverReply));
		}

	}*/

	else /* TODO */
	{
		printf ("*Error: Bad request\n");

		strncpy("ERR ", serverReply, 4);
		strncpy("BAD_REQUEST", &serverReply[24], 11); /* We skip the key part of the message */
		write(sock, serverReply, sizeof(serverReply));
	}

    	//Free the socket pointer
    	free(socket_desc);
     
    	return (void *) 0;
}


void *incoming_connections_handler (void* data)
{
     
    printf ("Initializing incoming connection handler...\n");
     
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

    /* TODO: Port */
    IN_server.sin_port = htons( PORT );
     
    //Bind
    if( bind(IN_socket_desc,(struct sockaddr *) &IN_server , sizeof(IN_server)) < 0)
    {
        perror("*Error binding");
        return (void *) 1;
    }
     
    //Listen
    listen(IN_socket_desc , 1000); // The server can handle 1000 simulteaneous connections
     
    //Accept and incoming connection
    printf("Waiting for incoming connections in port %i...\n", PORT);
    c = sizeof(struct sockaddr_in);

    while( (IN_new_socket = accept(IN_socket_desc, (struct sockaddr *)&client, (socklen_t*)&c)) )
    {
        puts("+New connection accepted");
         
        pthread_t sniffer_thread;
        IN_new_sock = malloc(1);
        *IN_new_sock = IN_new_socket;
         
        if( pthread_create( &sniffer_thread , NULL ,  connection_handler , (void*) IN_new_sock) < 0)
        {
            perror("*Error creating thread for incoming request");
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
	/* TODO */

	/* Open file */

	FILE *fp;
	fp = fopen(configFilePath, "r");
	char line[512];
	int found = 0;
	//int bytes_read;
	char *var;
	char *servers;
	char *server;
	char *auxIP;
	int auxPort;
	int i;

    	if (fp != NULL)
    	{
		printf ("File %s opened\n", configFilePath);

		while (!found)
		{
			if (fgets (line, sizeof(line), fp))
			{
				var = strtok(line, "=");
				if (strcmp(var, "SERVER_INFO") == 0)
				{
					found = 1;
					strtok(NULL, "[");
					servers = strtok(NULL, "]");

					for (i = 1; i < configNumber; i++)
					{
						strtok(NULL, ",");
					}
					
					if ( i == 8)
					{
						server = strtok(NULL, "]");
					}
					else
					{
						server = strtok(NULL, ",");
					}
					auxIP = strtok(server,":");
					auxPort = atoi(strtok(NULL,":"));

					ip = auxIP;
					port = auxPort;
				}
			}
			else
			{
				fclose (fp);
				return -1;
			}
		}
		
		fclose(fp);
	}

	else
	{
		printf("ERROR\n");
		perror("*Error when opening configuration file");
		return -1;
	}
		
		/*Read file and parse it */

		/* Server info --> Set up with your option */

	return 0;
}


int main(int argc , char *argv[])
{
	int res = 0;
	char *configFile = "test";

	if (argc != 3)
	{
		printf("Usage: %s CONFIG_FILE_PATH CONF_OPTION\n", argv[0]);
		return 0;
	}

	/* INITIALIZATION */
	printf("Initializing server... \n");
	printf("\t+Accessing config file... ");
	int configNumber = atoi(argv[2]);

	// Accessing configuration file and loading internal values
	res = loadConfigFile(configFile, configNumber);
	if (res == 0)
	{
		printf ("OK\n");
	}
	else
	{
		printf ("ERROR\n");
		perror ("*Error when accessing config file");
		return -1;
	}

	printf("\t+Setting up...");
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
	pthread_join(incoming_connections_thread , NULL);
	
	return 0;
}



