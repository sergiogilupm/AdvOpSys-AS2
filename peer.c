/*
* File: peer.c
* Author: Sergio Gil Luque
* Version: 1.5
* Date: 10-15-15
*/

#include<stdlib.h>
#include<stdio.h>
#include<string.h>  
#include<sys/socket.h>
#include<arpa/inet.h> 
#include<time.h>
#include<fcntl.h> 
#include<unistd.h> 
#include<pthread.h>
#include <string.h>

#include "uthash.h" // For Hashtable implementation

#define MAX_SERVERS 8

/* Prototypes */
int selectOption(int option, char* key);

/* Global variables */
int port = 0;
int testingMode = 0;
int seqRuns = 1;
int globalOpt = 0;

/* Hash table struct */

typedef struct keyStruct {
	char key[20];
	char value[1000];
	UT_hash_handle hh;
} keyStruct_t;


int serverSockets[MAX_SERVERS];

keyStruct_t *registeredValues = NULL;
keyStruct_t *registeredServers = NULL;


void printLocalKeys() 
{
	struct keyStruct *s;
	printf("The keys that the peer has registered locally are below. Blank if there are no keys\n");
	for(s=registeredValues; s != NULL; s=(struct keyStruct*)(s->hh.next)) 
	{
        	printf("*Key %s* \nValue: %s\n", s->key, s->value);
	}
	printf("\n\n**End of local key registry\n");
}


/* This is just for debugging purposes */
void printServers() 
{
	struct keyStruct *s;
	printf("The keys that the peer has registered locally are below. Blank if there are no keys\n");
	for(s=registeredServers; s != NULL; s=(struct keyStruct*)(s->hh.next)) 
	{
        	printf("*Key %s* \nValue: %s\n", s->key, s->value);
	}
	printf("\n\n**End of local key registry\n");
}


/* Look for server info in the hashtable */
char *getServer (char* index) { 
    	struct keyStruct *s;
    	HASH_FIND_STR (registeredServers, index, s );  
	return s->value;
}

/* Insert server in hash table. Only when setting up */
int insertServer (char* key, char* value) 
{
	struct keyStruct *newValue = malloc (sizeof (struct keyStruct));
	strncpy(newValue -> key, key, strlen(key));
	strncpy(newValue -> value, value, strlen(value));
	HASH_ADD_STR(registeredServers, key, newValue);

	return 0;
}

/* Look for key in the hashtable */
char *getValue (char* key) { 

    	struct keyStruct *s;
    	HASH_FIND_STR (registeredValues, key, s );  /* s: output pointer */
    
	return s->value;
}


/*Insert a new key in the hash table */
int insertValue (char* key, char* value)
{
	struct keyStruct *newValue = malloc (sizeof (struct keyStruct));
	strncpy(newValue -> key, key, strlen(key));
	strncpy(newValue -> value, value, strlen(value));
	HASH_ADD_STR(registeredValues, key, newValue);

	return 0;
}

/* Delete a key */
int deleteKey (char* key) 
{
    	struct keyStruct *s;

    	HASH_FIND_STR (registeredValues, key, s );  /* s: output pointer */
    
	if (s == NULL)
	{
		return -1;
	}
	else
	{
		HASH_DEL(registeredValues, s);
		free(s);
		return 0;
	}
}



int loadConfigFile (char* configFilePath)
{
	/* Open file */

	FILE *fp;
	fp = fopen(configFilePath, "r");
	char line[512];
	int count = 0;
	char strCount[4];	

    	if (fp != NULL)
    	{
		printf ("File %s opened\n", configFilePath);

		while (count < MAX_SERVERS)
		{
			fgets (line, sizeof(line), fp);
			if (strlen(line) != 0)
			{
				sprintf(strCount, "%d", count);
				insertServer(strCount, line);
   				count++;
				//printf("\t+Adding server number %i, with info: %s\n", count ,line);			
			}
		}

		fclose(fp);
		//printServers();	/* For debug purposes*/
	}

	else
	{
		printf("ERROR\n");
		perror("*Error when opening configuration file %s");
		return -1;
	}

	return 0;
}


int getServerFromHash (char* key)
{
	return atoi(key) % 8; /* TODO */
}


int obtainValueFromPeer(char *key, int destPort)
{
	int sock;
	struct sockaddr_in server;
	char *request = malloc (1024 * sizeof(char));
	char *header = malloc(1024 * sizeof(char));
	char *message = malloc (1024 * sizeof(char));
    	char *serverReply = malloc(1024 * sizeof(char));


    	//Create socket
    	sock = socket(AF_INET , SOCK_STREAM , 0);
    	if (sock == -1)
    	{
        	printf("Could not create socket");
		return -1;
    	}
     
    	server.sin_addr.s_addr = inet_addr("127.0.0.1");
    	server.sin_family = AF_INET;
    	server.sin_port = htons( destPort );
 
    	//Connect to remote server
    	if (connect(sock , (struct sockaddr *)&server , sizeof(server)) < 0)
    	{
        	perror("connect failed. Error");
        	return -1;
    	}
     
    	printf("Connected to port %i\n", port);

	strncpy(request, "OBT ", 4);
	strncat(request, key, strlen(key));	

    	printf("Request key %s to peer on port %i...\n", key, port);
    	write(sock, request, 1024);

	if (read (sock, serverReply , 1024) <= 0)
	{
		printf("*Error: Connection with peer lost\n");
		return -1;
	}

	strncpy(header, serverReply, 2);

	if (strcmp(header,"OK")==0)
	{
		strncpy(message, &serverReply[24], 1000);
		printf("\n\n--------------\n\n");
		printf("\t+The value from key %s is: \n\n", key);
		printf("%s\n", message);
		printf("\n--------------\n\n");	
	}
	else
	{
		printf("*Error: Peer returned key not found\n");
	}

    	close(sock);

	return 0;
}

int getCall(char* key)
{
	char *request = malloc(1024 * sizeof(char));
	char *serverReply = malloc(1024 * sizeof(char));
	int serverIndex = 0;
	int sock = 0;
	char *reply = malloc(1024 * sizeof(char));	
	char destPort[10];
 
	/* Preparing the GET call request */
	strncpy(request, "GET ", 4);
	strncat(request, key, strlen(key));
	serverIndex = getServerFromHash(key); /* Gets the index of the server that has to store that key */
	printf("Sending request to server %i\n", serverIndex);
	sock = serverSockets[serverIndex];

	write(sock, request, 1024);
	read (sock , serverReply , 1024);
	strncpy(reply, serverReply, 4);
	
	if (strcmp(reply,"OK  ")==0) 
	{
		strncpy(destPort, &serverReply[24], 50);
		if (atoi(destPort) == port)
		{
			printf("*Error: The port returned by the server belongs to this peer. This means you are asking for a key you registered\n");
			return -1;
		}
		if (obtainValueFromPeer(key,atoi(destPort)) != 0)
		{
			printf("*Error when obtaining the value from the peer\n");
			return -1;
		}
		return 0;
	}
	else
	{
		printf("*Error when looking for the key on the server\n");
		return -1;
	}



	return 0;
}


int putCall(char* key)
{
	char *request = malloc(1024 * sizeof(char));
	char *serverReply = malloc(1024 * sizeof(char));
	int serverIndex = 0;
	int sock = 0;
	int i;
	char auxPort[5];


	sprintf(auxPort, "%d", port);

	/* Preparing the Put call request */
	strncpy(request, "PUT ", 4);
	strncat(request, key, strlen(key));
	for (i = 4 + strlen(key); i < 24; i++)
	{
		strncat(request, " ", 1);
	}
	strncat(request, auxPort, strlen(auxPort));

	serverIndex = getServerFromHash(key);
	printf("Sending request to server %i: %s\n", serverIndex, request);
	sock = serverSockets[serverIndex];

	write(sock, request, 1024);
	read (sock , serverReply , 1024);
	
	if (strcmp(serverReply,"OK  ")==0) 
	{
		return 0;
	}
	else
	{
		return -1;
	}
}




int deleteCall (char* key)
{
	char *request = malloc(1024 * sizeof(char));
	char *serverReply = malloc(1024 * sizeof(char));
	int serverIndex = 0;
	int sock = 0;

 
	/* Preparing delete call request */
	strncpy(request, "DEL ", 4);
	strncat(request, key, strlen(key));
	serverIndex = getServerFromHash(key);
	printf("Sending request to server %i: %s\n", serverIndex, request);
	sock = serverSockets[serverIndex];

	write(sock, request, 1024);
	read (sock , serverReply , 1024);
	
	if (strcmp(serverReply,"OK  ")==0) 
	{
		return 0;
	}
	else
	{
		return -1;
	}
}


int selectOptionTestMode (int option, char *auxLowerRange, char *auxUpperRange)
{
	int lowerRange = atoi (auxLowerRange);
	int upperRange = atoi (auxUpperRange);
	int i;
	char key[20];
	char *operation;
	clock_t begin, end;
	double time_spent;


	switch(option)
	{
	   	case '1' :
			operation = "PUT";
		break;
		
		case '2' :
			operation = "GET";
		break;

		case '3' :
			operation = "DELETE";
		break;

		default:
			operation = "UNKNOWN";
	}
	printf("\n\n -- Testing Mode --\n\n");
	printf("Range keys: %i - %i \n", lowerRange, upperRange);

	if (upperRange <= lowerRange)	
	{
		printf("*Error: Upper range can not be lower than the lower range\n");
		return -1;
	}
	

	begin = clock();
	for (i = lowerRange; i < upperRange; i++)
	{
		sprintf(key, "%d", i);
		selectOption(option, key);
	}
	end = clock();
	time_spent = (double)(end - begin) / CLOCKS_PER_SEC;
	printf("\n************************************\n");
	printf("******** PERFORMANCE STATS *********\n");
	printf("************************************\n\n");

	printf("+Type of operation: %s\n", operation);
	printf("+Number of keys: %i\n", upperRange - lowerRange);
	printf("+Total time: %f seconds\n", time_spent);
	printf("\n************************************\n\n");
	printf("Please wait. The prompt will reappear shortly...\n");

	sleep(5);
	
	return 0;
}


int selectOption(int option, char *overrideKey)
{
	
	char key[20];
	char value[1000];
	char *fixedKey;
	char *fixedValue;


	switch(option)
	{
	   case '1' :  /* PUT OPTION */
	
		printf("Register a new key\n" );
		printf("Type in the key you want to register: \n");

		if (testingMode)
		{
			fixedKey = overrideKey;
			fixedValue = "test";
		}
		else 
		{
			if(fgets (key, 20, stdin) == NULL ) 
			{
				perror("Error when saving the key");	
				return -1;
			}
			//Ask for the new key and save it

			/*In order to get rid of that annoying '\n' escape char*/
			fixedKey = strtok(key, "\n"); 
			printf("Now type the value for that key: \n");

			//Ask for value and save it
			if(fgets (value, 1000, stdin) == NULL ) 
			{
				perror("Error when saving the value");	
				return -1;
			}
			/*In order to get rid of that annoying '\n' escape char*/
			fixedValue = strtok(value, "\n"); 
	
		}
		


		printf ("Registering key %s...\n", fixedKey);
		if (putCall(fixedKey) < 0)
		{
			printf("*Error: Key %s could not be registered into the server\n", fixedKey);
			return -1;
		}
		else
		{
			if (insertValue(fixedKey, fixedValue) < 0)
			{
				printf("*Error: Key could not be created locally\n");
				return -1;
			}
			printf("Key %s registered sucessfully\n", fixedKey);
		}
		break;

	   case '2' : /* PUT OPTION */

		globalOpt = 3;
		//showPromptMessage(globalOpt);

		printf("Search for a key\n" );
		//globalOpt = 2;
		printf("Type in the key you want to search: \n");

		if (testingMode)
		{
			fixedKey = overrideKey;
			
		}
		else
		{
			//Ask for the new key and save it
			if(fgets (key, 20, stdin) == NULL ) 
			{
				perror("Error when saving the key");	
				return -1;
			}
			/*In order to get rid of that annoying '\n' escape char*/
			fixedKey = strtok(key, "\n"); 
	
		}

		if (getCall(fixedKey) < 0)
		{
			printf("*Error when obtaining the value for key %s\n", fixedKey);
			return -1;
		}

		break;


	   case '3' :   /* DELETE OPTION */
	

		printf("Delete a key\n" );
		printf("Type in the key you want to delete: \n");

		if (testingMode)
		{
			fixedKey = overrideKey;
		}
		else
		{
			//Ask for the new key and save it
			if(fgets (key, 20, stdin) == NULL ) 
			{
				perror("Error when saving the key");	
				return -1;
			}
			/*In order to get rid of that annoying '\n' escape char*/
			fixedKey = strtok(key, "\n"); 
		}
		
		
		if (getValue(fixedKey) == NULL)
		{
			printf("*Error: You can not delete keys from other peers\n");
		}
		if (deleteCall(fixedKey) < 0)
		{
			printf("*Error: Key %s could not be deleted\n", fixedKey);
			return -1;
		}
		else
		{
			deleteKey(fixedKey); /*Delete local key*/
			printf("The key %s was sucessfully deleted\n", fixedKey);
		}

		break;

	   case '4' :

		printLocalKeys();
		
		break;

	   default :
	      printf("Invalid option\n" );
	}
	return -1;
}


/*
 * This function will handle the connection for each request
 */

void *connection_handler(void *socket_desc)
{
	int sock = *(int*)socket_desc;
	char *requestReceived = malloc(1024 * sizeof(char));
	char *header = malloc(1024 * sizeof(char));
	char *key = malloc(1024 * sizeof(char));
	char *message = malloc (1024 * sizeof(char));

	char *serverReply = malloc (1024 * sizeof(char));
	     
	read (sock , requestReceived , 1024);
	printf ("+A request has been received: %s\n", requestReceived);

	strncpy(header, requestReceived, 4);
	strncpy(key, &requestReceived[4], 20);


	//The only call that the peer can recieve is an obtain ("OBT") call. Otherwise error

	if (strcmp(header,"OBT ")==0) /* Check for errors and exceptions */
	{	
		printf("+Obtain value from key %s\n", key);
		
		message = getValue(key);

		if (message == NULL)
		{
			printf("*Error: Key %s not found\n", key);
			strncpy(serverReply, "ERR ", 4);
		}
		else 
		{
			strncpy(serverReply, "OK  ", 4);
			strncat(serverReply, "                    ", 20);
			strncat(serverReply, message, strlen(message)); /* We skip the key part of the message */	
		}

		write(sock, serverReply, strlen(serverReply)); /* We send the result back to the peer who made the request */
	}

	else
	{
		printf("*Error: Bad request\n");
		strncpy("ERR ", serverReply, 4);
		//strncpy("BAD_REQUEST ", &serverReply[24], 12);
		write(sock, serverReply, sizeof(serverReply));
	}

    return (void *) 0;
}


void *incoming_connections_handler (void* data)
{
    printf ("\t+Initializing incoming connection handler...\n");
     
    int IN_socket_desc, IN_new_socket, c, *IN_new_sock;
    struct sockaddr_in IN_server, client;
     
    //Create socket
    IN_socket_desc = socket(AF_INET, SOCK_STREAM, 0);

    if (IN_socket_desc == -1)
    {
        perror("*Could not create socket");
	return (void *) 1;
    }
     
    //Prepare the sockaddr_in structure
    IN_server.sin_family = AF_INET;
    IN_server.sin_addr.s_addr = INADDR_ANY;
    IN_server.sin_port = htons( port );
     
    //Bind
    if( bind(IN_socket_desc,(struct sockaddr *) &IN_server , sizeof(IN_server)) < 0)
    {
        perror("*Bind failed");
        return (void *) 1;
    }
     
    //Listen
    listen(IN_socket_desc , 1000); // The peer can handle 1000 simulteaneous connections
     
    //Accept an incoming connection
    printf("\t+Waiting for incoming connections in port %i...\n", port);
    c = sizeof(struct sockaddr_in);
	
    while( (IN_new_socket = accept(IN_socket_desc, (struct sockaddr *)&client, (socklen_t*)&c)) )
    {
        printf("+New connection accepted\n");
         
        pthread_t sniffer_thread;
        IN_new_sock = malloc(1);
        *IN_new_sock = IN_new_socket;
         
        if( pthread_create( &sniffer_thread , NULL ,  connection_handler , (void*) IN_new_sock) < 0)
        {
            perror("*Could not create thread");
            return (void *) 1;
        }
         
        //Now join the thread , so that we dont terminate before the thread
        pthread_join( sniffer_thread , NULL);
    }
     
    if (IN_new_socket<0)
    {
        perror("*Accept failed");
        return (void *) 1;
    }
     
    return (void *) 0;
}

int connectToSingleServer(int index)
{
	int sock;
    	struct sockaddr_in server;
	char *ip = "127.0.0.1";
	int port = 0;
	char strIndex[50];
	char *actualServer;


	sprintf(strIndex, "%d", index);
	actualServer = getServer(strIndex);
	strtok(actualServer, ":");
	port = atoi(strtok(NULL, " "));


	//Create socket
	sock = socket(AF_INET , SOCK_STREAM , 0);
    	if (sock == -1)
	{
		printf("*Could not create socket");
   	}
	     
	server.sin_addr.s_addr = inet_addr(ip);
	server.sin_family = AF_INET;
	server.sin_port = htons( port );
	 
	//Connect to server
	if (connect(sock , (struct sockaddr *)&server , sizeof(server)) < 0)
	{
		perror("*Connection to server failed");
		return -1;
	}
	else
	{
		serverSockets[index] = sock;
	}

	return 0;
}

/* Connects to all servers at startup and keeps the connection open. This way we save the cost of creating and destroying threads and connections */
int connectToServers()
{
	int i;
	int connectedToAll = 0;

	for (i = 0; i < MAX_SERVERS ;i++)
	{
		printf("\t+Connecting to server number %i... ", i+1);
		if (connectToSingleServer(i) == 0) 
		{
			printf("OK\n");
		}
		else
		{
			connectedToAll = -1;
			printf("*ERROR*\n");
		}
	}
	return connectedToAll;
}



int main(int argc , char *argv[])
{

	char auxOption[10];
	int option;  
	int res = 0;
	char lowerRange[20];
	char upperRange[20];


	/* INITIALIZATION */

	srand(time(NULL));

	if (argc <= 2 || argc > 4)
	{
		printf("Usage: %s PEER_PORT CONFIG_FILE_PATH [-t]\n", argv[0]);
		return 0;
	}

	port = atoi(argv[1]);
	char* configFile = argv[2];


	printf("+Initializing peer...\n");
	printf("+Loading config file... ");
	res = loadConfigFile(configFile);
	if (res < 0)
	{
		printf("ERROR\n");
		return -1;	
	}

	if(connectToServers() < 0)
	{
		printf("Connecting to servers... ERROR\n");
		printf("Could not connect to all 8 servers. Aborting...\n");
		return -1;
	}

	printf("\t+Setting up listener... ");
	pthread_t incoming_connections_thread;
		 
	if( pthread_create( &incoming_connections_thread , NULL ,  incoming_connections_handler , NULL ) < 0)
	{
		printf("ERROR\n");
		perror("*Error creating listener thread");
		return -1;
	}

	if (argc == 4 && (strcmp(argv[3], "-t") == 0))
	{
		testingMode = 1;
		printf ("\n\n\n\t************************\n");
		printf ("\t***** TESTING MODE *****\n");
		printf ("\t************************\n\n");
	}

	while(1)
	{
		//globalOpt = 1;

		printf ("Peer initialized. Available options:\n");
		printf("\t[1] - Register a new key (PUT call)\n");
		printf("\t[2] - Search for a key (GET call)\n");
		printf("\t[3] - Delete a Key\n");
		printf("\t[4] - Show local keys\n");
	
		printf ("\n**SELECT OPTION NUMBER:");
		

		fgets(auxOption, sizeof(auxOption), stdin);
		option = auxOption[0];
		if (testingMode == 1 && option != '4')
		{
			printf("Select the range of keys to perform this operation\n");
			printf("Lower range: ");
			fgets(lowerRange, sizeof(lowerRange), stdin);
			printf("Upper range: ");
			fgets(upperRange, sizeof(upperRange), stdin);
			selectOptionTestMode(option, lowerRange, upperRange);
		}
		else
		{
			selectOption(option, NULL);
		}

		
	}
    	
	return 0;
}
