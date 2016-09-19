/* Redis benchmark utility.
 *
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "fmacros.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>
#include <pthread.h>
#include <stdint.h>

#include "ae.h"
#include "anet.h"
#include "sds.h"
#include "adlist.h"
#include "zmalloc.h"

#define REPLY_RETCODE 0
#define REPLY_BULK 1

#define CLIENT_CONNECTING 0
#define CLIENT_SENDQUERY 1
#define CLIENT_READREPLY 2

#define MAX_LATENCY 5000

#define MCB_NOTUSED(V) ((void) V)

struct thread_info {
    pthread_t tid;
    int id;
    aeEventLoop *el;
    list *clients;
    int numclients;
    int liveclients;
    int numrequests;
    int donerequests;
    int *latency;
    sds payload;
};

static struct config {
    int debug;
    int numthreads;
    int numclients;
    int requests;
    int keysize;
    int datasize;
    int randomkeys;
    int randomkeys_keyspacelen;
    char *hostip;
    int hostport;
    int keepalive;
    long long start;
    char *title;
    int quiet;
    int loop;
    int idlemode;
    struct thread_info *threads;
} config;


typedef struct _client {
    struct thread_info *owner; /* Client own to which thread */
    int state;
    int fd;
    sds obuf;
    sds ibuf;
    int mbulk;          /* Number of elements in an mbulk reply */
    int readlen;        /* readlen == -1 means read a single line */
    int totreceived;
    unsigned int written;        /* bytes of 'obuf' already written */
    int replytype;
    long long start;    /* start time in milliseconds */
} *client;

/* Prototypes */
static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void createMissingClients(client c);

/* Implementation */
static long long mstime(void) {
    struct timeval tv;
    long long mst;

    gettimeofday(&tv, NULL);
    mst = ((long)tv.tv_sec)*1000;
    mst += tv.tv_usec/1000;
    return mst;
}

static void freeClient(client c) {
    listNode *ln;
    struct thread_info *thread;

    thread = c->owner;
    aeDeleteFileEvent(thread->el,c->fd,AE_WRITABLE);
    aeDeleteFileEvent(thread->el,c->fd,AE_READABLE);
    sdsfree(c->ibuf);
    sdsfree(c->obuf);
    close(c->fd);
    zfree(c);
    thread->liveclients--;
    ln = listSearchKey(thread->clients,c);
    assert(ln != NULL);
    listDelNode(thread->clients,ln);
}

static void resetClient(client c) {
    struct thread_info *thread;

    thread = c->owner;
    aeDeleteFileEvent(thread->el,c->fd,AE_WRITABLE);
    aeDeleteFileEvent(thread->el,c->fd,AE_READABLE);
    aeCreateFileEvent(thread->el,c->fd, AE_WRITABLE,writeHandler,c);
    sdsfree(c->ibuf);
    c->ibuf = sdsempty();
    c->readlen = (c->replytype == REPLY_BULK) ? -1 : 0;
    c->mbulk = -1;
    c->written = 0;
    c->totreceived = 0;
    c->state = CLIENT_SENDQUERY;
    c->start = mstime();
    createMissingClients(c);
}

static void randomizeClientKey(client c) {
    char *p;
    char buf[32];
    long r;

    p = strstr(c->obuf, "_rand");
    if (!p) return;
    p += 5;
    r = random() % config.randomkeys_keyspacelen;
    sprintf(buf,"%ld",r);
    memcpy(p,buf,strlen(buf));
}

static void prepareClientForReply(client c, int type) {
    if (type == REPLY_BULK) {
        c->replytype = REPLY_BULK;
        c->readlen = -1;
    } else {
        c->replytype = type;
        c->readlen = 0;
    }
}

static void clientDone(client c) {
    long long latency;
    struct thread_info *thread;

    thread = c->owner;
    thread->donerequests ++;
    latency = mstime() - c->start;
    if (latency > MAX_LATENCY) latency = MAX_LATENCY;
    thread->latency[latency]++ ;
    if (thread->donerequests >= thread->numrequests) {
        freeClient(c);
        aeStop(thread->el);
        return;
    }
    if (config.keepalive) {
        resetClient(c);
        if (config.randomkeys) randomizeClientKey(c);
    } else {
        thread->liveclients--;
        createMissingClients(c);
        thread->liveclients++;
        freeClient(c);
    }
}

/* Read a length from the buffer pointed to by *p, store the length in *len,
 * and return the number of bytes that the cursor advanced. */
static int readLen(char *p, int *len) {
    char *tail = strstr(p,"\r\n");
    if (tail == NULL)
        return 0;

    if (p[0] == 'E') { /* END -- key not present */
        *len = -1;
    } else {
        char *x = tail;
        while (*x != ' ') x--;
        *tail = '\0';
        *len = atoi(x)+5; /* We add 5 bytes for the final "END\r\n" */
    }
    return tail+2-p;
}

static void readHandler(aeEventLoop *el, int fd, void *privdata, int mask)
{
    char buf[1024*16], *p;
    int nread, pos=0, len=0;
    client c = privdata;
    MCB_NOTUSED(el);
    MCB_NOTUSED(fd);
    MCB_NOTUSED(mask);

    nread = read(c->fd,buf,sizeof(buf));
    if (nread == -1) {
        fprintf(stderr, "Reading from socket: %s\n", strerror(errno));
        freeClient(c);
        return;
    }
    if (nread == 0) {
        fprintf(stderr, "EOF from client\n");
        freeClient(c);
        return;
    }
    c->totreceived += nread;
    c->ibuf = sdscatlen(c->ibuf,buf,nread);
    len = sdslen(c->ibuf);

    if (c->replytype == REPLY_RETCODE) {
        /* Check if the first line is complete. This is everything we need
         * when waiting for an integer or status code reply.*/
        if ((p = strstr(c->ibuf,"\r\n")) != NULL)
            goto done;
    } else if (c->replytype == REPLY_BULK) {
        int advance = 0;
        if (c->readlen < 0) {
            advance = readLen(c->ibuf+pos,&c->readlen);
            if (advance) {
                pos += advance;
                if (c->readlen == -1) {
                    goto done;
                } else {
                    /* include the trailing \r\n */
                    c->readlen += 2;
                }
            } else {
                goto skip;
            }
        }

        int canconsume;
        if (c->readlen > 0) {
            canconsume = c->readlen > (len-pos) ? (len-pos) : c->readlen;
            c->readlen -= canconsume;
            pos += canconsume;
        }

        if (c->readlen == 0)
            goto done;
    }

skip:
    c->ibuf = sdsrange(c->ibuf,pos,-1);
    return;
done:
    clientDone(c);
    return;
}

static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask)
{
    client c = privdata;
    MCB_NOTUSED(el);
    MCB_NOTUSED(fd);
    MCB_NOTUSED(mask);

    if (c->state == CLIENT_CONNECTING) {
        c->state = CLIENT_SENDQUERY;
        c->start = mstime();
    }
    if (sdslen(c->obuf) > c->written) {
        void *ptr = c->obuf+c->written;
        int len = sdslen(c->obuf) - c->written;
        int nwritten = write(c->fd, ptr, len);
        if (nwritten == -1) {
            if (errno != EPIPE)
                fprintf(stderr, "Writing to socket: %s\n", strerror(errno));
            freeClient(c);
            return;
        }
        c->written += nwritten;
        if (sdslen(c->obuf) == c->written) {
            aeDeleteFileEvent(c->owner->el,c->fd,AE_WRITABLE);
            aeCreateFileEvent(c->owner->el,c->fd,AE_READABLE,readHandler,c);
            c->state = CLIENT_READREPLY;
        }
    }
}

static client createClient(struct thread_info *thread) {
    client c = zmalloc(sizeof(struct _client));
    char err[ANET_ERR_LEN];

    c->owner = thread;
    c->fd = anetTcpNonBlockConnect(err,config.hostip,config.hostport);
    if (c->fd == ANET_ERR) {
        zfree(c);
        fprintf(stderr,"Connect: %s\n",err);
        return NULL;
    }
    anetTcpNoDelay(NULL,c->fd);
    c->obuf = sdsempty();
    c->ibuf = sdsempty();
    c->mbulk = -1;
    c->readlen = 0;
    c->written = 0;
    c->totreceived = 0;
    c->state = CLIENT_CONNECTING;
    aeCreateFileEvent(thread->el, c->fd, AE_WRITABLE, writeHandler, c);
    thread->liveclients++;
    listAddNodeTail(thread->clients,c);
    return c;
}

static void createMissingClients(client c) {
    struct thread_info *thread = c->owner;
    while(thread->liveclients < thread->numclients) {
        client new = createClient(thread);
        if (!new) continue;
        sdsfree(new->obuf);
        new->obuf = sdsdup(c->obuf);
        if (config.randomkeys) randomizeClientKey(c);
        prepareClientForReply(new,c->replytype);
    }
}

static void showLatencyReport(void) {
    int i, j, seen = 0, numthreads, donerequests = 0;
    float perc, reqpersec;
    int latency[MAX_LATENCY+1], totlatency;
    memset(latency, 0, sizeof(int) * (MAX_LATENCY+1));
    
    totlatency = mstime()-config.start;
    numthreads = config.numthreads;
    for (i = 0; i <= MAX_LATENCY; i++) {
        for(j = 0; j < numthreads; j++) {
            latency[i] += config.threads[j].latency[i];
        }
    }
    for (i = 0; i < numthreads; i++) {
        donerequests += config.threads[i].donerequests;
    }

    reqpersec = (float)donerequests/((float)totlatency/1000);
    if (!config.quiet) {
        printf("====== %s ======\n", config.title);
        printf("  %d requests completed in %.2f seconds\n", donerequests,
            (float)totlatency/1000);
        printf("  %d parallel clients\n", config.numclients);
        printf("  %d bytes payload\n", config.datasize);
        printf("  keep alive: %d\n", config.keepalive);
        printf("\n");
        for (j = 0; j <= MAX_LATENCY; j++) {
            if (latency[j]) {
                seen += latency[j];
                perc = ((float)seen*100)/donerequests;
                printf("%.2f%% <= %d milliseconds\n", perc, j);
            }
        }
        printf("%.2f requests per second\n\n", reqpersec);
    } else {
        printf("%s: %.2f requests per second\n", config.title, reqpersec);
    }
}


void parseOptions(int argc, char **argv) {
    int i;

    for (i = 1; i < argc; i++) {
        int lastarg = i==argc-1;
        
        if (!strcmp(argv[i],"-c") && !lastarg) {
            config.numclients = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-t") && !lastarg) {
            config.numthreads = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-n") && !lastarg) {
            config.requests = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-k") && !lastarg) {
            config.keepalive = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-h") && !lastarg) {
            char *ip = zmalloc(32);
            if (anetResolve(NULL,argv[i+1],ip) == ANET_ERR) {
                printf("Can't resolve %s\n", argv[i]);
                exit(1);
            }
            free(config.hostip);
            config.hostip = ip;
            i++;
        } else if (!strcmp(argv[i],"-p") && !lastarg) {
            config.hostport = atoi(argv[i+1]);
            i++;
        } else if (!strcmp(argv[i],"-d") && !lastarg) {
            config.datasize = atoi(argv[i+1]);
            i++;
            if (config.datasize < 1) config.datasize=1;
            if (config.datasize > 1024*1024*1024) config.datasize = 1024*1024*1024;
        } else if (!strcmp(argv[i],"-r") && !lastarg) {
            config.randomkeys = 1;
            config.randomkeys_keyspacelen = atoi(argv[i+1]);
            if (config.randomkeys_keyspacelen < 0)
                config.randomkeys_keyspacelen = 0;
            i++;
        } else if (!strcmp(argv[i],"-q")) {
            config.quiet = 1;
        } else if (!strcmp(argv[i],"-l")) {
            config.loop = 1;
        } else if (!strcmp(argv[i],"-D")) {
            config.debug = 1;
        } else if (!strcmp(argv[i],"-I")) {
            config.idlemode = 1;
        } else {
            printf("Wrong option '%s' or option argument missing\n\n",argv[i]);
            printf("Usage: mc-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests]> [-k <boolean>]\n\n");
            printf(" -h <hostname>      Server hostname (default 127.0.0.1)\n");
            printf(" -p <hostname>      Server port (default 6379)\n");
            printf(" -c <clients>       Number of parallel connections (default 50)\n");
            printf(" -t <threads>       Number of parallel threads (default 4)\n");
            printf(" -n <requests>      Total number of requests (default 10000)\n");
            printf(" -d <size>          Data size of SET/GET value in bytes (default 2)\n");
            printf(" -k <boolean>       1=keep alive 0=reconnect (default 1)\n");
            printf(" -r <keyspacelen>   Use random keys for SET/GET/INCR, random values for SADD\n");
            printf("  Using this option the benchmark will get/set keys\n");
            printf("  in the form mykey_rand000000012456 instead of constant\n");
            printf("  keys, the <keyspacelen> argument determines the max\n");
            printf("  number of values for the random number. For instance\n");
            printf("  if set to 10 only rand000000000000 - rand000000000009\n");
            printf("  range will be allowed.\n");
            printf(" -q                 Quiet. Just show query/sec values\n");
            printf(" -l                 Loop. Run the tests forever\n");
            printf(" -I                 Idle mode. Just open N idle connections and wait.\n");
            printf(" -D                 Debug mode. more verbose.\n");
            exit(1);
        }
    }
}

int showThroughput(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    int i, donerequests = 0;
    MCB_NOTUSED(eventLoop);
    MCB_NOTUSED(id);
    MCB_NOTUSED(clientData);

    float dt = (float)(mstime()-config.start)/1000.0;
    for (i = 0; i < config.numthreads; i++) {
        donerequests += config.threads[i].donerequests;
    }
    float rps = (float)donerequests/dt;
    printf("%s: %.2f\r", config.title, rps);
    fflush(stdout);
    return 250; /* every 250ms */
}

void run(void *args) {
    client c;
    struct thread_info *thread;
    
    thread = (struct thread_info *) args;
    c = createClient(thread);
    if (!c) exit(1);

    // first thread show throughput
    if (thread->id == 0) {
        aeCreateTimeEvent(thread->el,1,showThroughput,NULL,NULL);
    }
    if (thread->payload) {
        c->obuf = sdscatlen(c->obuf, thread->payload, sdslen(thread->payload));
    }
    /*
    c->obuf = sdscatprintf(c->obuf,"set foo_rand000000000000 0 0 %d\r\n",config.datasize);
    {
        char *data = zmalloc(config.datasize+2);
        memset(data,'x',config.datasize);
        data[config.datasize] = '\r';
        data[config.datasize+1] = '\n';
        c->obuf = sdscatlen(c->obuf,data,config.datasize+2);
        zfree(data);
    }
    */
    prepareClientForReply(c,REPLY_RETCODE);
    createMissingClients(c);
    aeMain(thread->el);
    fprintf(stderr, "thread-%d stoped.\n", thread->id);
}

void freeThreads(int num, struct thread_info *threads) {
    int i;
    struct thread_info *thread;

    for (i = 0; i < num; i++) {
        thread = &threads[i];
        zfree(thread->latency);
        listNode *ln = thread->clients->head, *next;
        while(ln) {
            next = ln->next;
            freeClient(ln->value);
            ln = next;
        }
        listRelease(thread->clients);
        aeDelteAllTimeEvent(thread->el);
        aeDeleteEventLoop(thread->el);
    }
    free(threads);
}

void spawnThreads(int numthreads, int numclients, int numrequests, sds payload) {
    int i, rest, rc;
    struct thread_info *threads;

    if (numthreads <= 0) numthreads = 4;
    if (numthreads > 64) numthreads = 64;

    // init thread info
    threads = malloc(numthreads * sizeof(struct thread_info));
    if (!threads) {
        return;
    }
    for (i = 0; i < numthreads; i++) {
        threads[i].id = i;
        threads[i].el = aeCreateEventLoop();
        threads[i].liveclients = 0;
        threads[i].donerequests = 0;
        threads[i].numclients = numclients / numthreads;
        threads[i].numrequests = numrequests / numthreads;
        threads[i].clients = listCreate();
        threads[i].latency = zcalloc(sizeof(int)*(MAX_LATENCY+1)); 
        threads[i].payload = NULL;
        if (sdslen(payload)) {
            threads[i].payload = sdsdup(payload);
        }
    }
    rest = numclients % numthreads;
    for (i = 0; i < rest; i++) {
        threads[i].numclients++;
    }
    rest = numrequests % numthreads;
    for (i = 0; i < rest; i++) {
        threads[i].numrequests++;
    }
    config.threads = threads;

    // start thread connection
    for (i = 0; i < numthreads; i++) {
        rc = pthread_create(&threads[i].tid, NULL, (void*(*)(void*))run, &threads[i]);
        if (rc < 0 ) {
            // TODO: create thread errors
            fprintf(stderr, "create thread error, %s\n", strerror(rc));
            free(threads);
            return;
        }
    }
    config.threads = threads;
    config.numthreads = numthreads;
}

static void prepareForBenchmark(char *title) {
    config.title = title;
    config.start = mstime();
}

static void endBenchmark(void) {
    int i;

    for (i = 0; i < config.numthreads; i++) {
        pthread_join(config.threads[i].tid, NULL);
    }
    showLatencyReport();
    freeThreads(config.numthreads, config.threads);
}

int main(int argc, char **argv) {

    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    config.debug = 0;
    config.numthreads = 4;
    config.numclients = 50;
    config.requests = 10000;
    // TODO: add show throughput later
    config.keepalive = 1;
    config.datasize = 3;
    config.randomkeys = 0;
    config.randomkeys_keyspacelen = 0;
    config.quiet = 0;
    config.loop = 0;
    config.idlemode = 0;
    config.hostip = strdup("127.0.0.1");
    config.hostport = 11211;

    parseOptions(argc,argv);
    if (config.requests > config.numclients) {
        config.numclients = config.requests;
    }
    if (config.numthreads > config.numthreads) {
        config.numthreads = config.numthreads;
    }

    if (config.keepalive == 0) {
        printf("WARNING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux and 'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order to use a lot of clients/requests\n");
    }
    do {
        sds payload;

        /* SET benchmark*/
        prepareForBenchmark("SET");
        payload = sdsempty();
        payload = sdscatprintf(payload,"set foo_rand000000000000 0 0 %d\r\n",config.datasize);
        {
            char *data = zmalloc(config.datasize+2);
            memset(data,'x',config.datasize);
            data[config.datasize] = '\r';
            data[config.datasize+1] = '\n';
            payload = sdscatlen(payload,data,config.datasize+2);
            zfree(data);
        }
        spawnThreads(config.numthreads, config.numclients, config.requests, payload);
        if (!config.threads) exit(1);
        sdsfree(payload);
        endBenchmark();

        /* GET benchmark*/
        prepareForBenchmark("GET");
        payload = sdsempty();
        payload = sdscat(payload,"get foo_rand000000000000\r\n");
        spawnThreads(config.numthreads, config.numclients, config.requests, payload);
        if (!config.threads) exit(1);
        sdsfree(payload);
        endBenchmark();

        printf("\n");
    } while(config.loop);
    zfree(config.hostip);
    return 0;
}
