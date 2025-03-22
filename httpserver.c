#include "connection.h"
#include "debug.h"
#include "queue.h"
#include "request.h"
#include "response.h"
#include "rwlock.h"
#include "listener_socket.h"
#include "iowrapper.h"
#include "protocol.h"
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/select.h>
#include <err.h>
#include <fcntl.h>
#include <sys/file.h>

rwlock_t *rws[256];
pthread_mutex_t lock_table_mutex = PTHREAD_MUTEX_INITIALIZER,
                audit_log_mutex = PTHREAD_MUTEX_INITIALIZER;
queue_t *query;

void audit_log(const char *method, const char *uri, int status_code, const char *req_id) {
    pthread_mutex_lock(&audit_log_mutex);
    fprintf(stderr, "%s,%s,%d,%s\n", method, uri, status_code, req_id ? req_id : "0");
    pthread_mutex_unlock(&audit_log_mutex);
}

void handle_unsupported(conn_t *conn) {
    audit_log("UNSUPPORTED", "", 501, conn_get_header(conn, "Request-Id"));
    conn_send_response(conn, &RESPONSE_NOT_IMPLEMENTED);
}

void put_audit(char *uri, const Response_t *res, conn_t *conn) {
    audit_log("PUT", uri, response_get_code(res), conn_get_header(conn, "Request-Id"));
    conn_send_response(conn, res);
}

void clean_up(char *uri, conn_t *conn, int fd, int fd2, rwlock_t *rw, char buff_tem[]) {
    put_audit(uri, &RESPONSE_INTERNAL_SERVER_ERROR, conn);
    close(fd), close(fd2), writer_unlock(rw), unlink(buff_tem);
    return;
}

void unlinking(char *uri, conn_t *conn, const Response_t *res, rwlock_t *rw, char buff_tem[]) {
    put_audit(uri, res, conn);
    writer_unlock(rw), unlink(buff_tem);
}

unsigned int hash_filename(const char *uri) {
    unsigned int hash = 0;
    while (*uri)
        hash = (hash << 5) + *uri++;
    return hash % 256;
}

void handle_get(conn_t *conn) {
    const Response_t *res = NULL;
    char *uri = conn_get_uri(conn);

    rwlock_t *file_lock = rws[hash_filename(uri)];

    reader_lock(file_lock);
    struct stat st;
    int fd = open(uri, O_RDWR, 0600);
    if (fd < 0 || fstat(fd, &st) < 0) {
        if (fstat(fd, &st) < 0)
            close(fd);
        if (fd < 0)
            res = (errno == 21 || errno == 13) ? &RESPONSE_FORBIDDEN : &RESPONSE_NOT_FOUND;
        else
            res = &RESPONSE_INTERNAL_SERVER_ERROR;
        conn_send_response(conn, res);
    } else {
        res = &RESPONSE_OK;
        conn_send_file(conn, fd, st.st_size);
    }
    fprintf(
        stderr, "GET,%s,%d,%s\n", uri, response_get_code(res), conn_get_header(conn, "Request-Id"));
    close(fd);
    reader_unlock(file_lock);
}

void handle_put(conn_t *conn) {
    char *uri = conn_get_uri(conn), buff_tem[] = "/tmp/httpserver_put_XXXXXX", buff[5000];
    ssize_t r_bytes;
    int fd2 = mkstemp(buff_tem);
    const Response_t *res = conn_recv_file(conn, fd2);

    if (fd2 < 0 || res != NULL) {
        put_audit(uri, (fd2 < 0) ? &RESPONSE_INTERNAL_SERVER_ERROR : res, conn);
        if (res != NULL)
            close(fd2), unlink(buff_tem);
        return;
    }
    close(fd2);

    rwlock_t *rw = rws[hash_filename(uri)];
    writer_lock(rw);
    int fd_acc = !access(uri, 0), fd = open(uri, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        unlinking(uri, conn,
            (errno == 13 || errno == 21) ? &RESPONSE_FORBIDDEN : &RESPONSE_INTERNAL_SERVER_ERROR,
            rw, buff_tem);
        return;
    }
    fd2 = open(buff_tem, O_RDONLY);
    if (fd2 < 0)
        clean_up(uri, conn, fd, fd2, rw, buff_tem);

    while ((r_bytes = read(fd2, buff, sizeof(buff)))) {
        if (write(fd, buff, r_bytes) < 0)
            clean_up(uri, conn, fd, fd2, rw, buff_tem);
    }
    close(fd), close(fd2);
    unlinking(uri, conn, (!fd_acc) ? &RESPONSE_CREATED : &RESPONSE_OK, rw, buff_tem);
}

void handle_connection(int connfd) {
    conn_t *conn = conn_new(connfd);
    const Response_t *response = conn_parse(conn);

    if (response != NULL) {
        conn_send_response(conn, response);
    } else {
        const Request_t *request = conn_get_request(conn);
        (request == &REQUEST_GET)   ? handle_get(conn)
        : (request == &REQUEST_PUT) ? handle_put(conn)
                                    : handle_unsupported(conn);
    }
    conn_delete(&conn);
}

void *worker_thread(void *arg) {
    (void) arg;
    while (1) {
        int connfd;
        if (!queue_pop(query, (void **) &connfd))
            continue;
        handle_connection(connfd), close(connfd);
    }
    return NULL;
}

int main(int argc, char **argv) {
    for (int i = 0; i < 256; i++)
        rws[i] = rwlock_new(READERS, 0);
    int opt, threads = 4;

    while ((opt = getopt(argc, argv, "t:")) != -1) {
        if (opt == 't') {
            threads = atoi(optarg);
            if (threads <= 0) {
                fprintf(stderr, "Invalid number of threads\n");
                return 1;
            }
        } else {
            fprintf(stderr, "Usage: %s [-t threads] <port>\n", argv[0]);
            return 1;
        }
    }
    if (optind >= argc) {
        fprintf(stderr, "Error: Port number is required\n");
        return 1;
    }
    char *endptr = NULL;
    size_t port = (size_t) strtoull(argv[optind], &endptr, 10);
    if ((endptr && *endptr != '\0') || port < 1 || port > 65535) {
        fprintf(stderr, "Invalid port\n");
        return 1;
    }
    signal(SIGPIPE, SIG_IGN);
    Listener_Socket_t *socket = ls_new(port);
    if (!socket)
        err(1, "listener socket isn't initialized");

    query = queue_new(threads);
    pthread_t *pthreads = malloc(threads * sizeof(pthread_t));
    if (!pthreads)
        err(1, "No pthreads found");

    for (int i = 0; i < threads; i++)
        if (pthread_create(&pthreads[i], NULL, &worker_thread, (void *) query) != 0)
            err(1, "Creating pthreads failed");

    while (1) {
        int connfd = ls_accept(socket);
        if (connfd >= 0)
            queue_push(query, (void *) (uintptr_t) connfd);
    }
    for (int i = 0; i < 256; i++)
        rwlock_delete(&rws[i]);

    free(pthreads);
    queue_delete(&query);
    ls_delete(&socket);
    return 0;
}
