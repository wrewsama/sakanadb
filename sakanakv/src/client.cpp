#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>

const size_t MAX_MSG_SIZE = 4096;

static void die(const char *msg) {
    fprintf(stderr, "[%d] %s\n", errno, msg);
    abort();
}

// read exactly size bytes from the fd
static int32_t read_full(int fd, char *buf, size_t size) {
    while (size > 0) {
        ssize_t res = read(fd, buf, size);
        if (res <= 0) {
            // error or unexpected EOF reached
            return -1; 
        }

        size -= (size_t) res;
        buf += res;
    }
    return 0;
}

// write size bytes from the buffer into the fd
static int32_t write_all(int fd, char *buf, size_t size) {
    while (size > 0) {
        ssize_t res = write(fd, buf, size);
        if (res <= 0) {
            // error
            return -1; 
        }

        size -= (size_t) res;
        buf += res;
    }
    return 0;
}

static int32_t send_req(int conn_fd, const char *req) {
    uint32_t len = (uint32_t) strlen(req);
    if (len > MAX_MSG_SIZE) {
        return -1;
    }

    // write
    char write_buf[4 + MAX_MSG_SIZE];
    memcpy(write_buf, &len, 4);
    memcpy(&write_buf[4], req, len);
    int32_t err = write_all(conn_fd, write_buf, len+4);
    if (err) {
        return err;
    }

    return 0;
}

static int32_t read_res(int conn_fd) {
    // read
    char read_buf[4 + MAX_MSG_SIZE + 1];
    int32_t err = read_full(conn_fd, read_buf, 4);
    if (err) {
        return err;
    }

    uint32_t len = 0;
    memcpy(&len, read_buf, 4);
    if (len > MAX_MSG_SIZE) {
        return -1;
    }
    err = read_full(conn_fd, &read_buf[4], len);
    if (err) {
        return err;
    }
    read_buf[4+len] = '\0';
    printf("[CLIENT]: received response '%s' from server\n", &read_buf[4]);
    return 0;
}

int main() {
    // open up client socket
    int client_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (client_fd < 0) {
        die("socket()");
    }

    // tcp handshake with server
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(3535);
    addr.sin_addr.s_addr = ntohl(INADDR_LOOPBACK); // address: 127.0.0.1
    int res = connect(client_fd, (const struct sockaddr *)&addr, sizeof(addr));
    if (res) {
        die("connect()");
    }
    
    const char *queries[3] = {"test", "testy", "tested"};
    for (size_t i = 0; i < 3; i++) {
        int32_t err = send_req(client_fd, queries[i]);
        if (err) {
            close(client_fd);
            return 0;
        }
    }

    for (size_t i = 0; i < 3; i++) {
        int32_t err = read_res(client_fd);
        if (err) {
            close(client_fd);
            return 0;
        }
    }

    close(client_fd);
    return 0;
}