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

// read exactly size bytes from the fd and shift the buffer ptr accordingly
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

static int32_t handle_req(int conn_fd) {
    // read header
    char read_buf[4 + MAX_MSG_SIZE + 1]; // 4 bytes for header, 1 byte for escape char
    int32_t err = read_full(conn_fd, read_buf, 4);
    if (err) {
        printf("read() error");
        return err;
    }

    // get the msg length from the header
    uint32_t len = 0;
    memcpy(&len, read_buf, 4);
    if (len > MAX_MSG_SIZE) {
        printf("message is too long");
        return -1;
    }

    // read the msg
    err = read_full(conn_fd, &read_buf[4], len);
    if (err) {
        printf("read() error");
        return err;
    }

    // add in the escape char at the end
    read_buf[4+len] =  '\0';

    // print the msg
    printf("[SERVER]: received response '%s' from client\n", &read_buf[4]);

    // reply
    const char reply[] = "peko";
    char write_buf[4+sizeof(reply)];
    len = (uint32_t) strlen(reply);
    memcpy(write_buf, &len, 4); // write the len into the 1st 4 bytes of the write buf
    memcpy(&write_buf[4], reply, len);
    return write_all(conn_fd, write_buf, 4+len);
}

int main() {
    // open socket
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd< 0) {
        die("socket()");
    }

    // enable reuse of addresses
    int val = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));
    
    // bind address and port number to socket
    struct sockaddr_in addr = {};
    addr.sin_family = AF_INET;
    addr.sin_port = ntohs(3535); // need to convert port num 3535 to host byte order
    addr.sin_addr.s_addr = ntohl(INADDR_ANY); // same as above, on wildcard addr 0.0.0.0
    int res = bind(server_fd, (const sockaddr *)&addr, sizeof(addr));
    if (res) {
        die("bind()");
    }

    // start listening on sock
    res = listen(server_fd, SOMAXCONN);
    if (res) {
        die("listen()");
    }

    while (true) {
        // accept TCP handshake
        struct sockaddr_in client_addr = {};
        socklen_t socklen = sizeof(client_addr);
        int conn_fd = accept(server_fd, (struct sockaddr *)&client_addr, &socklen);
        if (conn_fd < 0) {
            continue;
        }

        // serve that connection
        while (true) {
            int32_t err = handle_req(conn_fd);
            if (err) {
                break;
            }
        }
        close(conn_fd);
    }

    return 0;
}