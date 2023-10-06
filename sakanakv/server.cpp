#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>

static void recv_and_reply(int conn_fd) {
    // recv msg from connection socket 
    char read_buf[64] = {};
    ssize_t n = read(conn_fd, read_buf, sizeof(read_buf) - 1);
    if (n < 0) {
        fprintf(stderr, "%s\n", "read() error");
        return;
    }
    printf("[SERVER]: received msg '%s' from client", read_buf);

    // write reply to connection socket
    char write_buf[] = "peko";
    write(conn_fd, write_buf, strlen(write_buf));
}

static void die(const char *msg) {
    fprintf(stderr, "[%d] %s\n", errno, msg);
    abort();
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
    addr.sin_addr.s_addr = ntohl(0); // same as above, on wildcard addr 0.0.0.0
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

        recv_and_reply(conn_fd);
        close(conn_fd);
    }

    return 0;
}