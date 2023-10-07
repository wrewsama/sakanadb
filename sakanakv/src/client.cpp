#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>

static void die(const char *msg) {
    fprintf(stderr, "[%d] %s\n", errno, msg);
    abort();
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
    
    // write message
    char write_buf[] = "ogey";
    write(client_fd, write_buf, strlen(write_buf));

    // read response
    char read_buf[64];
    ssize_t n = read(client_fd, read_buf, sizeof(read_buf)-1);

    if (n < 0) {
        die("read()");
    }

    printf("[CLIENT]: received response '%s' from server\n", read_buf);
    close(client_fd);
    return 0;
}