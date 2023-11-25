#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <string>
#include <vector>

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

static int32_t send_req(int conn_fd, const std::vector<std::string> &cmd) {
    uint32_t len = 4;
    for (const std::string &s: cmd) {
        len += 4 + s.size();
    }

    if (len > MAX_MSG_SIZE) {
        return -1;
    }

    // write header
    char write_buf[4 + MAX_MSG_SIZE];
    memcpy(write_buf, &len, 4);
    uint32_t arg_cnt = cmd.size();
    memcpy(&write_buf[4], &arg_cnt, len);

    size_t idx = 8;
    for (const std::string &s : cmd) {
        uint32_t string_size = (uint32_t) s.size();
        memcpy(&write_buf[idx], &string_size, 4); // size
        memcpy(&write_buf[idx+4], s.data(), s.size()); // data
        idx += 4 + s.size();
    }
    return write_all(conn_fd, write_buf, len+4);
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

    // ensure a header is present to avoid segfaults
    if (len < 4) {
        printf("bad response");
        return -1;
    }

    uint32_t rescode = 0;
    memcpy(&rescode, &read_buf[4], 4);
    printf("[CLIENT]: Received [%u] %.*s\n", rescode, len-4, &read_buf[8]);

    return 0;
}

int main(int argc, char **argv) {
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
    
    std::vector<std::string> cmd;
    for (int i = 1; i < argc; i++) {
        cmd.push_back(argv[i]);
    }
    int32_t err = send_req(client_fd, cmd);
    if (err) {
        close(client_fd);
        return 0;
    }
    read_res(client_fd);

    close(client_fd);
    return 0;
}