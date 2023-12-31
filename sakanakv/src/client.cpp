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

// indicates the type of data we are serialising
enum {
    SER_NIL = 0, // null
    SER_ERR = 1, // err code and message
    SER_STR = 2, // string
    SER_INT = 3, // 64 bit integer
    SER_ARR = 4, // array of strings
};

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

static int32_t on_response(const uint8_t *data, size_t size) {
    if (size < 1) {
        printf("bad response");
        return -1;
    }

    // switch based on 1st byte (serial code)
    switch(data[0]) {
        case SER_NIL:
            printf("[NIL]\n");
            return 1;
        case SER_ERR:
            if (size < 9) {
                printf("bad response");
                return -1;
            }
            {
                int32_t code;
                uint32_t len;
                memcpy(&code, &data[1], 4); // byte 1-4 store err code
                memcpy(&len, &data[1+4], 4); // byte 5-9 store len
                if (size < 9 + len) {
                    printf("bad response");
                    return -1;
                }
                printf("[ERR] %d %.*s\n", code, len, &data[9]);
                return 9 + len;
            }
        case SER_STR:
            if (size < 5) {
                printf("bad response");
                return -1;
            }
            {
                uint32_t len;
                memcpy(&len, &data[1], 4);
                if (size < 5 + len) {
                    printf("bad response");
                    return -1;
                }
                printf("[STR] %.*s\n", len, &data[5]);
                return 5 + len;
            }
        case SER_INT:
            if (size < 1+8) {
                printf("bad response");
                return -1;
            }
            {
                int64_t val;
                memcpy(&val, &data[1], 8);
                printf("[INT] %ld\n", val);
                return 9;
            }
        case SER_ARR:
            if (size < 5) {
                printf("bad response");
                return -1;
            }
            {
                uint32_t len;
                memcpy(&len, &data[1], 4);
                printf("[ARR] len = %u\n", len);
                size_t total_bytes = 5;
                for (uint32_t i = 0; i < len; i++) {
                    int32_t return_val = on_response(&data[total_bytes], size - total_bytes);
                    if (return_val < 0) {
                        return return_val;
                    }
                    total_bytes += (size_t) return_val;
                }
                printf("[ARR] end\n");
                return (int32_t) total_bytes;
            }
        default:
            printf("bad response");
            return -1;
    }
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

    // handle response
    int32_t return_val = on_response((uint8_t *)&read_buf[4], len);
    if (return_val > 0 && (uint32_t)return_val != len) {
        return_val = -1;
    }

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
