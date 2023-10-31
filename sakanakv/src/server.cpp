#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/ip.h>
#include <vector>

const size_t MAX_MSG_SIZE = 4096;

enum {
    STATE_REQ = 0,
    STATE_RES = 1,
    STATE_END = 2
};

struct Conn {
    int fd = -1;
    uint32_t state = 0;
    size_t read_buf_size; // number of bytes saved in read buffer
    uint8_t read_buf[4+MAX_MSG_SIZE];
    size_t write_buf_size; // number of bytes stored in write buffer
    size_t write_buf_sent; // number of bytes written from the write buffer
    uint8_t write_buf[4+MAX_MSG_SIZE];
};

static void die(const char *msg) {
    fprintf(stderr, "[%d] %s\n", errno, msg);
    abort();
}

static void fd_set_nonblocking(int fd) {
    // TODO
    return;
}

static bool try_one_req(Conn *conn) {
    if (conn->read_buf_size < 4) {
        // insufficient data in buf, can't read header, try again next iter
        return false;
    }

    uint32_t len = 0;
    memcpy(&len, conn->read_buf, 4); // read len from header 
    if (len > MAX_MSG_SIZE) {
        printf("msg too long");
        conn->state = STATE_END;
        return false;
    }
    if (4 + len > conn->read_buf_size) {
        // insufficient data in buf, try again next iter
        return false;
    }

    printf("[SERVER] Received msg %.*s from client", len, &conn->read_buf[4]);

    // store response in write buf
    memcpy(conn->write_buf, conn->read_buf, len+4);
    conn->write_buf_size = 4 + len;

    // shift the next request in the buffer forward
    size_t remaining_bytes = conn->read_buf_size - 4 - len;
    if (remaining_bytes) {
        memmove(conn->read_buf, &conn->read_buf[4 + len], remaining_bytes);
    }
    conn->read_buf_size = remaining_bytes;

    // update state
    conn->state = STATE_RES;
    handle_state_res(conn);

    // if the req was fully processed, continue outer loop
    return (conn->state == STATE_REQ);
}

static bool try_fill_buffer(Conn *conn) {
    ssize_t res;

    do {
        // get num of bytes left to fill buffer
        size_t cap = sizeof(conn->read_buf) - conn->read_buf_size; 

        // read at most cap bytes
        res = read(conn->fd, &conn->read_buf[conn->read_buf_size], cap);
    } while (res < 0 && errno == EINTR);

    if (res < 0 && errno == EAGAIN) {
        // fd is not ready to be read
        return false;
    }
    if (res < 0) {
        // fd is ready but still throws error
        printf("read error");
        conn->state = STATE_END;
        return false;
    }
    if (res == 0) {
        if (conn->read_buf_size > 0) {
            printf("unexpected EOF");
        } else {
            printf("EOF");
        }
        conn->state = STATE_END;
        return false;
    }

    // update read buffer size
    conn->read_buf_size += (size_t) res;

    // process the requests
    while (try_one_req(conn)) {}

    return (conn->state == STATE_REQ);
}

static void handle_state_req(Conn *conn) {
    while (try_fill_buffer(conn)) {}
}

static bool try_flush_buffer(Conn *conn) {
    ssize_t res = 0;
    do {
        size_t remaining_bytes = conn->write_buf_size - conn->write_buf_sent;
        res = write(conn->fd, &conn->write_buf[conn->write_buf_sent], remaining_bytes);
    } while (res < 0 && errno == EINTR);

    if (res < 0 && errno == EAGAIN) {
        return false;
    }

    if (res < 0) {
        printf("write() error");
        conn->state = STATE_END;
        return false;
    }

    conn->write_buf_sent += (size_t) res;
    if (conn->write_buf_sent == conn->write_buf_size) {
        // change state back since msg has been fully sent
        conn->state = STATE_REQ;
        conn->write_buf_sent = 0;
        conn->write_buf_size = 0;
        return false;
    }

    // try write again
    return true;
}

static void handle_state_res(Conn *conn) {
    while (try_flush_buffer(conn)) {}
}

static void connection_io(Conn *conn) {
    if (conn->state == STATE_REQ) {
        handle_state_req(conn);
    } else if (conn->state == STATE_RES) {
        handle_state_res(conn);
    }
}

static void save_conn(std::vector<Conn *> &fd_to_conn, struct Conn *conn) {
    if (fd_to_conn.size() <= (size_t)conn->fd) {
        // resize to accommodate the fd number
        // since we use the fd number as the index of the corresponding conn
        fd_to_conn.resize(conn->fd + 1);
    }
    fd_to_conn[conn->fd] = conn;
}

static int32_t accept_new_conn(std::vector<Conn *> &fd_to_conn, int server_fd) {
    struct sockaddr_in client_addr = {};
    socklen_t socklen = sizeof(client_addr);
    int conn_fd = accept(server_fd, (struct sockaddr *)&client_addr, &socklen);
    if (conn_fd < 0) {
        printf("accept() error");
        return -1;
    }

    fd_set_nonblocking(conn_fd);

    struct Conn *conn = (struct Conn *) malloc(sizeof(struct Conn));
    if (!conn) {
        close(conn_fd);
        return -1;
    }
    conn->fd = conn_fd;
    conn->state = STATE_REQ;
    conn->read_buf_size = 0;
    conn->write_buf_size = 0;
    conn->write_buf_sent = 0;
    save_conn(fd_to_conn, conn);
    return 0;
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

    // maps fds to connections
    std::vector<Conn *> fd_to_conn;

    // set server fd to nonblocking mode 
    fd_set_nonblocking(server_fd);

    std::vector<struct pollfd> poll_args;
    while (true) {
        // prep args of the poll 
        poll_args.clear();

        struct pollfd poll_fd = {server_fd, POLLIN, 0};
        poll_args.push_back(poll_fd);

        // add connection fds to poll args
        for (Conn *conn : fd_to_conn) {
            if (!conn) {
                continue;
            }

            struct pollfd poll_fd = {};
            poll_fd.fd = conn->fd;
            poll_fd.events = conn->state == STATE_REQ
                ? POLLIN // read inputs if this is a request fd
                : POLLOUT; // write inputs otherwise
            poll_fd.events = poll_fd.events | POLLERR; // read error conditions
            poll_args.push_back(poll_fd);
        }

        // poll for active fds
        int res = poll(poll_args.data(), (nfds_t) poll_args.size(), 1000);
        if (res < 0) {
            die("poll");
        }

        // process active conns
        for (size_t i = 1; i < poll_args.size(); i++) {
            if (!poll_args[i].revents) {
                // if inactive, skip
                continue;
            }
            Conn *conn = fd_to_conn[poll_args[i].fd];
            connection_io(conn);

            if (conn->state != STATE_END) {
                continue;
            }
            // if this is the end state, need to destroy conn
            fd_to_conn[conn->fd] = NULL;
            close(conn->fd);
            free(conn);
        }

        // accept new conn if server fd is active 
        if (poll_args[0].revents) {
            accept_new_conn(fd_to_conn, server_fd);
        }

    }

    return 0;
}