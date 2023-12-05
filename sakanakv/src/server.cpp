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
#include <string>
#include <vector>
# include "hashmap.h"

// macro to convert Nodes to Entries
#define container_of(ptr, type, member) ({                  \
    const typeof( ((type *)0)->member ) *__mptr = (ptr);    \
    (type *)( (char *)__mptr - offsetof(type, member) );})

const size_t MAX_MSG_SIZE = 4096;
const size_t MAX_ARGS_SIZE = 1024;

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
    errno = 0;
    int flags = fcntl(fd, F_GETFL, 0); // get the file status flags
    if (errno) {
        die("fcntl error");
        return;
    }

    flags |= O_NONBLOCK; // add the NONBLOCK flag

    errno = 0;
    fcntl(fd, F_SETFL, flags);
    if (errno) {
        die("fcntl error");
        return;
    }
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

enum {
    RES_OK = 0,
    RES_ERR = 1,
    RES_NX = 2
};

static struct {
    HashMap db;
} data;

struct Entry {
    struct HashTableNode node;
    std::string key;
    std::string value;
};

static bool entry_eq(HashTableNode *node1, HashTableNode *node2) {
    // convert from nodes to entries
    struct Entry *entry1 = container_of(node1, Entry, node);
    struct Entry *entry2 = container_of(node2, Entry, node);

    return node1->hashcode == node2->hashcode && entry1->key == entry2->key;
};

static uint64_t hash_string(const uint8_t *data, size_t len) {
    uint32_t hash = 0x911C9DC5;
    for (size_t i = 0; i < len; i++) {
        hash = (hash + data[i]) * 0x01000193;
    }
    return hash;
}

static uint32_t do_get(
    std::vector<std::string> &cmd,
    uint8_t *res,
    uint32_t *res_len
) {
    Entry entry;
    entry.key.swap(cmd[1]);
    entry.node.hashcode = hash_string((uint8_t *)entry.key.data(), entry.key.size());

    HashTableNode *node = hm_get(&data.db, &entry.node, &entry_eq);
    if (!node) {
        return RES_NX;
    }

    const std::string &val = container_of(node, Entry, node)->value;
    memcpy(res, val.data(), val.size());
    *res_len = (uint32_t) val.size();
    return RES_OK;
}

static uint32_t do_set(
    std::vector<std::string> &cmd,
    uint8_t *res,
    uint32_t *res_len
) {
    Entry entry;
    entry.key.swap(cmd[1]);
    entry.node.hashcode = hash_string((uint8_t *)entry.key.data(), entry.key.size());
    
    HashTableNode *node = hm_get(&data.db, &entry.node, &entry_eq);
    if (node) {
        container_of(node, Entry, node)->value.swap(cmd[2]);
    } else {
        Entry *newEntry = new Entry();
        newEntry->key.swap(entry.key);
        newEntry->node.hashcode = entry.node.hashcode;
        newEntry->value.swap(cmd[2]);
        hm_put(&data.db, &newEntry->node);
    }
    return RES_OK;
}

static uint32_t do_del(
    std::vector<std::string> &cmd,
    uint8_t *res,
    uint32_t *res_len
) {
    Entry entry;
    entry.key.swap(cmd[1]);
    entry.node.hashcode = hash_string((uint8_t *)entry.key.data(), entry.key.size());

    HashTableNode *deletedNode = hm_del(&data.db, &entry.node, &entry_eq);
    if (deletedNode) {
        delete container_of(deletedNode, Entry, node);
    }

    return RES_OK;
}

static int32_t parse_req(
    const uint8_t *data,
    size_t len,
    std::vector<std::string> &out) {
        if (len < 4) {
            // can't even read header
            return -1; 
        }

        uint32_t args_size = 0;
        memcpy(&args_size, data, 4);
        if (args_size > MAX_ARGS_SIZE) {
            return -1;
        }

        size_t cur_pos = 4;
        while (args_size--) {
            if (cur_pos + 4 > len) {
                return -1;
            }
            uint32_t size = 0;
            memcpy(&size, &data[cur_pos], 4);
            if (cur_pos + 4 + size > len) {
                return -1;
            }
            out.push_back(std::string((char *) &data[cur_pos+4], size));
            cur_pos += 4 + size;

        }

        if (cur_pos != len) {
            return -1; // extra garbage trailing
        }
        return 0;
    }

static int32_t do_request(const uint8_t *req,
    uint32_t req_len,
    uint32_t *res_code,
    uint8_t *res,
    uint32_t *res_len) {
        std::vector<std::string> cmd;

        // parse the req and store it in the cmd vector
        if (parse_req(req, req_len, cmd) != 0) {
            printf("bad req");
            return -1;
        }

        // strcasecmp just checks if the cmd keyword is equal to the RHS
        if (cmd.size() == 2 && strcasecmp(cmd[0].c_str(), "get") == 0) {
            *res_code = do_get(cmd, res, res_len);
        } else if (cmd.size() == 3 && strcasecmp(cmd[0].c_str(), "set") == 0) {
            *res_code = do_set(cmd, res, res_len);
        } else if (cmd.size() == 2 && strcasecmp(cmd[0].c_str(), "del") == 0) {
            *res_code = do_del(cmd, res, res_len);
        } else {
            *res_code = RES_ERR;
            const char *msg = "unknown command";
            strcpy((char *) res, msg);
            *res_len = strlen(msg);
            return 0;
        }
        return 0;
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

    // parse and handle req
    uint32_t res_code = 0;
    uint32_t write_len = 0;
    int32_t err = do_request(
        &conn->read_buf[4],
        len,
        &res_code,
        &conn->write_buf[4 + 4],
        &write_len
    );

    // handle bad req
    if (err) {
        conn->state = STATE_END;
        return false;
    }

    // add res to the buffer
    write_len += 4;
    memcpy(conn->write_buf, &write_len, 4);
    memcpy(&conn->write_buf[4], &res_code, 4);
    conn->write_buf_size = 4 + write_len;

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
