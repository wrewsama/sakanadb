package main

import (
	"fmt"
	"net"

	"github.com/wrewsama/sakanadb/sakanakv2/server/command"
	"github.com/wrewsama/sakanadb/sakanakv2/server/handler"
	"github.com/wrewsama/sakanadb/sakanakv2/server/repository"
	"github.com/wrewsama/sakanadb/sakanakv2/server/tcp"
)

func main() {
    // TODO: make these env vars
    HOST := "localhost"
    PORT := 3535

    server, err := net.Listen("tcp", fmt.Sprintf("%s:%d", HOST, PORT))
    if err != nil {
        fmt.Printf("Error listening: err=%v", err)
    }
    defer server.Close()

    fmt.Printf("Listening on host %s with port %d\n", HOST, PORT)

    tcpService := tcp.NewTCPService()
    cmdReg := command.NewCommandRegistry()
    repo := repository.NewRepo()
    handler := handler.NewHandler(tcpService, cmdReg, repo)
    for {
        conn, err := server.Accept()
        if err != nil {
            fmt.Printf("Error accepting client connection: err=%+v", err)
        }

        go func(conn net.Conn) {
            defer func() {
                if err := recover(); err != nil {
                    fmt.Printf("Function panicked while handling connection %+v: err=%+v", conn, err)
                }
            }()
            defer conn.Close()

            for {
                if err := handler.HandleOneReq(conn); err != nil {
                    break
                }
            }
        }(conn)
    }
}
