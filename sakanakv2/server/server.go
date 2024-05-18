package main

import (
	"fmt"
	"net"
)

func handleCli(conn net.Conn) error {
    buf := make([]byte, 1024)
    readLen, err := conn.Read(buf)
    if err != nil {
        return fmt.Errorf("error reading from conn: err=%w", err)
    }

    fmt.Printf("Received: %s\n", string(buf[:readLen]))

    reply := fmt.Sprintf("Echo: %s", string(buf[:readLen]))
    if _, err := conn.Write([]byte(reply)); err != nil {
        return fmt.Errorf("error writing to conn: err=%w", err)
    }
    return nil
}

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
    for {
        conn, err := server.Accept()
        if err != nil {
            fmt.Printf("Error accepting client connection: err=%+v", err)
        }

        handleCli(conn) 
    }
}
