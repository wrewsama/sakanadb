package main

import (
	"fmt"
	"net"
)

func main() {
    HOST := "localhost"
    PORT := 3535

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", HOST, PORT))
	if err != nil {
        fmt.Printf("Error connecting: err=%v", err)
	}
	defer conn.Close()

	msg := "xdd"
	if _, err := conn.Write([]byte(msg)); err != nil {
        fmt.Printf("Error writing to conn: err=%v", err)
	}

    buf := make([]byte, 1024)
    readLen, err := conn.Read(buf)
    if err != nil {
        fmt.Printf("error reading from conn: err=%v", err)
    }
	fmt.Printf("Received: %s\n", buf[:readLen])
}