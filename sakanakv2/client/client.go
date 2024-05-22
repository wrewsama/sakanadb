package main

import (
	"fmt"
	"net"

	"github.com/wrewsama/sakanadb/sakanakv2/client/handler"
	"github.com/wrewsama/sakanadb/sakanakv2/client/tcp"
)

func main() {
    HOST := "localhost"
    PORT := 3535

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", HOST, PORT))
	if err != nil {
        fmt.Printf("Error connecting: err=%v", err)
	}
	defer conn.Close()

	tcpSvc := tcp.NewTCPService()
	handler := handler.NewHandler(tcpSvc)

	msgs := []string{
		"towa sama",
		"sora chan",
		"elite mikochi",
	}

	for _, msg := range msgs {
		if err := handler.SendQuery(conn, msg); err != nil {
			panic(err)
		}
	}
}