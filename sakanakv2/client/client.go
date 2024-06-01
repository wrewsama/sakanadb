package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"

	"github.com/wrewsama/sakanadb/sakanakv2/client/handler"
	"github.com/wrewsama/sakanadb/sakanakv2/client/tcp"
)

const EXIT_CMD = "exit"

func main() {
	HOST := "localhost"
	PORT := 3535

	fmt.Println("SakanaKV2 shell starting...")

	conn, err := net.Dial("tcp", fmt.Sprintf("%s:%d", HOST, PORT))
	if err != nil {
		fmt.Printf("Error connecting: err=%v", err)
	}
	defer conn.Close()

	tcpSvc := tcp.NewTCPService()
	handler := handler.NewHandler(tcpSvc)
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("\n><>")
		scanner.Scan()

		input := scanner.Text()
		if input == EXIT_CMD {
			fmt.Println("exiting...")
			break
		}
		args := strings.Split(input, " ")
		handler.HandleQuery(conn, args)
	}
}
