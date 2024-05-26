package handler

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/wrewsama/sakanadb/sakanakv2/client/tcp"
)

const HEADER_SIZE = 4
const LEN_SIZE = 4
const CODE_SIZE = 4

type Handler interface {
	HandleQuery(conn net.Conn, args []string) error
}

type handler struct {
	tcpSvc tcp.TCPService
}

func NewHandler(tcps tcp.TCPService) Handler {
	return &handler{
		tcpSvc: tcps,
	}
}

func (s *handler) HandleQuery(conn net.Conn, args []string) error {
	numArgs := len(args)	
	writeBuf := make([]byte, HEADER_SIZE)
	binary.LittleEndian.PutUint32(writeBuf, uint32(numArgs))

	for _, text := range args {
		lenBytes := make([]byte, HEADER_SIZE)
		binary.LittleEndian.PutUint32(lenBytes, uint32(len(text)))
		writeBuf = append(writeBuf, lenBytes...)
		writeBuf = append(writeBuf, []byte(text)...)
	}
	if err := s.tcpSvc.WriteBytes(conn, writeBuf); err != nil {
		return fmt.Errorf("failed to write reply: err=%w", err)
	}

	headerBytes, err := s.tcpSvc.ReadNBytes(conn, HEADER_SIZE)
	if err != nil {
		return fmt.Errorf("failed to read header: err=%w", err)
	}
	reqLen := binary.LittleEndian.Uint32(headerBytes)
	reqBytes, err := s.tcpSvc.ReadNBytes(conn, int(reqLen))
	if err != nil {
		return fmt.Errorf("failed to read body: err=%w", err)
	}

	code := string(reqBytes[:CODE_SIZE])
	payload := string(reqBytes[CODE_SIZE:])
	fmt.Printf("[%s] %s", code, payload)
	return nil
}