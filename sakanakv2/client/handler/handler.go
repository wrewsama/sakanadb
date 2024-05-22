package handler

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/wrewsama/sakanadb/sakanakv2/client/tcp"
)

const HEADER_SIZE = 4
const MAX_MSG_SIZE = 4096

type Handler interface {
	SendQuery(conn net.Conn, text string) error
}

type handler struct {
	tcpSvc tcp.TCPService
}

func NewHandler(tcps tcp.TCPService) Handler {
	return &handler{
		tcpSvc: tcps,
	}
}

func (s *handler) SendQuery(conn net.Conn, text string) error {
	msgLen := uint32(len(text))
	if msgLen > MAX_MSG_SIZE {
		return fmt.Errorf("Message too big!")
	}

	lenBytes := make([]byte, HEADER_SIZE)
	binary.LittleEndian.PutUint32(lenBytes, msgLen)
	writeBuf := append(lenBytes, []byte(text)...)
	if err := s.tcpSvc.WriteBytes(conn, writeBuf); err != nil {
		return fmt.Errorf("failed to write reply: err=%w", err)
	}

	headerBytes, err := s.tcpSvc.ReadNBytes(conn, 4)
	if err != nil {
		return fmt.Errorf("failed to read header: err=%w", err)
	}
	reqLen := binary.LittleEndian.Uint32(headerBytes)
	reqBytes, err := s.tcpSvc.ReadNBytes(conn, int(reqLen))
	if err != nil {
		return fmt.Errorf("failed to read body: err=%w", err)
	}

	req := string(reqBytes)
	fmt.Printf("Received: %s\n", req)
	return nil
}