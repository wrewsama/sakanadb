package handler

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/wrewsama/sakanadb/sakanakv2/server/tcp"
)

const HEADER_SIZE = 4
const MAX_MSG_SIZE = 4096

type Handler interface {
	HandleOneReq(conn net.Conn) error
}

type handler struct {
	tcpSvc tcp.TCPService
}

func NewHandler(tcps tcp.TCPService) Handler {
	return &handler{
		tcpSvc: tcps,
	}
}

func (s *handler) HandleOneReq(conn net.Conn) error {
	headerBytes, err :=  s.tcpSvc.ReadNBytes(conn, HEADER_SIZE)
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

	reply := []byte("ECHO: " + req)
	replyLen:= make([]byte, HEADER_SIZE)
	binary.LittleEndian.PutUint32(replyLen, uint32(len(reply)))

	writeBuf := append(replyLen, reply...)
	if err := s.tcpSvc.WriteBytes(conn, writeBuf); err != nil {
		return fmt.Errorf("failed to write reply: err=%w", err)
	}
	return nil
}