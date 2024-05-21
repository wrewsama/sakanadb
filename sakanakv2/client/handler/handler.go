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
	SendQuery(conn net.Conn) error
}

type handler struct {
	tcpSvc tcp.TCPService
}

func NewHandler(tcps tcp.TCPService) Handler {
	return &handler{
		tcpSvc: tcps,
	}
}

func (s *handler) SendQuery(conn net.Conn) error {
	// TODO
	return nil
}