package handler

import (
	"encoding/binary"
	"fmt"
	"net"

	"github.com/wrewsama/sakanadb/sakanakv2/server/command"
	"github.com/wrewsama/sakanadb/sakanakv2/server/repository"
	"github.com/wrewsama/sakanadb/sakanakv2/server/tcp"
)

const HEADER_SIZE = 4
const LEN_SIZE = 4
const MAX_ARGS = 1024
const RESPCODE_SIZE = 4

type Handler interface {
	HandleOneReq(conn net.Conn)
}

type handler struct {
	tcpSvc tcp.TCPService
	cmdReg command.CommandRegistry
	repo repository.Repository
}

func NewHandler(
		tcps tcp.TCPService,
		cr command.CommandRegistry,
		repo repository.Repository,
	) Handler {
	return &handler{
		tcpSvc: tcps,
		cmdReg: cr,
		repo: repo,
	}
}

func (s *handler) readReq(conn net.Conn) ([]string, error) {
	headerBytes, err := s.tcpSvc.ReadNBytes(conn, HEADER_SIZE)
	if err != nil {
		return nil, fmt.Errorf("failed to read header: err=%w", err)
	}

	numReqs := binary.LittleEndian.Uint32(headerBytes)
	if numReqs > MAX_ARGS {
		return nil, fmt.Errorf("too many arguments")
	}

	var reqStrs []string
	for i := 0; i < int(numReqs); i++ {
		lenBytes, err := s.tcpSvc.ReadNBytes(conn, LEN_SIZE)
		if err != nil {
			return nil, fmt.Errorf("failed to read len: err=%w", err)
		}
		strLen := binary.LittleEndian.Uint32(lenBytes)
		strBytes, err := s.tcpSvc.ReadNBytes(conn, int(strLen))
		if err != nil {
			return nil, fmt.Errorf("failed to read string: err=%w", err)
		}
		str := string(strBytes)
		reqStrs = append(reqStrs, str)
	}

	return reqStrs, nil
}

func (s *handler) writeResp(conn net.Conn, resp command.CommandResp) error {
	codeBytes := make([]byte, RESPCODE_SIZE)
	binary.LittleEndian.PutUint32(codeBytes, uint32(resp.Code))	
	payloadBytes := []byte(fmt.Sprintf("%v", resp.Payload))
	lenBytes := make([]byte, LEN_SIZE)
	binary.LittleEndian.PutUint32(
		codeBytes, uint32(len(payloadBytes) + len(codeBytes)))	

	respBytes := append(lenBytes, codeBytes...)
	respBytes = append(respBytes, payloadBytes...)
	if err := s.tcpSvc.WriteBytes(conn, respBytes); err != nil {
		return fmt.Errorf("error writing to TCP: err=%w", err)
	}
	return nil
}

func (s *handler) handleErr(conn net.Conn, err error) {
	s.writeResp(conn, command.CommandResp{
		Code: command.RESP_Err,
		Payload: err.Error(),
	})
}

func (s *handler) HandleOneReq(conn net.Conn) {
	reqStrs, err := s.readReq(conn)
	if err != nil {
		s.handleErr(conn, fmt.Errorf("error reading request: err=%w", err))
	}
	if len(reqStrs) == 0 {
		s.handleErr(conn, fmt.Errorf("no args supplied"))
	}
	fmt.Printf("Received: %+v\n", reqStrs)

	cmd, ok := s.cmdReg[reqStrs[0]]
	if !ok {
		s.handleErr(conn, fmt.Errorf("command %s not supported", reqStrs[0]))
	}

	resp := cmd.Execute(reqStrs, s.repo)
	s.writeResp(conn, resp)
}
