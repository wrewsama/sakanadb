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
	HandleOneReq(conn net.Conn) error
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

	numArgs:= binary.LittleEndian.Uint32(headerBytes)
	if numArgs > MAX_ARGS {
		return nil, fmt.Errorf("too many arguments")
	}
	fmt.Printf("%d args in req\n", numArgs)

	var reqStrs []string
	for i := 0; i < int(numArgs); i++ {
		lenBytes, err := s.tcpSvc.ReadNBytes(conn, LEN_SIZE)
		if err != nil {
			return nil, fmt.Errorf("failed to read len: err=%w", err)
		}
		strLen := binary.LittleEndian.Uint32(lenBytes)
		fmt.Printf("arg %d has len %d\n", i, strLen)

		strBytes, err := s.tcpSvc.ReadNBytes(conn, int(strLen))
		if err != nil {
			return nil, fmt.Errorf("failed to read string: err=%w", err)
		}
		str := string(strBytes)
		fmt.Printf("arg %d = %s\n", i, str)
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

func (s *handler) HandleOneReq(conn net.Conn) error {
	fmt.Println("Handling request")
	reqStrs, err := s.readReq(conn)
	if err != nil {
		wrappedErr := fmt.Errorf("error reading request: err=%w", err)
		s.handleErr(conn, wrappedErr)
		return wrappedErr
	}
	if len(reqStrs) == 0 {
		wrappedErr := fmt.Errorf("no args supplied")
		s.handleErr(conn, wrappedErr)
		return wrappedErr
	}
	fmt.Printf("Received: %+v\n", reqStrs)

	cmd, ok := s.cmdReg[reqStrs[0]]
	if !ok {
		s.handleErr(conn, fmt.Errorf("command %s not supported", reqStrs[0]))
	}

	resp := cmd.Execute(reqStrs, s.repo)
	s.writeResp(conn, resp)
	return nil
}
