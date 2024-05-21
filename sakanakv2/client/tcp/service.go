package tcp 

import (
	"fmt"
	"net"
)

type TCPService interface {
	ReadNBytes(conn net.Conn, numBytes int) ([]byte, error)
	WriteBytes(conn net.Conn, writeBuf []byte) error
}

type svc struct {}

func NewTCPService() TCPService {
	return &svc{} 
}

func (s *svc) ReadNBytes(conn net.Conn, numBytes int) ([]byte, error) {
	buf := make([]byte, numBytes)
	bytesRead := 0

	for bytesRead < numBytes {
		numRead, err := conn.Read(buf[bytesRead:])
		if err != nil {
			return nil, fmt.Errorf(
				"failed to read %d bytes from conn: err=%w", numBytes, err)
		}
		bytesRead += numRead
	}
	return buf, nil
}

func (s *svc) WriteBytes(conn net.Conn, writeBuf []byte) error {
	bytesWritten := 0

	for bytesWritten < len(writeBuf) {
		numWritten, err := conn.Write(writeBuf[bytesWritten:])
		if err != nil {
			return fmt.Errorf("failed to write to conn: err=%w", err)
		}
		bytesWritten += numWritten
	}
	return nil
}