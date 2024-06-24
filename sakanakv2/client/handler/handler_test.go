package handler

import (
	"encoding/binary"
	"errors"
	"net"
	"testing"

	. "github.com/ovechkin-dm/mockio/mock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wrewsama/sakanadb/sakanakv2/client/tcp"
)

func Test_HandleQuery(t *testing.T) {
	Convey("Test HandleQuery", t, func() {
		mockTCPSvc := Mock[tcp.TCPService]()
		mockConn := Mock[net.Conn]()
		h := NewHandler(mockTCPSvc)
		Convey("write fail", func() {
			WhenSingle(
				mockTCPSvc.WriteBytes(Any[net.Conn](), Any[[]byte]())).ThenReturn(
					errors.New(""),
				)
			err := h.HandleQuery(mockConn, []string{})

			So(err, ShouldNotBeNil)
		})
		Convey("happy path", func() {
			WhenSingle(
				mockTCPSvc.WriteBytes(Any[net.Conn](), Any[[]byte]())).ThenReturn(nil)
			fakeBytes := make([]byte, 4)
			binary.LittleEndian.PutUint32(fakeBytes, 69)
			WhenDouble(
				mockTCPSvc.ReadNBytes(Any[net.Conn](), AnyInt())).ThenReturn(fakeBytes, nil)
			err := h.HandleQuery(mockConn, []string{})

			So(err, ShouldBeNil)
		})

	})
}