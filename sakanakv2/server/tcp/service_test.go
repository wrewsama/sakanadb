package tcp

import (
	"errors"
	"net"
	"testing"

	. "github.com/ovechkin-dm/mockio/mock"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_ReadNBytes(t *testing.T) {
	Convey("Test ReadNBytes", t, func() {
		service := NewTCPService()
		Convey("conn read err", func() {
			mockConn := Mock[net.Conn]()
			WhenDouble(
				mockConn.Read(Any[[]byte]())).ThenReturn(0, errors.New(""))

			buf, err := service.ReadNBytes(mockConn, 69)

			Verify(mockConn, Once()).Read(Any[[]byte]())
			So(err, ShouldNotBeNil)
			So(buf, ShouldBeNil)
		})
		Convey("conn read ok", func() {
			mockConn := Mock[net.Conn]()
			WhenDouble(
				mockConn.Read(Any[[]byte]())).ThenReturn(20, nil)

			buf, err := service.ReadNBytes(mockConn, 100)

			Verify(mockConn, Times(5)).Read(Any[[]byte]())
			So(err, ShouldBeNil)
			So(buf, ShouldResemble, make([]byte, 100))
		})
	})	
}

func Test_WriteBytes(t *testing.T) {
	Convey("Test ReadNBytes", t, func() {
		service := NewTCPService()
		Convey("conn read err", func() {
			mockConn := Mock[net.Conn]()
			WhenDouble(
				mockConn.Write(Any[[]byte]())).ThenReturn(0, errors.New(""))

			err := service.WriteBytes(mockConn, []byte("xdd"))

			Verify(mockConn, Once()).Write(Any[[]byte]())
			So(err, ShouldNotBeNil)
		})
		Convey("conn read ok", func() {
			mockConn := Mock[net.Conn]()
			WhenDouble(
				mockConn.Write(Any[[]byte]())).ThenReturn(1, nil)

			err := service.WriteBytes(mockConn, []byte("xdd"))

			Verify(mockConn, Times(3)).Write(Any[[]byte]())
			So(err, ShouldBeNil)
		})
	})	
}