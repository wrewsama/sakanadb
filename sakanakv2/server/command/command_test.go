package command

import (
	"testing"

	. "github.com/ovechkin-dm/mockio/mock"
	. "github.com/smartystreets/goconvey/convey"
	"github.com/wrewsama/sakanadb/sakanakv2/server/repository"
)

func Test_CmdRegistry(t *testing.T) {
	Convey("Test Command Registry", t, func() {
		cmdReg := NewCommandRegistry()

		Convey("test get", func() {
			Convey("not 2 args", func() {
				resp := cmdReg["get"].Execute([]string{}, nil)

				So(resp, ShouldResemble, CommandResp{Code: RESP_Err})
			})
			Convey("not found", func() {
				mockRepo := Mock[repository.Repository]()
				WhenDouble(mockRepo.Get("key")).ThenReturn(nil, false)

				resp := cmdReg["get"].Execute([]string{"xdd", "key"}, mockRepo)

				So(resp, ShouldResemble, CommandResp{Code: RESP_DoesNotExist})
			})
			Convey("ok", func() {
				mockRepo := Mock[repository.Repository]()
				WhenDouble(mockRepo.Get("key")).ThenReturn("val", true)

				resp := cmdReg["get"].Execute([]string{"xdd", "key"}, mockRepo)

				So(resp, ShouldResemble, CommandResp{Code: RESP_OK, Payload: "val"})
			})

		})
	})	
}