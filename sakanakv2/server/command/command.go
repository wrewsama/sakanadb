package command

import "github.com/wrewsama/sakanadb/sakanakv2/server/repository"

type CommandRespCode uint32

const (
	RESP_OK CommandRespCode = iota
	RESP_Err
	RESP_DoesNotExist
)
type CommandResp struct {
	Code CommandRespCode	
	Payload any
}

type Command interface {
	Execute(args []string, repo repository.Repository) (CommandResp)
}

type get struct {}

func (*get) Execute(args []string, repo repository.Repository) (CommandResp) {
	if len(args) != 2 {
		return CommandResp{
			Code: RESP_Err,
		}
	}
	val, ok := repo.Get(args[1])
	if !ok {
		return CommandResp{
			Code: RESP_DoesNotExist,
		}
	}

	return CommandResp{
		Code: RESP_OK,
		Payload: val,
	}
}

type set struct {}

func (*set) Execute(args []string, repo repository.Repository) (CommandResp) {
	if len(args) != 3 {
		return CommandResp{
			Code: RESP_Err,
		}
	}
	repo.Set(args[1], args[2])

	return CommandResp{
		Code: RESP_OK,
	}
}

type delete struct {}

func (*delete) Execute(args []string, repo repository.Repository) (CommandResp) {
	if len(args) != 2 {
		return CommandResp{
			Code: RESP_Err,
		}
	}
	repo.Delete(args[1])

	return CommandResp{
		Code: RESP_OK,
	}
}