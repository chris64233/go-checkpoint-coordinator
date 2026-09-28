package gocheckpointcoordinator

import "errors"

// 协调器对外暴露的哨兵错误。调用方可用 errors.Is 判定具体失败原因。
var (
	// ErrTaskNotFound 任务未登记。
	ErrTaskNotFound = errors.New("gocheckpoint: task not found")
	// ErrTaskExists 重复登记同一任务 ID。
	ErrTaskExists = errors.New("gocheckpoint: task already exists")
	// ErrRoundInProgress 上一轮检查点尚未进入终态，不能发起新一轮。
	ErrRoundInProgress = errors.New("gocheckpoint: previous round still in progress")
	// ErrRoundNotFound 指定轮次不存在。
	ErrRoundNotFound = errors.New("gocheckpoint: round not found")
	// ErrUnknownMember 上报成员不在该轮冻结集合中（例如成员临时退出后仍上报）。
	ErrUnknownMember = errors.New("gocheckpoint: member not in frozen member set")
	// ErrConflict 同一成员对同一轮次提交了与首次不同的内容。
	ErrConflict = errors.New("gocheckpoint: conflicting report for member and round")
	// ErrRoundTerminal 轮次已进入终态（成功/中止/超时），拒绝再次上报或中止。
	ErrRoundTerminal = errors.New("gocheckpoint: round already terminal")
	// ErrStaleRound 旧轮次的迟到上报，且内容与历史不一致；不得混入当前检查点。
	ErrStaleRound = errors.New("gocheckpoint: late report for a non-current round")
	// ErrInvalidArgument 参数不合法。
	ErrInvalidArgument = errors.New("gocheckpoint: invalid argument")
)
