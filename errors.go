package redislocks

import "errors"

var ErrAcquireTimeout = errors.New("acquire timeout")
var ErrAcquireAttemptsLimit = errors.New("acquire attempts limit")
var ErrLockLost = errors.New("lock lost")
