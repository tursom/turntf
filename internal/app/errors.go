package app

import "errors"

var (
	ErrClockNotSynchronized = errors.New("cluster clock not synchronized")
	ErrServiceUnavailable   = errors.New("service unavailable")
)
