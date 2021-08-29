package errrorx

import "sync/atomic"

type AtomicError struct {
	v atomic.Value
}

func (ae *AtomicError) Store(err error) {
	ae.v.Store(err)
}

func (ae *AtomicError) Load() error {
	if v := ae.v.Load(); v != nil {
		return v.(error)
	}
	return nil
}
