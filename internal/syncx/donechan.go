package syncx

import "sync"

// DoneChan is a chan with no buffer, can be saved close multiple times and wait for done
type DoneChan struct {
	isDone chan struct{}
	once   sync.Once
}

func NewDoneChan() *DoneChan {
	return &DoneChan{
		isDone: make(chan struct{}),
	}
}

func (dc *DoneChan) Close() {
	dc.once.Do(func() {
		close(dc.isDone)
	})
}

// Done returns a channel that can be notified on dc closed.
func (dc *DoneChan) Done() chan struct{} {
	return dc.isDone
}
