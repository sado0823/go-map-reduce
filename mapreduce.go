package main

import (
	"errors"
	"fmt"
	"github.com/sado0823/go-map-reduce/internal/errrorx"
	"github.com/sado0823/go-map-reduce/internal/syncx"
	"sync"
)

const (
	defaultWorkers = 16
	minWorkers     = 1
)

var (
	ErrCancelWithNil  = errors.New("mapreduce cancel with nil")
	ErrReduceNoOutput = errors.New("reduce not writing value")
)

type (
	// GenerateFunc generate source to mapreduce
	GenerateFunc func(source chan<- interface{})

	// Iterator store processing value
	Iterator interface {
		Write(v interface{})
	}

	options struct {
		workers int
	}

	WithOption func(opts *options)

	// MapFunc do processing and set output to iterator
	MapFunc func(item interface{}, iterator Iterator)

	// MapFuncVoid do processing with no output
	MapFuncVoid func(item interface{})

	// MapFuncCancel do processing with cancel func, call cancel to stop processing
	MapFuncCancel func(item interface{}, iterator Iterator, cancel func(err error))

	// ReduceFunc reduce mapping output and set it to iterator, call cancel to stop processing
	ReduceFunc func(pipe <-chan interface{}, iterator Iterator, cancel func(err error))

	// ReduceFuncVoid mapping output and set it to iterator with no output, call cancel to stop processing
	ReduceFuncVoid func(pipe <-chan interface{}, cancel func(err error))
)

// WithWorkers customizes a mapreduce processing with given workers.
func WithWorkers(workers int) WithOption {
	return func(opts *options) {
		if workers < minWorkers {
			opts.workers = minWorkers
		} else {
			opts.workers = workers
		}
	}
}

func newOptions() *options {
	return &options{
		workers: defaultWorkers,
	}
}

func buildOptions(fns ...WithOption) *options {
	op := newOptions()
	for _, fn := range fns {
		fn(op)
	}
	return op
}

func buildOldOptions(op *options, fns ...WithOption) {
	for _, fn := range fns {
		fn(op)
	}
}

// FinishVoid run fns in parallel without error return
func FinishVoid(fns ...func()) {
	if len(fns) == 0 {
		return
	}

	MapVoid(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}) {
		fn := item.(func())
		fn()
	}, WithWorkers(len(fns)))

}

// MapVoid maps all elements without output
func MapVoid(generateFunc GenerateFunc, mapFunc MapFuncVoid, opts ...WithOption) {
	source := Map(generateFunc, func(item interface{}, iterator Iterator) {
		mapFunc(item)
	}, opts...)
	drain(source)
}

// Map maps all elements generated from given generate func, and returns an output channel.
func Map(generateFunc GenerateFunc, mapFunc MapFunc, opts ...WithOption) chan interface{} {
	op := buildOptions(opts...)
	source := buildSource(generateFunc)
	collector := make(chan interface{}, op.workers)
	dc := syncx.NewDoneChan()
	go executeMapFunc(mapFunc, source, collector, dc.Done(), op.workers)
	return collector
}

// Finish run fns in parallel, cancel with error occurred
func Finish(fns ...func() error) error {

	if len(fns) == 0 {
		return nil
	}

	return MapReduceVoid(func(source chan<- interface{}) {
		for _, fn := range fns {
			source <- fn
		}
	}, func(item interface{}, iterator Iterator, cancel func(err error)) {
		fn := item.(func() error)
		if err := fn(); err != nil {
			cancel(err)
		}
	}, func(pipe <-chan interface{}, cancel func(err error)) {
		drain(pipe)
	}, WithWorkers(len(fns)))

}

// MapReduceVoid mapping all source from generateFunc, reduce the output with reduceFunc with no output
func MapReduceVoid(generateFunc GenerateFunc, mapFunc MapFuncCancel, reduceFunc ReduceFuncVoid, opts ...WithOption) error {
	_, err := MapReduce(generateFunc, mapFunc, func(pipe <-chan interface{}, iterator Iterator, cancel func(err error)) {
		reduceFunc(pipe, cancel)
		// write Nil task to make MapReduce continue
		iterator.Write(struct{}{})
	}, opts...)
	return err
}

// MapReduce mapping all source from generateFunc, reduce the output with reduceFunc
func MapReduce(generateFunc GenerateFunc, mapFunc MapFuncCancel, reduceFunc ReduceFunc, opts ...WithOption) (interface{}, error) {
	source := buildSource(generateFunc)
	return MapReduceWithSource(source, mapFunc, reduceFunc, opts...)
}

// buildSource generate original source from generateFunc
func buildSource(generateFunc GenerateFunc) chan interface{} {
	source := make(chan interface{})
	syncx.Go(func() {
		defer close(source)
		generateFunc(source)
	})
	return source
}

func MapReduceWithSource(source <-chan interface{}, mapFunc MapFuncCancel, reduceFunc ReduceFunc, opts ...WithOption) (interface{}, error) {
	op := buildOptions(opts...)
	output := make(chan interface{})
	defer func() {
		for range output {
			panic("reduce output should be one element")
		}
	}()

	var (
		collector = make(chan interface{}, op.workers)
		dc        = syncx.NewDoneChan()
		iterator  = newDefaultIterator(output, dc.Done())
		closeOnce sync.Once
		retErr    errrorx.AtomicError
		finish    = func() {
			closeOnce.Do(func() {
				dc.Close()
				close(output)
			})
		}
		cancel = once(func(err error) {
			if err != nil {
				retErr.Store(err)
			} else {
				retErr.Store(ErrCancelWithNil)
			}
			drain(source)
			finish()
		})
	)

	// do reduce
	go func() {
		defer func() {
			drain(collector)

			if r := recover(); r != nil {
				fmt.Printf("got cancel---: %v \n", r)
				cancel(fmt.Errorf("%v", r))
			} else {
				finish()
			}
		}()

		reduceFunc(collector, iterator, cancel)
	}()

	// do map
	go executeMapFunc(func(item interface{}, iterator Iterator) {
		mapFunc(item, iterator, cancel)
	}, source, collector, dc.Done(), op.workers)

	// final result, exactly one element
	v, ok := <-output
	if err := retErr.Load(); err != nil {
		return nil, err
	} else if ok {
		return v, nil
	} else {
		return nil, ErrReduceNoOutput
	}

}

func executeMapFunc(mapFunc MapFunc, input <-chan interface{}, collector chan<- interface{}, done chan struct{}, workers int) {
	var wg sync.WaitGroup
	defer func() {
		wg.Wait()
		close(collector)
	}()

	pool := make(chan struct{}, workers)
	iterator := newDefaultIterator(collector, done)
	for {
		select {
		case <-done:
			return
		case pool <- struct{}{}:
			v, ok := <-input
			if !ok {
				<-pool
				return
			}
			wg.Add(1)
			syncx.Go(func() {
				defer func() {
					wg.Done()
					<-pool
				}()
				mapFunc(v, iterator)
			})
		}
	}

}

// drain drains the channel.
func drain(channel <-chan interface{}) {
	// drain the channel
	for range channel {
	}
}

func once(fn func(error)) func(err error) {
	var doOnce sync.Once
	return func(err error) {
		doOnce.Do(func() {
			fn(err)
		})
	}
}

type defaultIterator struct {
	ch   chan<- interface{}
	done <-chan struct{}
}

func newDefaultIterator(ch chan<- interface{}, done <-chan struct{}) defaultIterator {
	return defaultIterator{
		ch:   ch,
		done: done,
	}
}

// Write implement Iterator, store v to source
func (di defaultIterator) Write(v interface{}) {
	select {
	case <-di.done:
		return
	default:
		di.ch <- v
	}
}
