package main

import (
	"errors"
	"fmt"
	"math/rand"
	"runtime"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

var errDummy = errors.New("dummy")

func randName() string {
	return fmt.Sprintf("%d-%d", time.Now().Nanosecond(), rand.Int())
}

// without error
func TestFinish(t *testing.T) {
	var total uint32
	err := Finish(func() error {
		atomic.AddUint32(&total, 2)
		return nil
	}, func() error {
		atomic.AddUint32(&total, 3)
		return nil
	}, func() error {
		atomic.AddUint32(&total, 5)
		return nil
	})

	assert.Equal(t, uint32(10), atomic.LoadUint32(&total))
	assert.Nil(t, err)
}

// with error
func TestFinish2(t *testing.T) {
	var (
		foo struct {
			Name string
			Age  int
			Bar  bool
		}
		name = "test"
		age  = 18
		bar  = true
	)
	err := Finish(func() error {
		foo.Name = name
		return nil
	}, func() error {
		foo.Age = age
		return nil
	}, func() error {
		foo.Bar = bar
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, name, foo.Name)
	assert.Equal(t, age, foo.Age)
	assert.True(t, foo.Bar)

	err = Finish(func() error {
		return nil
	}, func() error {
		return errDummy
	})
	assert.EqualError(t, err, errDummy.Error())
}

// with none input
func TestFinish3(t *testing.T) {
	assert.Nil(t, Finish())
}

// with none input
func TestFinishVoid(t *testing.T) {
	FinishVoid()
}

func TestFinishVoid2(t *testing.T) {
	var total uint32
	FinishVoid(func() {
		atomic.AddUint32(&total, 2)
	}, func() {
		atomic.AddUint32(&total, 3)
	}, func() {
		atomic.AddUint32(&total, 5)
	})

	assert.Equal(t, uint32(10), atomic.LoadUint32(&total))
}

func TestMap(t *testing.T) {
	tests := []struct {
		mapper MapFunc
		expect int
	}{
		// normal
		{
			mapper: func(item interface{}, writer Iterator) {
				v := item.(int)
				writer.Write(v * v)
			},
			expect: 30,
		},
		// condition normal
		{
			mapper: func(item interface{}, writer Iterator) {
				v := item.(int)
				if v%2 == 0 {
					return
				}
				writer.Write(v * v)
			},
			expect: 10,
		},
		// with panic
		{
			mapper: func(item interface{}, writer Iterator) {
				v := item.(int)
				if v%2 == 0 {
					panic(v)
				}
				writer.Write(v * v)
			},
			expect: 10,
		},
	}

	for _, test := range tests {
		t.Run(randName(), func(t *testing.T) {
			channel := Map(func(source chan<- interface{}) {
				for i := 1; i < 5; i++ {
					source <- i
				}
			}, test.mapper, WithWorkers(-1))

			var result int
			for v := range channel {
				result += v.(int)
			}

			assert.Equal(t, test.expect, result)
		})
	}
}

func TestMapVoid(t *testing.T) {
	const tasks = 1000
	var count uint32
	MapVoid(func(source chan<- interface{}) {
		for i := 0; i < tasks; i++ {
			source <- i
		}
	}, func(item interface{}) {
		atomic.AddUint32(&count, 1)
	})

	assert.Equal(t, tasks, int(count))
}

func TestMapReduce(t *testing.T) {
	tests := []struct {
		mapper      MapFuncCancel
		reducer     ReduceFunc
		expectErr   error
		expectValue interface{}
	}{
		{
			expectErr:   nil,
			expectValue: 30,
		},
		{
			mapper: func(item interface{}, writer Iterator, cancel func(error)) {
				v := item.(int)
				if v%3 == 0 {
					cancel(errDummy)
				}
				writer.Write(v * v)
			},
			expectErr: errDummy,
		},
		{
			mapper: func(item interface{}, writer Iterator, cancel func(error)) {
				v := item.(int)
				if v%3 == 0 {
					cancel(nil)
				}
				writer.Write(v * v)
			},
			expectErr:   ErrCancelWithNil,
			expectValue: nil,
		},
		{
			reducer: func(pipe <-chan interface{}, writer Iterator, cancel func(error)) {
				var result int
				for item := range pipe {
					result += item.(int)
					if result > 10 {
						cancel(errDummy)
					}
				}
				writer.Write(result)
			},
			expectErr: errDummy,
		},
	}

	for _, test := range tests {
		t.Run(randName(), func(t *testing.T) {
			if test.mapper == nil {
				test.mapper = func(item interface{}, writer Iterator, cancel func(error)) {
					v := item.(int)
					writer.Write(v * v)
				}
			}
			if test.reducer == nil {
				test.reducer = func(pipe <-chan interface{}, writer Iterator, cancel func(error)) {
					var result int
					for item := range pipe {
						result += item.(int)
					}
					writer.Write(result)
				}
			}
			value, err := MapReduce(func(source chan<- interface{}) {
				for i := 1; i < 5; i++ {
					source <- i
				}
			}, test.mapper, test.reducer, WithWorkers(runtime.NumCPU()))

			assert.Equal(t, test.expectErr, err)
			assert.Equal(t, test.expectValue, value)
		})
	}
}

func TestMapReduceCancel(t *testing.T) {

	var (
		result1 []int
		tests   = []struct {
			mapper  MapFuncCancel
			reducer ReduceFunc
		}{
			{
				mapper: func(item interface{}, writer Iterator, cancel func(error)) {
					i := item.(int)
					if i == 1 {
						cancel(errDummy)
					}
					writer.Write(i)
				},
				reducer: func(pipe <-chan interface{}, iterator Iterator, cancel func(err error)) {
					for item := range pipe {
						i := item.(int)
						result1 = append(result1, i)
					}
					iterator.Write(result1)
				},
			},
			{
				mapper: func(item interface{}, writer Iterator, cancel func(error)) {
					writer.Write(item)
				},
				reducer: func(pipe <-chan interface{}, iterator Iterator, cancel func(err error)) {
					cancel(errDummy)
				},
			},
		}
		generate GenerateFunc = func(source chan<- interface{}) {
			source <- 0
			source <- 1
		}
	)

	for _, test := range tests {
		t.Run(randName(), func(t *testing.T) {
			v, err := MapReduce(generate, test.mapper, test.reducer, WithWorkers(runtime.NumCPU()))
			assert.NotNil(t, err)
			assert.EqualError(t, err, errDummy.Error())
			assert.Nil(t, v)
		})
	}
}

func TestMapReduceVoid(t *testing.T) {
	var value uint32
	tests := []struct {
		mapper      MapFuncCancel
		reducer     ReduceFuncVoid
		expectValue uint32
		expectErr   error
	}{
		{
			expectValue: 30,
			expectErr:   nil,
		},
		{
			mapper: func(item interface{}, writer Iterator, cancel func(error)) {
				v := item.(int)
				if v%3 == 0 {
					cancel(errDummy)
				}
				writer.Write(v * v)
			},
			expectErr: errDummy,
		},
		{
			mapper: func(item interface{}, writer Iterator, cancel func(error)) {
				v := item.(int)
				if v%3 == 0 {
					cancel(nil)
				}
				writer.Write(v * v)
			},
			expectErr: ErrCancelWithNil,
		},
		{
			reducer: func(pipe <-chan interface{}, cancel func(error)) {
				for item := range pipe {
					result := atomic.AddUint32(&value, uint32(item.(int)))
					if result > 10 {
						cancel(errDummy)
					}
				}
			},
			expectErr: errDummy,
		},
	}

	for _, test := range tests {
		t.Run(randName(), func(t *testing.T) {
			atomic.StoreUint32(&value, 0)

			if test.mapper == nil {
				test.mapper = func(item interface{}, writer Iterator, cancel func(error)) {
					v := item.(int)
					writer.Write(v * v)
				}
			}
			if test.reducer == nil {
				test.reducer = func(pipe <-chan interface{}, cancel func(error)) {
					for item := range pipe {
						atomic.AddUint32(&value, uint32(item.(int)))
					}
				}
			}
			err := MapReduceVoid(func(source chan<- interface{}) {
				for i := 1; i < 5; i++ {
					source <- i
				}
			}, test.mapper, test.reducer)

			assert.Equal(t, test.expectErr, err)
			if err == nil {
				assert.Equal(t, test.expectValue, atomic.LoadUint32(&value))
			}
		})
	}
}

func TestMapReduceVoidCancel(t *testing.T) {
	var result []int
	err := MapReduceVoid(func(source chan<- interface{}) {
		source <- 0
		source <- 1
	}, func(item interface{}, writer Iterator, cancel func(error)) {
		i := item.(int)
		if i == 1 {
			cancel(errors.New("anything"))
		}
		writer.Write(i)
	}, func(pipe <-chan interface{}, cancel func(error)) {
		for item := range pipe {
			i := item.(int)
			result = append(result, i)
		}
	})
	assert.NotNil(t, err)
	assert.Equal(t, "anything", err.Error())
}

func TestMapReduceWithIteratorWriteMoreThanOnce(t *testing.T) {
	assert.Panics(t, func() {
		MapReduce(func(source chan<- interface{}) {
			for i := 0; i < 10; i++ {
				source <- i
			}
		}, func(item interface{}, writer Iterator, cancel func(error)) {
			writer.Write(item)
		}, func(pipe <-chan interface{}, writer Iterator, cancel func(error)) {
			drain(pipe)
			writer.Write("one")
			writer.Write("two")
		})
	})
}

func TestMapReduceWithoutReducerWrite(t *testing.T) {
	uids := []int{1, 2, 3}
	res, err := MapReduce(func(source chan<- interface{}) {
		for _, uid := range uids {
			source <- uid
		}
	}, func(item interface{}, writer Iterator, cancel func(error)) {
		writer.Write(item)
	}, func(pipe <-chan interface{}, writer Iterator, cancel func(error)) {
		drain(pipe)
		// not calling writer.Write(...), should not panic
	})
	assert.Equal(t, ErrReduceNoOutput, err)
	assert.Nil(t, res)
}

func BenchmarkMapReduce(b *testing.B) {
	b.ReportAllocs()

	mapper := func(v interface{}, writer Iterator, cancel func(error)) {
		writer.Write(v.(int64) * v.(int64))
	}
	reducer := func(input <-chan interface{}, writer Iterator, cancel func(error)) {
		var result int64
		for v := range input {
			result += v.(int64)
		}
		writer.Write(result)
	}

	for i := 0; i < b.N; i++ {
		MapReduce(func(input chan<- interface{}) {
			for j := 0; j < 2; j++ {
				input <- int64(j)
			}
		}, mapper, reducer)
	}
}

