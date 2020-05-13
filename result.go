package data

import (
	"sync"
)

type Result struct {
	data      sync.Map
	WaitGroup sync.WaitGroup
}

func (r *Result) Add() {
	r.WaitGroup.Add(1)
}

func (r *Result) Done() {
	r.WaitGroup.Done()
}

func (r *Result) Wait() {
	r.WaitGroup.Wait()
}

func (r *Result) write(key string, val interface{}) {
	r.data.Store(key, val)
}

func (r *Result) read(key string) interface{} {
	result, _ := r.data.Load(key)
	return result
}

func (r *Result) exist(key string) bool {
	_, result := r.data.Load(key)
	return result
}

func (r *Result) del(key string) {
	r.data.Delete(key)
}

func (r *Result) Map() map[string]interface{} {
	var result map[string]interface{}
	r.data.Range(func(key, value interface{}) bool {
		k := key.(string)
		if k == "" {
			return false
		}
		result[k] = value
		return true
	})
	return result
}
