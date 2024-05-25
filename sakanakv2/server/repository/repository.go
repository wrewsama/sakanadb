package repository

import "sync"

type Repository interface {
	Get(key string) (any, bool)
	Set(key, value string)
	Delete(key string)
}

type repo struct {
	hashmap sync.Map
}

func NewRepo() Repository {
	return &repo{
		hashmap: sync.Map{},
	}
}
		
func (r *repo) Get(key string) (any, bool) {
	return r.hashmap.Load(key)
}

func (r *repo) Set(key, value string) {
	r.hashmap.Store(key, value)
}

func (r *repo) Delete(key string) {
	r.hashmap.Delete(key)
}