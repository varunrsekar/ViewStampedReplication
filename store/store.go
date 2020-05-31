package store

import "fmt"

var kvs *KeyValueStore

type KeyValueStore struct {
	cache map[string] *string
}

func (kvs *KeyValueStore) Create(key, val string) error {
	_, ok := kvs.cache[key]
	if ok {
		return fmt.Errorf("key '%s' already exists", key)
	}
	kvs.cache[key] = &val
	return nil
}

func (kvs *KeyValueStore) Delete(key string) error {
	_, ok := kvs.cache[key]
	if !ok {
		return fmt.Errorf("key '%s' missing", key)
	}
	kvs.cache[key] = nil
	return nil
}

func (kvs *KeyValueStore) Read(key string) (string, error) {
	val, ok := kvs.cache[key]
	if !ok {
		return "", fmt.Errorf("key '%s' missing", key)
	}
	return *val, nil
}

func (kvs *KeyValueStore) Update(key, val string) error {
	_, ok := kvs.cache[key]
	if !ok {
		return fmt.Errorf("key '%s' missing", key)
	}
	kvs.cache[key] = &val
	return nil
}

func Get() *KeyValueStore {
	return kvs
}
func init() {
	kvs = &KeyValueStore{
		cache: make(map[string]*string),
	}
}
