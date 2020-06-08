package store

import (
	"fmt"
	"log"
)

var kvs *KeyValueStore

type KeyValueStore struct {
	cache map[string] *string
}

func (kvs *KeyValueStore) Create(key, val string) error {
	v, ok := kvs.cache[key]
	log.Printf("[STORE] CREATE KEY: %v, EXISTING VALUE: %v, OK: %v", key, v, ok)
	if ok && v != nil {
		return fmt.Errorf("key '%s' already exists", key)
	}
	kvs.cache[key] = &val
	return nil
}

func (kvs *KeyValueStore) Delete(key string) error {
	v, ok := kvs.cache[key]
	log.Printf("[STORE] DELETE KEY: %v, EXISTING VALUE: %v, OK: %v", key, v, ok)
	if !ok || v == nil {
		return fmt.Errorf("key '%s' missing", key)
	}
	kvs.cache[key] = nil
	return nil
}

func (kvs *KeyValueStore) Read(key string) (string, error) {
	val, ok := kvs.cache[key]
	log.Printf("[STORE] READ KEY: %v, EXISTING VALUE: %v, OK: %v", key, val, ok)
	if !ok || val == nil {
		return "", fmt.Errorf("key '%s' missing", key)
	}
	return *val, nil
}

func (kvs *KeyValueStore) Update(key, val string) error {
	v, ok := kvs.cache[key]
	log.Printf("[STORE] UPDATE KEY: %v, EXISTING VALUE: %v, OK: %v", key, v, ok)
	if !ok || v == nil {
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
