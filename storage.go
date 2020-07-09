package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"log"
	"math/rand"
)

// Storage is an interface common for storage engines
type Storage interface {
	// Gets the current hash for the key
	Get(context.Context, []byte) ([]byte, error)
	// Store stores the passed in pointer at that key
	Store(context.Context, []byte, []byte) (string, error)
	removeNotify(context.Context, []byte) error
	getNotifys(context.Context) ([]string, error)
	Close()
}

// RedisStorage is storage engine storing things to Redis
type RedisStorage struct {
	client *redis.Client
}

func vsnKey(key []byte) string {
	prefix := "vsn:"
	return prefix + string(key)
}

func notifySetEntry(key []byte) string {
	bfr := bytes.NewBuffer(key)
	randBuf := make([]byte, 4)
	// cannot return err
	rand.Read(randBuf)
	bfr.WriteString("\000")
	bfr.Write(randBuf)
	return bfr.String()

}

const notifySetKey = "vsn_notify"

func (r *RedisStorage) Get(ctx context.Context, key []byte) ([]byte, error) {
	data, err := r.client.LRange(ctx, string(vsnKey(key)), 0, 0).Result()
	if err == redis.Nil {
		return []byte(""), nil
	}
	n_data := len(data)
	if n_data == 0 {
		return []byte(""), nil
	} else {
		return []byte(data[0]), err
	}
}

func (r *RedisStorage) Store(ctx context.Context, key []byte, data []byte) (string, error) {
	pipe := r.client.TxPipeline()
	pipe.LPush(ctx, string(vsnKey(key)), data)
	setEntry := notifySetEntry(key)
	sadd := pipe.SAdd(ctx, notifySetKey, setEntry)
	pipe.Exec(ctx)
	if err := sadd.Err(); err != nil {
		return "", err
	}
	return setEntry, nil
}

func (r *RedisStorage) removeNotify(ctx context.Context, data []byte) error {
	n, err := r.client.SRem(ctx, notifySetKey, data).Result()
	if err != nil {
		return err
	}
	if n < 1 {
		return errors.New("Not properly removed")
	}
	return nil
}

func (r *RedisStorage) getNotifys(ctx context.Context) ([]string, error) {
	return r.client.SRandMemberN(ctx, notifySetKey, 35).Result()
}

func (r *RedisStorage) Close() {
	if err := r.client.Close(); err != nil {
		log.Printf("Error closing storage driver %s", err)
	}
}

// newRedisStorage creates a new redis storage
func newRedisStorage() (Storage, error) {
	redisURL := getRequiredEnv("REDIS_URL")
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return Storage(&RedisStorage{
		client: redis.NewClient(opts),
	}), nil
}

var ErrNoSuchStorageDriver = errors.New("No such storage driver found")

func newStorage() (Storage, error) {
	storageDriver := getRequiredEnv("STORAGE_DRIVER")
	if storageDriver == "redis" {
		return newRedisStorage()
	} else {
		return nil, ErrNoSuchStorageDriver
	}
}
