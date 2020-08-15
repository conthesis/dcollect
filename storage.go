package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/go-redis/redis/v8"
	"math/rand"
	"encoding/base64"
	"strings"

	"go.uber.org/fx"
)

const RoundSize = 10

// Storage is an interface common for storage engines
type Storage interface {
	// Gets the current hash for the key
	Get(context.Context, []byte) ([]byte, error)
	// Store stores the passed in pointer at that key
	Store(context.Context, []byte, []byte) (string, error)
	List(context.Context, []byte) ([]string, error)
	removeNotify(context.Context, []byte) error
	getNotifys(context.Context) ([]string, error)
	Close(context.Context) error
}

// RedisStorage is storage engine storing things to Redis
type RedisStorage struct {
	client *redis.Client
}

const KEY_PREFIX = "vsn:"

func vsnKey(key []byte) string {
	return KEY_PREFIX + string(key)
}

func notifySetEntry(key []byte) string {
	bfr := bytes.NewBuffer(key)
	randBuf := make([]byte, 4)
	// cannot return err
	rand.Read(randBuf)
	bfr.WriteString("\000")
	bfr.WriteString(base64.StdEncoding.EncodeToString(randBuf))
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
	return r.client.SRandMemberN(ctx, notifySetKey, RoundSize).Result()
}

func (r *RedisStorage) List(ctx context.Context, path []byte) ([]string, error) {
	res := r.client.Keys(ctx, vsnKey(path) + "*")
	if err := res.Err(); err != nil {
		return nil, err
	}
	rawData := res.Val()
	buf := make([]string, 0, len(rawData))
	for _, v := range rawData {
		buf = append(buf, strings.TrimPrefix(v, KEY_PREFIX))
	}
	return buf, nil
}

func (r *RedisStorage) Close(ctx context.Context) error {
	return r.client.Close()
}

// newRedisStorage creates a new redis storage
func newRedisStorage() (Storage, error) {
	redisURL, err := getRequiredEnv("REDIS_URL")
	if err != nil {
		return nil, err
	}
	opts, err := redis.ParseURL(redisURL)
	if err != nil {
		return nil, err
	}
	return Storage(&RedisStorage{
		client: redis.NewClient(opts),
	}), nil
}

var ErrNoSuchStorageDriver = errors.New("No such storage driver found")

func NewStorage(lc fx.Lifecycle) (Storage, error) {
	storageDriver, err := getRequiredEnv("STORAGE_DRIVER")
	if err != nil {
		return nil, err
	}
	if storageDriver == "redis" {
		storage, err := newRedisStorage()
		if err != nil {
			return nil, err
		}
		lc.Append(fx.Hook{
			OnStop: storage.Close,
		})
		return storage, nil
	} else {
		return nil, ErrNoSuchStorageDriver
	}
}
