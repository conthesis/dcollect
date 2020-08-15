package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	nats "github.com/nats-io/nats.go"
	"log"
	"net/url"
	"os"
	"time"
	"go.uber.org/fx"
)

const base = "conthesis.dcollect."
const vsnGetTopic = base + "get"
const vsnStoreTopic = base + "store"
const listTopic = base + "list"

const notifyTopic = "entity-updates-v1"
const notifyAcceptedTopic = "entity-updates-v1.accepted"

func getRequiredEnv(env string) (string, error) {
	val := os.Getenv(env)
	if val == "" {
		return "", fmt.Errorf("`%s`, a required environment variable was not set", env)
	}
	return val, nil
}

func NewNats(lc fx.Lifecycle) (*nats.Conn, error) {
	natsURL, err := getRequiredEnv("NATS_URL")
	if err != nil {
		return nil, err
	}
	nc, err := nats.Connect(natsURL)

	if err != nil {
		if err, ok := err.(*url.Error); ok {
			return nil, fmt.Errorf("NATS_URL is of an incorrect format: %w", err)
		}
		return nil, err
	}

	lc.Append(fx.Hook{
		OnStop: func(ctx context.Context) error {
			return nc.Drain()
		},
	})

	return nc, nil
}

func NewVSN(nc *nats.Conn, storage Storage) *vsn {
	return &vsn{nc: nc, storage: storage}
}

type vsn struct {
	nc      *nats.Conn
	storage Storage
	done    chan bool
}

// URLs on the internet are generally fewer than 2048 charactrs. With 4096 we can store maxed out URLs
const keyMaxLength = 4096

func (v *vsn) getHandler(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	if len(m.Data) > keyMaxLength {
		log.Printf("Provided key too long key = '%s'", m.Data)
	}
	data, err := v.storage.Get(ctx, m.Data)
	if err != nil {
		log.Printf("Error fetching pointer %+q, err: %s", m.Data, err)
		m.Respond([]byte("ERR"))
		return
	}
	m.Respond(data)
}

func (v *vsn) storeHandler(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	parts := bytes.SplitN(m.Data, []byte("\n"), 2)
	if len(parts) != 2 {
		log.Printf("Incorrectly formated store request ignored")
		m.Respond([]byte("ERR"))
		return
	}

	key := parts[0]
	pointer := parts[1]

	notify, err := v.storage.Store(ctx, key, pointer)
	if err != nil {
		log.Printf("Error storing key: %+q, err: %s", key, err)
		m.Respond([]byte("ERR"))
	}
	v.nc.Publish(notifyTopic, []byte(notify))
	m.Respond([]byte(fmt.Sprintf("OK %+q", notify)))
}

func (v *vsn) acceptedHandler(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err := v.storage.removeNotify(ctx, m.Data)
	if err != nil {
		log.Printf("Error removing notify entry: %s, was %v", err, string(m.Data))
	}
}


type ListResponse struct {
	Success bool `json:"success"`
	Status string `json:"status,omitempty"`
	Contents []string `json:"contents,omitempty"`
}

func respondWithList(m *nats.Msg, response ListResponse) error {
	data, err := json.Marshal(response)
	if err != nil {
		return err
	}
	return m.Respond(data)
}

func (v *vsn) listHandler(m *nats.Msg) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	res, err := v.storage.List(ctx, m.Data)

	if err != nil {
		resp := ListResponse{Success: false, Status: "INTERNAL_ERROR"}
		if err := respondWithList(m, resp); err != nil {
			log.Printf("Unable to respond %v", err)
		}
	}

	resp := ListResponse{Success: true, Contents: res}
	if err := respondWithList(m, resp); err != nil {
		log.Printf("Unable to respond %v", err)
	}
}

func setupSubscriptions(v *vsn) error {
	x := map[string]nats.MsgHandler{
		vsnStoreTopic: v.storeHandler,
		vsnGetTopic: v.getHandler,
		notifyAcceptedTopic: v.acceptedHandler,
		listTopic: v.listHandler,
	}
	for subj, handler := range x {
		if _, err := v.nc.Subscribe(subj, handler); err != nil {
			return err
		}
	}
	return nil
}

func (v *vsn) watcherRound() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	ns, err := v.storage.getNotifys(ctx)
	if err != nil {
		log.Printf("Error while fetching pending notifications: %s", err)
		return
	}
	for _, x := range ns {
		err := v.nc.Publish(notifyTopic, []byte(x))
		if err != nil {
			log.Printf("Error while retrying pending notification: %s", err)
		}
	}
	if n := len(ns); n > 0 {
		log.Printf("Sent %d notifications", n)
	}
}
func watcherLoop(v *vsn) {
	ticker := time.NewTicker(5 * time.Second)
	for {
		select {
		case <-v.done:
			return
		case <-ticker.C:
			v.watcherRound()
		}
	}
}

func setupWatcherLoop(lc fx.Lifecycle, v *vsn) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			go watcherLoop(v)
			return nil
		},
		OnStop: func(ctx context.Context) error {
			v.done<- true
			return nil
		},
	})
}

func main() {
	fx.New(
		fx.Provide(
			NewNats,
			NewStorage,
			NewVSN,
		),
		fx.Invoke(setupSubscriptions),
		fx.Invoke(setupWatcherLoop),
	).Run()
}
