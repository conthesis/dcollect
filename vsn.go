package main

import (
	"context"
	nats "github.com/nats-io/nats.go"
	"log"
	"os"
	"os/signal"
	"syscall"
	"net/url"
	"bytes"
	"time"
	"fmt"
)

const vsnGetTopic = "conthesis.dcollect.get"
const vsnStoreTopic = "conthesis.dcollect.store"
const notifyTopic = "entity-updates-v1"
const notifyAcceptedTopic = "entity-updates-v1.accepted"


func getRequiredEnv(env string) string {
	val := os.Getenv(env)
	if val == "" {
		log.Fatalf("`%s`, a required environment variable was not set", env)
	}
	return val
}

func connectNats() *nats.Conn {
	natsURL := getRequiredEnv("NATS_URL")
	nc, err := nats.Connect(natsURL)

	if err != nil {
		if err, ok := err.(*url.Error); ok {
			log.Fatalf("NATS_URL is of an incorrect format: %s", err.Error())
		}
		log.Fatalf("Failed to connect to NATS %T: %s", err, err)
	}
	return nc
}

func setupStorage() Storage {
	storage, err := newStorage()
	if err != nil {
		log.Fatalf("Error setting up storage %s", err)
	}
	return storage
}

func setupVSN() *vsn {
	nc := connectNats()
	storage := setupStorage()
	return &vsn{nc: nc, storage: storage}
}

type vsn struct {
	nc      *nats.Conn
	storage Storage
	done chan bool
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
		log.Printf("Error removing notify entry: %s", err)
	}
}

func waitForTerm() {
	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		done <- true
	}()
	<-done
}

func (v *vsn) setupSubscriptions() {
	_, err := v.nc.Subscribe(vsnStoreTopic, v.storeHandler)
	if err != nil {
		log.Fatalf("Unable to subscribe to topic %s: %s", vsnGetTopic, err)
	}
	_, err = v.nc.Subscribe(vsnGetTopic, v.getHandler)
	if err != nil {
		log.Fatalf("Unable to subscribe to topic %s: %s", vsnStoreTopic, err)
	}

	_, err = v.nc.Subscribe(notifyAcceptedTopic, v.acceptedHandler)
	if err != nil {
		log.Fatalf("Unable to subscribe to topic %s: %s", vsnStoreTopic, err)
	}
}

func (v *vsn) watcherRound() {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	ns, err := v.storage.getNotifys(ctx)
	if (err != nil) {
		log.Printf("Error while fetching pending notifications %s", err)
		return
	}
	for _, x := range(ns) {
		v.nc.Publish(notifyTopic, []byte(x))
	}

}
func (v *vsn) watcherLoop() {
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

func (v *vsn) Close() {
	v.done <- true
	log.Printf("Shutting down...")
	v.nc.Drain()
	v.storage.Close()
}

func main() {
	vsn := setupVSN()
	defer vsn.Close()
	vsn.setupSubscriptions()
	go vsn.watcherLoop()
	log.Printf("Connected to NATS")
	waitForTerm()
}
