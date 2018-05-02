package etcd

import (
	"context"
	"log"
	"strings"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/leaxoy/grpc-addon/registry"
)

type (
	etcdRegistry struct {
		client *clientv3.Client
		kv     clientv3.KV

		keyTTL        int64
		heartBeatRate int64

		putHandler     PutHandler
		deleteHandler  DeleteHandler
		kvBuilder      KVBuilder
		endpointParser EndpointParser
	}
	// Option is the common interface do operation on etcd registry
	Option func(*etcdRegistry)

	// PutHandler handle etcd put result
	PutHandler func(*clientv3.PutResponse, error)
	// DeleteHandler handle etcd delete result
	DeleteHandler func(*clientv3.DeleteResponse, error)
	// EndpointParser set how to parse endpoint
	EndpointParser func(string) []string
	// KVBuilder build k-v from ServiceInfo
	KVBuilder func(registry.ServiceInfo) (string, string)
)

const (
	// Name is registry name, useful for external user
	Name                     = "_etcd_registry"
	defaultHeartBeatInterval = 3 * 60
	defaultTTL               = 5 * 60
	// keyPrefix set prefix of etcd storage, this should sync with `ectd resolver` setting
	keyPrefix = "/_grpc/service/"
)

// WithKeyTTL return a set ttl Option, default is 300
func WithKeyTTL(ttl int64) Option {
	return func(i *etcdRegistry) {
		i.keyTTL = ttl
	}
}

// WithHeartBeatRate return a set heart beat rate Option, default is 180
func WithHeartBeatRate(rate int64) Option {
	return func(i *etcdRegistry) {
		i.heartBeatRate = rate
	}
}

// WithPutHandler set etcd put handler, default is nil
func WithPutHandler(fn PutHandler) Option {
	return func(i *etcdRegistry) {
		i.putHandler = fn
	}
}

// WithDeleteHandler set etcd delete handler, default is nil
func WithDeleteHandler(fn DeleteHandler) Option {
	return func(i *etcdRegistry) {
		i.deleteHandler = fn
	}
}

// WithEndpointParser set how to parse endpoint, default is split string by `,`
func WithEndpointParser(fn EndpointParser) Option {
	return func(i *etcdRegistry) {
		i.endpointParser = fn
	}
}

// WithKVBuilder set kv builder, default is nil, must set.
func WithKVBuilder(fn KVBuilder) Option {
	return func(i *etcdRegistry) {
		i.kvBuilder = fn
	}
}

// NewEtcdRegistry return an etcd based ServiceRegistry
func NewEtcdRegistry(endpoint string, opts ...Option) registry.ServiceRegistry {
	p := &etcdRegistry{
		keyTTL:        defaultTTL,
		heartBeatRate: defaultHeartBeatInterval,
		endpointParser: func(endpoint string) []string {
			return strings.Split(endpoint, ",")
		},
	}
	for _, opt := range opts {
		opt(p)
	}
	c, err := clientv3.New(clientv3.Config{
		Endpoints: p.endpointParser(endpoint),
	})
	if err != nil {
		return nil
	}
	p.client = c
	p.kv = clientv3.NewKV(c)
	return p
}

func (p *etcdRegistry) Register(ctx context.Context, info registry.ServiceInfo) {
	key, value := p.kvBuilder(info)
	lease, _ := p.client.Grant(context.Background(), p.keyTTL)
	resp, err := p.kv.Put(ctx, keyPrefix+key, value, clientv3.WithLease(lease.ID))
	if p.putHandler != nil {
		p.putHandler(resp, err)
	}
	if err != nil {
		log.Fatal(err)
	}
	go p.heartBeat(context.Background(), key, value)
}

func (p *etcdRegistry) heartBeat(ctx context.Context, key, value string) {
	ticker := time.Tick(time.Duration(p.heartBeatRate) * time.Second)
	for {
		select {
		case <-ticker:
			p.kv.Put(ctx, keyPrefix+key, value)
		}
	}
}

func (p *etcdRegistry) Unregister(ctx context.Context, info registry.ServiceInfo) {
	resp, err := p.kv.Delete(ctx, "")
	if p.deleteHandler != nil {
		p.deleteHandler(resp, err)
	}
	if err != nil {
		log.Fatal(err)
	}
}
