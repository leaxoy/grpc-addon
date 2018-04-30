package etcd

import (
	"context"
	"strings"
	"time"

	etcd "github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/mvcc/mvccpb"
	"google.golang.org/grpc/resolver"
)

func init() {
	resolver.Register(&etcdBuilder{})
}

const (
	keyPrefix = "/_grpc/service/"
)

type (
	etcdBuilder  struct{}
	etcdResolver struct {
		cc      resolver.ClientConn
		client  *etcd.Client
		service string
		addrs   []resolver.Address
		ctx     context.Context
		cancel  context.CancelFunc

		initialized bool
	}
)

func (*etcdBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	client, _ := etcd.New(etcd.Config{
		Endpoints:        strings.Split(target.Authority, ","),
		Context:          ctx,
		AutoSyncInterval: time.Second * 10,
	})
	r := &etcdResolver{
		cc:      cc,
		client:  client,
		addrs:   []resolver.Address{},
		service: target.Endpoint,

		ctx:    ctx,
		cancel: cancel,
	}
	go r.start()
	return r, nil
}

func (*etcdBuilder) Scheme() string {
	return "etcd"
}

func (r *etcdResolver) start() {
	select {
	default:
	case <-r.ctx.Done():
		return
	}
	if !r.initialized {
		response, err := r.client.Get(r.ctx, keyPrefix+r.service, etcd.WithPrefix())
		if err == nil {
			for _, kv := range response.Kvs {
				r.addrs = append(r.addrs, resolver.Address{Addr: string(kv.Value), ServerName: r.service})
			}
			r.cc.NewAddress(r.addrs)
			r.initialized = true
		}
	}
	for {
		select {
		case <-r.ctx.Done():
			return
		case response := <-r.client.Watch(r.ctx, keyPrefix+r.service, etcd.WithPrefix()):
			for _, event := range response.Events {
				switch event.Type {
				case mvccpb.PUT:
					r.updateServer(string(event.PrevKv.Value), string(event.Kv.Value))
				case mvccpb.DELETE:
					r.removeServer(string(event.Kv.Value))
				}
			}
			r.cc.NewAddress(r.addrs)
		}
	}
}

func (r *etcdResolver) updateServer(prev, current string) {
	r.removeServer(prev)
	r.addrs = append(r.addrs, resolver.Address{Addr: current, ServerName: r.service})
}

func (r *etcdResolver) removeServer(server string) {
	var servers []resolver.Address
	for _, s := range r.addrs {
		if s.Addr == server {
			continue
		}
		servers = append(servers, s)
	}
	r.addrs = servers
}

func (*etcdResolver) ResolveNow(o resolver.ResolveNowOption) {}

func (r *etcdResolver) Close() {
	r.cancel()
	r.client.Close()
}
