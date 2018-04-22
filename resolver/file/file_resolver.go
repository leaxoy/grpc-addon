package file

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/fsnotify/fsnotify"
	"google.golang.org/grpc/resolver"
)

func init() {
	resolver.Register(&fileBuilder{})
}

type (
	fileBuilder  struct{}
	fileResolver struct {
		cc          resolver.ClientConn
		location    string
		addrs       []resolver.Address
		initialized bool

		ctx    context.Context
		cancel context.CancelFunc
	}
)

func (*fileBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	ctx, cancel := context.WithCancel(context.Background())
	r := &fileResolver{
		cc:       cc,
		location: target.Authority,

		ctx:    ctx,
		cancel: cancel,
	}
	go r.start()
	return r, nil
}

func (*fileBuilder) Scheme() string {
	return "file"
}

func (r *fileResolver) start() {
	select {
	default:
	case <-r.ctx.Done():
		return
	}
	if !r.initialized {
		buf, err := ioutil.ReadFile(r.location)
		if err == nil {
			r.addrs = extractAddrs(buf)
			r.cc.NewAddress(r.addrs)
			r.initialized = true
		}
	}
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		r.Close()
		return
	}
	if err = watcher.Add(r.location); err != nil {
		r.Close()
		return
	}
	for {
		select {
		case <-r.ctx.Done():
			watcher.Close()
			return
		case event := <-watcher.Events:
			if event.Op&fsnotify.Write != 0 {

			}
		case err := <-watcher.Errors:
			fmt.Printf("%s", err.Error())
		}
	}
}

func extractAddrs(data []byte) []resolver.Address {
	return nil
}

func (*fileResolver) ResolveNow(o resolver.ResolveNowOption) {}

func (r *fileResolver) Close() {
	r.cancel()
}
