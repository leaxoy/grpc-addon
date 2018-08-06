package list

import (
	"strings"

	"google.golang.org/grpc/resolver"
)

func init() {
	resolver.Register(new(listBuilder))
}

type (
	listBuilder  struct{}
	listResolver struct {
		target resolver.Target
		cc     resolver.ClientConn

		service string
	}
)

func (*listBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOption) (resolver.Resolver, error) {
	r := &listResolver{
		target:  target,
		cc:      cc,
		service: target.Endpoint,
	}
	go r.start()
	return r, nil
}

func (*listBuilder) Scheme() string {
	return "list"
}

func (r *listResolver) start() {
	var addrs []resolver.Address
	for _, s := range strings.Split(r.target.Authority, ",") {
		addrs = append(addrs, resolver.Address{Addr: s, ServerName: r.service})
	}
	r.cc.NewAddress(addrs)
}

func (*listResolver) ResolveNow(o resolver.ResolveNowOption) {}

func (*listResolver) Close() {}
