package random

import (
	"context"
	"math/rand"

	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

type (
	randomBuilder struct{}
	randomPicker  struct {
		conns []balancer.SubConn
		seed  int64
	}
)

const (
	// Name is the balancer name.
	Name = "random"
)

func newBuilder() balancer.Builder {
	return base.NewBalancerBuilder(Name, &randomBuilder{})
}

func (*randomBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	var conns []balancer.SubConn
	for _, conn := range readySCs {
		conns = append(conns, conn)
	}
	return &randomPicker{
		conns: conns,
		seed:  rand.Int63(),
	}
}

func (p *randomPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.SubConn, func(balancer.DoneInfo), error) {
	if len(p.conns) == 0 {
		return nil, nil, balancer.ErrNoSubConnAvailable
	}
	return p.conns[rand.Intn(len(p.conns))], nil, nil
}

func init() {
	balancer.Register(newBuilder())
}
