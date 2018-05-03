package consistenthash

import (
	"fmt"
	"strconv"

	"github.com/leaxoy/consistenthash"
	"golang.org/x/net/context"
	"google.golang.org/grpc/balancer"
	"google.golang.org/grpc/balancer/base"
	"google.golang.org/grpc/resolver"
)

type (
	consistentHashBuilder struct{}
	consistentHashPicker  struct {
		hash       *consistenthash.Map
		addrToConn map[string]balancer.SubConn
		pickStatus int
	}
)

const (
	// Name is this balancer name
	Name = "consistent_hash"
	// ContextKey to carry the metadata decide how to select a remove server, value should be string.
	ContextKey = "_consistent_context_key"
)

func builder() balancer.Builder {
	return base.NewBalancerBuilder(Name, &consistentHashBuilder{})
}

func (*consistentHashBuilder) Build(readySCs map[resolver.Address]balancer.SubConn) balancer.Picker {
	p := &consistentHashPicker{
		hash:       consistenthash.New(20, nil),
		addrToConn: make(map[string]balancer.SubConn),
	}
	for addr, conn := range readySCs {
		p.hash.Add(addr.Addr)
		p.addrToConn[addr.Addr] = conn
	}
	return p
}

func (p *consistentHashPicker) Pick(ctx context.Context, opts balancer.PickOptions) (balancer.SubConn, func(balancer.DoneInfo), error) {
	if p.hash.IsEmpty() {
		return nil, nil, balancer.ErrNoSubConnAvailable
	}
	var pickerKey string
	if meta := ctx.Value(ContextKey); meta != nil {
		if metaString, ok := meta.(string); ok {
			pickerKey = metaString
		} else {
			pickerKey = fmt.Sprintf("%+v", meta)
		}
	} else {
		pickerKey = strconv.Itoa(p.pickStatus)
		p.pickStatus++
	}
	addr := p.hash.Get(pickerKey)
	return p.addrToConn[addr], nil, nil
}

func init() {
	balancer.Register(builder())
}
