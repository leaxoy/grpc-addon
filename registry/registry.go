package registry

import (
	"context"
)

// ServiceInfo is abstraction of service
type ServiceInfo struct {
	ServiceName string `yaml:"service_name"`
	Addr        string `yaml:"addr"`
	Port        int    `yaml:"port"`
	Weight      int    `yaml:"weight"`
}

// ServiceRegistry is interface that user can implement this to make a new register
type ServiceRegistry interface {
	Name() string
	Register(context.Context, ServiceInfo)
	Unregister(context.Context, ServiceInfo)
}

type nopRegistry struct{}

func (*nopRegistry) Name() string {
	return "nop"
}

func (*nopRegistry) Register(context.Context, ServiceInfo) {}

func (*nopRegistry) Unregister(context.Context, ServiceInfo) {}

var (
	m map[string]ServiceRegistry
)

// Register add a ServiceRegistry with name
func Register(rc ServiceRegistry) {
	m[rc.Name()] = rc
}

func GetRegistry(name string) ServiceRegistry {
	r, ok := m[name]
	if ok {
		return r
	}
	return new(nopRegistry)
}
