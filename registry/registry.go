package registry

import (
	"context"
	"sync"
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
	Register(context.Context, ServiceInfo)
	Unregister(context.Context, ServiceInfo)
}

var (
	m  map[string]ServiceRegistry
	mu sync.RWMutex
)

// AddServiceRegistry add a ServiceRegistry with name
func AddServiceRegistry(name string, rc ServiceRegistry) {
	mu.Lock()
	defer mu.Unlock()
	if _, ok := m[name]; ok {
		panic("err: already registered, name: " + name)
	}
	m[name] = rc
}

// Register register a service with ServiceRegistry identify by name.
func Register(name string, info ServiceInfo) {
	RegisterContext(context.Background(), name, info)
}

func RegisterContext(ctx context.Context, name string, info ServiceInfo) {
	mu.Lock()
	rc, ok := m[name]
	mu.Unlock()
	if !ok {
		panic("err: missing register center info, forget import: " + name)
	}
	rc.Register(ctx, info)
}

// Unregister unregister a service with ServiceRegistry identify by name.
func Unregister(name string, info ServiceInfo) {
	UnregisterContext(context.Background(), name, info)
}

func UnregisterContext(ctx context.Context, name string, info ServiceInfo) {
	mu.Lock()
	rc, ok := m[name]
	mu.Unlock()
	if !ok {
		panic("err: missing register center info, forget import: " + name)
	}
	rc.Unregister(ctx, info)
}
