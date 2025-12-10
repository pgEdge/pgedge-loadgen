package apps

import (
	"fmt"
	"sync"
)

var (
	registry = make(map[string]App)
	mu       sync.RWMutex
)

// Register adds an application to the registry.
func Register(app App) {
	mu.Lock()
	defer mu.Unlock()
	registry[app.Name()] = app
}

// Get retrieves an application by name.
func Get(name string) (App, error) {
	mu.RLock()
	defer mu.RUnlock()

	app, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("unknown application: %s", name)
	}
	return app, nil
}

// List returns all registered application names.
func List() []string {
	mu.RLock()
	defer mu.RUnlock()

	names := make([]string, 0, len(registry))
	for name := range registry {
		names = append(names, name)
	}
	return names
}

// All returns all registered applications.
func All() []App {
	mu.RLock()
	defer mu.RUnlock()

	apps := make([]App, 0, len(registry))
	for _, app := range registry {
		apps = append(apps, app)
	}
	return apps
}
