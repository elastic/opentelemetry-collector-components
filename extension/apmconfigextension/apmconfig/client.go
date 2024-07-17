package apmconfig

import (
	"context"
)

// Params holds parameters for watching and notifying for config changes.
type Params struct {
	// Service holds the name and optionally environment name used
	// for filtering the config to watch.
	Service struct {
		Name        string
		Environment string
	}

	// Config holds the optionally hash used for filtering the remote config query.
	Config struct {
		Hash []byte
	}
}

type RemoteClient interface {
	// RemoteConfig returns the upstream remote configuration that needs to be applied. Empty RemoteConfig Attrs if no remote configuration is available for the specified service.
	RemoteConfig(context.Context, Params) (RemoteConfig, error)

	// EffectiveConfig notifies the upstream server about the latest applied configuration.
	EffectiveConfig(context.Context, Params) error
}

// RemoteConfig holds an agent remote configuration.
type RemoteConfig struct {
	Hash []byte

	Attrs map[string]string
}
