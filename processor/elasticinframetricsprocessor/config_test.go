package elasticinframetricsprocessor

import (
	"testing"
)

// TestConfigValidate tests the Validate method of the Config struct.
func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		config  Config
		wantErr bool
	}{
		{
			name:    "Default configuration",
			config:  Config{},
			wantErr: false,
		},
		{
			name:    "AddSystemMetrics true",
			config:  Config{AddSystemMetrics: true},
			wantErr: false,
		},
		{
			name:    "AddSystemMetrics false",
			config:  Config{AddSystemMetrics: false},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.config.Validate(); (err != nil) != tt.wantErr {
				t.Errorf("Config.Validate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
