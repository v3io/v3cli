/*
Copyright 2016 Iguazio Systems Ltd.

Licensed under the Apache License, Version 2.0 (the "License") with
an addition restriction as set forth herein. You may not use this
file except in compliance with the License. You may obtain a copy of
the License at http://www.apache.org/licenses/LICENSE-2.0.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License.

In addition, you may not use the software for any purposes that are
illegal under applicable law, and the grant of the foregoing license
under the Apache 2.0 license is conditioned upon your compliance with
such restriction.
*/

package config

import (
	"github.com/ghodss/yaml"
	"github.com/pkg/errors"
	"io/ioutil"
	"os"
	"strings"
)

const (
	DefaultLoggingLevel           = "debug"
	V3ioConfigEnvironmentVariable = "V3IO_CONF"
	DefaultConfigurationFileName  = "v3io.yaml"
	defaultNumberOfIngestWorkers  = 8
	defaultNumberOfQueryWorkers   = 8
)

type V3ioConfig struct {
	// V3IO TSDB connection information - web-gateway service endpoint,
	// TSDB data container, relative TSDB table path within the container, and
	// authentication credentials for the web-gateway service
	WebApiEndpoint string `json:"webApiEndpoint"`
	Container      string `json:"container"`
	TablePath      string `json:"tablePath"`
	Username       string `json:"username,omitempty"`
	Password       string `json:"password,omitempty"`

	// Logging level (for verbose output) - "debug" | "info" | "warn" | "error"
	LogLevel string `json:"logLevel,omitempty"`
	// Number of parallel V3IO worker routines
	Workers int `json:"workers"`
	// Number of parallel V3IO worker routines for queries;
	// default = the minimum value between 8 and Workers
	QryWorkers int `json:"qryWorkers"`
}

func GetOrLoadFromFile(path string) (*V3ioConfig, error) {
	var resolvedPath string

	if strings.TrimSpace(path) != "" {
		resolvedPath = path
	} else {
		envPath := os.Getenv(V3ioConfigEnvironmentVariable)
		if envPath != "" {
			resolvedPath = envPath
		}
	}

	if resolvedPath == "" {
		resolvedPath = DefaultConfigurationFileName
	}

	var data []byte
	if _, err := os.Stat(resolvedPath); err != nil {
		if os.IsNotExist(err) {
			data = []byte{}
		} else {
			return nil, errors.Wrap(err, "Failed to read the TSDB configuration.")
		}
	} else {
		data, err = ioutil.ReadFile(resolvedPath)
		if err != nil {
			return nil, err
		}

		if len(data) == 0 {
			return nil, errors.Errorf("Configuration file '%s' exists but its content is invalid.", resolvedPath)
		}
	}

	return loadFromData(data)
}

func loadFromData(data []byte) (*V3ioConfig, error) {
	cfg := V3ioConfig{}
	err := yaml.Unmarshal(data, &cfg)

	if err != nil {
		return nil, err
	}

	initDefaults(&cfg)

	return &cfg, err
}

func initDefaults(cfg *V3ioConfig) {
	if cfg.Workers == 0 {
		cfg.Workers = defaultNumberOfIngestWorkers
	}

	// Initialize the default number of Query workers if not set to Min(8,Workers)
	if cfg.QryWorkers == 0 {
		if cfg.Workers < defaultNumberOfQueryWorkers {
			cfg.QryWorkers = cfg.Workers
		} else {
			cfg.QryWorkers = defaultNumberOfQueryWorkers
		}
	}
}
