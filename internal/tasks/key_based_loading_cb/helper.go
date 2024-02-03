package key_based_loading_cb

import "github.com/couchbaselabs/sirius/internal/task_errors"

type SingleOperationConfig struct {
	Keys     []string `json:"keys" doc:"true"`
	Template string   `json:"template" doc:"true"`
	DocSize  int      `json:"docSize" doc:"true"`
}

func ConfigSingleOperationConfig(s *SingleOperationConfig) error {
	if s == nil {
		return task_errors.ErrParsingSingleOperationConfig
	}
	return nil
}

type SingleSubDocOperationConfig struct {
	Key     string   `json:"key" doc:"true"`
	Paths   []string `json:"paths" doc:"true"`
	DocSize int      `json:"docSize" doc:"true"`
}

func ConfigSingleSubDocOperationConfig(s *SingleSubDocOperationConfig) error {
	if s == nil {
		return task_errors.ErrParsingSingleSubDocOperationConfig
	}
	return nil
}
