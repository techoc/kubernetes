/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package options

import (
	"fmt"

	"github.com/spf13/pflag"

	jobconfig "k8s.io/kubernetes/pkg/controller/job/config"
)

// JobControllerOptions 保存 Job 控制器的选项。
type JobControllerOptions struct {
	*jobconfig.JobControllerConfiguration
}

// AddFlags 将 Job 控制器相关参数注册到指定 FlagSet。
func (o *JobControllerOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.Int32Var(&o.ConcurrentJobSyncs, "concurrent-job-syncs", o.ConcurrentJobSyncs, "The number of job objects that are allowed to sync concurrently. Larger number = more responsive jobs, but more CPU (and network) load")
}

// ApplyTo 将选项写入 Job 控制器配置。
func (o *JobControllerOptions) ApplyTo(cfg *jobconfig.JobControllerConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.ConcurrentJobSyncs = o.ConcurrentJobSyncs

	return nil
}

// Validate 校验 Job 控制器选项。
func (o *JobControllerOptions) Validate() []error {
	if o == nil {
		return nil
	}

	errs := []error{}
	if o.ConcurrentJobSyncs < 1 {
		errs = append(errs, fmt.Errorf("concurrent-job-syncs must be greater than 0, but got %d", o.ConcurrentJobSyncs))
	}
	return errs
}
