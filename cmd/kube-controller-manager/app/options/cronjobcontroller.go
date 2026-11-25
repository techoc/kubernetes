/*
Copyright 2020 The Kubernetes Authors.

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

	cronjobconfig "k8s.io/kubernetes/pkg/controller/cronjob/config"
)

// CronJobControllerOptions 保存 CronJob 控制器的选项。
type CronJobControllerOptions struct {
	*cronjobconfig.CronJobControllerConfiguration
}

// AddFlags 将 CronJob 控制器相关参数注册到指定 FlagSet。
func (o *CronJobControllerOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.Int32Var(&o.ConcurrentCronJobSyncs, "concurrent-cron-job-syncs", o.ConcurrentCronJobSyncs, "The number of cron job objects that are allowed to sync concurrently. Larger number = more responsive jobs, but more CPU (and network) load")
}

// ApplyTo 将选项写入 CronJob 控制器配置。
func (o *CronJobControllerOptions) ApplyTo(cfg *cronjobconfig.CronJobControllerConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.ConcurrentCronJobSyncs = o.ConcurrentCronJobSyncs

	return nil
}

// Validate 校验 CronJob 控制器选项。
func (o *CronJobControllerOptions) Validate() []error {
	if o == nil {
		return nil
	}

	errs := []error{}
	if o.ConcurrentCronJobSyncs < 1 {
		errs = append(errs, fmt.Errorf("concurrent-cron-job-syncs must be greater than 0, but got %d", o.ConcurrentCronJobSyncs))
	}

	return errs
}
