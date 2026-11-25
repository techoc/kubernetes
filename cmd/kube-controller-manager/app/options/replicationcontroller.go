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
	"github.com/spf13/pflag"

	replicationconfig "k8s.io/kubernetes/pkg/controller/replication/config"
)

// ReplicationControllerOptions 保存复制控制器的选项。
type ReplicationControllerOptions struct {
	*replicationconfig.ReplicationControllerConfiguration
}

// AddFlags 将复制控制器相关参数注册到指定 FlagSet。
func (o *ReplicationControllerOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.Int32Var(&o.ConcurrentRCSyncs, "concurrent_rc_syncs", o.ConcurrentRCSyncs, "The number of replication controllers that are allowed to sync concurrently. Larger number = more responsive replica management, but more CPU (and network) load")
}

// ApplyTo 将选项写入复制控制器配置。
func (o *ReplicationControllerOptions) ApplyTo(cfg *replicationconfig.ReplicationControllerConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.ConcurrentRCSyncs = o.ConcurrentRCSyncs

	return nil
}

// Validate 校验复制控制器选项。
func (o *ReplicationControllerOptions) Validate() []error {
	if o == nil {
		return nil
	}

	errs := []error{}
	return errs
}
