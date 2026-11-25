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

	namespaceconfig "k8s.io/kubernetes/pkg/controller/namespace/config"
)

// NamespaceControllerOptions 保存命名空间控制器的选项。
type NamespaceControllerOptions struct {
	*namespaceconfig.NamespaceControllerConfiguration
}

// AddFlags 将命名空间控制器相关参数注册到指定 FlagSet。
func (o *NamespaceControllerOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.DurationVar(&o.NamespaceSyncPeriod.Duration, "namespace-sync-period", o.NamespaceSyncPeriod.Duration, "The period for syncing namespace life-cycle updates")
	fs.Int32Var(&o.ConcurrentNamespaceSyncs, "concurrent-namespace-syncs", o.ConcurrentNamespaceSyncs, "The number of namespace objects that are allowed to sync concurrently. Larger number = more responsive namespace termination, but more CPU (and network) load")
}

// ApplyTo 将选项写入命名空间控制器配置。
func (o *NamespaceControllerOptions) ApplyTo(cfg *namespaceconfig.NamespaceControllerConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.NamespaceSyncPeriod = o.NamespaceSyncPeriod
	cfg.ConcurrentNamespaceSyncs = o.ConcurrentNamespaceSyncs

	return nil
}

// Validate 校验命名空间控制器选项。
func (o *NamespaceControllerOptions) Validate() []error {
	if o == nil {
		return nil
	}

	errs := []error{}
	return errs
}
