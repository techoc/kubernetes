/*
Copyright The Kubernetes Authors.

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

	disruptionconfig "k8s.io/kubernetes/pkg/controller/disruption/config"
)

// DisruptionControllerOptions 保存干扰控制器的选项。
type DisruptionControllerOptions struct {
	*disruptionconfig.DisruptionControllerConfiguration
}

// AddFlags 将干扰控制器相关参数注册到指定 FlagSet。
func (o *DisruptionControllerOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.Int32Var(&o.ConcurrentDisruptionSyncs, "concurrent-disruption-syncs", o.ConcurrentDisruptionSyncs, "The number of PDB objects that are allowed to sync concurrently. Larger number = more responsive PDB updates, but more CPU (and network) load")
}

// ApplyTo 将选项写入干扰控制器配置。
func (o *DisruptionControllerOptions) ApplyTo(cfg *disruptionconfig.DisruptionControllerConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.ConcurrentDisruptionSyncs = o.ConcurrentDisruptionSyncs

	return nil
}

// Validate 校验干扰控制器选项。
func (o *DisruptionControllerOptions) Validate() []error {
	if o == nil {
		return nil
	}

	errs := []error{}
	return errs
}
