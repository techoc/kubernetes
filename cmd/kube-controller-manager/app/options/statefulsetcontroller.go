/*
Copyright 2019 The Kubernetes Authors.

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

	statefulsetconfig "k8s.io/kubernetes/pkg/controller/statefulset/config"
)

// StatefulSetControllerOptions 保存 StatefulSet 控制器的选项。
type StatefulSetControllerOptions struct {
	*statefulsetconfig.StatefulSetControllerConfiguration
}

// AddFlags 将 StatefulSet 控制器相关参数注册到指定 FlagSet。
func (o *StatefulSetControllerOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.Int32Var(&o.ConcurrentStatefulSetSyncs, "concurrent-statefulset-syncs", o.ConcurrentStatefulSetSyncs, "The number of statefulset objects that are allowed to sync concurrently. Larger number = more responsive statefulsets, but more CPU (and network) load")
}

// ApplyTo 将选项写入 StatefulSet 控制器配置。
func (o *StatefulSetControllerOptions) ApplyTo(cfg *statefulsetconfig.StatefulSetControllerConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.ConcurrentStatefulSetSyncs = o.ConcurrentStatefulSetSyncs

	return nil
}

// Validate 校验 StatefulSet 控制器选项。
func (o *StatefulSetControllerOptions) Validate() []error {
	if o == nil {
		return nil
	}

	errs := []error{}
	if o.ConcurrentStatefulSetSyncs < 1 {
		errs = append(errs, fmt.Errorf("concurrent-statefulset-syncs must be greater than 0, but got %d", o.ConcurrentStatefulSetSyncs))
	}
	return errs
}
