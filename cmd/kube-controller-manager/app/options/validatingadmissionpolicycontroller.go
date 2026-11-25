/*
Copyright 2023 The Kubernetes Authors.

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

	validatingadmissionpolicystatusconfig "k8s.io/kubernetes/pkg/controller/validatingadmissionpolicystatus/config"
)

// ValidatingAdmissionPolicyStatusControllerOptions 保存验证性准入策略状态控制器的选项。
type ValidatingAdmissionPolicyStatusControllerOptions struct {
	*validatingadmissionpolicystatusconfig.ValidatingAdmissionPolicyStatusControllerConfiguration
}

// AddFlags 将验证性准入策略状态控制器相关参数注册到指定 FlagSet。
func (o *ValidatingAdmissionPolicyStatusControllerOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}

	fs.Int32Var(&o.ConcurrentPolicySyncs, "concurrent-validating-admission-policy-status-syncs", o.ConcurrentPolicySyncs, "The number of ValidatingAdmissionPolicyStatusController workers that are allowed to sync concurrently.")
}

// ApplyTo 将选项写入验证性准入策略状态控制器配置。
func (o *ValidatingAdmissionPolicyStatusControllerOptions) ApplyTo(cfg *validatingadmissionpolicystatusconfig.ValidatingAdmissionPolicyStatusControllerConfiguration) error {
	if o == nil {
		return nil
	}

	cfg.ConcurrentPolicySyncs = o.ConcurrentPolicySyncs

	return nil
}

// Validate 校验验证性准入策略状态控制器选项。
func (o *ValidatingAdmissionPolicyStatusControllerOptions) Validate() []error {
	if o == nil {
		return nil
	}
	var errs []error
	if o.ConcurrentPolicySyncs <= 0 {
		// 错误消息已由 CLI 补充控制器或参数名称，此处不再重复。
		errs = append(errs, fmt.Errorf("must be positive, got %d", o.ConcurrentPolicySyncs))
	}
	return errs
}
