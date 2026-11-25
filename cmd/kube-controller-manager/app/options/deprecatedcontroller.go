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

	kubectrlmgrconfig "k8s.io/kubernetes/pkg/controller/apis/config"
)

// DeprecatedControllerOptions 保存已弃用的控制器选项。
// TODO：移除这些已弃用参数时删除对应字段。
type DeprecatedControllerOptions struct {
	*kubectrlmgrconfig.DeprecatedControllerConfiguration
}

// AddFlags 将已弃用控制器相关参数注册到指定 FlagSet。
func (o *DeprecatedControllerOptions) AddFlags(fs *pflag.FlagSet) {
	if o == nil {
		return
	}
}

// ApplyTo 将选项写入已弃用控制器配置。
func (o *DeprecatedControllerOptions) ApplyTo(cfg *kubectrlmgrconfig.DeprecatedControllerConfiguration) error {
	if o == nil {
		return nil
	}

	return nil
}

// Validate 校验已弃用控制器选项。
func (o *DeprecatedControllerOptions) Validate() []error {
	if o == nil {
		return nil
	}

	errs := []error{}
	return errs
}
