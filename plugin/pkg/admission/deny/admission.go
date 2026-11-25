/*
Copyright 2014 The Kubernetes Authors.

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

package deny

import (
	"context"
	"errors"
	"io"

	"k8s.io/klog/v2"

	"k8s.io/apiserver/pkg/admission"
)

// PluginName 是本准入插件的名称。
const PluginName = "AlwaysDeny"

// Register 向插件注册表注册本准入插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewAlwaysDeny(), nil
	})
}

// alwaysDeny 是始终拒绝所有准入请求的 admission.Interface 实现。
type alwaysDeny struct{}

var _ admission.MutationInterface = alwaysDeny{}
var _ admission.ValidationInterface = alwaysDeny{}

// Admit 一律返回 Forbidden，拒绝请求。
func (alwaysDeny) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	return admission.NewForbidden(a, errors.New("admission control is denying all modifications"))
}

// Validate 一律返回 Forbidden；该接口不允许修改对象。
func (alwaysDeny) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	return admission.NewForbidden(a, errors.New("admission control is denying all modifications"))
}

// Handles 对任意操作（CREATE/UPDATE/DELETE/CONNECT）都返回 true。
func (alwaysDeny) Handles(operation admission.Operation) bool {
	return true
}

// NewAlwaysDeny 创建始终拒绝的准入处理器，并记录弃用警告。
func NewAlwaysDeny() admission.Interface {
	// DEPRECATED: AlwaysDeny 拒绝所有准入请求，已无实际用途。
	// 该插件已弃用，仅保留兼容旧配置。
	klog.Warningf("%s admission controller is deprecated. "+
		"Please remove this controller from your configuration files and scripts.", PluginName)
	return new(alwaysDeny)
}
