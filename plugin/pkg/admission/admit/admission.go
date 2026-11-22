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

package admit

import (
	"context"
	"io"

	"k8s.io/apiserver/pkg/admission"
	"k8s.io/klog/v2"
)

// PluginName 表示准入插件的名称。
const PluginName = "AlwaysAdmit"

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewAlwaysAdmit(), nil
	})
}

// alwaysAdmit 是 admission.Interface 的实现，对任何准入请求都一概放行。
type alwaysAdmit struct{}

var _ admission.MutationInterface = alwaysAdmit{}
var _ admission.ValidationInterface = alwaysAdmit{}

// Admit 根据请求属性做出准入决定；这里始终放行。
func (alwaysAdmit) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	return nil
}

// Validate 根据请求属性做出准入决定，且不允许修改对象；这里始终放行。
func (alwaysAdmit) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) (err error) {
	return nil
}

// Handles 返回该准入控制器是否能处理给定的操作，
// 操作可以是 CREATE、UPDATE、DELETE 或 CONNECT；这里始终返回 true。
func (alwaysAdmit) Handles(operation admission.Operation) bool {
	return true
}

// NewAlwaysAdmit 创建一个始终放行的准入处理器。
func NewAlwaysAdmit() admission.Interface {
	// DEPRECATED: AlwaysAdmit 放行所有准入请求，已无实际用途。
	klog.Warningf("%s admission controller is deprecated. "+
		"Please remove this controller from your configuration files and scripts.", PluginName)
	return new(alwaysAdmit)
}
