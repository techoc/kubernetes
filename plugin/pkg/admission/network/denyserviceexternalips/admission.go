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

package denyserviceexternalips

import (
	"context"
	"fmt"
	"io"

	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/pkg/apis/core"
)

const (
	// PluginName 是该准入控制器插件的名称。
	PluginName = "DenyServiceExternalIPs"
)

// Register 注册该插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		plugin := newPlugin()
		return plugin, nil
	})
}

// externalIPsDenierPlugin 持有该准入插件的状态并实现其逻辑。
type externalIPsDenierPlugin struct {
	*admission.Handler
}

var _ admission.Interface = &externalIPsDenierPlugin{}
var _ admission.ValidationInterface = &externalIPsDenierPlugin{}

// newPlugin 创建一个新的准入插件实例。
func newPlugin() *externalIPsDenierPlugin {
	return &externalIPsDenierPlugin{
		Handler: admission.NewHandler(admission.Create, admission.Update),
	}
}

// Validate 拒绝任何对 Service.Spec.ExternalIPs 的修改，
// 即只允许该字段保持为空或沿用旧值。
func (plug *externalIPsDenierPlugin) Validate(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) error {
	if attr.GetResource().GroupResource() != core.Resource("services") {
		return nil
	}

	if len(attr.GetSubresource()) != 0 {
		return nil
	}

	// 类型转换失败说明不是本插件处理的对象，交由后续校验报错。
	newSvc, ok := attr.GetObject().(*core.Service)
	if !ok {
		klog.V(3).Infof("Expected Service resource, got: %v", attr.GetKind())
		return errors.NewInternalError(fmt.Errorf("Expected Service resource, got: %v", attr.GetKind()))
	}

	var oldSvc *core.Service
	if old := attr.GetOldObject(); old != nil {
		tmp, ok := old.(*core.Service)
		if !ok {
			klog.V(3).Infof("Expected Service resource, got: %v", attr.GetKind())
			return errors.NewInternalError(fmt.Errorf("Expected Service resource, got: %v", attr.GetKind()))
		}
		oldSvc = tmp
	}

	if isSubset(newSvc, oldSvc) {
		return nil
	}

	klog.V(4).Infof("Denying new use of ExternalIPs on Service %s/%s", newSvc.Namespace, newSvc.Name)
	return admission.NewForbidden(attr, fmt.Errorf("Use of external IPs is denied by admission control"))
}

// isSubset 判断新 Service 声明的 ExternalIPs 是否全部来自旧对象。
// 只有新增 IP 才会被拒绝，删除或保持原样都允许。
func isSubset(newSvc, oldSvc *core.Service) bool {
	// 新对象没有 ExternalIPs，自然不会引入新的 IP。
	if len(newSvc.Spec.ExternalIPs) == 0 {
		return true
	}
	// 有 ExternalIPs 却是创建操作，说明全是新增的。
	if oldSvc == nil {
		return false
	}
	oldIPs := map[string]bool{}
	for _, ip := range oldSvc.Spec.ExternalIPs {
		oldIPs[ip] = true
	}
	// 新对象中的每个 IP 都必须已在旧对象中存在。
	for _, ip := range newSvc.Spec.ExternalIPs {
		if oldIPs[ip] == false {
			return false
		}
	}
	return true
}
