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

package nodetaint

import (
	"context"
	"fmt"
	"io"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apiserver/pkg/admission"
	api "k8s.io/kubernetes/pkg/apis/core"
)

const (
	// PluginName 是插件的名称。
	PluginName = "TaintNodesByCondition"
)

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewPlugin(), nil
	})
}

// NewPlugin 创建一个新的 NodeTaint 准入插件，用于识别来自节点的请求。
func NewPlugin() *Plugin {
	return &Plugin{
		Handler: admission.NewHandler(admission.Create),
	}
}

// Plugin 保存状态并实现该准入插件。
type Plugin struct {
	*admission.Handler
}

var (
	_ = admission.Interface(&Plugin{})
)

var (
	nodeResource = api.Resource("nodes")
)

// Admit 检查节点身份并在必要时添加污点。
func (p *Plugin) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	// 本插件只负责给节点打污点，其他资源和子资源直接放行。
	if a.GetResource().GroupResource() != nodeResource || a.GetSubresource() != "" {
		return nil
	}

	node, ok := a.GetObject().(*api.Node)
	if !ok {
		return admission.NewForbidden(a, fmt.Errorf("unexpected type %T", a.GetObject()))
	}

	// 创建节点时打上 NotReady 污点，确保节点入群时即为 NotReady。否则新节点可能延迟收到污点，
	// 导致 Pod 被调度到尚未就绪的节点上；节点就绪后由节点控制器移除该污点。
	addNotReadyTaint(node)
	return nil
}

// addNotReadyTaint 为节点添加 NotReady 污点，已存在则不重复添加。
func addNotReadyTaint(node *api.Node) {
	notReadyTaint := api.Taint{
		Key:    v1.TaintNodeNotReady,
		Effect: api.TaintEffectNoSchedule,
	}
	for _, taint := range node.Spec.Taints {
		if taint.MatchTaint(notReadyTaint) {
			// 污点已存在，无需重复添加。
			return
		}
	}
	node.Spec.Taints = append(node.Spec.Taints, notReadyTaint)
}
