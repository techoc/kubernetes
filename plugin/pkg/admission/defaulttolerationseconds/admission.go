/*
Copyright 2017 The Kubernetes Authors.

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

package defaulttolerationseconds

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/spf13/pflag"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/component-base/featuregate"
	api "k8s.io/kubernetes/pkg/apis/core"
)

// PluginName 是本准入插件的名称。
const PluginName = "DefaultTolerationSeconds"

var (
	// tolerationSecondsMutex 保护标志注册与 InspectFeatureGates 中的读取。
	// 只要集成测试不真正使用命令行标志，这个粒度足以保证并发安全；
	// 在真实命令里注册、解析和 InspectFeatureGates 是串行的。
	tolerationSecondsMutex sync.RWMutex

	defaultNotReadyTolerationSeconds    = int64(300)
	defaultUnreachableTolerationSeconds = int64(300)
)

// RegisterFlags 向标志集注册两个默认容忍时长标志。
func RegisterFlags(fs *pflag.FlagSet) {
	// 标志直接写入默认值的变量，必须持锁以防与 InspectFeatureGates 并发读冲突。
	tolerationSecondsMutex.Lock()
	defer tolerationSecondsMutex.Unlock()

	fs.Int64Var(&defaultNotReadyTolerationSeconds, "default-not-ready-toleration-seconds", defaultNotReadyTolerationSeconds,
		"Indicates the tolerationSeconds of the toleration for notReady:NoExecute"+
			" that is added by default to every pod that does not already have such a toleration.")
	fs.Int64Var(&defaultUnreachableTolerationSeconds, "default-unreachable-toleration-seconds", defaultUnreachableTolerationSeconds,
		"Indicates the tolerationSeconds of the toleration for unreachable:NoExecute"+
			" that is added by default to every pod that does not already have such a toleration.")
}

// Register 向插件注册表注册本准入插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewDefaultTolerationSeconds(), nil
	})
}

// Plugin 为每个 Pod 补上容忍 notReady:NoExecute 和 unreachable:NoExecute 污点的
// 默认容忍（tolerationSeconds 默认 300 秒）；Pod 已有对应容忍时不做改动。
type Plugin struct {
	*admission.Handler
	notReadyToleration    api.Toleration
	unreachableToleration api.Toleration
}

var _ admission.MutationInterface = &Plugin{}
var _ initializer.WantsFeatures = &Plugin{}

// NewDefaultTolerationSeconds 创建 DefaultTolerationSeconds 准入控制器实例。
func NewDefaultTolerationSeconds() *Plugin {
	return &Plugin{
		Handler: admission.NewHandler(admission.Create, admission.Update),
	}
}

// InspectFeatureGates 在命令行标志解析后读取默认值，构建默认容忍对象。
func (p *Plugin) InspectFeatureGates(featureGates featuregate.FeatureGate) {
	// 命令行标志可能并发写入默认值，读取时须持读锁。
	tolerationSecondsMutex.RLock()
	defer tolerationSecondsMutex.RUnlock()

	notReadyTolerationSeconds := defaultNotReadyTolerationSeconds
	unreachableTolerationSeconds := defaultUnreachableTolerationSeconds

	p.notReadyToleration = api.Toleration{
		Key:               v1.TaintNodeNotReady,
		Operator:          api.TolerationOpExists,
		Effect:            api.TaintEffectNoExecute,
		TolerationSeconds: &notReadyTolerationSeconds,
	}
	p.unreachableToleration = api.Toleration{
		Key:               v1.TaintNodeUnreachable,
		Operator:          api.TolerationOpExists,
		Effect:            api.TaintEffectNoExecute,
		TolerationSeconds: &unreachableTolerationSeconds,
	}
}

// ValidateInitialization 确认插件已完成初始化（默认容忍已设置）。
func (p *Plugin) ValidateInitialization() error {
	if p.notReadyToleration == (api.Toleration{}) {
		return fmt.Errorf("%s was not initialized correctly, notReadyToleration is not set", PluginName)
	}
	if p.unreachableToleration == (api.Toleration{}) {
		return fmt.Errorf("%s was not initialized correctly, unreachableToleration is not set", PluginName)
	}
	return nil
}

// Admit 为缺少对应容忍的 Pod 追加默认容忍。
func (p *Plugin) Admit(ctx context.Context, attributes admission.Attributes, o admission.ObjectInterfaces) (err error) {
	if attributes.GetResource().GroupResource() != api.Resource("pods") {
		return nil
	}

	if len(attributes.GetSubresource()) > 0 {
		// 子资源更新不涉及 Pod spec，跳过。
		return nil
	}

	pod, ok := attributes.GetObject().(*api.Pod)
	if !ok {
		return errors.NewBadRequest(fmt.Sprintf("expected *api.Pod but got %T", attributes.GetObject()))
	}

	tolerations := pod.Spec.Tolerations

	toleratesNodeNotReady := false
	toleratesNodeUnreachable := false
	for _, toleration := range tolerations {
		if (toleration.Key == v1.TaintNodeNotReady || len(toleration.Key) == 0) &&
			(toleration.Effect == api.TaintEffectNoExecute || len(toleration.Effect) == 0) {
			toleratesNodeNotReady = true
		}

		if (toleration.Key == v1.TaintNodeUnreachable || len(toleration.Key) == 0) &&
			(toleration.Effect == api.TaintEffectNoExecute || len(toleration.Effect) == 0) {
			toleratesNodeUnreachable = true
		}
	}

	if !toleratesNodeNotReady {
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, p.notReadyToleration)
	}

	if !toleratesNodeUnreachable {
		pod.Spec.Tolerations = append(pod.Spec.Tolerations, p.unreachableToleration)
	}

	return nil
}
