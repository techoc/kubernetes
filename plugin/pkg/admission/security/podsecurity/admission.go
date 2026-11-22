/*
Copyright 2021 The Kubernetes Authors.

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

package podsecurity

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"

	// 导入 install 包以注册所需类型的转换函数。
	_ "k8s.io/kubernetes/pkg/apis/apps/install"
	_ "k8s.io/kubernetes/pkg/apis/batch/install"
	_ "k8s.io/kubernetes/pkg/apis/core/install"

	admissionv1 "k8s.io/api/admission/v1"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninit "k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/apiserver/pkg/audit"
	"k8s.io/apiserver/pkg/warning"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/component-base/compatibility"
	"k8s.io/component-base/metrics/legacyregistry"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/pkg/apis/apps"
	"k8s.io/kubernetes/pkg/apis/batch"
	"k8s.io/kubernetes/pkg/apis/core"
	podsecurityadmission "k8s.io/pod-security-admission/admission"
	podsecurityconfigloader "k8s.io/pod-security-admission/admission/api/load"
	podsecurityadmissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/pod-security-admission/metrics"
	"k8s.io/pod-security-admission/policy"
)

// PluginName 是插件名称字符串。
const PluginName = "PodSecurity"

// Register 注册插件。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(reader io.Reader) (admission.Interface, error) {
		return newPlugin(reader)
	})
}

// Plugin 保存状态并实现该准入插件。
type Plugin struct {
	*admission.Handler

	inspectedEffectiveVersion bool
	emulationVersion          *podsecurityadmissionapi.Version

	client          kubernetes.Interface
	namespaceLister corev1listers.NamespaceLister
	podLister       corev1listers.PodLister

	delegate *podsecurityadmission.Admission
}

var _ admission.ValidationInterface = &Plugin{}
var _ genericadmissioninit.WantsExternalKubeInformerFactory = &Plugin{}
var _ genericadmissioninit.WantsExternalKubeClientSet = &Plugin{}

var (
	defaultRecorder     *metrics.PrometheusRecorder
	defaultRecorderInit sync.Once
)

// getDefaultRecorder 返回全局共享的指标记录器，仅初始化并注册一次。
func getDefaultRecorder() metrics.Recorder {
	// 只初始化并注册一次，避免重复注册触发 panic。
	defaultRecorderInit.Do(func() {
		defaultRecorder = metrics.NewPrometheusRecorder(podsecurityadmissionapi.GetAPIVersion())
		defaultRecorder.MustRegister(legacyregistry.MustRegister)
	})
	return defaultRecorder
}

// newPlugin 创建一个新的准入插件。
func newPlugin(reader io.Reader) (*Plugin, error) {
	config, err := podsecurityconfigloader.LoadFromReader(reader)
	if err != nil {
		return nil, err
	}

	return &Plugin{
		Handler: admission.NewHandler(admission.Create, admission.Update),
		delegate: &podsecurityadmission.Admission{
			Configuration:    config,
			Metrics:          getDefaultRecorder(),
			PodSpecExtractor: podsecurityadmission.DefaultPodSpecExtractor{},
		},
	}, nil
}

// SetExternalKubeInformerFactory 注册 informer 并据此更新委托对象。
func (p *Plugin) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	namespaceInformer := f.Core().V1().Namespaces()
	p.namespaceLister = namespaceInformer.Lister()
	p.podLister = f.Core().V1().Pods().Lister()
	p.SetReadyFunc(namespaceInformer.Informer().HasSynced)
	p.updateDelegate()
}

// SetExternalKubeClientSet 设置插件使用的客户端。
func (p *Plugin) SetExternalKubeClientSet(client kubernetes.Interface) {
	p.client = client
	p.updateDelegate()
}

// updateDelegate 在依赖齐备后配置委托的 PodSecurity 准入对象。
func (p *Plugin) updateDelegate() {
	// 依赖尚未注入完整，等下一次调用再配置；否则 evaluator 会基于错误的版本创建。
	if p.namespaceLister == nil {
		return
	}
	if p.podLister == nil {
		return
	}
	if p.client == nil {
		return
	}
	if !p.inspectedEffectiveVersion {
		return
	}
	if p.delegate.PodLister == nil {
		p.delegate.PodLister = podsecurityadmission.PodListerFromInformer(p.podLister)
	}
	if p.delegate.NamespaceGetter == nil {
		p.delegate.NamespaceGetter = podsecurityadmission.NamespaceGetterFromListerAndClient(p.namespaceLister, p.client)
	}
	if p.delegate.Evaluator == nil {
		evaluator, err := policy.NewEvaluator(policy.DefaultChecks(), p.emulationVersion)
		if err != nil {
			panic(fmt.Errorf("could not create PodSecurityRegistry: %w", err))
		}
		p.delegate.Evaluator = evaluator
	}
}

// InspectEffectiveVersion 记录生效版本，并在模拟版本与二进制版本不同时传给策略评估器。
func (p *Plugin) InspectEffectiveVersion(version compatibility.EffectiveVersion) {
	p.inspectedEffectiveVersion = true
	binaryVersion := version.BinaryVersion()
	emulationVersion := version.EmulationVersion()
	binaryMajorMinor := podsecurityadmissionapi.MajorMinorVersion(int(binaryVersion.Major()), int(binaryVersion.Minor()))
	emulationMajorMinor := podsecurityadmissionapi.MajorMinorVersion(int(emulationVersion.Major()), int(emulationVersion.Minor()))
	if binaryMajorMinor != emulationMajorMinor {
		p.emulationVersion = &emulationMajorMinor
	}
}

// ValidateInitialization 确保所有必需项都已设置。
func (p *Plugin) ValidateInitialization() error {
	if !p.inspectedEffectiveVersion {
		return fmt.Errorf("%s did not see effective version", PluginName)
	}
	if err := p.delegate.CompleteConfiguration(); err != nil {
		return fmt.Errorf("%s configuration error: %w", PluginName, err)
	}
	if err := p.delegate.ValidateConfiguration(); err != nil {
		return fmt.Errorf("%s invalid: %w", PluginName, err)
	}
	return nil
}

var (
	applicableResources = map[schema.GroupResource]bool{
		corev1.Resource("pods"):       true,
		corev1.Resource("namespaces"): true,
	}
)

// Validate 对 Pod 及包含 PodSpec 的资源执行 PodSecurity 策略检查。
func (p *Plugin) Validate(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	gr := a.GetResource().GroupResource()
	if !applicableResources[gr] && !p.delegate.PodSpecExtractor.HasPodSpec(gr) {
		return nil
	}

	result := p.delegate.Validate(ctx, &lazyConvertingAttributes{Attributes: a})
	for _, w := range result.Warnings {
		warning.AddWarning(ctx, "", w)
	}
	if len(result.AuditAnnotations) > 0 {
		annotations := make([]string, len(result.AuditAnnotations)*2)
		i := 0
		for k, v := range result.AuditAnnotations {
			annotations[i], annotations[i+1] = podsecurityadmissionapi.AuditAnnotationPrefix+k, v
			i += 2
		}
		audit.AddAuditAnnotations(ctx, annotations...)
	}
	if !result.Allowed {
		// 先构造一个通用的禁止错误，再用策略库返回的详细信息覆盖。
		retval := admission.NewForbidden(a, errors.New("Not allowed by PodSecurity")).(*apierrors.StatusError)
		// 策略库若给出了更具体的信息，优先使用，保证错误信息可读。
		if result.Result != nil {
			if len(result.Result.Message) > 0 {
				retval.ErrStatus.Message = result.Result.Message
			}
			if len(result.Result.Reason) > 0 {
				retval.ErrStatus.Reason = result.Result.Reason
			}
			if result.Result.Details != nil {
				retval.ErrStatus.Details = result.Result.Details
			}
			if result.Result.Code != 0 {
				retval.ErrStatus.Code = result.Result.Code
			}
		}
		return retval
	}
	return nil
}

// lazyConvertingAttributes 包装准入属性，仅在实际需要时才把内部类型转换为版本化类型。
type lazyConvertingAttributes struct {
	admission.Attributes

	convertObjectOnce    sync.Once
	convertedObject      runtime.Object
	convertedObjectError error

	convertOldObjectOnce    sync.Once
	convertedOldObject      runtime.Object
	convertedOldObjectError error
}

// GetObject 返回转换后的对象，转换只执行一次。
func (l *lazyConvertingAttributes) GetObject() (runtime.Object, error) {
	l.convertObjectOnce.Do(func() {
		l.convertedObject, l.convertedObjectError = convert(l.Attributes.GetObject())
	})
	return l.convertedObject, l.convertedObjectError
}

// GetOldObject 返回转换后的旧对象，转换只执行一次。
func (l *lazyConvertingAttributes) GetOldObject() (runtime.Object, error) {
	l.convertOldObjectOnce.Do(func() {
		l.convertedOldObject, l.convertedOldObjectError = convert(l.Attributes.GetOldObject())
	})
	return l.convertedOldObject, l.convertedOldObjectError
}

// GetOperation 返回版本化后的操作类型。
func (l *lazyConvertingAttributes) GetOperation() admissionv1.Operation {
	return admissionv1.Operation(l.Attributes.GetOperation())
}

// GetUserName 返回发起请求的用户名。
func (l *lazyConvertingAttributes) GetUserName() string {
	return l.GetUserInfo().GetName()
}

// convert 将内部版本对象转换为版本化对象，供 PodSecurity 策略库处理。
func convert(in runtime.Object) (runtime.Object, error) {
	var out runtime.Object
	switch in.(type) {
	case *core.Namespace:
		out = &corev1.Namespace{}
	case *core.Pod:
		out = &corev1.Pod{}
	case *core.ReplicationController:
		out = &corev1.ReplicationController{}
	case *core.PodTemplate:
		out = &corev1.PodTemplate{}
	case *apps.ReplicaSet:
		out = &appsv1.ReplicaSet{}
	case *apps.Deployment:
		out = &appsv1.Deployment{}
	case *apps.StatefulSet:
		out = &appsv1.StatefulSet{}
	case *apps.DaemonSet:
		out = &appsv1.DaemonSet{}
	case *batch.Job:
		out = &batchv1.Job{}
	case *batch.CronJob:
		out = &batchv1.CronJob{}
	default:
		return in, fmt.Errorf("unexpected type %T", in)
	}
	if err := legacyscheme.Scheme.Convert(in, out, nil); err != nil {
		return in, err
	}
	return out, nil
}
