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

package resourcequota

import (
	"context"
	"fmt"
	"io"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apiserver/pkg/admission"
	genericadmissioninitializer "k8s.io/apiserver/pkg/admission/initializer"
	resourcequotaapi "k8s.io/apiserver/pkg/admission/plugin/resourcequota/apis/resourcequota"
	"k8s.io/apiserver/pkg/admission/plugin/resourcequota/apis/resourcequota/validation"
	quota "k8s.io/apiserver/pkg/quota/v1"
	"k8s.io/apiserver/pkg/quota/v1/generic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
)

// PluginName is a string with the name of the plugin
const PluginName = "ResourceQuota"

var (
	namespaceGVK          = corev1.SchemeGroupVersion.WithKind("Namespace").GroupKind()
	stopChUnconfiguredErr = fmt.Errorf("quota configuration configured between stop channel")
)

// Register registers a plugin
// 中文说明：配置在插件注册时解析并校验，配置错误会直接导致 apiserver 启动失败，
// 而不是在首个请求时才暴露。5 是评估器并发数。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName,
		func(config io.Reader) (admission.Interface, error) {
			// load the configuration provided (if any)
			configuration, err := LoadConfiguration(config)
			if err != nil {
				return nil, err
			}
			// validate the configuration (if any)
			if configuration != nil {
				if errs := validation.ValidateConfiguration(configuration); len(errs) != 0 {
					return nil, errs.ToAggregate()
				}
			}
			return NewResourceQuota(configuration, 5)
		})
}

// QuotaAdmission implements an admission controller that can enforce quota constraints
//
// 中文说明：只实现 ValidationInterface，即只在 create/update 时做校验；配额的实际
// 扣减由 quota controller 异步完成，这里负责拒绝超额请求。
type QuotaAdmission struct {
	*admission.Handler
	config             *resourcequotaapi.Configuration
	stopCh             <-chan struct{}
	quotaConfiguration quota.Configuration
	numEvaluators      int
	quotaAccessor      *quotaAccessor
	evaluator          Evaluator
	initializationErr  error
}

var _ admission.ValidationInterface = &QuotaAdmission{}
var _ = genericadmissioninitializer.WantsExternalKubeInformerFactory(&QuotaAdmission{})
var _ = genericadmissioninitializer.WantsExternalKubeClientSet(&QuotaAdmission{})
var _ = genericadmissioninitializer.WantsQuotaConfiguration(&QuotaAdmission{})
var _ = genericadmissioninitializer.WantsDrainedNotification(&QuotaAdmission{})

// liveLookupEntry 是 quotaAccessor 中按 namespace 缓存的一次配额查询结果。
// expiry 到期后重新从 informer 读取，避免热点 namespace 每次都遍历缓存。
type liveLookupEntry struct {
	expiry time.Time
	items  []*corev1.ResourceQuota
}

// NewResourceQuota configures an admission controller that can enforce quota constraints
// using the provided registry.  The registry must have the capability to handle group/kinds that
// are persisted by the server this admission controller is intercepting
//
// 中文说明：此处只注册 create/update；Delete 不需要准入校验，配额回收由 controller 处理。
func NewResourceQuota(config *resourcequotaapi.Configuration, numEvaluators int) (*QuotaAdmission, error) {
	quotaAccessor, err := newQuotaAccessor()
	if err != nil {
		return nil, err
	}

	return &QuotaAdmission{
		Handler:       admission.NewHandler(admission.Create, admission.Update),
		stopCh:        nil,
		numEvaluators: numEvaluators,
		config:        config,
		quotaAccessor: quotaAccessor,
	}, nil
}

// SetDrainedNotification sets the stop channel into QuotaAdmission.
func (a *QuotaAdmission) SetDrainedNotification(stopCh <-chan struct{}) {
	a.stopCh = stopCh
}

// SetExternalKubeClientSet registers the client into QuotaAdmission
func (a *QuotaAdmission) SetExternalKubeClientSet(client kubernetes.Interface) {
	a.quotaAccessor.client = client
}

// SetExternalKubeInformerFactory registers an informer factory into QuotaAdmission
func (a *QuotaAdmission) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	quotas := f.Core().V1().ResourceQuotas()
	a.quotaAccessor.lister = quotas.Lister()
	a.quotaAccessor.hasSynced = quotas.Informer().HasSynced
}

// SetQuotaConfiguration assigns and initializes configuration and evaluator for QuotaAdmission
//
// 中文说明：evaluator 需要 stopCh 才能启动后台 worker；initializer 的调用顺序不保证
// stopCh 先于配置注入，因此这里记录错误，留给 ValidateInitialization 统一报出。
func (a *QuotaAdmission) SetQuotaConfiguration(c quota.Configuration) {
	a.quotaConfiguration = c
	if a.stopCh == nil {
		a.initializationErr = stopChUnconfiguredErr
		return
	}
	a.evaluator = NewQuotaEvaluator(a.quotaAccessor, a.quotaConfiguration.IgnoredResources(), generic.NewRegistry(a.quotaConfiguration.Evaluators()), nil, a.config, a.numEvaluators, a.stopCh)
}

// ValidateInitialization ensures an authorizer is set.
func (a *QuotaAdmission) ValidateInitialization() error {
	if a.initializationErr != nil {
		return a.initializationErr
	}
	if a.stopCh == nil {
		return fmt.Errorf("missing stopCh")
	}
	if a.quotaAccessor == nil {
		return fmt.Errorf("missing quotaAccessor")
	}
	if a.quotaAccessor.client == nil {
		return fmt.Errorf("missing quotaAccessor.client")
	}
	if a.quotaAccessor.lister == nil {
		return fmt.Errorf("missing quotaAccessor.lister")
	}
	if a.quotaAccessor.hasSynced == nil {
		return fmt.Errorf("missing quotaAccessor.hasSynced")
	}
	if a.quotaConfiguration == nil {
		return fmt.Errorf("missing quotaConfiguration")
	}
	if a.evaluator == nil {
		return fmt.Errorf("missing evaluator")
	}
	return nil
}

// Validate makes admission decisions while enforcing quota
//
// 中文说明：非 namespace 作用域的请求不受配额约束；创建 Namespace 本身也跳过，
// 否则 Namespace 创建会因尚不存在配额对象而出现自引用问题。
func (a *QuotaAdmission) Validate(ctx context.Context, attr admission.Attributes, o admission.ObjectInterfaces) (err error) {
	// ignore all operations that are not namespaced or creation of namespaces
	if attr.GetNamespace() == "" || isNamespaceCreation(attr) {
		return nil
	}
	return a.evaluator.Evaluate(attr)
}

func isNamespaceCreation(attr admission.Attributes) bool {
	// 用 GroupKind 而不是 GroupVersionKind 判断，避免因版本升级而漏判。
	return attr.GetOperation() == admission.Create && attr.GetKind().GroupKind() == namespaceGVK
}
