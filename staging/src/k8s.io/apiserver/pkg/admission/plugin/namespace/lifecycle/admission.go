/*
Copyright 2015 The Kubernetes Authors.

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

// Package lifecycle 实现 NamespaceLifecycle 准入插件：基于 Namespace 的 Phase
// 强制命名空间生命周期约束，包括禁止删除 immortal 命名空间、
// 禁止向不存在的或正在终止的命名空间写入新对象。
package lifecycle

import (
	"context"
	"fmt"
	"io"
	"time"

	"k8s.io/klog/v2"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	utilcache "k8s.io/apimachinery/pkg/util/cache"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apiserver/pkg/admission"
	"k8s.io/apiserver/pkg/admission/initializer"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
	"k8s.io/utils/clock"
)

const (
	// PluginName 是准入插件的名称
	PluginName = "NamespaceLifecycle"
	// 命名空间在 force live lookup 缓存中保留多久后过期。
	forceLiveLookupTTL = 30 * time.Second
	// 命名空间缺失时，先等待一小段时间再复查缓存（之后才做实时查询），目的有两个：
	// 1. 给 watch 驱动的缓存时间去观察命名空间创建事件
	// 2. 给命名空间创建事件在存储集群各成员间传播留出时间，
	//    这样即使实时查询打到非 leader 节点，也有更大概率成功
	missingNamespaceWait = 50 * time.Millisecond
)

// Register 注册插件；immortal 命名空间固定为 default、kube-system、kube-public。
func Register(plugins *admission.Plugins) {
	plugins.Register(PluginName, func(config io.Reader) (admission.Interface, error) {
		return NewLifecycle(sets.New[string](metav1.NamespaceDefault, metav1.NamespaceSystem, metav1.NamespacePublic))
	})
}

// Lifecycle 实现 admission.Interface。
// 它依据 Namespace 的 Phase 强制执行命名空间生命周期约束。
type Lifecycle struct {
	*admission.Handler
	client             kubernetes.Interface
	immortalNamespaces sets.Set[string]
	namespaceLister    corelisters.NamespaceLister
	// forceLiveLookupCache 保存我们强烈怀疑在本地缓存中已过期的命名空间。
	// 命名空间一旦进入此缓存，就忽略本地状态，始终从 apiserver 取最新数据。
	forceLiveLookupCache *utilcache.LRUExpireCache
}

// 编译期断言：确保 Lifecycle 满足 initializer 要求的接口。
var _ = initializer.WantsExternalKubeInformerFactory(&Lifecycle{})
var _ = initializer.WantsExternalKubeClientSet(&Lifecycle{})

// Admit 根据请求属性做出准入决定。
func (l *Lifecycle) Admit(ctx context.Context, a admission.Attributes, o admission.ObjectInterfaces) error {
	// 禁止删除 immortal 命名空间
	if a.GetOperation() == admission.Delete && a.GetKind().GroupKind() == v1.SchemeGroupVersion.WithKind("Namespace").GroupKind() && l.immortalNamespaces.Has(a.GetName()) {
		return errors.NewForbidden(a.GetResource().GroupResource(), a.GetName(), fmt.Errorf("this namespace may not be deleted"))
	}

	// 非命名空间资源（没有 namespace 字段）一律放行
	if len(a.GetNamespace()) == 0 && a.GetKind().GroupKind() != v1.SchemeGroupVersion.WithKind("Namespace").GroupKind() {
		return nil
	}

	if a.GetKind().GroupKind() == v1.SchemeGroupVersion.WithKind("Namespace").GroupKind() {
		// 命名空间被删除后，在其终止过程中要阻止一切后续创建。
		// 为了降低缓存更新滞后带来的误判，把命名空间加入 force live lookup 列表，
		// 确保后续判断不基于过期状态。
		if a.GetOperation() == admission.Delete {
			l.forceLiveLookupCache.Add(a.GetName(), true, forceLiveLookupTTL)
		}
		// 对 Namespace 本身的其他操作一律放行
		return nil
	}

	// 其他资源的删除一律放行
	if a.GetOperation() == admission.Delete {
		return nil
	}

	// 其他资源的更新一律放行
	if a.GetOperation() == admission.Update {
		return nil
	}

	// 访问审查类请求一律放行。返回命名空间状态会泄露信息
	if isAccessReview(a) {
		return nil
	}

	// 需要等待缓存预热完成
	if !l.WaitForReady() {
		return admission.NewForbidden(a, fmt.Errorf("not yet ready to handle request"))
	}

	var (
		exists bool
		err    error
	)

	namespace, err := l.namespaceLister.Get(a.GetNamespace())
	if err != nil {
		if !errors.IsNotFound(err) {
			return errors.NewInternalError(err)
		}
	} else {
		exists = true
	}

	if !exists && a.GetOperation() == admission.Create {
		// 给缓存时间观察到命名空间，再决定是否拒绝创建。
		// 这有助于“创建命名空间后立刻在其中创建对象”的场景。
		time.Sleep(missingNamespaceWait)
		namespace, err = l.namespaceLister.Get(a.GetNamespace())
		switch {
		case errors.IsNotFound(err):
			// 仍然不存在，继续走后续逻辑
		case err != nil:
			return errors.NewInternalError(err)
		default:
			exists = true
		}
		if exists {
			klog.V(4).InfoS("Namespace existed in cache after waiting", "namespace", klog.KRef("", a.GetNamespace()))
		}
	}

	// forceLiveLookup 为 true 时跳过本地缓存，始终向 apiserver 实时查询。
	forceLiveLookup := false
	if _, ok := l.forceLiveLookupCache.Get(a.GetNamespace()); ok {
		// 我们认为该命名空间已被标记删除，但本地缓存显示它仍为 Active，
		// 状态不一致时以服务端为准。
		forceLiveLookup = exists && namespace.Status.Phase == v1.NamespaceActive
	}

	// 拒绝操作不存在的命名空间
	if !exists || forceLiveLookup {
		// 最后的手段：直接查询存储
		namespace, err = l.client.CoreV1().Namespaces().Get(context.TODO(), a.GetNamespace(), metav1.GetOptions{})
		switch {
		case errors.IsNotFound(err):
			return err
		case err != nil:
			return errors.NewInternalError(err)
		}

		klog.V(4).InfoS("Found namespace via storage lookup", "namespace", klog.KRef("", a.GetNamespace()))
	}

	// 确保不会在终止中的命名空间里创建对象
	if a.GetOperation() == admission.Create {
		if namespace.Status.Phase != v1.NamespaceTerminating {
			return nil
		}

		err := admission.NewForbidden(a, fmt.Errorf("unable to create new content in namespace %s because it is being terminated", a.GetNamespace()))
		// 附加结构化 cause，便于客户端程序化识别“命名空间正在终止”这一原因。
		if apierr, ok := err.(*errors.StatusError); ok {
			apierr.ErrStatus.Details.Causes = append(apierr.ErrStatus.Details.Causes, metav1.StatusCause{
				Type:    v1.NamespaceTerminatingCause,
				Message: fmt.Sprintf("namespace %s is being terminated", a.GetNamespace()),
				Field:   "metadata.namespace",
			})
		}
		return err
	}

	return nil
}

// NewLifecycle 创建命名空间 Lifecycle 准入控制器。
func NewLifecycle(immortalNamespaces sets.Set[string]) (*Lifecycle, error) {
	return newLifecycleWithClock(immortalNamespaces, clock.RealClock{})
}

// newLifecycleWithClock 允许测试注入假时钟，以验证 forceLiveLookupCache 的过期行为。
// 缓存容量固定为 100 个命名空间。
func newLifecycleWithClock(immortalNamespaces sets.Set[string], clock utilcache.Clock) (*Lifecycle, error) {
	forceLiveLookupCache := utilcache.NewLRUExpireCacheWithClock(100, clock)
	return &Lifecycle{
		Handler:              admission.NewHandler(admission.Create, admission.Update, admission.Delete),
		immortalNamespaces:   immortalNamespaces,
		forceLiveLookupCache: forceLiveLookupCache,
	}, nil
}

// SetExternalKubeInformerFactory 实现 WantsExternalKubeInformerFactory 接口。
// ready 条件绑定到 Namespace informer 的同步状态，未同步前 Admit 会拒绝请求。
func (l *Lifecycle) SetExternalKubeInformerFactory(f informers.SharedInformerFactory) {
	namespaceInformer := f.Core().V1().Namespaces()
	l.namespaceLister = namespaceInformer.Lister()
	l.SetReadyFunc(namespaceInformer.Informer().HasSynced)
}

// SetExternalKubeClientSet 实现 WantsExternalKubeClientSet 接口。
func (l *Lifecycle) SetExternalKubeClientSet(client kubernetes.Interface) {
	l.client = client
}

// ValidateInitialization 实现 InitializationValidator 接口。
func (l *Lifecycle) ValidateInitialization() error {
	if l.namespaceLister == nil {
		return fmt.Errorf("missing namespaceLister")
	}
	if l.client == nil {
		return fmt.Errorf("missing client")
	}
	return nil
}

// accessReviewResources 是能窥探命名空间内权限的资源。必须允许用户创建这些资源，
// 因为返回 "not found" 会让攻击者探测“我 2017 年要开除的人”这类命名空间是否存在。
var accessReviewResources = map[schema.GroupResource]bool{
	{Group: "authorization.k8s.io", Resource: "localsubjectaccessreviews"}: true,
}

// isAccessReview 判断请求是否针对访问审查资源。
func isAccessReview(a admission.Attributes) bool {
	return accessReviewResources[a.GetResource().GroupResource()]
}
