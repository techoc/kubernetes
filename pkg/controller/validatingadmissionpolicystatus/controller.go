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

package validatingadmissionpolicystatus

import (
	"context"
	"fmt"
	"sync"
	"time"

	"k8s.io/api/admissionregistration/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	validatingadmissionpolicy "k8s.io/apiserver/pkg/admission/plugin/policy/validating"
	admissionregistrationv1apply "k8s.io/client-go/applyconfigurations/admissionregistration/v1"
	informerv1 "k8s.io/client-go/informers/admissionregistration/v1"
	admissionregistrationv1 "k8s.io/client-go/kubernetes/typed/admissionregistration/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"
)

// ControllerName 中的 "Status" 用于区分本控制器与 API Server 中运行的同名类型检查控制器。
const ControllerName = "validatingadmissionpolicy-status"

// Controller 是 ValidatingAdmissionPolicy 的状态控制器，负责调和每个策略对象的 Status 字段。
// 它对每条策略定义中引用的类型执行类型检查，并将表达式告警写回状态。
type Controller struct {
	policyInformer informerv1.ValidatingAdmissionPolicyInformer
	policyQueue    workqueue.TypedRateLimitingInterface[string]
	policySynced   cache.InformerSynced
	policyClient   admissionregistrationv1.ValidatingAdmissionPolicyInterface

	// typeChecker 检查策略表达式中的类型错误：参数类型来自 policy.Spec.ParamsKind，
	// 对象类型根据 policy.Spec.MatchingConstraints 推导。
	typeChecker *validatingadmissionpolicy.TypeChecker
}

// Run 等待 informer 缓存同步后启动 worker，持续处理队列中的策略，直到 ctx 结束。
func (c *Controller) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash() // worker 中的 panic 不能让整个控制器进程退出。

	var wg sync.WaitGroup
	defer func() {
		c.policyQueue.ShutDown()
		wg.Wait()
	}()

	// 必须先等缓存同步，否则 lister 查不到对象会导致已入队的 key 被当作删除处理。
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.policySynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.runWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// NewController 构造状态控制器：创建限速队列、注册 Add/Update 事件处理器，并记录缓存同步状态。
func NewController(policyInformer informerv1.ValidatingAdmissionPolicyInformer, policyClient admissionregistrationv1.ValidatingAdmissionPolicyInterface, typeChecker *validatingadmissionpolicy.TypeChecker) (*Controller, error) {
	c := &Controller{
		policyInformer: policyInformer,
		policyQueue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{Name: ControllerName},
		),
		policyClient: policyClient,
		typeChecker:  typeChecker,
	}
	reg, err := policyInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueuePolicy(obj)
		},
		UpdateFunc: func(oldObj, newObj interface{}) {
			c.enqueuePolicy(newObj)
		},
	})
	if err != nil {
		return nil, err
	}
	c.policySynced = reg.HasSynced
	return c, nil
}

// enqueuePolicy 将策略对象名加入工作队列。
func (c *Controller) enqueuePolicy(policy any) {
	if policy, ok := policy.(*v1.ValidatingAdmissionPolicy); ok {
		// policy objects are cluster-scoped, no point include its namespace.
		key := policy.ObjectMeta.Name
		if key == "" {
			utilruntime.HandleError(fmt.Errorf("cannot get name of object %v", policy))
		}
		c.policyQueue.Add(key)
	}
}

// runWorker 循环调用 processNextWorkItem，直到队列关闭。
func (c *Controller) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 取出一个 key 并执行调和；成功则清除限速计数，失败则限速率重新入队。
func (c *Controller) processNextWorkItem(ctx context.Context) bool {
	key, shutdown := c.policyQueue.Get()
	if shutdown {
		return false
	}
	defer c.policyQueue.Done(key) // 必须配对 Done，否则相同 key 无法再次入队。

	err := func() error {
		policy, err := c.policyInformer.Lister().Get(key)
		if err != nil {
			if kerrors.IsNotFound(err) {
				// lister 中查不到说明策略已被删除，无需处理其状态。
				return nil
			}
			return err
		}
		return c.reconcile(ctx, policy)
	}()

	// 成功调和后重置退避，避免后续失败沿用旧的限频间隔。
	if err == nil {
		c.policyQueue.Forget(key)
		return true
	}

	utilruntime.HandleError(err)
	c.policyQueue.AddRateLimited(key)

	return true
}

// reconcile 对策略表达式执行类型检查，并用 server-side apply 将告警与 ObservedGeneration 写回状态。
func (c *Controller) reconcile(ctx context.Context, policy *v1.ValidatingAdmissionPolicy) error {
	if policy == nil {
		return nil
	}
	// Generation 未变化说明状态已是最新，跳过重复的类型检查与写入。
	if policy.Generation <= policy.Status.ObservedGeneration {
		return nil
	}
	warnings := c.typeChecker.Check(policy)
	warningsConfig := make([]*admissionregistrationv1apply.ExpressionWarningApplyConfiguration, 0, len(warnings))
	for _, warning := range warnings {
		warningsConfig = append(warningsConfig, admissionregistrationv1apply.ExpressionWarning().
			WithFieldRef(warning.FieldRef).
			WithWarning(warning.Warning))
	}
	// 使用 Force apply 接管 status 子资源字段，避免与其他字段管理器冲突。
	applyConfig := admissionregistrationv1apply.ValidatingAdmissionPolicy(policy.Name).
		WithStatus(admissionregistrationv1apply.ValidatingAdmissionPolicyStatus().
			WithObservedGeneration(policy.Generation).
			WithTypeChecking(admissionregistrationv1apply.TypeChecking().
				WithExpressionWarnings(warningsConfig...)))
	_, err := c.policyClient.ApplyStatus(ctx, applyConfig, metav1.ApplyOptions{FieldManager: ControllerName, Force: true})
	return err
}
