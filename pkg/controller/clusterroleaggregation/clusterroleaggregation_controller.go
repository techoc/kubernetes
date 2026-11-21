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

package clusterroleaggregation

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	rbacv1ac "k8s.io/client-go/applyconfigurations/rbac/v1"
	"k8s.io/klog/v2"

	rbacv1 "k8s.io/api/rbac/v1"
	"k8s.io/apimachinery/pkg/api/equality"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	rbacinformers "k8s.io/client-go/informers/rbac/v1"
	rbacclient "k8s.io/client-go/kubernetes/typed/rbac/v1"
	rbaclisters "k8s.io/client-go/listers/rbac/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/util/workqueue"

	"k8s.io/kubernetes/pkg/controller"
)

// ClusterRoleAggregationController 是负责聚合 ClusterRole 的控制器。
// 它根据带有 AggregationRule 的 ClusterRole 中的选择器，把其他 ClusterRole 的规则汇总进去。
type ClusterRoleAggregationController struct {
	clusterRoleClient  rbacclient.ClusterRolesGetter
	clusterRoleLister  rbaclisters.ClusterRoleLister
	clusterRolesSynced cache.InformerSynced

	syncHandler func(ctx context.Context, key string) error
	queue       workqueue.TypedRateLimitingInterface[string]
}

// NewClusterRoleAggregation 创建一个新的 ClusterRole 聚合控制器。
func NewClusterRoleAggregation(clusterRoleInformer rbacinformers.ClusterRoleInformer, clusterRoleClient rbacclient.ClusterRolesGetter) *ClusterRoleAggregationController {
	c := &ClusterRoleAggregationController{
		clusterRoleClient:  clusterRoleClient,
		clusterRoleLister:  clusterRoleInformer.Lister(),
		clusterRolesSynced: clusterRoleInformer.Informer().HasSynced,

		queue: workqueue.NewTypedRateLimitingQueueWithConfig(
			workqueue.DefaultTypedControllerRateLimiter[string](),
			workqueue.TypedRateLimitingQueueConfig[string]{
				Name: "ClusterRoleAggregator",
			},
		),
	}
	c.syncHandler = c.syncClusterRole

	clusterRoleInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			c.enqueue()
		},
		UpdateFunc: func(old, cur interface{}) {
			c.enqueue()
		},
		DeleteFunc: func(uncast interface{}) {
			c.enqueue()
		},
	})
	return c
}

// syncClusterRole 同步单个 ClusterRole：按其 AggregationRule 中的选择器找出所有被聚合的 ClusterRole，
// 汇总它们的规则，并在结果发生变化时回写。
func (c *ClusterRoleAggregationController) syncClusterRole(ctx context.Context, key string) error {
	_, name, err := cache.SplitMetaNamespaceKey(key)
	if err != nil {
		return err
	}
	sharedClusterRole, err := c.clusterRoleLister.Get(name)
	if errors.IsNotFound(err) {
		// 目标已被删除，无需再聚合。
		return nil
	}
	if err != nil {
		return err
	}
	if sharedClusterRole.AggregationRule == nil {
		// 没有聚合规则的对象不是本控制器的职责范围。
		return nil
	}

	newPolicyRules := []rbacv1.PolicyRule{}
	for i := range sharedClusterRole.AggregationRule.ClusterRoleSelectors {
		selector := sharedClusterRole.AggregationRule.ClusterRoleSelectors[i]
		runtimeLabelSelector, err := metav1.LabelSelectorAsSelector(&selector)
		if err != nil {
			return err
		}
		clusterRoles, err := c.clusterRoleLister.List(runtimeLabelSelector)
		if err != nil {
			return err
		}
		// 排序保证聚合结果的顺序稳定，避免规则顺序抖动引起无谓的更新。
		sort.Sort(byName(clusterRoles))

		for i := range clusterRoles {
			// 聚合角色自身的规则由聚合结果决定，必须排除，防止自引用。
			if clusterRoles[i].Name == sharedClusterRole.Name {
				continue
			}

			for j := range clusterRoles[i].Rules {
				currRule := clusterRoles[i].Rules[j]
				// 规则去重，多个被聚合角色可能声明相同的规则。
				if !ruleExists(newPolicyRules, currRule) {
					newPolicyRules = append(newPolicyRules, currRule)
				}
			}
		}
	}

	// 结果一致时跳过写操作，减少不必要的 API 请求。
	if equality.Semantic.DeepEqual(newPolicyRules, sharedClusterRole.Rules) {
		return nil
	}

	return c.applyClusterRoles(ctx, sharedClusterRole.Name, newPolicyRules)
}

// applyClusterRoles 通过 server-side apply 将聚合后的规则写入目标 ClusterRole。
func (c *ClusterRoleAggregationController) applyClusterRoles(ctx context.Context, name string, newPolicyRules []rbacv1.PolicyRule) error {
	clusterRoleApply := rbacv1ac.ClusterRole(name).
		WithRules(toApplyPolicyRules(newPolicyRules)...)

	opts := metav1.ApplyOptions{FieldManager: "clusterrole-aggregation-controller", Force: true}
	_, err := c.clusterRoleClient.ClusterRoles().Apply(ctx, clusterRoleApply, opts)
	return err
}

// toApplyPolicyRules 将一组 PolicyRule 批量转换为 server-side apply 所需的配置对象。
func toApplyPolicyRules(rules []rbacv1.PolicyRule) []*rbacv1ac.PolicyRuleApplyConfiguration {
	var result []*rbacv1ac.PolicyRuleApplyConfiguration
	for _, rule := range rules {
		result = append(result, toApplyPolicyRule(rule))
	}
	return result
}

// toApplyPolicyRule 将单条 PolicyRule 转换为 apply 配置对象。
func toApplyPolicyRule(rule rbacv1.PolicyRule) *rbacv1ac.PolicyRuleApplyConfiguration {
	result := rbacv1ac.PolicyRule()
	result.Resources = rule.Resources
	result.ResourceNames = rule.ResourceNames
	result.APIGroups = rule.APIGroups
	result.NonResourceURLs = rule.NonResourceURLs
	result.Verbs = rule.Verbs
	return result
}

// ruleExists 判断 haystack 中是否已存在与 needle 语义相等的规则。
func ruleExists(haystack []rbacv1.PolicyRule, needle rbacv1.PolicyRule) bool {
	for _, curr := range haystack {
		if equality.Semantic.DeepEqual(curr, needle) {
			return true
		}
	}
	return false
}

// Run 启动控制器，并阻塞直到 ctx 被取消。
func (c *ClusterRoleAggregationController) Run(ctx context.Context, workers int) {
	defer utilruntime.HandleCrash()

	logger := klog.FromContext(ctx)
	logger.Info("Starting ClusterRoleAggregator controller")

	var wg sync.WaitGroup
	defer func() {
		logger.Info("Shutting down ClusterRoleAggregator controller")
		c.queue.ShutDown()
		wg.Wait()
	}()

	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.clusterRolesSynced) {
		return
	}

	for i := 0; i < workers; i++ {
		wg.Go(func() {
			wait.UntilWithContext(ctx, c.runWorker, time.Second)
		})
	}
	<-ctx.Done()
}

// runWorker 是单个 worker 的主循环，持续处理工作队列中的条目。
func (c *ClusterRoleAggregationController) runWorker(ctx context.Context) {
	for c.processNextWorkItem(ctx) {
	}
}

// processNextWorkItem 从队列中取出一个条目并处理；成功则清除限流状态，失败则重新入队重试。
func (c *ClusterRoleAggregationController) processNextWorkItem(ctx context.Context) bool {
	dsKey, quit := c.queue.Get()
	if quit {
		return false
	}
	defer c.queue.Done(dsKey)

	err := c.syncHandler(ctx, dsKey)
	if err == nil {
		c.queue.Forget(dsKey)
		return true
	}

	utilruntime.HandleError(fmt.Errorf("%v failed with : %v", dsKey, err))
	c.queue.AddRateLimited(dsKey)

	return true
}

// enqueue 把所有需要聚合的 ClusterRole 都加入队列。
func (c *ClusterRoleAggregationController) enqueue() {
	// 这种做法不常见，但 ClusterRole 总量很小，且依赖关系无法预知，
	// 因此每次事件都把全部对象入队；这样单个角色更新失败时也可以针对性地重试。
	allClusterRoles, err := c.clusterRoleLister.List(labels.Everything())
	if err != nil {
		utilruntime.HandleError(fmt.Errorf("Couldn't list all objects %v", err))
		return
	}
	for _, clusterRole := range allClusterRoles {
		// 只入队可能需要聚合的对象，即设置了 AggregationRule 的 ClusterRole。
		if clusterRole.AggregationRule == nil {
			continue
		}
		key, err := controller.KeyFunc(clusterRole)
		if err != nil {
			utilruntime.HandleError(fmt.Errorf("Couldn't get key for object %#v: %v", clusterRole, err))
			return
		}
		c.queue.Add(key)
	}
}

// byName 按名称对 ClusterRole 排序，保证聚合结果顺序稳定。
type byName []*rbacv1.ClusterRole

// Len 返回待排序的对象数量。
func (a byName) Len() int { return len(a) }

// Swap 交换 i 与 j 位置上的两个对象。
func (a byName) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less 按名称比较两个 ClusterRole 的先后顺序。
func (a byName) Less(i, j int) bool { return a[i].Name < a[j].Name }
