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

package defaultservicecidr

import (
	"context"
	"net"
	"reflect"
	"time"

	v1 "k8s.io/api/core/v1"
	networkingapiv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apimeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	metav1apply "k8s.io/client-go/applyconfigurations/meta/v1"
	networkingapiv1apply "k8s.io/client-go/applyconfigurations/networking/v1"
	networkingv1informers "k8s.io/client-go/informers/networking/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	networkingv1listers "k8s.io/client-go/listers/networking/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/klog/v2"
)

// 默认 ServiceCIDR 控制器使用的常量。
const (
	controllerName         = "kubernetes-service-cidr-controller"
	DefaultServiceCIDRName = "kubernetes"
)

// NewController 创建控制器，根据 --service-cluster-ip-range 生成默认 ServiceCIDR，
// 必要时重建，
// 但配置不同时不会更新已有对象。
// 逻辑与 kubernetes.default Service 一致。
func NewController(
	primaryRange net.IPNet,
	secondaryRange net.IPNet,
	client clientset.Interface,
) *Controller {
	c := &Controller{
		client:   client,
		interval: 10 * time.Second, // 与 DefaultEndpointReconcilerInterval 相同
	}

	// 从命令行 flag 获取配置。
	c.cidrs = append(c.cidrs, primaryRange.String())
	if secondaryRange.IP != nil {
		c.cidrs = append(c.cidrs, secondaryRange.String())
	}
	// 只关心 kubernetes.default ServiceCIDR 这一小部分数据，因此自建 informer 而非使用控制面共享 informer。
	c.serviceCIDRInformer = networkingv1informers.NewFilteredServiceCIDRInformer(client, 12*time.Hour,
		cache.Indexers{cache.NamespaceIndex: cache.MetaNamespaceIndexFunc},
		func(options *metav1.ListOptions) {
			options.FieldSelector = fields.OneTermEqualSelector("metadata.name", DefaultServiceCIDRName).String()
		})

	c.serviceCIDRLister = networkingv1listers.NewServiceCIDRLister(c.serviceCIDRInformer.GetIndexer())
	c.serviceCIDRsSynced = c.serviceCIDRInformer.HasSynced

	return c
}

// Controller 管理基于选择器的 Service IP 地址（ServiceCIDR）。
type Controller struct {
	cidrs []string // 顺序有意义，首个 CIDR 决定默认 IP 族

	client           clientset.Interface
	eventBroadcaster record.EventBroadcaster
	eventRecorder    record.EventRecorder

	serviceCIDRInformer cache.SharedIndexInformer
	serviceCIDRLister   networkingv1listers.ServiceCIDRLister
	serviceCIDRsSynced  cache.InformerSynced

	interval                  time.Duration
	reportedMismatchedCIDRs   bool
	reportedNotReadyCondition bool
}

// Start 在默认 ServiceCIDR 存在或 stopCh 关闭前不会返回。
func (c *Controller) Start(ctx context.Context) {
	defer utilruntime.HandleCrashWithContext(ctx)
	logger := klog.FromContext(ctx)

	c.eventBroadcaster = record.NewBroadcaster(record.WithContext(ctx))
	c.eventRecorder = c.eventBroadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: controllerName})
	c.eventBroadcaster.StartStructuredLogging(0)
	c.eventBroadcaster.StartRecordingToSink(&v1core.EventSinkImpl{Interface: c.client.CoreV1().Events("")})

	logger.Info("Starting", "controller", controllerName)
	go func() {
		<-ctx.Done()
		logger.Info("Shutting down", "controller", controllerName)
		c.eventBroadcaster.Shutdown()
	}()

	go c.serviceCIDRInformer.RunWithContext(ctx)
	if !cache.WaitForNamedCacheSyncWithContext(ctx, c.serviceCIDRsSynced) {
		return
	}

	// 等待首次同步成功；
	// 这会阻塞 apiserver 启动，因此使用较短的轮询间隔。
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(ctx context.Context) (bool, error) {
		syncErr := c.sync(ctx)
		return syncErr == nil, nil
	})
	if err != nil {
		logger.Error(err, "error initializing the default ServiceCIDR")
	}

	// 在后台按指定间隔运行同步循环。
	go wait.UntilWithContext(ctx, func(ctx context.Context) {
		err := c.sync(ctx)
		if err != nil {
			logger.Error(err, "error trying to sync the default ServiceCIDR")
		}
	}, c.interval)
}

// sync 确保默认 ServiceCIDR 对象存在且配置正确。
func (c *Controller) sync(ctx context.Context) error {
	logger := klog.FromContext(ctx)

	// 检查默认 ServiceCIDR 是否已存在。
	serviceCIDR, err := c.serviceCIDRLister.Get(DefaultServiceCIDRName)
	// 若已存在：
	if err == nil {
		// 单栈升级到双栈。
		if len(c.cidrs) == 2 && len(serviceCIDR.Spec.CIDRs) == 1 && c.cidrs[0] == serviceCIDR.Spec.CIDRs[0] {
			logger.Info("Updating default ServiceCIDR from single-stack to dual-stack", "from", serviceCIDR.Spec.CIDRs, "to", c.cidrs)
			serviceCIDRcopy := serviceCIDR.DeepCopy()
			serviceCIDRcopy.Spec.CIDRs = c.cidrs
			_, err := c.client.NetworkingV1().ServiceCIDRs().Update(ctx, serviceCIDRcopy, metav1.UpdateOptions{})
			if err != nil {
				logger.Error(err, "The default ServiceCIDR can not be updated to dual stack", "from", c.cidrs[0], "to", c.cidrs)
				c.eventRecorder.Eventf(serviceCIDR, v1.EventTypeWarning, "KubernetesDefaultServiceCIDRError", "The default ServiceCIDR can not be upgraded from %s to dual stack %v : %v", c.cidrs[0], c.cidrs, err)
			}
		} else {
			c.syncStatus(ctx, serviceCIDR)
		}
		return nil
	}

	// 未知错误。
	if !apierrors.IsNotFound(err) {
		return err
	}

	// 默认 ServiceCIDR 不存在。
	logger.Info("Creating default ServiceCIDR", "CIDRs", c.cidrs)
	serviceCIDR = &networkingapiv1.ServiceCIDR{
		ObjectMeta: metav1.ObjectMeta{
			Name: DefaultServiceCIDRName,
		},
		Spec: networkingapiv1.ServiceCIDRSpec{
			CIDRs: c.cidrs,
		},
	}
	serviceCIDR, err = c.client.NetworkingV1().ServiceCIDRs().Create(ctx, serviceCIDR, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		c.eventRecorder.Eventf(serviceCIDR, v1.EventTypeWarning, "KubernetesDefaultServiceCIDRError", "The default ServiceCIDR can not be created")
		return err
	}
	c.syncStatus(ctx, serviceCIDR)
	return nil
}

// syncStatus 同步 ServiceCIDR 的状态字段。
func (c *Controller) syncStatus(ctx context.Context, serviceCIDR *networkingapiv1.ServiceCIDR) {
	logger := klog.FromContext(ctx)

	// 正在删除的 ServiceCIDR 不同步状态，
	// 删除由 controller-manager 负责。
	if !serviceCIDR.GetDeletionTimestamp().IsZero() {
		logger.V(6).Info("ServiceCIDR is being deleted, skipping status sync", "serviceCIDR", klog.KObj(serviceCIDR))
		return
	}

	// 当 Ready 条件不存在且 CIDR 值匹配本控制器配置时，
	// 将 Ready 设为 true。
	sameConfig := reflect.DeepEqual(c.cidrs, serviceCIDR.Spec.CIDRs)
	currentReadyCondition := apimeta.FindStatusCondition(serviceCIDR.Status.Conditions, networkingapiv1.ServiceCIDRConditionReady)

	// 处理配置不一致。
	if !sameConfig {
		if !c.reportedMismatchedCIDRs {
			logger.Info("Inconsistent ServiceCIDR status", "serviceCIDR", klog.KObj(serviceCIDR), "controllerCIDRs", c.cidrs, "serviceCIDRs", serviceCIDR.Spec.CIDRs)
			c.eventRecorder.Eventf(serviceCIDR, v1.EventTypeWarning, "KubernetesDefaultServiceCIDRInconsistent", "The default ServiceCIDR %v does not match the controller flag configurations %s", serviceCIDR.Spec.CIDRs, c.cidrs)
			c.reportedMismatchedCIDRs = true
		}
		// 无论当前 Ready 条件如何，配置不一致都是问题，但此时不修改 Ready 状态。
		return
	}

	// 配置一致（sameConfig）。
	switch {
	// 当前 Ready=False 属于不应出现的状态。
	// 不要覆盖其他组件设置的 False，避免热循环。
	// 默认 ServiceCIDR 不应被设为 False，
	// 若出现则需要集群管理员介入。
	case currentReadyCondition != nil && currentReadyCondition.Status == metav1.ConditionFalse:
		if !c.reportedNotReadyCondition {
			logger.Info("Default ServiceCIDR condition Ready is False, but controller configuration matches. Please validate your cluster's network configuration.", "serviceCIDR", klog.KObj(serviceCIDR), "status", currentReadyCondition.Status, "reason", currentReadyCondition.Reason, "message", currentReadyCondition.Message)
			c.eventRecorder.Eventf(serviceCIDR, v1.EventTypeWarning, currentReadyCondition.Reason, "Configuration matches, but %s", currentReadyCondition.Message)
			c.reportedNotReadyCondition = true
		}

	// 当前 Ready=True 且配置一致，无需处理。
	case currentReadyCondition != nil && currentReadyCondition.Status == metav1.ConditionTrue:
		logger.V(6).Info("ServiceCIDR is Ready and configuration matches. No status update needed.", "serviceCIDR", klog.KObj(serviceCIDR))

	// 未设置条件且 ServiceCIDR 与本 apiserver 配置一致，将条件设为 True。
	case currentReadyCondition == nil || currentReadyCondition.Status == metav1.ConditionUnknown:
		logger.Info("Setting default ServiceCIDR condition Ready to True")
		svcApplyStatus := networkingapiv1apply.ServiceCIDRStatus().WithConditions(
			metav1apply.Condition().
				WithType(networkingapiv1.ServiceCIDRConditionReady).
				WithStatus(metav1.ConditionTrue).
				WithMessage("Kubernetes default Service CIDR is ready").
				WithLastTransitionTime(metav1.Now()))
		svcApply := networkingapiv1apply.ServiceCIDR(DefaultServiceCIDRName).WithStatus(svcApplyStatus)
		if _, errApply := c.client.NetworkingV1().ServiceCIDRs().ApplyStatus(ctx, svcApply, metav1.ApplyOptions{FieldManager: controllerName, Force: true}); errApply != nil {
			logger.Error(errApply, "error updating default ServiceCIDR status")
			c.eventRecorder.Eventf(serviceCIDR, v1.EventTypeWarning, "KubernetesDefaultServiceCIDRError", "The default ServiceCIDR Status can not be set to Ready=True")
		}
	default:
	}
}
