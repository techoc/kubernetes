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

package kubernetesservice

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apiserver/pkg/storage"
	v1informers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	v1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/klog/v2"

	"k8s.io/kubernetes/pkg/controlplane/reconcilers"
)

// kubernetes service 控制器使用的常量。
const (
	kubernetesServiceName = "kubernetes"
)

// Controller 管理核心引导控制器循环，
// 负责创建 "kubernetes" Service，
// 并检查 Service IP。
type Controller struct {
	Config

	client        kubernetes.Interface
	serviceLister v1listers.ServiceLister
	serviceSynced cache.InformerSynced

	lock   sync.Mutex
	stopCh chan struct{} // 由 Stop 关闭
}

// Config 保存 kubernetes Service 控制器的配置。
type Config struct {
	PublicIP net.IP

	EndpointReconciler reconcilers.EndpointReconciler
	EndpointInterval   time.Duration

	// ServiceIP 指定 kubernetes Service 的 IP，不可为 nil。
	ServiceIP                 net.IP
	ServicePort               int
	PublicServicePort         int
	KubernetesServiceNodePort int
}

// New 返回监听 kubernetes Service 端点的控制器。
func New(config Config, client kubernetes.Interface, serviceInformer v1informers.ServiceInformer) *Controller {
	return &Controller{
		Config:        config,
		client:        client,
		serviceLister: serviceInformer.Lister(),
		serviceSynced: serviceInformer.Informer().HasSynced,
		stopCh:        make(chan struct{}),
	}
}

// Start 启动集群引导所必需的核心控制循环，
// 这些循环在引导集群时必须存在。
func (c *Controller) Start(stopCh <-chan struct{}) {
	if !cache.WaitForCacheSync(stopCh, c.serviceSynced) {
		runtime.HandleError(fmt.Errorf("timed out waiting for caches to sync"))
		return
	}
	// 首次运行时反复协调并移除自身端点，直到服务器就绪。
	endpointPorts := createEndpointPortSpec(c.PublicServicePort, "https")
	if err := c.EndpointReconciler.RemoveEndpoints(kubernetesServiceName, c.PublicIP, endpointPorts); err == nil {
		klog.Error("Found stale data, removed previous endpoints on kubernetes service, apiserver didn't exit successfully previously")
	} else if !storage.IsNotFound(err) {
		klog.Errorf("Error removing old endpoints from kubernetes service: %v", err)
	}

	localStopCh := make(chan struct{})
	go func() {
		defer close(localStopCh)
		select {
		case <-stopCh: // 来自 Start
		case <-c.stopCh: // 来自 Stop
		}
	}()

	go c.Run(localStopCh)
}

// Stop 清理本 apiserver 的端点协调租约，使其他 master 更快接管。
func (c *Controller) Stop() {
	c.lock.Lock()
	defer c.lock.Unlock()

	select {
	case <-c.stopCh:
		return // 只关闭一次
	default:
		close(c.stopCh)
	}

	endpointPorts := createEndpointPortSpec(c.PublicServicePort, "https")
	finishedReconciling := make(chan struct{})
	go func() {
		defer close(finishedReconciling)
		klog.Infof("Shutting down kubernetes service endpoint reconciler")
		c.EndpointReconciler.StopReconciling()
		if err := c.EndpointReconciler.RemoveEndpoints(kubernetesServiceName, c.PublicIP, endpointPorts); err != nil {
			klog.Errorf("Unable to remove endpoints from kubernetes service: %v", err)
		}
		c.EndpointReconciler.Destroy()
	}()

	select {
	case <-finishedReconciling:
		// 完成。
	case <-time.After(2 * c.EndpointInterval):
		// 无法连接 etcd 清理自身时不要无限阻塞服务器关闭。
		klog.Warning("RemoveEndpoints() timed out")
	}
}

// Run 定期更新 kubernetes Service。
func (c *Controller) Run(ch <-chan struct{}) {
	// 等待进程就绪。
	ctx := wait.ContextForChannel(ch)
	err := wait.PollUntilContextCancel(ctx, 100*time.Millisecond, true, func(context.Context) (bool, error) {
		var code int
		c.client.CoreV1().RESTClient().Get().AbsPath("/readyz").Do(context.TODO()).StatusCode(&code)
		return code == http.StatusOK, nil
	})
	if err != nil {
		return
	}

	wait.NonSlidingUntil(func() {
		// Service 定义在首次运行后不再协调，端口与类型只在启动时修正。
		if err := c.UpdateKubernetesService(false); err != nil {
			runtime.HandleError(fmt.Errorf("unable to sync kubernetes service: %v", err))
		}
	}, c.EndpointInterval, ch)
}

// UpdateKubernetesService 尝试更新默认的 kubernetes Service。
func (c *Controller) UpdateKubernetesService(reconcile bool) error {
	// 更新 Service 与 Endpoints 记录。
	servicePorts, serviceType := createPortAndServiceSpec(c.ServicePort, c.PublicServicePort, c.KubernetesServiceNodePort, "https")
	if err := c.CreateOrUpdateMasterServiceIfNeeded(kubernetesServiceName, c.ServiceIP, servicePorts, serviceType, reconcile); err != nil {
		return err
	}
	endpointPorts := createEndpointPortSpec(c.PublicServicePort, "https")
	if err := c.EndpointReconciler.ReconcileEndpoints(kubernetesServiceName, c.PublicIP, endpointPorts, reconcile); err != nil {
		return err
	}
	return nil
}

// createPortAndServiceSpec 创建 Service 端口数组。
// NodePort 为 0 时仅使用 servicePort，否则暴露节点端口。
func createPortAndServiceSpec(servicePort int, targetServicePort int, nodePort int, servicePortName string) ([]corev1.ServicePort, corev1.ServiceType) {
	// 未提供 NodePort 时使用 ClusterIP 类型，否则将 master Service 绑定到 NodePort。
	servicePorts := []corev1.ServicePort{{
		Protocol:   corev1.ProtocolTCP,
		Port:       int32(servicePort),
		Name:       servicePortName,
		TargetPort: intstr.FromInt32(int32(targetServicePort)),
	}}
	serviceType := corev1.ServiceTypeClusterIP
	if nodePort > 0 {
		servicePorts[0].NodePort = int32(nodePort)
		serviceType = corev1.ServiceTypeNodePort
	}
	return servicePorts, serviceType
}

// createEndpointPortSpec 创建 Endpoints 端口。
func createEndpointPortSpec(endpointPort int, endpointPortName string) []corev1.EndpointPort {
	return []corev1.EndpointPort{{
		Protocol: corev1.ProtocolTCP,
		Port:     int32(endpointPort),
		Name:     endpointPortName,
	}}
}

// CreateOrUpdateMasterServiceIfNeeded 在指定 Service 不存在时创建它，
// 已存在则按需更新。
func (c *Controller) CreateOrUpdateMasterServiceIfNeeded(serviceName string, serviceIP net.IP, servicePorts []corev1.ServicePort, serviceType corev1.ServiceType, reconcile bool) error {
	if s, err := c.serviceLister.Services(metav1.NamespaceDefault).Get(serviceName); err == nil {
		// Service 已存在。
		// 自 1.17（2a9a9fa）起该分支不再执行，保留以备将来重新审视。
		if reconcile {
			if svc, updated := getMasterServiceUpdateIfNeeded(s, servicePorts, serviceType); updated {
				klog.Warningf("Resetting master service %q to %#v", serviceName, svc)
				_, err := c.client.CoreV1().Services(metav1.NamespaceDefault).Update(context.TODO(), svc, metav1.UpdateOptions{})
				return err
			}
		}
		return nil
	}
	singleStack := corev1.IPFamilyPolicySingleStack
	svc := &corev1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:      serviceName,
			Namespace: metav1.NamespaceDefault,
			Labels:    map[string]string{"provider": "kubernetes", "component": "apiserver"},
		},
		Spec: corev1.ServiceSpec{
			Ports: servicePorts,
			// 由本代码维护，而非 pod selector。
			Selector:        nil,
			ClusterIP:       serviceIP.String(),
			IPFamilyPolicy:  &singleStack,
			SessionAffinity: corev1.ServiceAffinityNone,
			Type:            serviceType,
		},
	}

	_, err := c.client.CoreV1().Services(metav1.NamespaceDefault).Create(context.TODO(), svc, metav1.CreateOptions{})
	if errors.IsAlreadyExists(err) {
		return c.CreateOrUpdateMasterServiceIfNeeded(serviceName, serviceIP, servicePorts, serviceType, reconcile)
	}
	return err
}

// getMasterServiceUpdateIfNeeded 设置给定 apiserver Service 的属性。
func getMasterServiceUpdateIfNeeded(svc *corev1.Service, servicePorts []corev1.ServicePort, serviceType corev1.ServiceType) (s *corev1.Service, updated bool) {
	// 判断 Service 是否符合预期格式。
	// （端口存在且 Service 类型匹配）
	formatCorrect := checkServiceFormat(svc, servicePorts, serviceType)
	if formatCorrect {
		return svc, false
	}
	svc.Spec.Ports = servicePorts
	svc.Spec.Type = serviceType
	return svc, true
}

// 判断 Service 格式是否正确；
// getMasterServiceUpdateIfNeeded 假定端口正确，
// 且 Service 类型匹配。
func checkServiceFormat(s *corev1.Service, ports []corev1.ServicePort, serviceType corev1.ServiceType) (formatCorrect bool) {
	if s.Spec.Type != serviceType {
		return false
	}
	if len(ports) != len(s.Spec.Ports) {
		return false
	}
	for i, port := range ports {
		if port != s.Spec.Ports[i] {
			return false
		}
	}
	return true
}
