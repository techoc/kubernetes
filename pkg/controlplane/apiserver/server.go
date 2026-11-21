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

package apiserver

import (
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	coordinationapiv1 "k8s.io/api/coordination/v1"
	apiv1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	apiserverfeatures "k8s.io/apiserver/pkg/features"
	peerreconcilers "k8s.io/apiserver/pkg/reconcilers"
	genericregistry "k8s.io/apiserver/pkg/registry/generic"
	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/dynamiccertificates"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	clientgoinformers "k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/component-helpers/apimachinery/lease"
	"k8s.io/klog/v2"
	"k8s.io/utils/clock"

	"k8s.io/kubernetes/pkg/controlplane/controller/apiserverleasegc"
	"k8s.io/kubernetes/pkg/controlplane/controller/clusterauthenticationtrust"
	"k8s.io/kubernetes/pkg/controlplane/controller/leaderelection"
	"k8s.io/kubernetes/pkg/controlplane/controller/legacytokentracking"
	"k8s.io/kubernetes/pkg/controlplane/controller/systemnamespaces"
	"k8s.io/kubernetes/pkg/routes"
	"k8s.io/kubernetes/pkg/serviceaccount"
)

var (
	// IdentityLeaseGCPeriod 是 identity lease GC 控制器检查过期租约的间隔。
	// 暴露该变量以便集成测试调整。
	IdentityLeaseGCPeriod = 3600 * time.Second
	// IdentityLeaseDurationSeconds 是 kube-apiserver identity 租约的时长（秒）。
	// 暴露该变量以便集成测试调整。
	IdentityLeaseDurationSeconds = 3600
	// IdentityLeaseRenewIntervalPeriod 是 kube-apiserver 续租间隔（秒）。
	// 暴露该变量以便集成测试调整。
	IdentityLeaseRenewIntervalPeriod = 10 * time.Second

	// LeaseCandidateGCPeriod 是 LeaseCandidate GC 控制器检查过期租约的间隔。
	// 暴露该变量以便集成测试调整。
	LeaseCandidateGCPeriod = 30 * time.Minute
)

const (
	// IdentityLeaseComponentLabelKey 用于给 identity 租约打组件标签，表明：
	//   1. 该租约是 identity 租约（不同于 leader election 租约）
	//   2. 该租约归属哪个组件
	IdentityLeaseComponentLabelKey = "apiserver.kubernetes.io/identity"
	// KubeAPIServer 是内部引用 kube-apiserver 组件时使用的名称。
	KubeAPIServer = "kube-apiserver"
)

// Server 持有一个可运行并开始提供 API 服务的通用控制面 apiserver 实例。
type Server struct {
	GenericAPIServer *genericapiserver.GenericAPIServer

	APIResourceConfigSource   serverstorage.APIResourceConfigSource
	RESTOptionsGetter         genericregistry.RESTOptionsGetter
	ClusterAuthenticationInfo clusterauthenticationtrust.ClusterAuthenticationInfo
	VersionedInformers        clientgoinformers.SharedInformerFactory
}

// New 根据给定配置创建 Server 实例。
// 部分未设置的配置字段会被赋予默认值，另一些字段则必须显式指定。
func (c completedConfig) New(name string, delegationTarget genericapiserver.DelegationTarget) (*Server, error) {
	generic, err := c.Generic.New(name, delegationTarget)
	if err != nil {
		return nil, err
	}

	if c.EnableLogsSupport {
		routes.Logs{}.Install(generic.Handler.GoRestfulContainer)
	}

	md, err := serviceaccount.NewOpenIDMetadataProvider(
		c.ServiceAccountIssuerURL,
		c.ServiceAccountJWKSURI,
		c.Generic.ExternalAddress,
		c.ServiceAccountPublicKeysGetter,
	)
	if err != nil {
		// 构造失败时仅记录错误并跳过安装端点，不中断启动：metadata 响应需要额外的、不向后兼容的命令行参数校验。
		msg := fmt.Sprintf("Could not construct pre-rendered responses for"+
			" ServiceAccountIssuerDiscovery endpoints. Endpoints will not be"+
			" enabled. Error: %v", err)
		if c.ServiceAccountIssuerURL != "" {
			// 若设置了 issuer URL，用户很可能期望该特性启用，故记为 Error；反之记为 Info。
			klog.Error(msg)
		} else {
			klog.Info(msg)
		}
	} else {
		routes.NewOpenIDMetadataServer(md).Install(generic.Handler.GoRestfulContainer)
	}

	s := &Server{
		GenericAPIServer: generic,

		APIResourceConfigSource:   c.APIResourceConfigSource,
		RESTOptionsGetter:         c.Generic.RESTOptionsGetter,
		ClusterAuthenticationInfo: c.ClusterAuthenticationInfo,
		VersionedInformers:        c.VersionedInformers,
	}

	client, err := kubernetes.NewForConfig(s.GenericAPIServer.LoopbackClientConfig)
	if err != nil {
		return nil, err
	}
	if len(c.SystemNamespaces) > 0 {
		s.GenericAPIServer.AddPostStartHookOrDie("start-system-namespaces-controller", func(hookContext genericapiserver.PostStartHookContext) error {
			go systemnamespaces.NewController(c.SystemNamespaces, client, s.VersionedInformers.Core().V1().Namespaces()).Run(hookContext.Done())
			return nil
		})
	}

	_, publicServicePort, err := c.Generic.SecureServing.HostPort()
	if err != nil {
		return nil, fmt.Errorf("failed to get listener address: %w", err)
	}

	if utilfeature.DefaultFeatureGate.Enabled(apiserverfeatures.CoordinatedLeaderElection) {
		leaseInformer := s.VersionedInformers.Coordination().V1().Leases()
		lcInformer := s.VersionedInformers.Coordination().V1beta1().LeaseCandidates()
		// 启动前先注册 informer：协调式 Leader Election 选主后可能才注册 handler，若 informer 未提前注册将错过事件。
		_ = leaseInformer.Informer()
		_ = lcInformer.Informer()
		s.GenericAPIServer.AddPostStartHookOrDie("start-kube-apiserver-coordinated-leader-election-controller", func(hookContext genericapiserver.PostStartHookContext) error {
			go leaderelection.RunWithLeaderElection(hookContext, s.GenericAPIServer.LoopbackClientConfig, func() (func(ctx context.Context, workers int), error) {
				controller, err := leaderelection.NewController(
					leaseInformer,
					lcInformer,
					client.CoordinationV1(),
					client.CoordinationV1beta1(),
				)
				gccontroller := leaderelection.NewLeaseCandidateGC(
					client,
					LeaseCandidateGCPeriod,
					lcInformer,
				)
				return func(ctx context.Context, workers int) {
					var wg sync.WaitGroup
					wg.Go(func() { controller.Run(ctx, workers) })
					wg.Go(func() { gccontroller.Run(ctx) })
					wg.Wait()
				}, err
			}, leaderelection.LeaderElectionTimers{
				LeaseDuration: c.CoordinatedLeadershipLeaseDuration,
				RenewDeadline: c.CoordinatedLeadershipRenewDeadline,
				RetryPeriod:   c.CoordinatedLeadershipRetryPeriod,
			})
			return nil
		})
	}

	if utilfeature.DefaultFeatureGate.Enabled(apiserverfeatures.UnknownVersionInteroperabilityProxy) {
		peeraddress := getPeerAddress(c.Extra.PeerAdvertiseAddress, c.Generic.PublicAddress, publicServicePort)
		peerEndpointCtrl := peerreconcilers.New(
			c.Generic.APIServerID,
			peeraddress,
			c.Extra.PeerEndpointLeaseReconciler,
			c.Extra.PeerEndpointReconcileInterval,
			client)
		s.GenericAPIServer.AddPostStartHookOrDie("peer-endpoint-reconciler-controller",
			func(hookContext genericapiserver.PostStartHookContext) error {
				peerEndpointCtrl.Start(hookContext.Done())
				return nil
			})
		s.GenericAPIServer.AddPreShutdownHookOrDie("peer-endpoint-reconciler-controller",
			func() error {
				peerEndpointCtrl.Stop()
				return nil
			})
		if c.Extra.PeerProxy != nil {
			// 等待代理 handler 缓存同步完成后再对外服务。
			s.GenericAPIServer.AddPostStartHookOrDie("mixed-version-proxy-handler", func(context genericapiserver.PostStartHookContext) error {
				err := c.Extra.PeerProxy.WaitForCacheSync(context.Done())
				return err
			})

			// 启动本地发现缓存同步循环。
			s.GenericAPIServer.AddPostStartHookOrDie("local-discovery-cache-sync", func(context genericapiserver.PostStartHookContext) error {
				err := c.Extra.PeerProxy.RunLocalDiscoveryCacheSync(context.Done())
				return err
			})

			// 启动对等发现相关 worker。
			s.GenericAPIServer.AddPostStartHookOrDie("peer-discovery-workers", func(context genericapiserver.PostStartHookContext) error {
				go c.Extra.PeerProxy.RunPeerDiscoveryCacheSync(context, 1)
				go c.Extra.PeerProxy.RunPeerDiscoveryActiveGVTracker(context)
				go c.Extra.PeerProxy.RunPeerDiscoveryRefilter(context)
				return nil
			})
		}
	}

	s.GenericAPIServer.AddPostStartHookOrDie("start-cluster-authentication-info-controller", func(hookContext genericapiserver.PostStartHookContext) error {
		controller := clusterauthenticationtrust.NewClusterAuthenticationTrustController(s.ClusterAuthenticationInfo, client)
		// 预热 CA 值并启动监听。
		if s.ClusterAuthenticationInfo.ClientCA != nil {
			s.ClusterAuthenticationInfo.ClientCA.AddListener(controller)
			if controller, ok := s.ClusterAuthenticationInfo.ClientCA.(dynamiccertificates.ControllerRunner); ok {
				// 先执行一次确保拿到初始值。
				if err := controller.RunOnce(hookContext); err != nil {
					runtime.HandleError(err)
				}
				go controller.Run(hookContext, 1)
			}
		}
		if s.ClusterAuthenticationInfo.RequestHeaderCA != nil {
			s.ClusterAuthenticationInfo.RequestHeaderCA.AddListener(controller)
			if controller, ok := s.ClusterAuthenticationInfo.RequestHeaderCA.(dynamiccertificates.ControllerRunner); ok {
				// 同样先执行一次确保拿到初始值。
				if err := controller.RunOnce(hookContext); err != nil {
					runtime.HandleError(err)
				}
				go controller.Run(hookContext, 1)
			}
		}

		go controller.Run(hookContext, 1)
		return nil
	})

	if utilfeature.DefaultFeatureGate.Enabled(apiserverfeatures.APIServerIdentity) {
		leaseName := s.GenericAPIServer.APIServerID
		holderIdentity := s.GenericAPIServer.APIServerID + "_" + string(uuid.NewUUID())
		peeraddress := getPeerAddress(c.Extra.PeerAdvertiseAddress, c.Generic.PublicAddress, publicServicePort)

		// [ip:port] 中的 ':,' 和 '[]' 必须替换，才能作为合法的 label 值存储。
		controller := lease.NewController(
			clock.RealClock{},
			client,
			holderIdentity,
			int32(IdentityLeaseDurationSeconds),
			nil,
			IdentityLeaseRenewIntervalPeriod,
			leaseName,
			metav1.NamespaceSystem,
			// TODO: receive identity label value as a parameter when post start hook is moved to generic apiserver.
			labelAPIServerHeartbeatFunc(name, peeraddress),
		)
		s.GenericAPIServer.AddPostStartHookOrDie("start-kube-apiserver-identity-lease-controller", func(hookContext genericapiserver.PostStartHookContext) error {
			return controller.Start(hookContext.Done())
		})
		s.GenericAPIServer.AddPreShutdownHookOrDie("stop-kube-apiserver-identity-lease-controller", func() error {
			controller.Stop()
			return nil
		})

		// TODO: move this into generic apiserver and make the lease identity value configurable
		s.GenericAPIServer.AddPostStartHookOrDie("start-kube-apiserver-identity-lease-garbage-collector", func(hookContext genericapiserver.PostStartHookContext) error {
			go apiserverleasegc.NewAPIServerLeaseGC(
				client,
				IdentityLeaseGCPeriod,
				metav1.NamespaceSystem,
				IdentityLeaseComponentLabelKey+"="+name,
			).RunWithContext(hookContext)
			return nil
		})
	}

	if utilfeature.DefaultFeatureGate.Enabled(apiserverfeatures.WatchCacheInitializationPostStartHook) {
		s.GenericAPIServer.AddPostStartHookOrDie("storage-readiness", s.GenericAPIServer.StorageReadinessHook.Hook)
	}

	s.GenericAPIServer.AddPostStartHookOrDie("start-legacy-token-tracking-controller", func(hookContext genericapiserver.PostStartHookContext) error {
		go legacytokentracking.NewController(client).RunWithContext(hookContext)
		return nil
	})

	return s, nil
}

// labelAPIServerHeartbeatFunc 返回处理函数，为 identity 租约打上组件、主机名与对等地址标签。
func labelAPIServerHeartbeatFunc(identity string, peeraddress string) lease.ProcessLeaseFunc {
	return func(lease *coordinationapiv1.Lease) error {
		if lease.Labels == nil {
			lease.Labels = map[string]string{}
		}

		if lease.Annotations == nil {
			lease.Annotations = map[string]string{}
		}

		// 该 label 标识租约对象所属组件的身份。
		lease.Labels[IdentityLeaseComponentLabelKey] = identity

		hostname, err := os.Hostname()
		if err != nil {
			return err
		}

		// 便于将租约对象映射到具体 apiserver 的主机名标签。
		lease.Labels[apiv1.LabelHostname] = hostname

		// 记录 apiserver 的网络位置 <ip_port>，供对等节点在 apiserver 间代理请求。
		if utilfeature.DefaultFeatureGate.Enabled(apiserverfeatures.UnknownVersionInteroperabilityProxy) {
			if peeraddress != "" {
				lease.Annotations[apiv1.AnnotationPeerAdvertiseAddress] = peeraddress
			}
		}
		return nil
	}
}
