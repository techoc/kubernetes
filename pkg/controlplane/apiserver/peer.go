/*
Copyright 2024 The Kubernetes Authors.

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
	"fmt"
	"net"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apiserver/pkg/reconcilers"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/transport"

	genericapiserver "k8s.io/apiserver/pkg/server"
	"k8s.io/apiserver/pkg/server/egressselector"
	serverstorage "k8s.io/apiserver/pkg/server/storage"
	utilpeerproxy "k8s.io/apiserver/pkg/util/peerproxy"
	coordinationv1informers "k8s.io/client-go/informers/coordination/v1"
	api "k8s.io/kubernetes/pkg/apis/core"
)

const (
	// DefaultPeerEndpointReconcileInterval 是对等端点租约的默认重建间隔。
	DefaultPeerEndpointReconcileInterval = 10 * time.Second
	// DefaultPeerEndpointReconcilerTTL 是存储层中对等端点租约的默认 TTL。
	DefaultPeerEndpointReconcilerTTL = 15 * time.Second
)

// BuildPeerProxy 构造对等代理 handler，使无法本地服务的请求能被转发到其他 apiserver。
func BuildPeerProxy(
	leaseInformer coordinationv1informers.LeaseInformer,
	loopbackClientConfig *rest.Config,
	proxyClientCertFile string,
	proxyClientKeyFile string,
	peerCAFile string,
	peerAdvertiseAddress reconcilers.PeerAdvertiseAddress,
	apiServerID string,
	reconciler reconcilers.PeerEndpointLeaseReconciler,
	serializer runtime.NegotiatedSerializer,
	es *egressselector.EgressSelector,
) (utilpeerproxy.Interface, error) {
	if proxyClientCertFile == "" {
		return nil, fmt.Errorf("error building peer proxy handler, proxy-cert-file not specified")
	}
	if proxyClientKeyFile == "" {
		return nil, fmt.Errorf("error building peer proxy handler, proxy-key-file not specified")
	}

	proxyClientConfig := &transport.Config{
		TLS: transport.TLSConfig{
			Insecure:   false,
			CertFile:   proxyClientCertFile,
			KeyFile:    proxyClientKeyFile,
			CAFile:     peerCAFile,
			ServerName: "kubernetes.default.svc",
		}}

	if es != nil {
		egressDialer, err := es.Lookup(egressselector.ControlPlane.AsNetworkContext())
		if err != nil {
			return nil, err
		}

		// 仅为 ControlPlane 网络上下文显式配置的 egress dialer 设置 DialHolder，否则 transport 回退到默认直连。
		if egressDialer != nil {
			proxyClientConfig.DialHolder = &transport.DialHolder{Dial: egressDialer}
		}
	}

	return utilpeerproxy.NewPeerProxyHandler(
		apiServerID,
		IdentityLeaseComponentLabelKey+"="+KubeAPIServer,
		leaseInformer,
		reconciler,
		serializer,
		loopbackClientConfig,
		proxyClientConfig,
	)
}

// CreatePeerEndpointLeaseReconciler 创建 apiserver 端点租约重建循环，租约供对等代理定位各 apiserver 的网络地址。
func CreatePeerEndpointLeaseReconciler(c genericapiserver.Config, storageFactory serverstorage.StorageFactory) (reconcilers.PeerEndpointLeaseReconciler, error) {
	ttl := DefaultPeerEndpointReconcilerTTL
	config, err := storageFactory.NewConfig(api.Resource("apiServerPeerIPInfo"), &api.Endpoints{})
	if err != nil {
		return nil, fmt.Errorf("error creating storage factory config: %w", err)
	}
	reconciler, err := reconcilers.NewPeerEndpointLeaseReconciler(config, "/peerserverleases/", ttl)
	return reconciler, err
}

// getPeerAddress 返回对等 apiserver 用于代理请求到本 apiserver 的地址；优先使用显式通告地址。
func getPeerAddress(peerAdvertiseAddress reconcilers.PeerAdvertiseAddress, publicAddress net.IP, publicServicePort int) string {
	if peerAdvertiseAddress.PeerAdvertiseIP != "" && peerAdvertiseAddress.PeerAdvertisePort != "" {
		return net.JoinHostPort(peerAdvertiseAddress.PeerAdvertiseIP, peerAdvertiseAddress.PeerAdvertisePort)
	} else {
		return net.JoinHostPort(publicAddress.String(), strconv.Itoa(publicServicePort))
	}
}
