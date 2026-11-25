//go:build windows

/*
Copyright The Kubernetes Authors.

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

package services

import (
	"context"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/metadata"
	restclient "k8s.io/client-go/rest"
	namespacecontroller "k8s.io/kubernetes/pkg/controller/namespace"
	"k8s.io/kubernetes/test/e2e/framework"
)

const (
	// ncName 是 namespace controller 的名称
	ncName = "namespace-controller"
	// ncResyncPeriod 是 namespace controller 的重同步周期
	ncResyncPeriod = 5 * time.Minute
	// ncConcurrency 是 namespace controller 的并发度
	ncConcurrency = 2
)

// NamespaceController 是管理 namespace controller 的服务。
type NamespaceController struct {
	host   string
	stopCh chan struct{}
}

// NewNamespaceController 创建一个新的 namespace controller。
func NewNamespaceController(host string) *NamespaceController {
	return &NamespaceController{host: host, stopCh: make(chan struct{})}
}

// Start 启动 namespace controller。
func (n *NamespaceController) Start(ctx context.Context) error {
	config := restclient.AddUserAgent(&restclient.Config{
		Host:        n.host,
		BearerToken: framework.TestContext.BearerToken,
		TLSClientConfig: restclient.TLSClientConfig{
			Insecure: true,
		},
	}, ncName)

	// namespace 清理控制器非常“话多”：会发起大量 discovery 调用，随后又发起大量 delete 调用。
	config.QPS = 50
	config.Burst = 200

	client, err := clientset.NewForConfig(config)
	if err != nil {
		return err
	}
	metadataClient, err := metadata.NewForConfig(config)
	if err != nil {
		return err
	}
	discoverResourcesFn := client.Discovery().ServerPreferredNamespacedResources
	informerFactory := informers.NewSharedInformerFactory(client, ncResyncPeriod)

	nc := namespacecontroller.NewNamespaceController(
		ctx,
		client,
		metadataClient,
		discoverResourcesFn,
		informerFactory.Core().V1().Namespaces(),
		ncResyncPeriod, v1.FinalizerKubernetes,
	)
	informerFactory.Start(n.stopCh)
	go nc.Run(ctx, ncConcurrency)
	return nil
}

// Stop 停止 namespace controller。
func (n *NamespaceController) Stop() error {
	close(n.stopCh)
	return nil
}

// Name 返回 namespace controller 的名称。
func (n *NamespaceController) Name() string {
	return ncName
}
