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
	"os"
	"testing"

	etcd3testing "k8s.io/apiserver/pkg/storage/etcd3/testing"
	"k8s.io/apiserver/pkg/storage/storagebackend"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/kubernetes/test/utils/ktesting"

	"k8s.io/klog/v2"
)

// e2eService 管理当前进程中的 e2e service。
type e2eServices struct {
	rmDirs []string
	// 静态链接的 e2e service
	etcdServer   *etcd3testing.EtcdTestServer
	etcdStorage  *storagebackend.Config
	apiServer    *APIServer
	nsController *NamespaceController
}

func newE2EServices() *e2eServices {
	return &e2eServices{}
}

// run 启动所有 e2e service 并等待终止信号。收到终止信号后，
// 会优雅地停止这些 e2e service。
func (es *e2eServices) run(t *testing.T) error {
	defer es.stop(t)
	if err := es.start(t); err != nil {
		return err
	}
	// 等待收到终止信号。
	waitForTerminationSignal()
	return nil
}

// start 启动测试内嵌的 service，或返回错误。
func (es *e2eServices) start(t *testing.T) error {
	tCtx := ktesting.Init(t)
	klog.Info("Starting e2e services...")
	err := es.startEtcd(t)
	if err != nil {
		return err
	}
	err = es.startAPIServer(tCtx, es.etcdStorage)
	if err != nil {
		return err
	}
	err = es.startNamespaceController(tCtx)
	if err != nil {
		return nil
	}
	klog.Info("E2E services started.")
	return nil
}

// stop 停止内嵌的 e2e service。
func (es *e2eServices) stop(t *testing.T) {
	klog.Info("Stopping e2e services...")
	// TODO(random-liu): 引入 service 接口后，使用循环停止所有 service。
	//
	klog.Info("Stopping namespace controller")
	if es.nsController != nil {
		if err := es.nsController.Stop(); err != nil {
			klog.Errorf("Failed to stop %q: %v", es.nsController.Name(), err)
		}
	}

	klog.Info("Stopping API server")
	if es.apiServer != nil {
		if err := es.apiServer.Stop(); err != nil {
			klog.Errorf("Failed to stop %q: %v", es.apiServer.Name(), err)
		}
	}

	klog.Info("Stopping etcd")
	if es.etcdServer != nil {
		es.etcdServer.Terminate(t)
	}

	for _, d := range es.rmDirs {
		klog.Infof("Deleting directory %v", d)
		err := os.RemoveAll(d)
		if err != nil {
			klog.Errorf("Failed to delete directory %s.\n%v", d, err)
		}
	}

	klog.Info("E2E services stopped.")
}

// startEtcd 启动内嵌的 etcd 实例，或返回错误。
func (es *e2eServices) startEtcd(t *testing.T) error {
	klog.Info("Starting etcd")
	server, etcdStorage := etcd3testing.NewUnsecuredEtcd3TestClientServer(t)
	es.etcdServer = server
	es.etcdStorage = etcdStorage
	return nil
}

// startAPIServer 启动内嵌的 API server，或返回错误。
func (es *e2eServices) startAPIServer(ctx context.Context, etcdStorage *storagebackend.Config) error {
	klog.Info("Starting API server")
	es.apiServer = NewAPIServer(*etcdStorage)
	return es.apiServer.Start(ctx)
}

// startNamespaceController 启动内嵌的 namespace controller，或返回错误。
func (es *e2eServices) startNamespaceController(ctx context.Context) error {
	klog.FromContext(ctx).Info("Starting namespace controller")
	es.nsController = NewNamespaceController(framework.TestContext.Host)
	return es.nsController.Start(ctx)
}

// getServicesHealthCheckURLs 返回内部 service 的健康检查 URL。
func getServicesHealthCheckURLs() []string {
	return []string{
		getAPIServerHealthCheckURL(),
	}
}

func SetFeatureGatesForInProcessComponents(featureGates map[string]bool) error {
	return utilfeature.DefaultMutableFeatureGate.SetFromMap(featureGates)
}
