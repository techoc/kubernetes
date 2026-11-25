/*
Copyright 2020 The Kubernetes Authors.

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
	"sync"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/controlplane"
	"k8s.io/kubernetes/pkg/controlplane/reconcilers"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// setup 创建一个由两个独立 etcd 支撑的 kube-apiserver，
// 其中一个存放 events，另一个存放其他所有对象。
func multiEtcdSetup(ctx context.Context, t *testing.T) (clientset.Interface, framework.TearDownFunc) {
	etcdArgs := []string{"--watch-progress-notify-interval", "1s"}
	etcd0URL, stopEtcd0, err := framework.RunCustomEtcd(klog.FromContext(ctx).WithName("etcd0"), "etcd_watchcache0", etcdArgs)
	if err != nil {
		t.Fatalf("Couldn't start etcd: %v", err)
	}

	etcd1URL, stopEtcd1, err := framework.RunCustomEtcd(klog.FromContext(ctx).WithName("etcd1"), "etcd_watchcache1", etcdArgs)
	if err != nil {
		t.Fatalf("Couldn't start etcd: %v", err)
	}

	etcdOptions := framework.DefaultEtcdOptions()
	// 将 etcd 配置覆盖为我们的自定义 etcd 实例。
	etcdOptions.StorageConfig.Transport.ServerList = []string{etcd0URL}
	etcdOptions.EtcdServersOverrides = []string{fmt.Sprintf("/events#%s", etcd1URL)}
	etcdOptions.EnableWatchCache = true

	clientSet, _, tearDownFn := framework.StartTestServer(ctx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			// 确保 apiserver 重启时使用同一个 etcd。
			opts.Etcd = etcdOptions
		},
		ModifyServerConfig: func(config *controlplane.Config) {
			// 关闭 endpoints reconciler，避免不必要的操作。
			config.Extra.EndpointReconcilerType = reconcilers.NoneEndpointReconcilerType
		},
	})

	closeFn := func() {
		tearDownFn()
		stopEtcd1()
		stopEtcd0()
	}

	// 等待 apiserver 稳定。
	// 除了默认 service 的创建之外，其他部分已在上面的 StartTestServer 中通过
	// 等待 post start hook 检查过了，因此这里只需等待默认 service 存在。
	// TODO(wojtek-t): 找出一种不那么脆弱的方式。
	if err := wait.Poll(100*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		_, err := clientSet.CoreV1().Services("default").Get(ctx, "kubernetes", metav1.GetOptions{})
		return err == nil, nil
	}); err != nil {
		t.Fatalf("Failed to wait for kubernetes service: %v:", err)
	}
	return clientSet, closeFn
}

func TestWatchCacheUpdatedByEtcd(t *testing.T) {
	tCtx := ktesting.Init(t)
	c, closeFn := multiEtcdSetup(tCtx, t)
	defer closeFn()

	makeConfigMap := func(name string) *v1.ConfigMap {
		return &v1.ConfigMap{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	makeSecret := func(name string) *v1.Secret {
		return &v1.Secret{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}
	makeEvent := func(name string) *v1.Event {
		return &v1.Event{ObjectMeta: metav1.ObjectMeta{Name: name}}
	}

	cm, err := c.CoreV1().ConfigMaps("default").Create(tCtx, makeConfigMap("name"), metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Couldn't create configmap: %v", err)
	}
	ev, err := c.CoreV1().Events("default").Create(tCtx, makeEvent("name"), metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Couldn't create event: %v", err)
	}

	listOptions := metav1.ListOptions{
		ResourceVersion:      "0",
		ResourceVersionMatch: metav1.ResourceVersionMatchNotOlderThan,
	}

	// 等待从缓存中列出资源时返回相应资源的 resource version
	// （即最后一次更新的版本）。
	t.Logf("Waiting for configmaps watchcache synced to %s", cm.ResourceVersion)
	if err := wait.Poll(100*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		res, err := c.CoreV1().ConfigMaps("default").List(tCtx, listOptions)
		if err != nil {
			return false, nil
		}
		return res.ResourceVersion == cm.ResourceVersion, nil
	}); err != nil {
		t.Errorf("Failed to wait for configmaps watchcache synced: %v", err)
	}
	t.Logf("Waiting for events watchcache synced to %s", ev.ResourceVersion)
	if err := wait.Poll(100*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		res, err := c.CoreV1().Events("default").List(tCtx, listOptions)
		if err != nil {
			return false, nil
		}
		return res.ResourceVersion == ev.ResourceVersion, nil
	}); err != nil {
		t.Errorf("Failed to wait for events watchcache synced: %v", err)
	}

	// 创建一个 secret，它与 configmap 存放在同一个 etcd 中，
	// 但与 events 不同。
	se, err := c.CoreV1().Secrets("default").Create(tCtx, makeSecret("name"), metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Couldn't create secret: %v", err)
	}

	t.Logf("Waiting for configmaps watchcache synced to %s", se.ResourceVersion)
	if err := wait.Poll(100*time.Millisecond, wait.ForeverTestTimeout, func() (bool, error) {
		res, err := c.CoreV1().ConfigMaps("default").List(tCtx, listOptions)
		if err != nil {
			return false, nil
		}
		return res.ResourceVersion == se.ResourceVersion, nil
	}); err != nil {
		t.Errorf("Failed to wait for configmaps watchcache synced: %v", err)
	}
	t.Logf("Waiting for events watchcache NOT synced to %s", se.ResourceVersion)
	if err := wait.Poll(100*time.Millisecond, 5*time.Second, func() (bool, error) {
		res, err := c.CoreV1().Events("default").List(tCtx, listOptions)
		if err != nil {
			return false, nil
		}
		return res.ResourceVersion == se.ResourceVersion, nil
	}); err == nil || !wait.Interrupted(err) {
		t.Errorf("Events watchcache unexpected synced: %v", err)
	}
}

func BenchmarkListFromWatchCache(b *testing.B) {
	tCtx := ktesting.Init(b)
	c, _, tearDownFn := framework.StartTestServer(tCtx, b, framework.TestServerSetup{
		ModifyServerConfig: func(config *controlplane.Config) {
			// 关闭 endpoints reconciler，避免不必要的操作。
			config.Extra.EndpointReconcilerType = reconcilers.NoneEndpointReconcilerType
		},
	})
	defer tearDownFn()

	namespaces, secretsPerNamespace := 100, 1000
	wg := sync.WaitGroup{}

	errCh := make(chan error, namespaces)
	for index := range namespaces {
		wg.Add(1)
		go func() {
			defer wg.Done()

			ns := &v1.Namespace{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("namespace-%d", index)},
			}
			ns, err := c.CoreV1().Namespaces().Create(tCtx, ns, metav1.CreateOptions{})
			if err != nil {
				errCh <- err
				return
			}

			for j := range secretsPerNamespace {
				secret := &v1.Secret{
					ObjectMeta: metav1.ObjectMeta{
						Name: fmt.Sprintf("secret-%d", j),
					},
				}
				_, err := c.CoreV1().Secrets(ns.Name).Create(tCtx, secret, metav1.CreateOptions{})
				if err != nil {
					errCh <- err
					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)
	for err := range errCh {
		b.Error(err)
	}

	b.ResetTimer()

	opts := metav1.ListOptions{
		ResourceVersion: "0",
	}
	for i := 0; i < b.N; i++ {
		secrets, err := c.CoreV1().Secrets("").List(tCtx, opts)
		if err != nil {
			b.Errorf("failed to list secrets: %v", err)
		}
		b.Logf("Number of secrets: %d", len(secrets.Items))
	}
}
