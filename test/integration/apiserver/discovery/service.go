/*
Copyright 2022 The Kubernetes Authors.

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

package discovery

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"

	"k8s.io/client-go/kubernetes"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	corev1apply "k8s.io/client-go/applyconfigurations/core/v1"
)

type FakeService interface {
	Run(ctx context.Context) error
	Port() *int32
	Name() string
}

// 创建并注册一个进程内 Service，它能够与
// kubernetes 集成测试 apiserver 通信
type fakeService struct {
	name    string
	client  kubernetes.Interface
	handler http.Handler

	lock       sync.RWMutex
	activePort *int32
}

func NewFakeService(name string, client kubernetes.Interface, handler http.Handler) *fakeService {
	return &fakeService{
		name:    name,
		client:  client,
		handler: handler,
	}
}

func (f *fakeService) Run(ctx context.Context) error {
	aggregatedServer := httptest.NewUnstartedServer(f.handler)
	aggregatedServer.StartTLS()
	defer aggregatedServer.Close()

	serverURL, err := url.Parse(aggregatedServer.URL)
	if err != nil {
		// 这不应发生
		panic(err)
	}

	serverPort, err := strconv.Atoi(serverURL.Port())
	if err != nil {
		// 这不应发生
		panic(err)
	}

	port := int32(serverPort)

	// 将 service 安装到集群中
	service, err := f.client.CoreV1().Services("default").Apply(
		ctx,
		corev1apply.Service(f.name, "default").
			WithSpec(corev1apply.ServiceSpec().
				WithPorts(
					corev1apply.ServicePort().
						WithPort(port)).
				WithType("ExternalName").
				WithExternalName("localhost")),
		metav1.ApplyOptions{
			FieldManager: "test-manager",
		},
	)
	if err != nil {
		return err
	}

	f.lock.Lock()
	f.activePort = &port
	f.lock.Unlock()

	<-ctx.Done()

	f.lock.Lock()
	f.activePort = nil
	f.lock.Unlock()

	// 从集群中卸载 service
	err = f.client.CoreV1().Services("default").Delete(ctx, service.Name, metav1.DeleteOptions{})
	if errors.Is(err, context.Canceled) {
		err = nil
	}
	return err
}

func (f *fakeService) WaitForReady(ctx context.Context) error {
	err := wait.PollUntilContextTimeout(ctx, 200*time.Millisecond, time.Second, false, func(ctx context.Context) (done bool, err error) {
		return f.Port() != nil, nil
	})

	if errors.Is(err, context.Canceled) {
		err = nil
	} else if err != nil {
		err = fmt.Errorf("service should have come alive in a reasonable amount of time: %w", err)
	}

	return err
}

func (f *fakeService) Port() *int32 {
	// 如果 server 正在运行，返回其端口；
	// 如果未运行，则返回 nil
	f.lock.RLock()
	defer f.lock.RUnlock()
	return f.activePort
}

func (f *fakeService) Name() string {
	return f.name
}
