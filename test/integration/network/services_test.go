/*
Copyright 2021 The Kubernetes Authors.

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

package network

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/cmd/kube-apiserver/app/options"
	"k8s.io/kubernetes/pkg/controlplane"

	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestServicesFinalizersRepairLoop 测试 Service 在使用 finalizer 时参与对象删除，
// 并且 Services Repair 控制器不会错误地修复正在删除的 Service
// 所分配的 ClusterIP。
// https://issues.k8s.io/87603
func TestServicesFinalizersRepairLoop(t *testing.T) {
	serviceCIDR := "10.0.0.0/16"
	clusterIP := "10.0.0.20"
	interval := 5 * time.Second

	tCtx := ktesting.Init(t)
	client, _, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			opts.ServiceClusterIPRanges = serviceCIDR
		},
		ModifyServerConfig: func(cfg *controlplane.Config) {
			cfg.Extra.RepairServicesInterval = interval
		},
	})
	defer tearDownFn()

	// 确认客户端正常工作
	if err := wait.PollImmediate(5*time.Second, 2*time.Minute, func() (bool, error) {
		_, err := client.CoreV1().Endpoints(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
		if err != nil {
			t.Logf("error fetching endpoints: %v", err)
			return false, nil
		}
		return true, nil
	}); err != nil {
		t.Errorf("server without enabled endpoints failed to register: %v", err)
	}

	// 创建一个带有一个 finalizer 的 NodePort Service
	svcNodePort := v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "svc",
			Finalizers: []string{"foo.bar/some-finalizer"},
		},
		Spec: v1.ServiceSpec{
			ClusterIP: clusterIP,
			Ports: []v1.ServicePort{{
				Port:       8443,
				NodePort:   30443,
				TargetPort: intstr.FromInt32(8443),
				Protocol:   v1.ProtocolTCP,
			}},
			Type: v1.ServiceTypeNodePort,
		},
	}

	// 创建 Service
	if _, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, &svcNodePort, metav1.CreateOptions{}); err != nil {
		t.Errorf("unexpected error creating service: %v", err)
	}
	t.Logf("Created service: %s", svcNodePort.Name)

	// 检查 Service 是否正确创建
	svc, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svcNodePort.Name, metav1.GetOptions{})
	if err != nil || svc.Spec.ClusterIP != clusterIP {
		t.Errorf("created service is not correct: %v", err)
	}
	t.Logf("Service created successfully: %v", svc)

	// 删除 Service
	if err := client.CoreV1().Services(metav1.NamespaceDefault).Delete(tCtx, svcNodePort.Name, metav1.DeleteOptions{}); err != nil {
		t.Errorf("unexpected error deleting service: %v", err)
	}
	t.Logf("Deleted service: %s", svcNodePort.Name)

	// 等待修复循环恢复被删除的资源
	time.Sleep(interval + 1)

	// 检查 Service 没有被删除，且 IP 已经分配
	svc, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svcNodePort.Name, metav1.GetOptions{})
	if err != nil || svc.Spec.ClusterIP != clusterIP {
		t.Errorf("created service is not correct: %v", err)
	}
	t.Logf("Service after Delete: %v", svc)

	// 移除 finalizer
	if _, err = client.CoreV1().Services(metav1.NamespaceDefault).Patch(tCtx, svcNodePort.Name, types.JSONPatchType, []byte(`[{"op":"remove","path":"/metadata/finalizers"}]`), metav1.PatchOptions{}); err != nil {
		t.Errorf("unexpected error removing finalizer: %v", err)
	}
	t.Logf("Removed service finalizer: %s", svcNodePort.Name)

	// 检查 Service 已被删除
	_, err = client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, svcNodePort.Name, metav1.GetOptions{})
	if err == nil {
		t.Errorf("service was not delete: %v", err)
	}

	// 再次尝试创建 Service
	if _, err := client.CoreV1().Services(metav1.NamespaceDefault).Create(tCtx, &svcNodePort, metav1.CreateOptions{}); err != nil {
		t.Errorf("unexpected error creating service: %v", err)
	}
	t.Logf("Created service: %s", svcNodePort.Name)
}

func TestServicesFinalizersPatchStatus(t *testing.T) {
	serviceCIDR := "10.0.0.0/16"
	clusterIP := "10.0.0.21"
	nodePort := 30443
	tCtx := ktesting.Init(t)
	client, _, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			opts.ServiceClusterIPRanges = serviceCIDR
		},
	})
	defer tearDownFn()

	for _, testcase := range []string{"spec", "status"} {
		t.Run(testcase, func(t *testing.T) {
			// 创建一个带有一个 finalizer 的 NodePort Service
			svcNodePort := v1.Service{
				ObjectMeta: metav1.ObjectMeta{
					Name:       "svc" + testcase,
					Finalizers: []string{"foo.bar/some-finalizer"},
				},
				Spec: v1.ServiceSpec{
					ClusterIP: clusterIP,
					Ports: []v1.ServicePort{{
						Port:       8443,
						NodePort:   int32(nodePort),
						TargetPort: intstr.FromInt32(8443),
						Protocol:   v1.ProtocolTCP,
					}},
					Type: v1.ServiceTypeNodePort,
				},
			}

			ns := framework.CreateNamespaceOrDie(client, "test-service-finalizers-"+testcase, t)
			defer framework.DeleteNamespaceOrDie(client, ns, t)

			// 创建 Service
			if _, err := client.CoreV1().Services(ns.Name).Create(tCtx, &svcNodePort, metav1.CreateOptions{}); err != nil {
				t.Fatalf("unexpected error creating service: %v", err)
			}
			t.Logf("Created service: %s", svcNodePort.Name)

			// 检查 Service 是否正确创建
			svc, err := client.CoreV1().Services(ns.Name).Get(tCtx, svcNodePort.Name, metav1.GetOptions{})
			if err != nil || svc.Spec.ClusterIP != clusterIP {
				t.Fatalf("created service is not correct: %v", err)
			}
			t.Logf("Service created successfully: %+v", svc)

			// 删除 Service
			if err := client.CoreV1().Services(ns.Name).Delete(tCtx, svcNodePort.Name, metav1.DeleteOptions{}); err != nil {
				t.Fatalf("unexpected error deleting service: %v", err)
			}
			t.Logf("Deleted service: %s", svcNodePort.Name)

			// 检查 Service 没有被删除，且 IP 已经分配
			svc, err = client.CoreV1().Services(ns.Name).Get(tCtx, svcNodePort.Name, metav1.GetOptions{})
			if err != nil ||
				svc.Spec.ClusterIP != clusterIP ||
				int(svc.Spec.Ports[0].NodePort) != nodePort ||
				svc.DeletionTimestamp == nil ||
				len(svc.ObjectMeta.Finalizers) != 1 {
				t.Fatalf("Service expected to be deleting and with the same values: %v", err)
			}
			t.Logf("Service after Delete: %+v", svc)

			// 移除 finalizer
			updated := svc.DeepCopy()
			updated.ObjectMeta.Finalizers = []string{}
			patchBytes, err := getPatchBytes(svc, updated)
			if err != nil {
				t.Fatalf("unexpected error getting patch bytes: %v", err)
			}

			if testcase == "spec" {
				if _, err = client.CoreV1().Services(ns.Name).Patch(tCtx, svcNodePort.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}); err != nil {
					t.Fatalf("unexpected error removing finalizer: %v", err)
				}
			} else {
				if _, err = client.CoreV1().Services(ns.Name).Patch(tCtx, svcNodePort.Name, types.StrategicMergePatchType, patchBytes, metav1.PatchOptions{}, "status"); err != nil {
					t.Fatalf("unexpected error removing finalizer: %v", err)
				}
			}
			t.Logf("Removed service finalizer: %s", svcNodePort.Name)

			// 检查 Service 已被删除
			_, err = client.CoreV1().Services(ns.Name).Get(tCtx, svcNodePort.Name, metav1.GetOptions{})
			if err == nil {
				t.Fatalf("service was not delete: %v", err)
			}

			// 不带 finalizer 再次创建 Service，以检查 ClusterIP 和 NodePort 是否被释放
			svc = svcNodePort.DeepCopy()
			svc.Finalizers = []string{}
			if _, err := client.CoreV1().Services(ns.Name).Create(tCtx, svc, metav1.CreateOptions{}); err != nil {
				t.Fatalf("unexpected error creating service: %v", err)
			}
			// 删除 Service
			if err := client.CoreV1().Services(ns.Name).Delete(tCtx, svc.Name, metav1.DeleteOptions{}); err != nil {
				t.Fatalf("unexpected error deleting service: %v", err)
			}
		})
	}
}

// 针对 https://issues.k8s.io/115316 的回归测试
func TestServiceCIDR28bits(t *testing.T) {
	serviceCIDR := "10.0.0.0/28"

	tCtx := ktesting.Init(t)
	client, _, tearDownFn := framework.StartTestServer(tCtx, t, framework.TestServerSetup{
		ModifyServerRunOptions: func(opts *options.ServerRunOptions) {
			opts.ServiceClusterIPRanges = serviceCIDR
		},
	})
	defer tearDownFn()

	// 等待默认的 "kubernetes" Service 创建完成。
	if err := wait.Poll(250*time.Millisecond, time.Minute, func() (bool, error) {
		_, err := client.CoreV1().Services(metav1.NamespaceDefault).Get(tCtx, "kubernetes", metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		return true, nil
	}); err != nil {
		t.Fatalf("creating kubernetes service timed out")
	}

	ns := framework.CreateNamespaceOrDie(client, "test-regression", t)
	defer framework.DeleteNamespaceOrDie(client, ns, t)

	service := &v1.Service{
		ObjectMeta: metav1.ObjectMeta{
			Name: "test-1234",
		},
		Spec: v1.ServiceSpec{
			Type: v1.ServiceTypeClusterIP,
			Ports: []v1.ServicePort{{
				Port: int32(80),
			}},
			Selector: map[string]string{
				"foo": "bar",
			},
		},
	}

	_, err := client.CoreV1().Services(ns.Name).Create(tCtx, service, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Error creating test service: %v", err)
	}
}

func getPatchBytes(oldSvc, newSvc *v1.Service) ([]byte, error) {
	oldData, err := json.Marshal(oldSvc)
	if err != nil {
		return nil, fmt.Errorf("failed to Marshal oldData for svc %s/%s: %v", oldSvc.Namespace, oldSvc.Name, err)
	}

	newData, err := json.Marshal(newSvc)
	if err != nil {
		return nil, fmt.Errorf("failed to Marshal newData for svc %s/%s: %v", newSvc.Namespace, newSvc.Name, err)
	}

	patchBytes, err := strategicpatch.CreateTwoWayMergePatch(oldData, newData, v1.Service{})
	if err != nil {
		return nil, fmt.Errorf("failed to CreateTwoWayMergePatch for svc %s/%s: %v", oldSvc.Namespace, oldSvc.Name, err)
	}
	return patchBytes, nil

}
