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

package network

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/onsi/ginkgo/v2"
	v1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	"k8s.io/kubernetes/test/e2e/network/common"
	admissionapi "k8s.io/pod-security-admission/api"
)

var _ = common.SIGDescribe("EndpointSliceMirroring", func() {
	f := framework.NewDefaultFramework("endpointslicemirroring")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	var cs clientset.Interface

	ginkgo.BeforeEach(func() {
		cs = f.ClientSet
	})

	/*
		Release: v1.21
		Testname: EndpointSlice Mirroring
		Description: /apis discovery 文档中 MUST 存在 discovery.k8s.io API 组。
		/apis/discovery.k8s.io discovery 文档中 MUST 存在 discovery.k8s.io/v1 API 组/版本。
		/apis/discovery.k8s.io/v1 discovery 文档中 MUST 存在 endpointslices 资源。
		endpointslices 镜像机制必须镜像 endpoint 的创建、更新和删除操作。
	*/
	framework.ConformanceIt("should mirror a custom Endpoints resource through create update and delete", func(ctx context.Context) {
		svc := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: "example-custom-endpoints",
			},
			Spec: v1.ServiceSpec{
				Ports: []v1.ServicePort{{
					Name:     "example",
					Port:     80,
					Protocol: v1.ProtocolTCP,
				}},
			},
		}
		svc, err := cs.CoreV1().Services(f.Namespace.Name).Create(ctx, svc, metav1.CreateOptions{})
		framework.ExpectNoError(err, "error creating Service")

		endpoints := &v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Name: svc.Name,
			},
			Subsets: []v1.EndpointSubset{{
				Ports: []v1.EndpointPort{{
					Port: 80,
				}},
				Addresses: []v1.EndpointAddress{{
					IP: "10.1.2.3",
				}},
			}},
		}

		ginkgo.By("mirroring a new custom Endpoint", func() {
			_, err := cs.CoreV1().Endpoints(f.Namespace.Name).Create(ctx, endpoints, metav1.CreateOptions{})
			framework.ExpectNoError(err, "Unexpected error creating Endpoints")

			if err := wait.PollImmediate(2*time.Second, 12*time.Second, func() (bool, error) {
				esList, err := cs.DiscoveryV1().EndpointSlices(f.Namespace.Name).List(ctx, metav1.ListOptions{
					LabelSelector: discoveryv1.LabelServiceName + "=" + svc.Name,
				})
				if err != nil {
					framework.Logf("Error listing EndpointSlices: %v", err)
					return false, nil
				}
				if len(esList.Items) == 0 {
					framework.Logf("Waiting for at least 1 EndpointSlice to exist, got %d", len(esList.Items))
					return false, nil
				}

				// 由于 informer 缓存，控制器若在同步期间没看到已创建的第一个 EndpointSlice，
				// 可能会再创建一个。不过创建出的所有 EndpointSlice 都应有效。
				for _, epSlice := range esList.Items {
					if len(epSlice.Ports) != 1 {
						return false, fmt.Errorf("Expected EndpointSlice to have 1 Port, got %d", len(epSlice.Ports))
					}
					port := epSlice.Ports[0]
					if *port.Port != int32(80) {
						return false, fmt.Errorf("Expected port to be 80, got %d", *port.Port)
					}
					if len(epSlice.Endpoints) != 1 {
						return false, fmt.Errorf("Expected EndpointSlice to have 1 endpoints, got %d", len(epSlice.Endpoints))
					}
					endpoint := epSlice.Endpoints[0]
					if len(endpoint.Addresses) != 1 {
						return false, fmt.Errorf("Expected EndpointSlice endpoint to have 1 address, got %d", len(endpoint.Addresses))
					}
					address := endpoint.Addresses[0]
					if address != "10.1.2.3" {
						return false, fmt.Errorf("Expected EndpointSlice to have 10.1.2.3 as address, got %s", address)
					}
				}

				return true, nil
			}); err != nil {
				framework.Failf("Did not find matching EndpointSlice for %s/%s: %s", svc.Namespace, svc.Name, err)
			}
		})

		ginkgo.By("mirroring an update to a custom Endpoint", func() {
			endpoints.Subsets[0].Addresses = []v1.EndpointAddress{{
				IP: "10.2.3.4",
			}}
			_, err := cs.CoreV1().Endpoints(f.Namespace.Name).Update(ctx, endpoints, metav1.UpdateOptions{})
			framework.ExpectNoError(err, "Unexpected error updating Endpoints")

			// 期望镜像生成的 EndpointSlice 资源被更新。
			if err := wait.PollImmediate(2*time.Second, 12*time.Second, func() (bool, error) {
				esList, err := cs.DiscoveryV1().EndpointSlices(f.Namespace.Name).List(ctx, metav1.ListOptions{
					LabelSelector: discoveryv1.LabelServiceName + "=" + svc.Name,
				})
				if err != nil {
					return false, err
				}
				if len(esList.Items) != 1 {
					framework.Logf("Waiting for 1 EndpointSlice to exist, got %d", len(esList.Items))
					return false, nil
				}
				epSlice := esList.Items[0]
				if len(epSlice.Ports) != 1 {
					framework.Logf("Expected EndpointSlice to have 1 Port, got %d", len(epSlice.Ports))
					return false, nil
				}
				port := epSlice.Ports[0]
				if *port.Port != int32(80) {
					framework.Logf("Expected port to be 80, got %d", *port.Port)
					return false, nil
				}
				if len(epSlice.Endpoints) != 1 {
					framework.Logf("Expected EndpointSlice to have 1 endpoints, got %d", len(epSlice.Endpoints))
					return false, nil
				}
				endpoint := epSlice.Endpoints[0]
				if len(endpoint.Addresses) != 1 {
					framework.Logf("Expected EndpointSlice endpoint to have 1 address, got %d", len(endpoint.Addresses))
					return false, nil
				}
				address := endpoint.Addresses[0]
				if address != "10.2.3.4" {
					framework.Logf("Expected EndpointSlice to have 10.2.3.4 as address, got %s", address)
					return false, nil
				}

				return true, nil
			}); err != nil {
				framework.Failf("Did not find matching EndpointSlice for %s/%s: %s", svc.Namespace, svc.Name, err)
			}
		})

		ginkgo.By("mirroring deletion of a custom Endpoint", func() {
			err := cs.CoreV1().Endpoints(f.Namespace.Name).Delete(ctx, endpoints.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Unexpected error deleting Endpoints")

			// 期望镜像生成的 EndpointSlice 资源被更新。
			if err := wait.PollImmediate(2*time.Second, 12*time.Second, func() (bool, error) {
				esList, err := cs.DiscoveryV1().EndpointSlices(f.Namespace.Name).List(ctx, metav1.ListOptions{
					LabelSelector: discoveryv1.LabelServiceName + "=" + svc.Name,
				})
				if err != nil {
					return false, err
				}
				if len(esList.Items) != 0 {
					framework.Logf("Waiting for 0 EndpointSlices to exist, got %d", len(esList.Items))
					return false, nil
				}

				return true, nil
			}); err != nil {
				framework.Failf("Did not find matching EndpointSlice for %s/%s: %s", svc.Namespace, svc.Name, err)
			}
		})
	})

	ginkgo.It("should mirror a custom Endpoint with multiple subsets and same IP address", func(ctx context.Context) {
		ns := f.Namespace.Name
		svc := &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: "example-custom-endpoints",
			},
			Spec: v1.ServiceSpec{
				Ports: []v1.ServicePort{
					{
						Name:     "port80",
						Port:     80,
						Protocol: v1.ProtocolTCP,
					},
					{
						Name:     "port81",
						Port:     81,
						Protocol: v1.ProtocolTCP,
					},
				},
			},
		}
		svc, err := cs.CoreV1().Services(f.Namespace.Name).Create(ctx, svc, metav1.CreateOptions{})
		framework.ExpectNoError(err, "error creating Service")

		// 在另一节点上为该 Service 添加后端 Pod
		port8080 := []v1.ContainerPort{
			{
				ContainerPort: 8090,
				Protocol:      v1.ProtocolTCP,
			},
		}
		port9090 := []v1.ContainerPort{
			{
				ContainerPort: 9090,
				Protocol:      v1.ProtocolTCP,
			},
		}

		serverPod := e2epod.NewAgnhostPodFromContainers(
			"", "pod-handle-http-request", nil,
			e2epod.NewAgnhostContainer("container-handle-8090-request", nil, port8080, "netexec", "--http-port", "8090", "--udp-port", "-1"),
			e2epod.NewAgnhostContainer("container-handle-9090-request", nil, port9090, "netexec", "--http-port", "9090", "--udp-port", "-1"),
		)

		pod := e2epod.NewPodClient(f).CreateSync(ctx, serverPod)

		if pod.Status.PodIP == "" {
			framework.Failf("PodIP not assigned for pod %s", pod.Name)
		}

		// 创建自定义 endpoints
		endpoints := &v1.Endpoints{
			ObjectMeta: metav1.ObjectMeta{
				Name: svc.Name,
			},
			Subsets: []v1.EndpointSubset{
				{
					Ports: []v1.EndpointPort{{
						Name: "port80",
						Port: 8090,
					}},
					Addresses: []v1.EndpointAddress{{
						IP: pod.Status.PodIP,
					}},
				},
				{
					Ports: []v1.EndpointPort{{
						Name: "port81",
						Port: 9090,
					}},
					Addresses: []v1.EndpointAddress{{
						IP: pod.Status.PodIP,
					}},
				},
			},
		}

		ginkgo.By("mirroring a new custom Endpoint", func() {
			_, err := cs.CoreV1().Endpoints(f.Namespace.Name).Create(context.TODO(), endpoints, metav1.CreateOptions{})
			framework.ExpectNoError(err, "Unexpected error creating Endpoints")

			if err := wait.PollImmediate(2*time.Second, 12*time.Second, func() (bool, error) {
				esList, err := cs.DiscoveryV1().EndpointSlices(f.Namespace.Name).List(context.TODO(), metav1.ListOptions{
					LabelSelector: discoveryv1.LabelServiceName + "=" + svc.Name,
				})
				if err != nil {
					framework.Logf("Error listing EndpointSlices: %v", err)
					return false, nil
				}
				if len(esList.Items) == 0 {
					framework.Logf("Waiting for at least 1 EndpointSlice to exist, got %d", len(esList.Items))
					return false, nil
				}
				return true, nil
			}); err != nil {
				framework.Failf("Did not find matching EndpointSlice for %s/%s: %s", svc.Namespace, svc.Name, err)
			}
		})

		// 必须能连接到该 Service
		ginkgo.By("Creating a pause pods that will try to connect to the webservers")
		pausePod := e2epod.NewAgnhostPod(ns, "pause-pod-0", nil, nil, nil)
		e2epod.NewPodClient(f).CreateSync(ctx, pausePod)
		dest1 := net.JoinHostPort(svc.Spec.ClusterIP, "80")
		dest2 := net.JoinHostPort(svc.Spec.ClusterIP, "81")
		execHostnameTest(*pausePod, dest1, pod.Name)
		execHostnameTest(*pausePod, dest2, pod.Name)

		// 删除自定义 endpoints，并等待对应的 EndpointSlice 也被删除
		ginkgo.By("mirroring deletion of a custom Endpoint", func() {
			err := cs.CoreV1().Endpoints(f.Namespace.Name).Delete(context.TODO(), endpoints.Name, metav1.DeleteOptions{})
			framework.ExpectNoError(err, "Unexpected error deleting Endpoints")

			// 期望镜像生成的 EndpointSlice 资源被更新。
			if err := wait.PollImmediate(2*time.Second, 12*time.Second, func() (bool, error) {
				esList, err := cs.DiscoveryV1().EndpointSlices(f.Namespace.Name).List(context.TODO(), metav1.ListOptions{
					LabelSelector: discoveryv1.LabelServiceName + "=" + svc.Name,
				})
				if err != nil {
					return false, err
				}
				if len(esList.Items) != 0 {
					framework.Logf("Waiting for 0 EndpointSlices to exist, got %d", len(esList.Items))
					return false, nil
				}

				return true, nil
			}); err != nil {
				framework.Failf("Did not find matching EndpointSlice for %s/%s: %s", svc.Namespace, svc.Name, err)
			}
		})
	})
})
