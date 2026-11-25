/*
Copyright 2015 The Kubernetes Authors.

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

/*
该浸泡测试在每个节点上放置指定数量的 Pod，
然后通过一个 service 反复向运行在这些 Pod 上的
服务发送查询。
*/

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	"k8s.io/kubernetes/test/e2e/framework/service"
	imageutils "k8s.io/kubernetes/test/utils/image"

	"k8s.io/klog/v2"
)

var (
	queriesAverage = flag.Int("queries", 100, "Number of hostname queries to make in each iteration per pod on average")
	podsPerNode    = flag.Int("pods_per_node", 1, "Number of serve_hostname pods per node")
	upTo           = flag.Int("up_to", 1, "Number of iterations or -1 for no limit")
	maxPar         = flag.Int("max_par", 500, "Maximum number of queries in flight")
	gke            = flag.String("gke_context", "", "Target GKE cluster with context gke_{project}_{zone}_{cluster-name}")
)

const (
	deleteTimeout          = 2 * time.Minute
	endpointTimeout        = 5 * time.Minute
	nodeListTimeout        = 2 * time.Minute
	podCreateTimeout       = 2 * time.Minute
	podStartTimeout        = 30 * time.Minute
	serviceCreateTimeout   = 2 * time.Minute
	namespaceDeleteTimeout = 5 * time.Minute
)

func main() {
	flag.Parse()

	klog.Infof("Starting serve_hostnames soak test with queries=%d and podsPerNode=%d upTo=%d",
		*queriesAverage, *podsPerNode, *upTo)

	var spec string
	if *gke != "" {
		spec = filepath.Join(os.Getenv("HOME"), ".config", "gcloud", "kubernetes", "kubeconfig")
	} else {
		spec = filepath.Join(os.Getenv("HOME"), ".kube", "config")
	}
	settings, err := clientcmd.LoadFromFile(spec)
	if err != nil {
		klog.Fatalf("Error loading configuration: %v", err.Error())
	}
	if *gke != "" {
		settings.CurrentContext = *gke
	}
	config, err := clientcmd.NewDefaultClientConfig(*settings, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		klog.Fatalf("Failed to construct config: %v", err)
	}

	client, err := clientset.NewForConfig(config)
	if err != nil {
		klog.Fatalf("Failed to make client: %v", err)
	}

	var nodes *v1.NodeList
	for start := time.Now(); time.Since(start) < nodeListTimeout; time.Sleep(2 * time.Second) {
		nodes, err = client.CoreV1().Nodes().List(context.TODO(), metav1.ListOptions{})
		if err == nil {
			break
		}
		klog.Warningf("Failed to list nodes: %v", err)
	}
	if err != nil {
		klog.Fatalf("Giving up trying to list nodes: %v", err)
	}

	if len(nodes.Items) == 0 {
		klog.Fatalf("Failed to find any nodes.")
	}

	klog.Infof("Found %d nodes on this cluster:", len(nodes.Items))
	for i, node := range nodes.Items {
		klog.Infof("%d: %s", i, node.Name)
	}

	queries := *queriesAverage * len(nodes.Items) * *podsPerNode

	// 创建命名空间
	got, err := client.CoreV1().Namespaces().Create(context.TODO(), &v1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "serve-hostnames-"}}, metav1.CreateOptions{})
	if err != nil {
		klog.Fatalf("Failed to create namespace: %v", err)
	}
	ns := got.Name
	defer func(ns string) {
		if err := client.CoreV1().Namespaces().Delete(context.TODO(), ns, metav1.DeleteOptions{}); err != nil {
			klog.Warningf("Failed to delete namespace %s: %v", ns, err)
		} else {
			// 等待命名空间消失
			for range int(namespaceDeleteTimeout / time.Second) {
				if _, err := client.CoreV1().Namespaces().Get(context.TODO(), ns, metav1.GetOptions{}); err != nil {
					if apierrors.IsNotFound(err) {
						return
					}
				}
				time.Sleep(time.Second)
			}
		}
	}(ns)
	klog.Infof("Created namespace %s", ns)

	// 为这些 Pod 创建一个 service。
	klog.Infof("Creating service %s/serve-hostnames", ns)
	// 多次尝试创建 service。
	var svc *v1.Service
	for start := time.Now(); time.Since(start) < serviceCreateTimeout; time.Sleep(2 * time.Second) {
		t := time.Now()
		svc, err = client.CoreV1().Services(ns).Create(context.TODO(), &v1.Service{
			ObjectMeta: metav1.ObjectMeta{
				Name: "serve-hostnames",
				Labels: map[string]string{
					"name": "serve-hostname",
				},
			},
			Spec: v1.ServiceSpec{
				Ports: []v1.ServicePort{{
					Protocol:   "TCP",
					Port:       9376,
					TargetPort: intstr.FromInt32(9376),
				}},
				Selector: map[string]string{
					"name": "serve-hostname",
				},
			},
		}, metav1.CreateOptions{})
		klog.V(4).Infof("Service create %s/server-hostnames took %v", ns, time.Since(t))
		if err == nil {
			break
		}
		klog.Warningf("After %v failed to create service %s/serve-hostnames: %v", time.Since(start), ns, err)
	}
	if err != nil {
		klog.Warningf("Unable to create service %s/%s: %v", ns, svc.Name, err)
		return
	}
	// 清理 service
	defer func() {
		klog.Infof("Cleaning up service %s/serve-hostnames", ns)
		// 多次尝试删除 service。
		for start := time.Now(); time.Since(start) < deleteTimeout; time.Sleep(1 * time.Second) {
			if err := client.CoreV1().Services(ns).Delete(context.TODO(), svc.Name, metav1.DeleteOptions{}); err == nil {
				return
			}
			klog.Warningf("After %v unable to delete service %s/%s: %v", time.Since(start), ns, svc.Name, err)
		}
	}()

	// 在每个节点上放置 serve-hostname Pod。
	podNames := []string{}
	for i, node := range nodes.Items {
		for j := 0; j < *podsPerNode; j++ {
			podName := fmt.Sprintf("serve-hostname-%d-%d", i, j)
			podNames = append(podNames, podName)
			// 多次尝试
			for start := time.Now(); time.Since(start) < podCreateTimeout; time.Sleep(2 * time.Second) {
				klog.Infof("Creating pod %s/%s on node %s", ns, podName, node.Name)
				t := time.Now()
				_, err = client.CoreV1().Pods(ns).Create(context.TODO(), &v1.Pod{
					ObjectMeta: metav1.ObjectMeta{
						Name: podName,
						Labels: map[string]string{
							"name": "serve-hostname",
						},
					},
					Spec: v1.PodSpec{
						Containers: []v1.Container{
							{
								Name:  "serve-hostname",
								Image: imageutils.GetE2EImage(imageutils.Agnhost),
								Ports: []v1.ContainerPort{{ContainerPort: 9376}},
							},
						},
						NodeName: node.Name,
					},
				}, metav1.CreateOptions{})
				klog.V(4).Infof("Pod create %s/%s request took %v", ns, podName, time.Since(t))
				if err == nil {
					break
				}
				klog.Warningf("After %s failed to create pod %s/%s: %v", time.Since(start), ns, podName, err)
			}
			if err != nil {
				klog.Warningf("Failed to create pod %s/%s: %v", ns, podName, err)
				return
			}
		}
	}
	// 清理 Pod
	defer func() {
		klog.Info("Cleaning up pods")
		// 多次尝试删除 Pod。
		for _, podName := range podNames {
			for start := time.Now(); time.Since(start) < deleteTimeout; time.Sleep(1 * time.Second) {
				if err = client.CoreV1().Pods(ns).Delete(context.TODO(), podName, metav1.DeleteOptions{}); err == nil {
					break
				}
				klog.Warningf("After %v failed to delete pod %s/%s: %v", time.Since(start), ns, podName, err)
			}
		}
	}()

	klog.Info("Waiting for the serve-hostname pods to be ready")
	for _, podName := range podNames {
		var pod *v1.Pod
		for start := time.Now(); time.Since(start) < podStartTimeout; time.Sleep(5 * time.Second) {
			pod, err = client.CoreV1().Pods(ns).Get(context.TODO(), podName, metav1.GetOptions{})
			if err != nil {
				klog.Warningf("Get pod %s/%s failed, ignoring for %v: %v", ns, podName, err, podStartTimeout)
				continue
			}
			if pod.Status.Phase == v1.PodRunning {
				break
			}
		}
		if pod.Status.Phase != v1.PodRunning {
			klog.Warningf("Gave up waiting on pod %s/%s to be running (saw %v)", ns, podName, pod.Status.Phase)
		} else {
			klog.Infof("%s/%s is running", ns, podName)
		}
	}

	rclient, err := restclient.RESTClientFor(config)
	if err != nil {
		klog.Warningf("Failed to build restclient: %v", err)
		return
	}
	proxyRequest, errProxy := service.GetServicesProxyRequest(client, rclient.Get())
	if errProxy != nil {
		klog.Warningf("Get services proxy request failed: %v", errProxy)
		return
	}

	// 等待 endpoints 传播完成。
	for start := time.Now(); time.Since(start) < endpointTimeout; time.Sleep(10 * time.Second) {
		hostname, err := proxyRequest.
			Namespace(ns).
			Name("serve-hostnames").
			DoRaw(context.TODO())
		if err != nil {
			klog.Infof("After %v while making a proxy call got error %v", time.Since(start), err)
			continue
		}
		var r metav1.Status
		if err := runtime.DecodeInto(legacyscheme.Codecs.UniversalDecoder(), hostname, &r); err != nil {
			break
		}
		if r.Status == metav1.StatusFailure {
			klog.Infof("After %v got status %v", time.Since(start), string(hostname))
			continue
		}
		break
	}

	// 反复发起请求。
	for iteration := 0; iteration != *upTo; iteration++ {
		responseChan := make(chan string, queries)
		// 使用大小为 *maxPar 的 channel 限制并发请求数，
		// 避免服务过载。
		inFlight := make(chan struct{}, *maxPar)
		start := time.Now()
		for q := range queries {
			go func(i int, query int) {
				inFlight <- struct{}{}
				t := time.Now()
				hostname, err := proxyRequest.
					Namespace(ns).
					Name("serve-hostnames").
					DoRaw(context.TODO())
				klog.V(4).Infof("Proxy call in namespace %s took %v", ns, time.Since(t))
				if err != nil {
					klog.Warningf("Call failed during iteration %d query %d : %v", i, query, err)
					// 若查询失败，返回一个以不能作为主机名一部分的字符
					// 开头的字符串。
					responseChan <- fmt.Sprintf("!failed in iteration %d to issue query %d: %v", i, query, err)
				} else {
					responseChan <- string(hostname)
				}
				<-inFlight
			}(iteration, q)
		}
		responses := make(map[string]int, *podsPerNode*len(nodes.Items))
		missing := 0
		for range queries {
			r := <-responseChan
			klog.V(4).Infof("Got response from %s", r)
			responses[r]++
			// 若返回的主机名以 '!' 开头，则表示
			// 这是一个错误响应。
			if len(r) > 0 && r[0] == '!' {
				klog.V(3).Infof("Got response %s", r)
				missing++
			}
		}
		if missing > 0 {
			klog.Warningf("Missing %d responses out of %d", missing, queries)
		}
		// 报告所有未响应的节点。
		for n, node := range nodes.Items {
			for i := 0; i < *podsPerNode; i++ {
				name := fmt.Sprintf("serve-hostname-%d-%d", n, i)
				if _, ok := responses[name]; !ok {
					klog.Warningf("No response from pod %s on node %s at iteration %d", name, node.Name, iteration)
				}
			}
		}
		klog.Infof("Iteration %d took %v for %d queries (%.2f QPS) with %d missing",
			iteration, time.Since(start), queries-missing, float64(queries-missing)/time.Since(start).Seconds(), missing)
	}
}
