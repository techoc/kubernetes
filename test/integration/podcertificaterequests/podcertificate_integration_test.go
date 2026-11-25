/*
Copyright 2025 The Kubernetes Authors.

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

package podcertificaterequests

import (
	"testing"

	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/x509"
	"fmt"
	"time"

	certsv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	rbacv1 "k8s.io/api/rbac/v1"
	k8serrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/klog/v2/ktesting"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/controller/certificates/cleaner"
	"k8s.io/kubernetes/test/integration/authutil"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/hermeticpodcertificatesigner"
	"k8s.io/utils/clock"
)

func TestMain(m *testing.M) {
	framework.EtcdMain(m.Run)
}

func TestCleanerController(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 启动一个启用了 PodCertificateRequest
	// 特性的 apiserver。
	s := kubeapiservertesting.StartTestServerOrDie(
		t,
		kubeapiservertesting.NewDefaultTestServerOptions(),
		[]string{
			"--authorization-mode=Node,RBAC",
			"--feature-gates=PodCertificateRequest=true",
		},
		framework.SharedEtcd(),
	)
	defer s.TearDownFn()

	client := clientset.NewForConfigOrDie(s.ClientConfig)
	informers := informers.NewSharedInformerFactory(clientset.NewForConfigOrDie(restclient.AddUserAgent(s.ClientConfig, "certificatesigningrequest-informers")), time.Second)

	// 注册 cleaner controller，并配置
	// 较短的超时时间。
	// PCR 应在 15 秒内被删除。
	cleanerClient, err := serviceAccountClient(s.ClientConfig, "kube-system", "podcertificaterequestcleaner")
	if err != nil {
		t.Fatalf("Unexpected error creating client that impersonates kube-system/podcertificaterequestcleaner: %v", err)
	}
	c := cleaner.NewPCRCleanerController(
		cleanerClient,
		informers.Certificates().V1().PodCertificateRequests(),
		clock.RealClock{},
		1*time.Second,
		1*time.Second,
	)
	go c.Run(ctx, 1)

	// 启动 controller 和 informer
	informers.Start(ctx.Done())

	// 创建一个节点
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
		Spec: corev1.NodeSpec{},
	}
	node, err = client.CoreV1().Nodes().Create(ctx, node, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating node1: %v", err)
	}

	// 创建一个 serviceaccount
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "sa1",
		},
	}
	sa, err = client.CoreV1().ServiceAccounts("default").Create(ctx, sa, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating sa1: %v", err)
	}

	// 创建一个 Pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod1",
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: sa.ObjectMeta.Name,
			NodeName:           node.ObjectMeta.Name,
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: "notarealimage",
				},
			},
		},
	}
	pod, err = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating pod1: %v", err)
	}

	// 创建一个模拟 node1 的 clientset
	node1Client, err := nodeClient(s.ClientConfig, "node1")
	if err != nil {
		t.Fatalf("Error in create clientset: %v", err)
	}

	// 让 node1 为 pod1 创建 PodCertificateRequest
	stubReq := mustMakeStubPKCS10Request(t)
	pcr := &certsv1.PodCertificateRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pcr1",
		},
		Spec: certsv1.PodCertificateRequestSpec{
			SignerName:                "kubernetes.io/foo",
			PodName:                   pod.ObjectMeta.Name,
			PodUID:                    pod.ObjectMeta.UID,
			ServiceAccountName:        sa.ObjectMeta.Name,
			ServiceAccountUID:         sa.ObjectMeta.UID,
			NodeName:                  types.NodeName(node.ObjectMeta.Name),
			NodeUID:                   node.ObjectMeta.UID,
			StubPKCS10Request:         stubReq,
			UnverifiedUserAnnotations: map[string]string{hermeticpodcertificatesigner.SpiffePathKey: "pod1"},
		},
	}
	pcr, err = node1Client.CertificatesV1().PodCertificateRequests(pcr.ObjectMeta.Namespace).Create(ctx, pcr, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating PodCertificateRequest: %v", err)
	}
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err := client.CertificatesV1().PodCertificateRequests(pcr.ObjectMeta.Namespace).Get(ctx, pcr.ObjectMeta.Name, metav1.GetOptions{})
		if k8serrors.IsNotFound(err) {
			return true, nil
		} else if err != nil {
			return false, err
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("Unexpected error after waiting for PodCertificateRequest to be deleted: %v", err)
	}

	// TODO(KEP-4317): 在 beta 阶段，通过审计日志
	// 确认删除操作确实由
	// cleaner controller 发起。
}

func TestNodeRestriction(t *testing.T) {
	// 搭建一个包含两个节点的环境，Pod 运行在
	// node1 上。node2 不能创建引用 node1 上 Pod 的
	// PodCertificateRequest。
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 启动一个启用了 PodCertificateRequest 特性的 apiserver。
	s := kubeapiservertesting.StartTestServerOrDie(
		t,
		kubeapiservertesting.NewDefaultTestServerOptions(),
		[]string{
			"--authorization-mode=Node,RBAC",
			"--enable-admission-plugins=NodeRestriction",
			"--feature-gates=PodCertificateRequest=true",
		},
		framework.SharedEtcd(),
	)
	defer s.TearDownFn()

	client := clientset.NewForConfigOrDie(s.ClientConfig)

	// 创建 node1 和 node2
	node1 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
		Spec: corev1.NodeSpec{},
	}
	node1, err := client.CoreV1().Nodes().Create(ctx, node1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating node1: %v", err)
	}
	node2 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node2",
		},
		Spec: corev1.NodeSpec{},
	}
	node2, err = client.CoreV1().Nodes().Create(ctx, node2, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating node1: %v", err)
	}

	// 为节点创建 clientset
	node1Client, err := nodeClient(s.ClientConfig, "node1")
	if err != nil {
		t.Fatalf("Error in create clientset: %v", err)
	}
	node2Client, err := nodeClient(s.ClientConfig, "node2")
	if err != nil {
		t.Fatalf("Error in create clientset: %v", err)
	}

	// 创建一个 serviceaccount
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "sa1",
		},
	}
	sa, err = client.CoreV1().ServiceAccounts("default").Create(ctx, sa, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating sa1: %v", err)
	}

	// 创建一个 Pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod1",
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: sa.ObjectMeta.Name,
			NodeName:           node1.ObjectMeta.Name,
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: "notarealimage",
				},
			},
			Volumes: podCertificateVolume("kubernetes.io/foo"),
		},
	}
	pod, err = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating pod1: %v", err)
	}

	t.Run("node1 can create PCR for pod on node1", func(t *testing.T) {
		// 让 node2 为 pod1 创建 PodCertificateRequest
		stubReq := mustMakeStubPKCS10Request(t)
		pcr := &certsv1.PodCertificateRequest{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pcr1",
			},
			Spec: certsv1.PodCertificateRequestSpec{
				SignerName:                "kubernetes.io/foo",
				PodName:                   pod.ObjectMeta.Name,
				PodUID:                    pod.ObjectMeta.UID,
				ServiceAccountName:        sa.ObjectMeta.Name,
				ServiceAccountUID:         sa.ObjectMeta.UID,
				NodeName:                  types.NodeName(node1.ObjectMeta.Name),
				NodeUID:                   node1.ObjectMeta.UID,
				StubPKCS10Request:         stubReq,
				UnverifiedUserAnnotations: map[string]string{hermeticpodcertificatesigner.SpiffePathKey: "pod1"},
			},
		}

		// kube-apiserver 内部的 informer 延迟
		// 可能导致偶发错误。
		var lastErr string
		err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 15*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err = node1Client.CertificatesV1().PodCertificateRequests("default").Create(ctx, pcr, metav1.CreateOptions{})
			if err != nil {
				if errStr := err.Error(); errStr != lastErr {
					t.Logf("Create PodCertificateRequest default/pcr1 as node1 failed with error: %v, retrying...", err)
					lastErr = errStr
				}
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			t.Fatalf("PCR creation unexpectedly failed: %v", err)
		}
	})

	t.Run("node2 cannot create PCR for pod on node1", func(t *testing.T) {
		// 让 node2 为 pod1 创建 PodCertificateRequest
		stubReq := mustMakeStubPKCS10Request(t)
		pcr := &certsv1.PodCertificateRequest{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pcr1",
			},
			Spec: certsv1.PodCertificateRequestSpec{
				SignerName:                "kubernetes.io/foo",
				PodName:                   pod.ObjectMeta.Name,
				PodUID:                    pod.ObjectMeta.UID,
				ServiceAccountName:        sa.ObjectMeta.Name,
				ServiceAccountUID:         sa.ObjectMeta.UID,
				NodeName:                  types.NodeName(node1.ObjectMeta.Name),
				NodeUID:                   node1.ObjectMeta.UID,
				StubPKCS10Request:         stubReq,
				UnverifiedUserAnnotations: map[string]string{hermeticpodcertificatesigner.SpiffePathKey: "pod1"},
			},
		}

		// kube-apiserver 内部的 informer 延迟可能导致 noderestriction
		// admission 插件返回非 Forbidden 错误。这应该是暂时现象，
		// 因此等待一段时间，看是否能达到稳定的错误状态。
		var lastErr string
		err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 15*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err = node2Client.CertificatesV1().PodCertificateRequests("default").Create(ctx, pcr, metav1.CreateOptions{})
			if err == nil || k8serrors.IsForbidden(err) {
				return true, err
			}
			if errStr := err.Error(); errStr != lastErr {
				t.Logf("Create PodCertificateRequest default/pcr1 as node2 failed with non-Forbidden error: %v, retrying...", err)
				lastErr = errStr
			}
			return false, nil
		})
		if err == nil {
			t.Fatalf("PCR creation unexpectedly succeeded")
		} else if !k8serrors.IsForbidden(err) {
			t.Fatalf("PCR creation failed with unexpected error code (wanted Forbidden): %v", err)
		}
	})

	t.Run("node2 cannot create PCR for pod that doesn't exist", func(t *testing.T) {
		// 让 node2 为 pod1 创建 PodCertificateRequest
		stubReq := mustMakeStubPKCS10Request(t)
		pcr := &certsv1.PodCertificateRequest{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pcr1",
			},
			Spec: certsv1.PodCertificateRequestSpec{
				SignerName:                "kubernetes.io/foo",
				PodName:                   "dnepod",
				PodUID:                    "dnepoduid",
				ServiceAccountName:        sa.ObjectMeta.Name,
				ServiceAccountUID:         sa.ObjectMeta.UID,
				NodeName:                  types.NodeName(node2.ObjectMeta.Name),
				NodeUID:                   node2.ObjectMeta.UID,
				StubPKCS10Request:         stubReq,
				UnverifiedUserAnnotations: map[string]string{hermeticpodcertificatesigner.SpiffePathKey: "pod1"},
			},
		}

		// noderestriction admission 插件不会返回
		// Forbidden，因为这种情况总有可能是由 informer 延迟
		// 造成的。这里只需等待 15 秒，如果仍然收到错误，
		// 就认为它不可能是由 informer 延迟导致的。
		err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 15*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err = node2Client.CertificatesV1().PodCertificateRequests("default").Create(ctx, pcr, metav1.CreateOptions{})
			if err == nil {
				return true, err
			}
			return false, nil
		})
		if err == nil { // 等于 nil
			t.Fatalf("PCR creation unexpectedly succeeded")
		}

	})
}

func TestNodeAuthorization(t *testing.T) {
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 启动一个启用了 PodCertificateRequest 特性的 apiserver。
	s := kubeapiservertesting.StartTestServerOrDie(
		t,
		kubeapiservertesting.NewDefaultTestServerOptions(),
		[]string{
			"--authorization-mode=Node,RBAC",
			"--enable-admission-plugins=NodeRestriction",
			"--feature-gates=PodCertificateRequest=true",
		},
		framework.SharedEtcd(),
	)
	defer s.TearDownFn()

	client := clientset.NewForConfigOrDie(s.ClientConfig)

	// 创建 node1 和 node2
	node1 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
		Spec: corev1.NodeSpec{},
	}
	node1, err := client.CoreV1().Nodes().Create(ctx, node1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating node1: %v", err)
	}
	node2 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node2",
		},
		Spec: corev1.NodeSpec{},
	}
	_, err = client.CoreV1().Nodes().Create(ctx, node2, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating node1: %v", err)
	}

	// 创建一个 serviceaccount
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "sa1",
		},
	}
	sa, err = client.CoreV1().ServiceAccounts("default").Create(ctx, sa, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating sa1: %v", err)
	}

	// 创建一个 Pod
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod1",
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: sa.ObjectMeta.Name,
			NodeName:           node1.ObjectMeta.Name,
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: "notarealimage",
				},
			},
			Volumes: podCertificateVolume("kubernetes.io/foo"),
		},
	}
	pod, err = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating pod1: %v", err)
	}

	// 创建模拟这些节点的 clientset
	node1Client, err := nodeClient(s.ClientConfig, "node1")
	if err != nil {
		t.Fatalf("Error in create clientset: %v", err)
	}
	node2Client, err := nodeClient(s.ClientConfig, "node2")
	if err != nil {
		t.Fatalf("Error in create clientset: %v", err)
	}

	// 让 node1 为 pod1 创建 PodCertificateRequest
	stubReq := mustMakeStubPKCS10Request(t)
	pcr := &certsv1.PodCertificateRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pcr1",
		},
		Spec: certsv1.PodCertificateRequestSpec{
			SignerName:                "kubernetes.io/foo",
			PodName:                   pod.ObjectMeta.Name,
			PodUID:                    pod.ObjectMeta.UID,
			ServiceAccountName:        sa.ObjectMeta.Name,
			ServiceAccountUID:         sa.ObjectMeta.UID,
			NodeName:                  types.NodeName(node1.ObjectMeta.Name),
			NodeUID:                   node1.ObjectMeta.UID,
			StubPKCS10Request:         stubReq,
			UnverifiedUserAnnotations: map[string]string{hermeticpodcertificatesigner.SpiffePathKey: "pod1"},
		},
	}

	// 如果 noderestriction 逻辑中存在 informer 延迟，创建 PCR 可能会失败。
	// 轮询直到成功。
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err = node1Client.CertificatesV1().PodCertificateRequests("default").Create(ctx, pcr, metav1.CreateOptions{})
		if err != nil {
			t.Logf("Create PodCertificateRequest default/pcr1 as node1 failed with error: %v, retrying...", err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("Unexpected error creating PodCertificateRequest: %v", err)
	}

	t.Run("node1 can directly get pcr1", func(t *testing.T) {
		// 如果 node authorizer 图填充存在延迟，获取 PCR 可能会失败。
		// 轮询直到成功。
		err := wait.PollUntilContextTimeout(ctx, 1*time.Second, 15*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := node1Client.CertificatesV1().PodCertificateRequests("default").Get(ctx, "pcr1", metav1.GetOptions{})
			if err != nil {
				t.Logf("Get PodCertificateRequest default/pcr1 as node1 failed with error: %v, retrying...", err)
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			t.Fatalf("Unexpected error listing PodCertificateRequests as node1: %v", err)
		}
	})

	t.Run("node1 can see pcr1 when listing", func(t *testing.T) {
		pcrList, err := node1Client.CertificatesV1().PodCertificateRequests("default").List(ctx, metav1.ListOptions{
			FieldSelector: "spec.nodeName=node1",
		})
		if err != nil {
			t.Fatalf("Unexpected error listing PodCertificateRequests as node1: %v", err)
		}

		if len(pcrList.Items) != 1 {
			t.Fatalf("Unexpected list length returned when node1 lists PodCertificateRequests; got %d, want 1", len(pcrList.Items))
		}

		if pcrList.Items[0].ObjectMeta.Name != "pcr1" {
			t.Fatalf("Unexpected list contents returned when node1 lists PodCertificateRequests; got %q want %q", pcrList.Items[0].ObjectMeta.Name, "pcr1")
		}
	})

	t.Run("node2 cannot list with field selector for node1", func(t *testing.T) {
		_, err := node2Client.CertificatesV1().PodCertificateRequests("default").List(ctx, metav1.ListOptions{
			FieldSelector: "spec.nodeName=node1",
		})
		if err == nil {
			t.Fatalf("Listing PodCertificateRequests unexpectedly succeeded")
		} else if !k8serrors.IsForbidden(err) {
			t.Fatalf("Listing PodCertificateRequests failed with unexpected error (want Forbidden): %v", err)
		}
	})

	t.Run("node2 cannot directly get pcr1", func(t *testing.T) {
		_, err := node2Client.CertificatesV1().PodCertificateRequests("default").Get(ctx, "pcr1", metav1.GetOptions{})
		if err == nil {
			t.Fatalf("Getting pcr1 unexpectedly succeeded")
		} else if !k8serrors.IsForbidden(err) {
			t.Fatalf("Getting pcr1 failed with unexpected error (want Forbidden): %v", err)
		}
	})

	t.Run("node2 cannot see pcr1 when listing", func(t *testing.T) {
		pcrList, err := node2Client.CertificatesV1().PodCertificateRequests("default").List(ctx, metav1.ListOptions{
			FieldSelector: "spec.nodeName=node2",
		})
		if err != nil {
			t.Fatalf("Unexpected error listing PodCertificateRequests as node2: %v", err)
		}

		if len(pcrList.Items) != 0 {
			t.Fatalf("Unexpected list length returned when node2 lists PodCertificateRequests; got %d, want 0", len(pcrList.Items))
		}
	})
}

func TestNodeAuthorizerNamespaceNameConfusion(t *testing.T) {
	// 这是一个针对性测试用例，确保 node authorizer
	// 不会混淆 namespace 和 name。

	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 启动一个启用了 PodCertificateRequest 特性的 apiserver。
	s := kubeapiservertesting.StartTestServerOrDie(
		t,
		kubeapiservertesting.NewDefaultTestServerOptions(),
		[]string{
			"--authorization-mode=Node,RBAC",
			"--enable-admission-plugins=NodeRestriction",
			"--feature-gates=PodCertificateRequest=true",
		},
		framework.SharedEtcd(),
	)
	defer s.TearDownFn()

	client := clientset.NewForConfigOrDie(s.ClientConfig)

	// 创建 node1 和 node2
	node1 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
		Spec: corev1.NodeSpec{},
	}
	node1, err := client.CoreV1().Nodes().Create(ctx, node1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating node1: %v", err)
	}
	node2 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node2",
		},
		Spec: corev1.NodeSpec{},
	}
	node2, err = client.CoreV1().Nodes().Create(ctx, node2, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating node1: %v", err)
	}

	// 创建命名空间 "foo" 和 "bar"
	_, err = client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "foo",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating namespace foo: %v", err)
	}
	_, err = client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "bar",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating namespace nar: %v", err)
	}

	// 在每个命名空间中创建一个 serviceaccount
	saFoo := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "foo",
			Name:      "foo",
		},
	}
	saFoo, err = client.CoreV1().ServiceAccounts("foo").Create(ctx, saFoo, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating serviceaccount foo: %v", err)
	}
	saBar := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "bar",
			Name:      "bar",
		},
	}
	saBar, err = client.CoreV1().ServiceAccounts("bar").Create(ctx, saBar, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating serviceaccount bar: %v", err)
	}

	// 在命名空间 "bar" 中创建一个名为 "foo" 的 Pod，反之亦然
	podBarFoo := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "bar",
			Name:      "foo",
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: saBar.ObjectMeta.Name,
			NodeName:           node1.ObjectMeta.Name,
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: "notarealimage",
				},
			},
			Volumes: podCertificateVolume("kubernetes.io/foo"),
		},
	}
	podBarFoo, err = client.CoreV1().Pods("bar").Create(ctx, podBarFoo, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating pod bar/foo: %v", err)
	}
	podFooBar := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "foo",
			Name:      "bar",
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: saFoo.ObjectMeta.Name,
			NodeName:           node2.ObjectMeta.Name,
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: "notarealimage",
				},
			},
			Volumes: podCertificateVolume("kubernetes.io/foo"),
		},
	}
	podFooBar, err = client.CoreV1().Pods("foo").Create(ctx, podFooBar, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating pod foo/bar: %v", err)
	}

	// 创建模拟这些节点的 clientset
	node1Client, err := nodeClient(s.ClientConfig, "node1")
	if err != nil {
		t.Fatalf("Error in create clientset: %v", err)
	}
	node2Client, err := nodeClient(s.ClientConfig, "node2")
	if err != nil {
		t.Fatalf("Error in create clientset: %v", err)
	}

	// 让 node1 为 bar/foo 创建 PodCertificateRequest
	stubReq := mustMakeStubPKCS10Request(t)
	pcrBarFoo := &certsv1.PodCertificateRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "bar",
			Name:      "foo",
		},
		Spec: certsv1.PodCertificateRequestSpec{
			SignerName:                "kubernetes.io/foo",
			PodName:                   podBarFoo.ObjectMeta.Name,
			PodUID:                    podBarFoo.ObjectMeta.UID,
			ServiceAccountName:        saBar.ObjectMeta.Name,
			ServiceAccountUID:         saBar.ObjectMeta.UID,
			NodeName:                  types.NodeName(node1.ObjectMeta.Name),
			NodeUID:                   node1.ObjectMeta.UID,
			StubPKCS10Request:         stubReq,
			UnverifiedUserAnnotations: map[string]string{hermeticpodcertificatesigner.SpiffePathKey: "foo"},
		},
	}
	// 如果 noderestriction 逻辑中存在 informer 延迟，创建 PCR 可能会失败。
	// 轮询直到成功。
	var lastErr string
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err = node1Client.CertificatesV1().PodCertificateRequests("bar").Create(ctx, pcrBarFoo, metav1.CreateOptions{})
		if err != nil {
			if errStr := err.Error(); errStr != lastErr {
				t.Logf("Create PodCertificateRequest bar/foo as node1 failed with error: %v, retrying...", err)
				lastErr = errStr
			}
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("Unexpected error creating pcr bar/foo: %v", err)
	}

	// 让 node2 为 foo/bar 创建 PodCertificateRequest
	stubReqFooBar := mustMakeStubPKCS10Request(t)
	pcrFooBar := &certsv1.PodCertificateRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "foo",
			Name:      "bar",
		},
		Spec: certsv1.PodCertificateRequestSpec{
			SignerName:                "kubernetes.io/foo",
			PodName:                   podFooBar.ObjectMeta.Name,
			PodUID:                    podFooBar.ObjectMeta.UID,
			ServiceAccountName:        saFoo.ObjectMeta.Name,
			ServiceAccountUID:         saFoo.ObjectMeta.UID,
			NodeName:                  types.NodeName(node2.ObjectMeta.Name),
			NodeUID:                   node2.ObjectMeta.UID,
			StubPKCS10Request:         stubReqFooBar,
			UnverifiedUserAnnotations: map[string]string{hermeticpodcertificatesigner.SpiffePathKey: "bar"},
		},
	}
	// 如果 noderestriction 逻辑中存在 informer 延迟，创建 PCR 可能会失败。
	// 轮询直到成功。
	lastErr = ""
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 15*time.Second, true, func(ctx context.Context) (bool, error) {
		_, err = node2Client.CertificatesV1().PodCertificateRequests("foo").Create(ctx, pcrFooBar, metav1.CreateOptions{})
		if err != nil {
			if errStr := err.Error(); errStr != lastErr {
				t.Logf("Create PodCertificateRequest foo/bar as node2 failed with error: %v, retrying...", err)
				lastErr = errStr
			}
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("Unexpected error creating pcr foo/bar: %v", err)
	}

	t.Run("node1 can directly get bar/foo", func(t *testing.T) {
		// 如果 node authorizer 图填充存在延迟，获取 PCR 可能会失败。
		// 轮询直到成功。
		var lastErr string
		err := wait.PollUntilContextTimeout(ctx, 1*time.Second, 15*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := node1Client.CertificatesV1().PodCertificateRequests("bar").Get(ctx, "foo", metav1.GetOptions{})
			if err != nil {
				if errStr := err.Error(); errStr != lastErr {
					t.Logf("Get PodCertificateRequest bar/foo as node1 failed with error: %v, retrying...", err)
					lastErr = errStr
				}
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			t.Fatalf("Unexpected error getting bar/foo as node1: %v", err)
		}
	})

	t.Run("node2 can directly get foo/bar", func(t *testing.T) {
		// 如果 node authorizer 图填充存在延迟，获取 PCR 可能会失败。
		// 轮询直到成功。
		var lastErr string
		err := wait.PollUntilContextTimeout(ctx, 1*time.Second, 15*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := node2Client.CertificatesV1().PodCertificateRequests("foo").Get(ctx, "bar", metav1.GetOptions{})
			if err != nil {
				if errStr := err.Error(); errStr != lastErr {
					t.Logf("Get PodCertificateRequest foo/bar as node2 failed with error: %v, retrying...", err)
					lastErr = errStr
				}
				return false, nil
			}
			return true, nil
		})
		if err != nil {
			t.Fatalf("Unexpected error getting foo/bar as node2: %v", err)
		}
	})

	// 删除 bar/foo
	err = client.CertificatesV1().PodCertificateRequests("bar").Delete(ctx, "foo", metav1.DeleteOptions{})
	if err != nil {
		t.Fatalf("Unexpected error deleting pcr bar/foo: %v", err)
	}

	t.Run("node2 can still directly get foo/bar after bar/foo was deleted", func(t *testing.T) {
		_, err := node2Client.CertificatesV1().PodCertificateRequests("foo").Get(ctx, "bar", metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Unexpected error getting foo/bar as node2: %v", err)
		}
	})
}

func TestSignerNameAuthorization(t *testing.T) {
	// noderestriction admission 插件通常要求 Pod 为某个 signer 挂载
	// podCertificate projected volume，然后才允许其所在节点为该 signer
	// 创建 PodCertificateRequest。作为管理员覆盖手段，
	// 为 service account 在 signer name 上授予
	// "request-serviceaccounts-podcertificate-signer" verb 也可以
	// 授权该请求。本测试使用一个*没有*挂载 podCertificate volume
	// 的 Pod 来验证这条覆盖路径。
	_, ctx := ktesting.NewTestContext(t)
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	// 启动一个启用了 PodCertificateRequest 特性的 apiserver。
	s := kubeapiservertesting.StartTestServerOrDie(
		t,
		kubeapiservertesting.NewDefaultTestServerOptions(),
		[]string{
			"--authorization-mode=Node,RBAC",
			"--enable-admission-plugins=NodeRestriction",
			"--feature-gates=PodCertificateRequest=true",
		},
		framework.SharedEtcd(),
	)
	defer s.TearDownFn()

	client := clientset.NewForConfigOrDie(s.ClientConfig)

	// 创建 node1
	node1 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
		Spec: corev1.NodeSpec{},
	}
	node1, err := client.CoreV1().Nodes().Create(ctx, node1, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating node1: %v", err)
	}

	// 创建一个 serviceaccount
	sa := &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "sa1",
		},
	}
	sa, err = client.CoreV1().ServiceAccounts("default").Create(ctx, sa, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating sa1: %v", err)
	}

	// 创建一个*没有*挂载 podCertificate projected volume 的 Pod。
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "pod1",
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: sa.ObjectMeta.Name,
			NodeName:           node1.ObjectMeta.Name,
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: "notarealimage",
				},
			},
		},
	}
	pod, err = client.CoreV1().Pods("default").Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating pod1: %v", err)
	}

	// 创建一个模拟 node1 的 clientset
	node1Client, err := nodeClient(s.ClientConfig, "node1")
	if err != nil {
		t.Fatalf("Error in create clientset: %v", err)
	}

	csrData := mustMakeStubPKCS10Request(t)
	makePCR := func() *certsv1.PodCertificateRequest {
		return &certsv1.PodCertificateRequest{
			ObjectMeta: metav1.ObjectMeta{
				Namespace: "default",
				Name:      "pcr1",
			},
			Spec: certsv1.PodCertificateRequestSpec{
				SignerName:                "kubernetes.io/foo",
				PodName:                   pod.ObjectMeta.Name,
				PodUID:                    pod.ObjectMeta.UID,
				ServiceAccountName:        sa.ObjectMeta.Name,
				ServiceAccountUID:         sa.ObjectMeta.UID,
				NodeName:                  types.NodeName(node1.ObjectMeta.Name),
				NodeUID:                   node1.ObjectMeta.UID,
				StubPKCS10Request:         csrData,
				UnverifiedUserAnnotations: map[string]string{hermeticpodcertificatesigner.SpiffePathKey: "pod1"},
			},
		}
	}

	t.Run("node1 cannot create PCR before the signer is authorized", func(t *testing.T) {
		// kube-apiserver 内部的 informer 延迟可能导致 noderestriction admission
		// 插件返回非 Forbidden 错误。这应该是暂时现象，因此等待一段时间，
		// 看是否能达到稳定的 Forbidden 状态。
		var lastErr string
		err := wait.PollUntilContextTimeout(ctx, 1*time.Second, 15*time.Second, true, func(ctx context.Context) (bool, error) {
			_, err := node1Client.CertificatesV1().PodCertificateRequests("default").Create(ctx, makePCR(), metav1.CreateOptions{})
			if err == nil || k8serrors.IsForbidden(err) {
				return true, err
			}
			if errStr := err.Error(); errStr != lastErr {
				t.Logf("Create PodCertificateRequest default/pcr1 as node1 failed with non-Forbidden error: %v, retrying...", err)
				lastErr = errStr
			}
			return false, nil
		})
		if err == nil {
			t.Fatalf("PCR creation unexpectedly succeeded before the signer was authorized")
		} else if !k8serrors.IsForbidden(err) {
			t.Fatalf("PCR creation failed with unexpected error code (wanted Forbidden): %v", err)
		}
	})

	// 授予 node1 为 sa1 请求 kubernetes.io/foo signer 的能力。
	authutil.GrantUserAuthorization(t, ctx, client, "system:node:node1", rbacv1.PolicyRule{
		APIGroups:     []string{certsv1.GroupName},
		Resources:     []string{"kubernetes.io:foo"},
		Verbs:         []string{"request-serviceaccounts-podcertificate-signer"},
		ResourceNames: []string{sa.ObjectMeta.Name},
	})

	t.Run("node1 can create PCR once the signer is authorized via RBAC", func(t *testing.T) {
		_, err := node1Client.CertificatesV1().PodCertificateRequests("default").Create(ctx, makePCR(), metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("PCR creation unexpectedly failed after the signer was authorized: %v", err)
		}
	})
}

func serviceAccountClient(cfg *restclient.Config, ns, sa string) (*clientset.Clientset, error) {
	newCfg := restclient.CopyConfig(cfg)
	newCfg.Impersonate.UserName = fmt.Sprintf("system:serviceaccount:%s:%s", ns, sa)
	newCfg.Impersonate.Groups = []string{"system:authenticated", "system:serviceaccounts"}
	return clientset.NewForConfig(newCfg)
}

func nodeClient(cfg *restclient.Config, node string) (*clientset.Clientset, error) {
	newCfg := restclient.CopyConfig(cfg)
	newCfg.Impersonate.UserName = fmt.Sprintf("system:node:%s", node)
	newCfg.Impersonate.Groups = []string{"system:authenticated", "system:nodes"}
	return clientset.NewForConfig(newCfg)
}

// podCertificateVolume 返回一个 projected volume，为给定 signer 挂载
// podCertificate。挂载了这种 volume 的 Pod，被 noderestriction admission
// 插件允许为匹配的 signer 创建 PodCertificateRequest。
func podCertificateVolume(signerName string) []corev1.Volume {
	return []corev1.Volume{
		{
			Name: "podcert",
			VolumeSource: corev1.VolumeSource{
				Projected: &corev1.ProjectedVolumeSource{
					Sources: []corev1.VolumeProjection{
						{
							PodCertificate: &corev1.PodCertificateProjection{
								SignerName:           signerName,
								KeyType:              "ED25519",
								CredentialBundlePath: "creds.pem",
							},
						},
					},
				},
			},
		},
	}
}

func mustMakeStubPKCS10Request(t *testing.T) []byte {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("Error while generating ed25519 key: %v", err)
	}
	tmpl := &x509.CertificateRequest{}
	pkcs10Req, err := x509.CreateCertificateRequest(rand.Reader, tmpl, priv)
	if err != nil {
		t.Fatalf("while generating stub PKCS#10 request: %v", err)
	}
	return pkcs10Req
}
