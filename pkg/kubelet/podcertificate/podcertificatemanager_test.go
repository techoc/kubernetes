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

package podcertificate

import (
	"context"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"math/rand"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	certsv1 "k8s.io/api/certificates/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	certlistersv1 "k8s.io/client-go/listers/certificates/v1"
	corelistersv1 "k8s.io/client-go/listers/core/v1"
	k8stesting "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/test/utils/hermeticpodcertificatesigner"
	"k8s.io/kubernetes/test/utils/ktesting"
	testclock "k8s.io/utils/clock/testing"
	"k8s.io/utils/ptr"
)

// TestTransitionInitialToWait 校验 IssuingManager 首次处理投影时创建
// PodCertificateRequest，并正确设置 spec 与 OwnerReferences。
func TestTransitionInitialToWait(t *testing.T) {
	ctx, cancel := context.WithCancel(ktesting.Init(t))
	defer cancel()

	kc := fake.NewSimpleClientset()
	clock := testclock.NewFakeClock(mustRFC3339(t, "2010-01-01T00:00:00Z"))

	signerName := "foo.com/signer"

	pcrStore := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	pcrLister := certlistersv1.NewPodCertificateRequestLister(pcrStore)

	nodeStore := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	nodeLister := corelistersv1.NewNodeLister(nodeStore)
	node1 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
			UID:  "node1-uid",
		},
	}
	nodeStore.Add(node1)

	workloadSA, err := kc.CoreV1().ServiceAccounts("ns1").Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			Name:      "workload",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating workload serviceaccount: %v", err)
	}

	node1PodManager := &FakeSynchronousPodManager{
		pods: []*corev1.Pod{},
	}

	workloadPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			Name:      "workload",
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: workloadSA.ObjectMeta.Name,
			NodeName:           "node1",
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: "notarealimage",
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "certificate",
							MountPath: "/run/foo-cert",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "certificate",
					VolumeSource: corev1.VolumeSource{
						Projected: &corev1.ProjectedVolumeSource{
							Sources: []corev1.VolumeProjection{
								{
									PodCertificate: &corev1.PodCertificateProjection{
										SignerName:           signerName,
										KeyType:              "ED25519",
										CredentialBundlePath: "creds.pem",
										MaxExpirationSeconds: ptr.To[int32](86400), // fake client 不会执行默认值填充。
										UserAnnotations:      map[string]string{"test.domain/foo": "bar"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	node1PodManager.pods = append(node1PodManager.pods, workloadPod)

	node1PodCertificateManager := &IssuingManager{
		kc:         kc,
		podManager: node1PodManager,
		pcrLister:  pcrLister,
		nodeLister: nodeLister,
		nodeName:   types.NodeName("node1"),
		clock:      clock,
		credStore:  map[projectionKey]*projectionRecord{},
	}

	// 手动推进一步状态机，避免依赖后台 Run 循环带来的时序不确定性。
	if err := node1PodCertificateManager.handleProjection(ctx, projectionKey{workloadPod.ObjectMeta.Namespace, workloadPod.ObjectMeta.Name, string(workloadPod.ObjectMeta.UID), "certificate", 0}); err != nil {
		t.Fatalf("Unexpected error while running handleProjection: %v", err)
	}

	gotPCRs, err := kc.CertificatesV1().PodCertificateRequests("ns1").List(ctx, metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Unexpected error listing PodCertificateRequests in fake client: %v", err)
	}

	if len(gotPCRs.Items) != 1 {
		t.Fatalf("Wrong number of PodCertificateRequests after calling handleProjection; got %d, want 1", len(gotPCRs.Items))
	}

	gotPCR := gotPCRs.Items[0]

	// 校验创建的 PCR spec 是否符合预期；先清空不关心的字段再比较。
	wantPCR := &certsv1.PodCertificateRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
		},
		Spec: certsv1.PodCertificateRequestSpec{
			SignerName:                workloadPod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].PodCertificate.SignerName,
			PodName:                   workloadPod.ObjectMeta.Name,
			PodUID:                    workloadPod.ObjectMeta.UID,
			ServiceAccountName:        workloadSA.ObjectMeta.Name,
			ServiceAccountUID:         workloadSA.ObjectMeta.UID,
			NodeName:                  types.NodeName("node1"),
			NodeUID:                   node1.ObjectMeta.UID,
			MaxExpirationSeconds:      ptr.To[int32](86400),
			UnverifiedUserAnnotations: map[string]string{"test.domain/foo": "bar"},
		},
	}
	gotPCRClone := gotPCR.DeepCopy()
	gotPCRClone.ObjectMeta = metav1.ObjectMeta{}
	gotPCRClone.ObjectMeta.Namespace = gotPCR.ObjectMeta.Namespace
	gotPCRClone.Spec.StubPKCS10Request = nil
	gotPCRClone.Status = certsv1.PodCertificateRequestStatus{}
	if diff := cmp.Diff(gotPCRClone, wantPCR); diff != "" {
		t.Fatalf("PodCertificateManager created a bad PCR; diff (-got +want)\n%s", diff)
	}

	// 校验 OwnerReferences 已正确设置，确保 PCR 能随 Pod 一起被回收。
	wantOwnerRefs := []metav1.OwnerReference{
		{
			APIVersion: "v1",
			Kind:       "Pod",
			Name:       workloadPod.ObjectMeta.Name,
			UID:        workloadPod.ObjectMeta.UID,
		},
	}
	if diff := cmp.Diff(gotPCR.ObjectMeta.OwnerReferences, wantOwnerRefs); diff != "" {
		t.Fatalf("PodCertificateRequest has incorrect OwnerReferences; diff (-got +want)\n%s", diff)
	}
}

// TestPCRDeletedWhileWaiting 校验等待期间 PCR 被删除且超过 assumeDeletedThreshold 后，
// handleProjection 返回错误而不是空指针 panic。
func TestPCRDeletedWhileWaiting(t *testing.T) {
	ctx, cancel := context.WithCancel(ktesting.Init(t))
	defer cancel()

	kc := fake.NewClientset()
	clock := testclock.NewFakeClock(mustRFC3339(t, "2010-01-01T00:00:00Z"))

	signerName := "foo.com/signer"

	pcrStore := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	pcrLister := certlistersv1.NewPodCertificateRequestLister(pcrStore)

	nodeStore := cache.NewIndexer(cache.MetaNamespaceKeyFunc, cache.Indexers{})
	nodeLister := corelistersv1.NewNodeLister(nodeStore)
	node1 := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
			UID:  "node1-uid",
		},
	}
	if err := nodeStore.Add(node1); err != nil {
		t.Fatalf("Unexpected error adding node: %v", err)
	}

	workloadSA, err := kc.CoreV1().ServiceAccounts("ns1").Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			Name:      "workload",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating workload serviceaccount: %v", err)
	}

	node1PodManager := &FakeSynchronousPodManager{
		pods: []*corev1.Pod{},
	}

	workloadPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "ns1",
			Name:      "workload",
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: workloadSA.ObjectMeta.Name,
			NodeName:           "node1",
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: "notarealimage",
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "certificate",
							MountPath: "/run/foo-cert",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "certificate",
					VolumeSource: corev1.VolumeSource{
						Projected: &corev1.ProjectedVolumeSource{
							Sources: []corev1.VolumeProjection{
								{
									PodCertificate: &corev1.PodCertificateProjection{
										SignerName:           signerName,
										KeyType:              "ED25519",
										CredentialBundlePath: "creds.pem",
										MaxExpirationSeconds: ptr.To[int32](86400), // fake client 不会执行默认值填充。
										UserAnnotations:      map[string]string{"test.domain/foo": "bar"},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	node1PodManager.pods = append(node1PodManager.pods, workloadPod)

	node1PodCertificateManager := &IssuingManager{
		kc:         kc,
		podManager: node1PodManager,
		pcrLister:  pcrLister,
		nodeLister: nodeLister,
		nodeName:   types.NodeName("node1"),
		clock:      clock,
		credStore:  map[projectionKey]*projectionRecord{},
	}

	// 推进状态机一步，此时应进入等待签发状态。
	if err := node1PodCertificateManager.handleProjection(ctx, projectionKey{workloadPod.ObjectMeta.Namespace, workloadPod.ObjectMeta.Name, string(workloadPod.ObjectMeta.UID), "certificate", 0}); err != nil {
		t.Fatalf("Unexpected error while running handleProjection: %v", err)
	}

	// 删除全部 PCR 并把时间推进到 assumeDeletedThreshold 之后。
	if err := kc.CertificatesV1().PodCertificateRequests("ns1").DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{}); err != nil {
		t.Fatalf("Unexpected error while deleting all PCRs in ns1: %v", err)
	}
	clock.Step(assumeDeletedThreshold + 1*time.Minute)

	// 再次调用 handleProjection 应返回错误，而不是空指针 panic。
	err = node1PodCertificateManager.handleProjection(ctx, projectionKey{workloadPod.ObjectMeta.Namespace, workloadPod.ObjectMeta.Name, string(workloadPod.ObjectMeta.UID), "certificate", 0})
	if err == nil { // 错误必须非 nil
		t.Fatalf("Got no error from handleProjection, but wanted an error")
	}
}

// TestFullFlow 端到端验证：创建 PCR、签名控制器签发、kubelet 读取凭证，
// 以及到期前刷新并返回新证书的完整流程。
func TestFullFlow(t *testing.T) {
	ctx, cancel := context.WithCancel(ktesting.Init(t))
	defer cancel()

	clock := testclock.NewFakeClock(mustRFC3339(t, "2010-01-01T00:00:00Z"))
	kc := fake.NewSimpleClientset()

	// 为 PCR 分配名称与创建时间戳。
	kc.Fake.PrependReactor("create", "podcertificaterequests",
		func(action k8stesting.Action) (handled bool, ret runtime.Object, err error) {
			obj := action.(k8stesting.CreateAction).GetObject().(*certsv1.PodCertificateRequest)
			// 模拟服务端 GenerateName 行为。
			if obj.Name == "" {
				obj.Name = fmt.Sprintf("%s-pcr-%d", obj.Spec.PodName, rand.Int63n(1_000_000))
			}
			obj.CreationTimestamp = metav1.NewTime(clock.Now())

			return false, obj, nil // 返回 false 以继续执行默认处理。
		})

	// fake.Clientset 与 informer 配合时存在竞态：
	// 它的 Watch 实现不支持 resource version，
	// 而是假定所有 watch 都已建立后才会发生后续变更。
	//
	// 若测试在缓存同步后立即添加对象，
	// 一旦竞态发生、Watch 调用尚未完成，
	// 事件处理器将永远看不到这个新对象。
	//
	// 规避方式：统计已建立的 watch，全部就位后再继续。
	var numWatches atomic.Int32
	kc.Fake.PrependWatchReactor("*", func(action k8stesting.Action) (handled bool, ret watch.Interface, err error) {
		// 复刻 fake client 默认的 watch 行为，同时计数。
		var opts metav1.ListOptions
		if wa, ok := action.(k8stesting.WatchActionImpl); ok {
			opts = wa.ListOptions
		}

		gvr := action.GetResource()
		ns := action.GetNamespace()
		w, err := kc.Tracker().Watch(gvr, ns, opts)
		if err != nil {
			return false, nil, err
		}

		numWatches.Add(1)
		return true, w, nil
	})
	informerFactory := informers.NewSharedInformerFactoryWithOptions(kc, 0)

	//
	// 配置并启动假的 PodCertificateRequest 签名控制器。
	//

	signerName := "foo.com/signer"

	caKeys, caCerts, err := hermeticpodcertificatesigner.GenerateCAHierarchy(1)
	if err != nil {
		t.Fatalf("Unexpected error generating CA hierarchy: %v", err)
	}
	pcrSigner := hermeticpodcertificatesigner.New(clock, signerName, "", caKeys, caCerts, kc)
	go pcrSigner.Run(ctx)
	//
	// 启动运行 IssuingManager 所需的最小子系统集合。
	//

	node1, err := kc.CoreV1().Nodes().Create(ctx, &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: "node1",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating %s: %v", node1.ObjectMeta.Name, err)
	}

	node1PodManager := &FakePodManager{
		podLister: informerFactory.Core().V1().Pods().Lister(),
	}

	node1PodCertificateManager := NewIssuingManager(
		kc,
		node1PodManager,
		nil,
		informerFactory.Certificates().V1().PodCertificateRequests(),
		informerFactory.Core().V1().Nodes(),
		types.NodeName(node1.ObjectMeta.Name),
		clock,
	)
	// 启动 informer。
	informerFactory.Start(ctx.Done())

	// 等待 informer 的 watch 真正注册完成，
	const expectedWatches = 4 // Manager 监听 Pods、Nodes、PodCertificateRequests；Signer 监听 PodCertificateRequests。
	if err := wait.PollUntilContextTimeout(
		ctx,
		100*time.Millisecond,
		5*time.Second,
		true,
		func(ctx context.Context) (bool, error) {
			return numWatches.Load() >= expectedWatches, nil
		},
	); err != nil {
		t.Fatalf("timed out waiting for informer watches, got %d (want >= %d): %v",
			numWatches.Load(), expectedWatches, err)
	}

	// 此时 informer 已在监听，可以安全启动管理器。
	go node1PodCertificateManager.Run(ctx)

	//
	// 构造一个使用 PodCertificate 投影卷的 Pod。
	//

	workloadNS, err := kc.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: "workload-ns",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating workload namespace: %v", err)
	}

	workloadSA, err := kc.CoreV1().ServiceAccounts(workloadNS.ObjectMeta.Name).Create(ctx, &corev1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: workloadNS.ObjectMeta.Name,
			Name:      "workload",
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating workload serviceaccount: %v", err)
	}

	workloadPod, err := kc.CoreV1().Pods(workloadNS.ObjectMeta.Name).Create(ctx, &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: workloadNS.ObjectMeta.Name,
			Name:      "workload",
			UID:       "test-workload-uid",
		},
		Spec: corev1.PodSpec{
			ServiceAccountName: workloadSA.ObjectMeta.Name,
			NodeName:           node1.ObjectMeta.Name,
			Containers: []corev1.Container{
				{
					Name:  "main",
					Image: "notarealimage",
					VolumeMounts: []corev1.VolumeMount{
						{
							Name:      "certificate",
							MountPath: "/run/foo-cert",
						},
					},
				},
			},
			Volumes: []corev1.Volume{
				{
					Name: "certificate",
					VolumeSource: corev1.VolumeSource{
						Projected: &corev1.ProjectedVolumeSource{
							Sources: []corev1.VolumeProjection{
								{
									PodCertificate: &corev1.PodCertificateProjection{
										SignerName:           signerName,
										KeyType:              "ED25519",
										CredentialBundlePath: "creds.pem",
										MaxExpirationSeconds: ptr.To[int32](86400), // fake client 不会执行默认值填充。
										UserAnnotations:      map[string]string{hermeticpodcertificatesigner.SpiffePathKey: "workload"},
									},
								},
							},
						},
					},
				},
			},
		},
	}, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating workload pod: %v", err)
	}
	// fake podManager 基于 informer，需要轮询等待 workloadPod 出现在缓存中。
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		_, ok := node1PodManager.GetPodByUID(workloadPod.ObjectMeta.UID)
		return ok, nil
	})
	if err != nil {
		t.Fatalf("Error while waiting for node1 podManager to know about workloadPod: %v", err)
	}

	// 只有被 Track 的 Pod 才会进入投影处理流程。
	node1PodCertificateManager.TrackPod(ctx, workloadPod)

	// 预期几秒内会为该 Pod 创建 PodCertificateRequest。
	var gotPCR *certsv1.PodCertificateRequest
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		pcrs, err := kc.CertificatesV1().PodCertificateRequests(workloadNS.ObjectMeta.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, fmt.Errorf("while listing PodCertificateRequests: %w", err)
		}

		if len(pcrs.Items) == 0 {
			return false, nil
		}

		gotPCR = &pcrs.Items[0]
		return true, nil
	})
	if err != nil {
		t.Fatalf("Error while waiting for PCR to be created: %v", err)
	}

	// 校验创建的 PCR spec 是否符合预期；先清空不关心的字段再比较。
	// 先清空不关心的字段；状态也要清空，因为控制器可能已经完成签发。
	wantPCR := &certsv1.PodCertificateRequest{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: workloadNS.ObjectMeta.Name,
		},
		Spec: certsv1.PodCertificateRequestSpec{
			SignerName:                workloadPod.Spec.Volumes[0].VolumeSource.Projected.Sources[0].PodCertificate.SignerName,
			PodName:                   workloadPod.ObjectMeta.Name,
			PodUID:                    workloadPod.ObjectMeta.UID,
			ServiceAccountName:        workloadSA.ObjectMeta.Name,
			ServiceAccountUID:         workloadSA.ObjectMeta.UID,
			NodeName:                  types.NodeName(node1.ObjectMeta.Name),
			NodeUID:                   node1.ObjectMeta.UID,
			MaxExpirationSeconds:      ptr.To[int32](86400),
			UnverifiedUserAnnotations: map[string]string{hermeticpodcertificatesigner.SpiffePathKey: "workload"},
		},
	}
	gotPCRClone := gotPCR.DeepCopy()
	gotPCRClone.ObjectMeta = metav1.ObjectMeta{}
	gotPCRClone.ObjectMeta.Namespace = gotPCR.ObjectMeta.Namespace
	gotPCRClone.Spec.StubPKCS10Request = nil
	gotPCRClone.Status = certsv1.PodCertificateRequestStatus{}
	if diff := cmp.Diff(gotPCRClone, wantPCR); diff != "" {
		t.Fatalf("PodCertificateManager created a bad PCR; diff (-got +want)\n%s", diff)
	}

	// 继续等待 PCR 完成签发。
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		pcrs, err := kc.CertificatesV1().PodCertificateRequests(workloadNS.ObjectMeta.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, fmt.Errorf("while listing PodCertificateRequests: %w", err)
		}

		if len(pcrs.Items) == 0 {
			return false, nil
		}

		gotPCR = &pcrs.Items[0]

		for _, cond := range gotPCR.Status.Conditions {
			switch cond.Type {
			case certsv1.PodCertificateRequestConditionTypeDenied,
				certsv1.PodCertificateRequestConditionTypeFailed,
				certsv1.PodCertificateRequestConditionTypeIssued:
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("Error while waiting for PCR to be issued: %v", err)
	}

	isIssued := slices.ContainsFunc(gotPCR.Status.Conditions, func(cond metav1.Condition) bool {
		return cond.Type == certsv1.PodCertificateRequestConditionTypeIssued
	})
	if !isIssued {
		t.Fatalf("The test signingController didn't issue the PCR:\n%+v", gotPCR)
	}

	// 校验 SPIFFE 路径已被 UserAnnotations 覆盖。
	issuedCertPem := []byte(gotPCR.Status.CertificateChain)
	block, _ := pem.Decode(issuedCertPem)
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("Failed to parse the issued certificate: %v", err)
	}

	if cert.URIs[0].Path != "/workload" {
		t.Logf("Certificate path is %s", cert.URIs[0].Path)
		t.Fatalf("Failed to override the spiffe path with the user annotations")
	}

	// 新 PCR 已签发，等待管理器开始返回新证书。
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		_, _, err := node1PodCertificateManager.GetPodCertificateCredentialBundle(ctx, workloadPod.ObjectMeta.Namespace, workloadPod.ObjectMeta.Name, string(workloadPod.ObjectMeta.UID), "certificate", 0)
		if err != nil {
			t.Logf("Credential bundle not ready yet: %v", err)
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		t.Fatalf("Error while waiting for podcertificate manager to return valid credentials: %v", err)
	}

	_, certChain, err := node1PodCertificateManager.GetPodCertificateCredentialBundle(ctx, workloadPod.ObjectMeta.Namespace, workloadPod.ObjectMeta.Name, string(workloadPod.ObjectMeta.UID), "certificate", 0)
	if err != nil {
		t.Fatalf("Unexpected error getting credentials from pod certificate manager: %v", err)
	}

	if diff := cmp.Diff(string(certChain), gotPCR.Status.CertificateChain); diff != "" {
		t.Fatalf("PodCertificate manager returned bad cert chain; diff (-got +want)\n%s", diff)
	}

	// 快进时间越过 beginRefreshAt（包含最多 5 分钟的抖动）。
	clock.Step(23*time.Hour + 37*time.Minute)

	// 预期几秒内会为该 Pod 创建新的 PodCertificateRequest。
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		pcrs, err := kc.CertificatesV1().PodCertificateRequests(workloadNS.ObjectMeta.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, fmt.Errorf("while listing PodCertificateRequests: %w", err)
		}

		if len(pcrs.Items) == 0 {
			return false, nil
		}

		gotPCR = &pcrs.Items[0]
		return true, nil
	})
	if err != nil {
		t.Fatalf("Error while waiting for PCR to be created: %v", err)
	}

	// 这里假定新建 PCR 的内容符合预期。

	// 继续等待新 PCR 完成签发。
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		pcrs, err := kc.CertificatesV1().PodCertificateRequests(workloadNS.ObjectMeta.Name).List(ctx, metav1.ListOptions{})
		if err != nil {
			return false, fmt.Errorf("while listing PodCertificateRequests: %w", err)
		}

		if len(pcrs.Items) == 0 {
			return false, nil
		}

		gotPCR = &pcrs.Items[0]

		for _, cond := range gotPCR.Status.Conditions {
			switch cond.Type {
			case certsv1.PodCertificateRequestConditionTypeDenied,
				certsv1.PodCertificateRequestConditionTypeFailed,
				certsv1.PodCertificateRequestConditionTypeIssued:
				return true, nil
			}
		}
		return false, nil
	})
	if err != nil {
		t.Fatalf("Error while waiting for PCR to be issued: %v", err)
	}

	// 新 PCR 已签发，等待管理器开始返回新证书。
	err = wait.PollUntilContextTimeout(ctx, 1*time.Second, 30*time.Second, true, func(ctx context.Context) (bool, error) {
		_, certChain, err := node1PodCertificateManager.GetPodCertificateCredentialBundle(ctx, workloadPod.ObjectMeta.Namespace, workloadPod.ObjectMeta.Name, string(workloadPod.ObjectMeta.UID), "certificate", 0)
		if err != nil {
			t.Logf("Refreshed credential bundle not ready yet: %v", err)
			return false, nil
		}

		if string(certChain) != gotPCR.Status.CertificateChain {
			return false, nil
		}

		return true, nil
	})
	if err != nil {
		t.Fatalf("Error while waiting for podcertificate manager to return valid credentials: %v", err)
	}

	_, certChain, err = node1PodCertificateManager.GetPodCertificateCredentialBundle(ctx, workloadPod.ObjectMeta.Namespace, workloadPod.ObjectMeta.Name, string(workloadPod.ObjectMeta.UID), "certificate", 0)
	if err != nil {
		t.Fatalf("Unexpected error getting credentials from pod certificate manager: %v", err)
	}

	if diff := cmp.Diff(string(certChain), gotPCR.Status.CertificateChain); diff != "" {
		t.Fatalf("PodCertificate manager returned bad cert chain; diff (-got +want)\n%s", diff)
	}
}

// FakeSynchronousPodManager 是同步的 PodManager 桩，直接返回内存中的 Pod。
type FakeSynchronousPodManager struct {
	pods []*corev1.Pod
}

// GetPods 返回内存中的全部 Pod。
func (f *FakeSynchronousPodManager) GetPods() []*corev1.Pod {
	return f.pods
}

// GetPodByUID 按 UID 在内存中查找 Pod。
func (f *FakeSynchronousPodManager) GetPodByUID(uid types.UID) (*corev1.Pod, bool) {
	for _, pod := range f.pods {
		if pod.ObjectMeta.UID == uid {
			return pod, true
		}
	}
	return nil, false
}

// FakePodManager 是基于 informer lister 的 PodManager 桩。
type FakePodManager struct {
	podLister corelistersv1.PodLister
}

// GetPods 从 lister 缓存中列出全部 Pod。
func (f *FakePodManager) GetPods() []*corev1.Pod {
	ret, _ := f.podLister.List(labels.Everything())
	return ret
}

// GetPodByUID 从 lister 缓存中按 UID 查找 Pod。
func (f *FakePodManager) GetPodByUID(uid types.UID) (*corev1.Pod, bool) {
	list, _ := f.podLister.List(labels.Everything())
	for _, pod := range list {
		if pod.ObjectMeta.UID == uid {
			return pod, true
		}
	}
	return nil, false
}

// mustRFC3339 解析 RFC3339 时间戳，失败时终止测试。
func mustRFC3339(t *testing.T, stamp string) time.Time {
	got, err := time.Parse(time.RFC3339, stamp)
	if err != nil {
		t.Fatalf("Error while parsing timestamp: %v", err)
	}
	return got
}
