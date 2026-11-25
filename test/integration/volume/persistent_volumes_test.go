/*
Copyright 2014 The Kubernetes Authors.

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

package volume

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/version"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	"k8s.io/client-go/informers"
	clientset "k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/client-go/util/retry"
	featuregatetesting "k8s.io/component-base/featuregate/testing"
	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/pkg/api/legacyscheme"
	persistentvolumecontroller "k8s.io/kubernetes/pkg/controller/volume/persistentvolume"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/pkg/volume"
	volumetest "k8s.io/kubernetes/pkg/volume/testing"
	"k8s.io/kubernetes/pkg/volume/util"
	"k8s.io/kubernetes/test/integration/framework"
	"k8s.io/kubernetes/test/utils/ktesting"

	"k8s.io/klog/v2"
)

// 本文件中的多个测试可通过环境变量配置：
// KUBE_INTEGRATION_PV_OBJECTS - 要创建的 PV/PVC 数量（默认 100）
// KUBE_INTEGRATION_PV_SYNC_PERIOD - volume controller 同步周期（默认 1s）
// KUBE_INTEGRATION_PV_END_SLEEP - TestPersistentVolumeMultiPVsPVCs 结束后的
// 休眠时长（默认 0s），用于测量周期性同步处理已绑定 PV/PVC 需要多久。
const defaultObjectCount = 100
const defaultSyncPeriod = 1 * time.Second

const provisionerPluginName = "kubernetes.io/mock-provisioner"

func getObjectCount() int {
	objectCount := defaultObjectCount
	if s := os.Getenv("KUBE_INTEGRATION_PV_OBJECTS"); s != "" {
		var err error
		objectCount, err = strconv.Atoi(s)
		if err != nil {
			klog.Fatalf("cannot parse value of KUBE_INTEGRATION_PV_OBJECTS: %v", err)
		}
	}
	klog.V(2).Infof("using KUBE_INTEGRATION_PV_OBJECTS=%d", objectCount)
	return objectCount
}

func getSyncPeriod(syncPeriod time.Duration) time.Duration {
	period := syncPeriod
	if s := os.Getenv("KUBE_INTEGRATION_PV_SYNC_PERIOD"); s != "" {
		var err error
		period, err = time.ParseDuration(s)
		if err != nil {
			klog.Fatalf("cannot parse value of KUBE_INTEGRATION_PV_SYNC_PERIOD: %v", err)
		}
	}
	klog.V(2).Infof("using KUBE_INTEGRATION_PV_SYNC_PERIOD=%v", period)
	return period
}

func testSleep() {
	var period time.Duration
	if s := os.Getenv("KUBE_INTEGRATION_PV_END_SLEEP"); s != "" {
		var err error
		period, err = time.ParseDuration(s)
		if err != nil {
			klog.Fatalf("cannot parse value of KUBE_INTEGRATION_PV_END_SLEEP: %v", err)
		}
	}
	klog.V(2).Infof("using KUBE_INTEGRATION_PV_END_SLEEP=%v", period)
	if period != 0 {
		time.Sleep(period)
		klog.V(2).Infof("sleep finished")
	}
}

func TestPersistentVolumeRecycler(t *testing.T) {
	klog.V(2).Infof("TestPersistentVolumeRecycler started")
	s := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=ServiceAccount,StorageObjectInUseProtection"}, framework.SharedEtcd())
	defer s.TearDownFn()
	namespaceName := "pv-recycler"

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")

	testClient, ctrl, informers, watchPV, watchPVC := createClients(tCtx, namespaceName, t, s, defaultSyncPeriod)
	defer watchPV.Stop()
	defer watchPVC.Stop()

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	// NOTE: 该测试不能并行运行，因为它会创建和删除 non-namespaced 对象（PersistentVolume）。
	defer testClient.CoreV1().PersistentVolumes().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})

	informers.Start(tCtx.Done())
	go ctrl.Run(tCtx)

	// 该 PV 将被 claim、release，然后回收。
	pv := createPV("fake-pv-recycler", "/tmp/foo", "10G", []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, v1.PersistentVolumeReclaimRecycle)
	pvc := createPVC("fake-pvc-recycler", ns.Name, "5G", []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, "")

	_, err := testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolume: %v", err)
	}
	klog.V(2).Infof("TestPersistentVolumeRecycler pvc created")

	_, err = testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolumeClaim: %v", err)
	}
	klog.V(2).Infof("TestPersistentVolumeRecycler pvc created")

	// 等待 controller 完成 volume 与 claim 的配对
	waitForPersistentVolumePhase(testClient, pv.Name, watchPV, v1.VolumeBound)
	klog.V(2).Infof("TestPersistentVolumeRecycler pv bound")
	waitForPersistentVolumeClaimPhase(testClient, pvc.Name, ns.Name, watchPVC, v1.ClaimBound)
	klog.V(2).Infof("TestPersistentVolumeRecycler pvc bound")

	// 删除 claim 会释放 volume，之后该 volume 可被回收
	if err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Delete(context.TODO(), pvc.Name, metav1.DeleteOptions{}); err != nil {
		t.Errorf("error deleting claim %s", pvc.Name)
	}
	klog.V(2).Infof("TestPersistentVolumeRecycler pvc deleted")

	waitForPersistentVolumePhase(testClient, pv.Name, watchPV, v1.VolumeReleased)
	klog.V(2).Infof("TestPersistentVolumeRecycler pv released")
	waitForPersistentVolumePhase(testClient, pv.Name, watchPV, v1.VolumeAvailable)
	klog.V(2).Infof("TestPersistentVolumeRecycler pv available")
}

func TestPersistentVolumeDeleter(t *testing.T) {
	klog.V(2).Infof("TestPersistentVolumeDeleter started")
	s := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=ServiceAccount,StorageObjectInUseProtection"}, framework.SharedEtcd())
	defer s.TearDownFn()
	namespaceName := "pv-deleter"

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, ctrl, informers, watchPV, watchPVC := createClients(tCtx, namespaceName, t, s, defaultSyncPeriod)
	defer watchPV.Stop()
	defer watchPVC.Stop()

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	// NOTE: 该测试不能并行运行，因为它会创建和删除 non-namespaced 对象（PersistentVolume）。
	defer testClient.CoreV1().PersistentVolumes().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})

	informers.Start(tCtx.Done())
	go ctrl.Run(tCtx)

	// 该 PV 将被 claim、release，然后删除。
	pv := createPV("fake-pv-deleter", "/tmp/foo", "10G", []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, v1.PersistentVolumeReclaimDelete)
	pvc := createPVC("fake-pvc-deleter", ns.Name, "5G", []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, "")

	_, err := testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolume: %v", err)
	}
	klog.V(2).Infof("TestPersistentVolumeDeleter pv created")
	_, err = testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolumeClaim: %v", err)
	}
	klog.V(2).Infof("TestPersistentVolumeDeleter pvc created")
	waitForPersistentVolumePhase(testClient, pv.Name, watchPV, v1.VolumeBound)
	klog.V(2).Infof("TestPersistentVolumeDeleter pv bound")
	waitForPersistentVolumeClaimPhase(testClient, pvc.Name, ns.Name, watchPVC, v1.ClaimBound)
	klog.V(2).Infof("TestPersistentVolumeDeleter pvc bound")

	// 删除 claim 会释放 volume
	if err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Delete(context.TODO(), pvc.Name, metav1.DeleteOptions{}); err != nil {
		t.Errorf("error deleting claim %s", pvc.Name)
	}
	klog.V(2).Infof("TestPersistentVolumeDeleter pvc deleted")

	waitForPersistentVolumePhase(testClient, pv.Name, watchPV, v1.VolumeReleased)
	klog.V(2).Infof("TestPersistentVolumeDeleter pv released")

	for {
		event := <-watchPV.ResultChan()
		if event.Type == watch.Deleted {
			break
		}
	}
	klog.V(2).Infof("TestPersistentVolumeDeleter pv deleted")
}

func TestPersistentVolumeBindRace(t *testing.T) {
	// 测试多个 claim 争抢绑定到某个预先绑定到指定 PVC 的 PV，只有该 PVC 应绑定成功。
	klog.V(2).Infof("TestPersistentVolumeBindRace started")
	s := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=ServiceAccount,StorageObjectInUseProtection"}, framework.SharedEtcd())
	defer s.TearDownFn()
	namespaceName := "pv-bind-race"

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, ctrl, informers, watchPV, watchPVC := createClients(tCtx, namespaceName, t, s, defaultSyncPeriod)
	defer watchPV.Stop()
	defer watchPVC.Stop()

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	// NOTE: 该测试不能并行运行，因为它会创建和删除 non-namespaced 对象（PersistentVolume）。
	defer testClient.CoreV1().PersistentVolumes().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})

	informers.Start(tCtx.Done())
	go ctrl.Run(tCtx)

	pv := createPV("fake-pv-race", "/tmp/foo", "10G", []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, v1.PersistentVolumeReclaimRetain)
	pvc := createPVC("fake-pvc-race", ns.Name, "5G", []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, "")
	counter := 0
	maxClaims := 100
	claims := []*v1.PersistentVolumeClaim{}
	for counter <= maxClaims {
		counter++
		newPvc := pvc.DeepCopy()
		newPvc.ObjectMeta = metav1.ObjectMeta{Name: fmt.Sprintf("fake-pvc-race-%d", counter)}
		claim, err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), newPvc, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Error creating newPvc: %v", err)
		}
		claims = append(claims, claim)
	}
	klog.V(2).Infof("TestPersistentVolumeBindRace claims created")

	// 手动在 PV 上指定绑定，应只匹配所绑定的那个 claim
	claim := claims[rand.Intn(maxClaims-1)]
	claimRef, err := ref.GetReference(legacyscheme.Scheme, claim)
	if err != nil {
		t.Fatalf("Unexpected error getting claimRef: %v", err)
	}
	pv.Spec.ClaimRef = claimRef
	pv.Spec.ClaimRef.UID = ""

	pv, err = testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Unexpected error creating pv: %v", err)
	}
	klog.V(2).Infof("TestPersistentVolumeBindRace pv created, pre-bound to %s", claim.Name)

	waitForPersistentVolumePhase(testClient, pv.Name, watchPV, v1.VolumeBound)
	klog.V(2).Infof("TestPersistentVolumeBindRace pv bound")
	if err := waitForAnyPersistentVolumeClaimPhase(watchPVC, v1.ClaimBound); err != nil {
		t.Fatalf("Unexpected error waiting for any pvc to be bound: %v", err)
	}
	klog.V(2).Infof("TestPersistentVolumeBindRace pvc bound")

	pv, err = testClient.CoreV1().PersistentVolumes().Get(context.TODO(), pv.Name, metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error getting pv: %v", err)
	}
	if pv.Spec.ClaimRef == nil {
		t.Fatalf("Unexpected nil claimRef")
	}
	if pv.Spec.ClaimRef.Namespace != claimRef.Namespace || pv.Spec.ClaimRef.Name != claimRef.Name {
		t.Fatalf("Bind mismatch! Expected %s/%s but got %s/%s", claimRef.Namespace, claimRef.Name, pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
	}
}

// TestPersistentVolumeClaimLabelSelector 测试使用 label selector 进行绑定
func TestPersistentVolumeClaimLabelSelector(t *testing.T) {
	s := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=ServiceAccount,StorageObjectInUseProtection"}, framework.SharedEtcd())
	defer s.TearDownFn()
	namespaceName := "pvc-label-selector"

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, controller, informers, watchPV, watchPVC := createClients(tCtx, namespaceName, t, s, defaultSyncPeriod)
	defer watchPV.Stop()
	defer watchPVC.Stop()

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	// NOTE: 该测试不能并行运行，因为它会创建和删除 non-namespaced 对象（PersistentVolume）。
	defer testClient.CoreV1().PersistentVolumes().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})

	informers.Start(tCtx.Done())
	go controller.Run(tCtx)

	var (
		err     error
		modes   = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
		reclaim = v1.PersistentVolumeReclaimRetain

		pvTrue  = createPV("pv-true", "/tmp/foo-label", "1G", modes, reclaim)
		pvFalse = createPV("pv-false", "/tmp/foo-label", "1G", modes, reclaim)
		pvc     = createPVC("pvc-ls-1", ns.Name, "1G", modes, "")
	)

	pvTrue.ObjectMeta.SetLabels(map[string]string{"foo": "true"})
	pvFalse.ObjectMeta.SetLabels(map[string]string{"foo": "false"})

	_, err = testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pvTrue, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create PersistentVolume: %v", err)
	}
	_, err = testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pvFalse, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create PersistentVolume: %v", err)
	}
	t.Log("volumes created")

	pvc.Spec.Selector = &metav1.LabelSelector{
		MatchLabels: map[string]string{
			"foo": "true",
		},
	}

	_, err = testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create PersistentVolumeClaim: %v", err)
	}
	t.Log("claim created")

	waitForAnyPersistentVolumePhase(watchPV, v1.VolumeBound)
	t.Log("volume bound")
	waitForPersistentVolumeClaimPhase(testClient, pvc.Name, ns.Name, watchPVC, v1.ClaimBound)
	t.Log("claim bound")

	pv, err := testClient.CoreV1().PersistentVolumes().Get(context.TODO(), "pv-false", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error getting pv: %v", err)
	}
	if pv.Spec.ClaimRef != nil {
		t.Fatalf("False PV shouldn't be bound")
	}
	pv, err = testClient.CoreV1().PersistentVolumes().Get(context.TODO(), "pv-true", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error getting pv: %v", err)
	}
	if pv.Spec.ClaimRef == nil {
		t.Fatalf("True PV should be bound")
	}
	if pv.Spec.ClaimRef.Namespace != pvc.Namespace || pv.Spec.ClaimRef.Name != pvc.Name {
		t.Fatalf("Bind mismatch! Expected %s/%s but got %s/%s", pvc.Namespace, pvc.Name, pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
	}
}

// TestPersistentVolumeClaimLabelSelectorMatchExpressions 测试使用 MatchExpressions
// label selector 进行绑定
func TestPersistentVolumeClaimLabelSelectorMatchExpressions(t *testing.T) {
	s := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=ServiceAccount,StorageObjectInUseProtection"}, framework.SharedEtcd())
	defer s.TearDownFn()
	namespaceName := "pvc-match-expressions"

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, controller, informers, watchPV, watchPVC := createClients(tCtx, namespaceName, t, s, defaultSyncPeriod)
	defer watchPV.Stop()
	defer watchPVC.Stop()

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	// NOTE: 该测试不能并行运行，因为它会创建和删除 non-namespaced 对象（PersistentVolume）。
	defer testClient.CoreV1().PersistentVolumes().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})

	informers.Start(tCtx.Done())
	go controller.Run(tCtx)

	var (
		err     error
		modes   = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
		reclaim = v1.PersistentVolumeReclaimRetain

		pvTrue  = createPV("pv-true", "/tmp/foo-label", "1G", modes, reclaim)
		pvFalse = createPV("pv-false", "/tmp/foo-label", "1G", modes, reclaim)
		pvc     = createPVC("pvc-ls-1", ns.Name, "1G", modes, "")
	)

	pvTrue.ObjectMeta.SetLabels(map[string]string{"foo": "valA", "bar": ""})
	pvFalse.ObjectMeta.SetLabels(map[string]string{"foo": "valB", "baz": ""})

	_, err = testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pvTrue, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create PersistentVolume: %v", err)
	}
	_, err = testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pvFalse, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create PersistentVolume: %v", err)
	}
	t.Log("volumes created")

	pvc.Spec.Selector = &metav1.LabelSelector{
		MatchExpressions: []metav1.LabelSelectorRequirement{
			{
				Key:      "foo",
				Operator: metav1.LabelSelectorOpIn,
				Values:   []string{"valA"},
			},
			{
				Key:      "foo",
				Operator: metav1.LabelSelectorOpNotIn,
				Values:   []string{"valB"},
			},
			{
				Key:      "bar",
				Operator: metav1.LabelSelectorOpExists,
				Values:   []string{},
			},
			{
				Key:      "baz",
				Operator: metav1.LabelSelectorOpDoesNotExist,
				Values:   []string{},
			},
		},
	}

	_, err = testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Fatalf("Failed to create PersistentVolumeClaim: %v", err)
	}
	t.Log("claim created")

	waitForAnyPersistentVolumePhase(watchPV, v1.VolumeBound)
	t.Log("volume bound")
	waitForPersistentVolumeClaimPhase(testClient, pvc.Name, ns.Name, watchPVC, v1.ClaimBound)
	t.Log("claim bound")

	pv, err := testClient.CoreV1().PersistentVolumes().Get(context.TODO(), "pv-false", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error getting pv: %v", err)
	}
	if pv.Spec.ClaimRef != nil {
		t.Fatalf("False PV shouldn't be bound")
	}
	pv, err = testClient.CoreV1().PersistentVolumes().Get(context.TODO(), "pv-true", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error getting pv: %v", err)
	}
	if pv.Spec.ClaimRef == nil {
		t.Fatalf("True PV should be bound")
	}
	if pv.Spec.ClaimRef.Namespace != pvc.Namespace || pv.Spec.ClaimRef.Name != pvc.Name {
		t.Fatalf("Bind mismatch! Expected %s/%s but got %s/%s", pvc.Namespace, pvc.Name, pv.Spec.ClaimRef.Namespace, pv.Spec.ClaimRef.Name)
	}
}

// TestPersistentVolumeMultiPVs 测试一个 PVC 从 100 个容量不同的 PV 中绑定
func TestPersistentVolumeMultiPVs(t *testing.T) {
	s := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=ServiceAccount,StorageObjectInUseProtection"}, framework.SharedEtcd())
	defer s.TearDownFn()
	namespaceName := "multi-pvs"

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, controller, informers, watchPV, watchPVC := createClients(tCtx, namespaceName, t, s, defaultSyncPeriod)
	defer watchPV.Stop()
	defer watchPVC.Stop()

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	// NOTE: 该测试不能并行运行，因为它会创建和删除 non-namespaced 对象（PersistentVolume）。
	defer testClient.CoreV1().PersistentVolumes().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})

	informers.Start(tCtx.Done())
	go controller.Run(tCtx)

	maxPVs := getObjectCount()
	pvs := make([]*v1.PersistentVolume, maxPVs)
	for i := range maxPVs {
		// 该 PV 将被 claim、release，然后删除
		pvs[i] = createPV("pv-"+strconv.Itoa(i), "/tmp/foo"+strconv.Itoa(i), strconv.Itoa(i+1)+"G",
			[]v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, v1.PersistentVolumeReclaimRetain)
	}

	pvc := createPVC("pvc-2", ns.Name, strconv.Itoa(maxPVs/2)+"G", []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, "")

	for i := range maxPVs {
		_, err := testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pvs[i], metav1.CreateOptions{})
		if err != nil {
			t.Errorf("Failed to create PersistentVolume %d: %v", i, err)
		}
		waitForPersistentVolumePhase(testClient, pvs[i].Name, watchPV, v1.VolumeAvailable)
	}
	t.Log("volumes created")

	_, err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolumeClaim: %v", err)
	}
	t.Log("claim created")

	// 等待 binder 完成 claim 与 volume 的配对
	waitForAnyPersistentVolumePhase(watchPV, v1.VolumeBound)
	t.Log("volume bound")
	waitForPersistentVolumeClaimPhase(testClient, pvc.Name, ns.Name, watchPVC, v1.ClaimBound)
	t.Log("claim bound")

	// 只应有一个 PV 被绑定
	bound := 0
	for i := range maxPVs {
		pv, err := testClient.CoreV1().PersistentVolumes().Get(context.TODO(), pvs[i].Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Unexpected error getting pv: %v", err)
		}
		if pv.Spec.ClaimRef == nil {
			continue
		}
		// 找到已绑定的 PV
		p := pv.Spec.Capacity[v1.ResourceStorage]
		pvCap := p.Value()
		expectedCap := resource.MustParse(strconv.Itoa(maxPVs/2) + "G")
		expectedCapVal := expectedCap.Value()
		if pv.Spec.ClaimRef.Name != pvc.Name || pvCap != expectedCapVal {
			t.Fatalf("Bind mismatch! Expected %s capacity %d but got %s capacity %d", pvc.Name, expectedCapVal, pv.Spec.ClaimRef.Name, pvCap)
		}
		t.Logf("claim bounded to %s capacity %v", pv.Name, pv.Spec.Capacity[v1.ResourceStorage])
		bound++
	}
	t.Log("volumes checked")

	if bound != 1 {
		t.Fatalf("Only 1 PV should be bound but got %d", bound)
	}

	// 删除 claim 会释放 volume
	if err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Delete(context.TODO(), pvc.Name, metav1.DeleteOptions{}); err != nil {
		t.Errorf("error deleting claim %s", pvc.Name)
	}
	t.Log("claim deleted")

	waitForAnyPersistentVolumePhase(watchPV, v1.VolumeReleased)
	t.Log("volumes released")
}

// TestPersistentVolumeClaimVolumeAttirbutesClassName 测试基于 volume attributes class name
// 的绑定
func TestPersistentVolumeClaimVolumeAttirbutesClassName(t *testing.T) {
	var (
		err           error
		modes         = []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}
		reclaim       = v1.PersistentVolumeReclaimRetain
		namespaceName = "pvc-volume-attributes-class-name"

		classEmpty  = ""
		classGold   = "gold"
		classSilver = "silver"

		pv       = createCSIPV("pv", "1G", modes, reclaim)
		pvGold   = createCSIPV("pv-gold", "1G", modes, reclaim)
		pv2Gold  = createCSIPV("pv2-gold", "1G", modes, reclaim)
		pvSilver = createCSIPV("pv-silver", "1G", modes, reclaim)

		pvc      = createPVC("pvc", namespaceName, "1G", modes, "")
		pvcEmpty = createPVC("pvc", namespaceName, "1G", modes, "")
		pvcGold  = createPVC("pvc-gold", namespaceName, "1G", modes, "")
	)

	// 准备 PV 与 PVC
	pv.Spec.VolumeAttributesClassName = nil
	pvGold.Spec.VolumeAttributesClassName = &classGold
	pv2Gold.Spec.VolumeAttributesClassName = &classGold
	pvSilver.Spec.VolumeAttributesClassName = &classSilver

	pvc.Spec.VolumeAttributesClassName = nil
	pvcEmpty.Spec.VolumeAttributesClassName = &classEmpty
	pvcGold.Spec.VolumeAttributesClassName = &classGold

	testCases := []struct {
		featureEnabled   bool
		name             string
		volumes          []*v1.PersistentVolume
		claim            *v1.PersistentVolumeClaim
		expectVolumeName string
	}{
		{
			featureEnabled:   true,
			name:             "claim with nil class bind to a pv",
			volumes:          []*v1.PersistentVolume{pv, pvGold, pvSilver},
			claim:            pvc,
			expectVolumeName: pv.Name,
		},
		{
			featureEnabled:   true,
			name:             "claim with empty class bind to a pv",
			volumes:          []*v1.PersistentVolume{pv, pvGold, pvSilver},
			claim:            pvcEmpty,
			expectVolumeName: pv.Name,
		},
		{
			featureEnabled:   true,
			name:             "claim bind to a pv with same class name",
			volumes:          []*v1.PersistentVolume{pv, pvGold, pvSilver},
			claim:            pvcGold,
			expectVolumeName: pvGold.Name,
		},
		{
			featureEnabled: true,
			name:           "claim bind to a user-asked pv with same class name",
			volumes:        []*v1.PersistentVolume{pv, pvGold, pv2Gold, pvSilver},
			claim: func() *v1.PersistentVolumeClaim {
				pvcGoldClone := pvcGold.DeepCopy()
				pvcGoldClone.Spec.VolumeName = pv2Gold.Name
				return pvcGoldClone
			}(),
			expectVolumeName: pv2Gold.Name,
		},
		{
			featureEnabled:   false,
			name:             "claim bind to a pv due to class name is dropped by kube-apiserver",
			volumes:          []*v1.PersistentVolume{pvGold},
			claim:            pvcGold,
			expectVolumeName: pvGold.Name,
		},
		{
			featureEnabled:   false,
			name:             "claim with nil class bind to a pv",
			volumes:          []*v1.PersistentVolume{pv},
			claim:            pvc,
			expectVolumeName: pv.Name,
		},
		{
			featureEnabled:   false,
			name:             "claim with empty class bind to a pv",
			volumes:          []*v1.PersistentVolume{pv},
			claim:            pvcEmpty,
			expectVolumeName: pv.Name,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if !tc.featureEnabled {
				featuregatetesting.SetFeatureGateEmulationVersionDuringTest(t, utilfeature.DefaultFeatureGate, version.MustParse("1.35"))
			}
			featuregatetesting.SetFeatureGateDuringTest(t, utilfeature.DefaultFeatureGate, features.VolumeAttributesClass, tc.featureEnabled)
			s := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=ServiceAccount,StorageObjectInUseProtection"}, framework.SharedEtcd())
			defer s.TearDownFn()

			tCtx := ktesting.Init(t)
			defer tCtx.Cancel("test has completed")
			testClient, controller, informers, watchPV, watchPVC := createClients(tCtx, namespaceName, t, s, defaultSyncPeriod)
			defer watchPV.Stop()
			defer watchPVC.Stop()

			ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
			defer framework.DeleteNamespaceOrDie(testClient, ns, t)

			// NOTE: 该测试不能并行运行，因为它会创建和删除 non-namespaced 对象（PersistentVolume）。
			defer func() {
				_ = testClient.CoreV1().PersistentVolumes().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})
			}()

			informers.Start(tCtx.Done())
			go controller.Run(tCtx)

			for _, volume := range tc.volumes {
				_, err = testClient.CoreV1().PersistentVolumes().Create(context.TODO(), volume, metav1.CreateOptions{})
				if err != nil {
					t.Fatalf("Failed to create PersistentVolume: %v", err)
				}
			}
			t.Log("volumes created")

			_, err = testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), tc.claim, metav1.CreateOptions{})
			if err != nil {
				t.Fatalf("Failed to create PersistentVolumeClaim: %v", err)
			}
			t.Log("claim created")

			waitForAnyPersistentVolumePhase(watchPV, v1.VolumeBound)
			t.Log("volume bound")

			waitForPersistentVolumeClaimPhase(testClient, tc.claim.Name, ns.Name, watchPVC, v1.ClaimBound)
			t.Log("claim bound")

			gotClaim, err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Get(context.TODO(), tc.claim.Name, metav1.GetOptions{})
			if err != nil {
				t.Fatalf("Unexpected error getting pvc: %v", err)
			}
			if !tc.featureEnabled {
				if gotClaim.Spec.VolumeAttributesClassName != nil || gotClaim.Status.CurrentVolumeAttributesClassName != nil {
					t.Fatalf("unexpected volume class name on claim %q", gotClaim.Name)
				}
			}

			for _, volume := range tc.volumes {
				gotVolume, err := testClient.CoreV1().PersistentVolumes().Get(context.TODO(), volume.Name, metav1.GetOptions{})
				if err != nil {
					t.Fatalf("Unexpected error getting pv: %v", err)
				}
				if !tc.featureEnabled {
					if gotVolume.Spec.VolumeAttributesClassName != nil {
						t.Fatalf("unexpected volume class name on volume %q", gotVolume.Name)
					}
				}
				if volume.Name == tc.expectVolumeName {
					if gotVolume.Spec.ClaimRef == nil {
						t.Fatalf("%s PV should be bound", volume.Name)
					}
					if gotVolume.Spec.ClaimRef.Namespace != tc.claim.Namespace || gotVolume.Spec.ClaimRef.Name != tc.claim.Name {
						t.Fatalf("Bind mismatch! Expected %s/%s but got %s/%s", tc.claim.Namespace, tc.claim.Name, gotVolume.Spec.ClaimRef.Namespace, gotVolume.Spec.ClaimRef.Name)
					}
				} else if gotVolume.Spec.ClaimRef != nil {
					t.Fatalf("%s PV shouldn't be bound", volume.Name)
				}
			}
		})
	}
}

// TestPersistentVolumeMultiPVsPVCs 测试 100 个 PVC 与 100 个 PV 的绑定。
// 可通过 KUBE_INTEGRATION_PV_* 环境变量配置。
func TestPersistentVolumeMultiPVsPVCs(t *testing.T) {
	s := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=ServiceAccount,StorageObjectInUseProtection"}, framework.SharedEtcd())
	defer s.TearDownFn()
	namespaceName := "multi-pvs-pvcs"

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, binder, informers, watchPV, watchPVC := createClients(tCtx, namespaceName, t, s, defaultSyncPeriod)
	defer watchPV.Stop()
	defer watchPVC.Stop()

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	// NOTE: 该测试不能并行运行，因为它会创建和删除 non-namespaced 对象（PersistentVolume）。
	defer testClient.CoreV1().PersistentVolumes().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})

	informers.Start(tCtx.Done())
	go binder.Run(tCtx)

	objCount := getObjectCount()
	pvs := make([]*v1.PersistentVolume, objCount)
	pvcs := make([]*v1.PersistentVolumeClaim, objCount)
	for i := range objCount {
		// 该 PV 将被 claim、release，然后删除
		pvs[i] = createPV("pv-"+strconv.Itoa(i), "/tmp/foo"+strconv.Itoa(i), "1G",
			[]v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, v1.PersistentVolumeReclaimRetain)
		pvcs[i] = createPVC("pvc-"+strconv.Itoa(i), ns.Name, "1G", []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, "")
	}

	// 先创建 PV
	klog.V(2).Infof("TestPersistentVolumeMultiPVsPVCs: start")

	// 在单独 goroutine 中创建 volume，以及早消费 watchPV 的事件：
	// 其容量似乎有限，volume 超过 3000 时会卡住。
	go func() {
		for i := range objCount {
			_, _ = testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pvs[i], metav1.CreateOptions{})
		}
	}()
	// 等待它们变为 Available
	for i := range objCount {
		waitForAnyPersistentVolumePhase(watchPV, v1.VolumeAvailable)
		klog.V(1).Infof("%d volumes available", i+1)
	}
	klog.V(2).Infof("TestPersistentVolumeMultiPVsPVCs: volumes are Available")

	// 启动单独 goroutine，在 binder 工作期间随机修改 PV 与 PVC，
	// 验证即使对象被并发修改，binder 仍能完成绑定。
	stopCh := make(chan struct{}, 0)
	go func() {
		for {
			// 随机决定修改 PV 还是 PVC
			if rand.Intn(2) == 0 {
				// 修改 PV
				i := rand.Intn(objCount)
				name := "pv-" + strconv.Itoa(i)
				pv, err := testClient.CoreV1().PersistentVolumes().Get(context.TODO(), name, metav1.GetOptions{})
				if err != nil {
					// 静默忽略错误：PV 可能已被删除或尚未创建。
					klog.V(4).Infof("Failed to read PV %s: %v", name, err)
					continue
				}
				if pv.Annotations == nil {
					pv.Annotations = map[string]string{"TestAnnotation": fmt.Sprint(rand.Int())}
				} else {
					pv.Annotations["TestAnnotation"] = fmt.Sprint(rand.Int())
				}
				_, err = testClient.CoreV1().PersistentVolumes().Update(context.TODO(), pv, metav1.UpdateOptions{})
				if err != nil {
					// 静默忽略错误：PV 可能已被 controller 更新。
					klog.V(4).Infof("Failed to update PV %s: %v", pv.Name, err)
					continue
				}
				klog.V(4).Infof("Updated PV %s", pv.Name)
			} else {
				// 修改 PVC
				i := rand.Intn(objCount)
				name := "pvc-" + strconv.Itoa(i)
				pvc, err := testClient.CoreV1().PersistentVolumeClaims(metav1.NamespaceDefault).Get(context.TODO(), name, metav1.GetOptions{})
				if err != nil {
					// 静默忽略错误：PVC 可能已被删除或尚未创建。
					klog.V(4).Infof("Failed to read PVC %s: %v", name, err)
					continue
				}
				if pvc.Annotations == nil {
					pvc.Annotations = map[string]string{"TestAnnotation": fmt.Sprint(rand.Int())}
				} else {
					pvc.Annotations["TestAnnotation"] = fmt.Sprint(rand.Int())
				}
				_, err = testClient.CoreV1().PersistentVolumeClaims(metav1.NamespaceDefault).Update(context.TODO(), pvc, metav1.UpdateOptions{})
				if err != nil {
					// 静默忽略错误：PVC 可能已被 controller 更新。
					klog.V(4).Infof("Failed to update PVC %s: %v", pvc.Name, err)
					continue
				}
				klog.V(4).Infof("Updated PVC %s", pvc.Name)
			}

			select {
			case <-stopCh:
				return
			default:
				continue
			}
		}
	}()

	// 同样在单独 goroutine 中创建 claim。
	go func() {
		for i := range objCount {
			_, _ = testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvcs[i], metav1.CreateOptions{})
		}
	}()

	// 等待 binder 完成所有 claim 的配对
	err := waitForSomePersistentVolumeClaimPhase(tCtx, testClient, namespaceName, objCount, watchPVC, v1.ClaimBound)
	if err != nil {
		t.Fatalf("Failed to wait for all claims to be bound: %v", err)
	}

	// 等待 binder 完成所有 volume 的配对
	for i := range objCount {
		waitForPersistentVolumePhase(testClient, pvs[i].Name, watchPV, v1.VolumeBound)
		klog.V(1).Infof("%d claims bound", i+1)
	}

	klog.V(2).Infof("TestPersistentVolumeMultiPVsPVCs: claims are bound")
	close(stopCh)

	// 检查每个对象都已绑定
	for i := range objCount {
		pv, err := testClient.CoreV1().PersistentVolumes().Get(context.TODO(), pvs[i].Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Unexpected error getting pv: %v", err)
		}
		if pv.Spec.ClaimRef == nil {
			t.Fatalf("PV %q is not bound", pv.Name)
		}
		klog.V(2).Infof("PV %q is bound to PVC %q", pv.Name, pv.Spec.ClaimRef.Name)

		pvc, err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Get(context.TODO(), pvcs[i].Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Unexpected error getting pvc: %v", err)
		}
		if pvc.Spec.VolumeName == "" {
			t.Fatalf("PVC %q is not bound", pvc.Name)
		}
		klog.V(2).Infof("PVC %q is bound to PV %q", pvc.Name, pvc.Spec.VolumeName)
	}
	testSleep()
}

// TestPersistentVolumeControllerStartup 测试 controller 启动行为。
// 启动时不应解绑任何 volume。
func TestPersistentVolumeControllerStartup(t *testing.T) {
	s := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=ServiceAccount,StorageObjectInUseProtection"}, framework.SharedEtcd())
	defer s.TearDownFn()
	namespaceName := "controller-startup"

	objCount := getObjectCount()

	const shortSyncPeriod = 2 * time.Second
	syncPeriod := getSyncPeriod(shortSyncPeriod)

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, binder, informers, watchPV, watchPVC := createClients(tCtx, namespaceName, t, s, shortSyncPeriod)
	defer watchPV.Stop()
	defer watchPVC.Stop()

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	// 创建已绑定的 volume 与 PVC
	pvs := make([]*v1.PersistentVolume, objCount)
	pvcs := make([]*v1.PersistentVolumeClaim, objCount)
	for i := range objCount {
		pvName := "pv-startup-" + strconv.Itoa(i)
		pvcName := "pvc-startup-" + strconv.Itoa(i)

		pvc := createPVC(pvcName, ns.Name, "1G", []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, "")
		pvc.Annotations = map[string]string{"annBindCompleted": ""}
		pvc.Spec.VolumeName = pvName
		newPVC, err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Cannot create claim %q: %v", pvc.Name, err)
		}
		// 单独一次请求写入 Bound 状态
		newPVC.Status.Phase = v1.ClaimBound
		newPVC, err = testClient.CoreV1().PersistentVolumeClaims(ns.Name).UpdateStatus(context.TODO(), newPVC, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Cannot update claim status %q: %v", pvc.Name, err)
		}
		pvcs[i] = newPVC
		// 消费 PVC 绑定前产生的所有 watch 事件，避免后续测试中误读到
		// "PVC created with Status.Phase == Pending"。
		if err := waitForAnyPersistentVolumeClaimPhase(watchPVC, v1.ClaimBound); err != nil {
			t.Fatalf("Unexpected error waiting for any pvc to be bound: %v", err)
		}

		pv := createPV(pvName, "/tmp/foo"+strconv.Itoa(i), "1G",
			[]v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, v1.PersistentVolumeReclaimRetain)
		claimRef, err := ref.GetReference(legacyscheme.Scheme, newPVC)
		if err != nil {
			klog.V(3).Infof("unexpected error getting claim reference: %v", err)
			return
		}
		pv.Spec.ClaimRef = claimRef
		newPV, err := testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv, metav1.CreateOptions{})
		if err != nil {
			t.Fatalf("Cannot create volume %q: %v", pv.Name, err)
		}
		// 单独一次请求写入 Bound 状态
		newPV.Status.Phase = v1.VolumeBound
		newPV, err = testClient.CoreV1().PersistentVolumes().UpdateStatus(context.TODO(), newPV, metav1.UpdateOptions{})
		if err != nil {
			t.Fatalf("Cannot update volume status %q: %v", pv.Name, err)
		}
		pvs[i] = newPV
		// 消费 PV 绑定前产生的所有 watch 事件，避免后续测试中误读到
		// "PV created with Status.Phase == Pending"。
		waitForAnyPersistentVolumePhase(watchPV, v1.VolumeBound)
	}

	// 所有 PV 与 PVC 已存入 etcd 后再启动 controller
	informers.Start(tCtx.Done())
	go binder.Run(tCtx)

	// 等待至少两个同步周期观察变化。在此期间不应有 volume 被 Release
	// 或 claim 变为 Lost。
	timer := time.NewTimer(2 * syncPeriod)
	defer timer.Stop()
	finished := false
	for !finished {
		select {
		case volumeEvent := <-watchPV.ResultChan():
			volume, ok := volumeEvent.Object.(*v1.PersistentVolume)
			if !ok {
				continue
			}
			if volume.Status.Phase != v1.VolumeBound {
				t.Errorf("volume %s unexpectedly changed state to %s", volume.Name, volume.Status.Phase)
			}

		case claimEvent := <-watchPVC.ResultChan():
			claim, ok := claimEvent.Object.(*v1.PersistentVolumeClaim)
			if !ok {
				continue
			}
			if claim.Status.Phase != v1.ClaimBound {
				t.Errorf("claim %s unexpectedly changed state to %s", claim.Name, claim.Status.Phase)
			}

		case <-timer.C:
			// 等待结束
			klog.V(2).Infof("Wait finished")
			finished = true
		}
	}

	// 检查每个对象都已绑定
	for i := range objCount {
		pv, err := testClient.CoreV1().PersistentVolumes().Get(context.TODO(), pvs[i].Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Unexpected error getting pv: %v", err)
		}
		if pv.Spec.ClaimRef == nil {
			t.Fatalf("PV %q is not bound", pv.Name)
		}
		klog.V(2).Infof("PV %q is bound to PVC %q", pv.Name, pv.Spec.ClaimRef.Name)

		pvc, err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Get(context.TODO(), pvcs[i].Name, metav1.GetOptions{})
		if err != nil {
			t.Fatalf("Unexpected error getting pvc: %v", err)
		}
		if pvc.Spec.VolumeName == "" {
			t.Fatalf("PVC %q is not bound", pvc.Name)
		}
		klog.V(2).Infof("PVC %q is bound to PV %q", pvc.Name, pvc.Spec.VolumeName)
	}
}

// TestPersistentVolumeProvisionMultiPVCs 测试批量 PVC 的 provisioning。
// 可通过 KUBE_INTEGRATION_PV_* 环境变量配置。
func TestPersistentVolumeProvisionMultiPVCs(t *testing.T) {
	s := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=ServiceAccount,StorageObjectInUseProtection"}, framework.SharedEtcd())
	defer s.TearDownFn()
	namespaceName := "provision-multi-pvs"

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, binder, informers, watchPV, watchPVC := createClients(tCtx, namespaceName, t, s, defaultSyncPeriod)
	defer watchPV.Stop()
	defer watchPVC.Stop()

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	// NOTE: 该测试不能并行运行，因为它会创建和删除 non-namespaced 对象
	// （PersistentVolume 和 StorageClass）。
	defer testClient.CoreV1().PersistentVolumes().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})
	defer testClient.StorageV1().StorageClasses().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})

	storageClass := storage.StorageClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: "gold",
		},
		Provisioner: provisionerPluginName,
	}
	testClient.StorageV1().StorageClasses().Create(context.TODO(), &storageClass, metav1.CreateOptions{})

	informers.Start(tCtx.Done())
	go binder.Run(tCtx)

	objCount := getObjectCount()
	pvcs := make([]*v1.PersistentVolumeClaim, objCount)
	for i := range objCount {
		pvc := createPVC("pvc-provision-"+strconv.Itoa(i), ns.Name, "1G", []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, "gold")
		pvcs[i] = pvc
	}

	klog.V(2).Infof("TestPersistentVolumeProvisionMultiPVCs: start")
	// 在单独 goroutine 中创建 claim，以及早消费 watchPVC 的事件：
	// claim 超过 3000 时会卡住。
	go func() {
		for i := range objCount {
			_, _ = testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvcs[i], metav1.CreateOptions{})
		}
	}()

	// 等待 controller 完成全部 PVC 的 provisioning 与绑定
	err := waitForSomePersistentVolumeClaimPhase(tCtx, testClient, namespaceName, objCount, watchPVC, v1.ClaimBound)
	if err != nil {
		t.Fatalf("Failed to wait for all claims to be bound: %v", err)
	}
	klog.V(2).Infof("TestPersistentVolumeProvisionMultiPVCs: claims are bound")

	// 检查已绑定的 PV 数量是否足够
	pvList, err := testClient.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to list volumes: %s", err)
	}
	if len(pvList.Items) != objCount {
		t.Fatalf("Expected to get %d volumes, got %d", objCount, len(pvList.Items))
	}
	for i := range objCount {
		pv := &pvList.Items[i]
		if pv.Status.Phase != v1.VolumeBound {
			t.Fatalf("Expected volume %s to be bound, is %s instead", pv.Name, pv.Status.Phase)
		}
		klog.V(2).Infof("PV %q is bound to PVC %q", pv.Name, pv.Spec.ClaimRef.Name)
	}

	// 删除所有 claim
	for i := range objCount {
		_ = testClient.CoreV1().PersistentVolumeClaims(ns.Name).Delete(context.TODO(), pvcs[i].Name, metav1.DeleteOptions{})
	}

	// 通过列出剩余 volume 等待 PV 被删除（delete 事件不可靠）
	for {
		volumes, err := testClient.CoreV1().PersistentVolumes().List(context.TODO(), metav1.ListOptions{})
		if err != nil {
			t.Fatalf("Failed to list volumes: %v", err)
		}

		klog.V(1).Infof("%d volumes remaining", len(volumes.Items))
		if len(volumes.Items) == 0 {
			break
		}
		time.Sleep(time.Second)
	}
	klog.V(2).Infof("TestPersistentVolumeProvisionMultiPVCs: volumes are deleted")
}

// TestPersistentVolumeMultiPVsDiffAccessModes 测试一个 PVC 从两个访问模式不同的 PV 中绑定
func TestPersistentVolumeMultiPVsDiffAccessModes(t *testing.T) {
	s := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=ServiceAccount,StorageObjectInUseProtection"}, framework.SharedEtcd())
	defer s.TearDownFn()
	namespaceName := "multi-pvs-diff-access"

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, controller, informers, watchPV, watchPVC := createClients(tCtx, namespaceName, t, s, defaultSyncPeriod)
	defer watchPV.Stop()
	defer watchPVC.Stop()

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	// NOTE: 该测试不能并行运行，因为它会创建和删除 non-namespaced 对象（PersistentVolume）。
	defer testClient.CoreV1().PersistentVolumes().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})

	informers.Start(tCtx.Done())
	go controller.Run(tCtx)

	// 该 PV 将被 claim、release，然后删除
	pvRwo := createPV("pv-rwo", "/tmp/foo", "10G",
		[]v1.PersistentVolumeAccessMode{v1.ReadWriteOnce}, v1.PersistentVolumeReclaimRetain)
	pvRwm := createPV("pv-rwm", "/tmp/bar", "10G",
		[]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}, v1.PersistentVolumeReclaimRetain)

	pvc := createPVC("pvc-rwm", ns.Name, "5G", []v1.PersistentVolumeAccessMode{v1.ReadWriteMany}, "")

	_, err := testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pvRwm, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolume: %v", err)
	}
	_, err = testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pvRwo, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolume: %v", err)
	}
	t.Log("volumes created")

	_, err = testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolumeClaim: %v", err)
	}
	t.Log("claim created")

	// 等待 controller 完成 volume 与 claim 的配对
	waitForAnyPersistentVolumePhase(watchPV, v1.VolumeBound)
	t.Log("volume bound")
	waitForPersistentVolumeClaimPhase(testClient, pvc.Name, ns.Name, watchPVC, v1.ClaimBound)
	t.Log("claim bound")

	// 只应绑定 RWM PV
	pv, err := testClient.CoreV1().PersistentVolumes().Get(context.TODO(), "pv-rwo", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error getting pv: %v", err)
	}
	if pv.Spec.ClaimRef != nil {
		t.Fatalf("ReadWriteOnce PV shouldn't be bound")
	}
	pv, err = testClient.CoreV1().PersistentVolumes().Get(context.TODO(), "pv-rwm", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error getting pv: %v", err)
	}
	if pv.Spec.ClaimRef == nil {
		t.Fatalf("ReadWriteMany PV should be bound")
	}
	if pv.Spec.ClaimRef.Name != pvc.Name {
		t.Fatalf("Bind mismatch! Expected %s but got %s", pvc.Name, pv.Spec.ClaimRef.Name)
	}

	// 删除 claim 会释放 volume
	if err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Delete(context.TODO(), pvc.Name, metav1.DeleteOptions{}); err != nil {
		t.Errorf("error deleting claim %s", pvc.Name)
	}
	t.Log("claim deleted")

	waitForAnyPersistentVolumePhase(watchPV, v1.VolumeReleased)
	t.Log("volume released")
}

// TestRetroactiveStorageClassAssignment 测试 PVC 的 storage class 回溯赋值，
// 以及 storage class 名为 nil 或 "" 的 PVC 在有无默认 StorageClass 时的绑定行为。
func TestRetroactiveStorageClassAssignment(t *testing.T) {
	s := kubeapiservertesting.StartTestServerOrDie(t, nil, []string{"--disable-admission-plugins=DefaultStorageClass"}, framework.SharedEtcd())
	defer s.TearDownFn()
	namespaceName := "retro-pvc-sc"
	defaultStorageClassName := "gold"
	storageClassName := "silver"

	tCtx := ktesting.Init(t)
	defer tCtx.Cancel("test has completed")
	testClient, binder, informers, watchPV, watchPVC := createClients(tCtx, namespaceName, t, s, defaultSyncPeriod)
	defer watchPV.Stop()
	defer watchPVC.Stop()

	ns := framework.CreateNamespaceOrDie(testClient, namespaceName, t)
	defer framework.DeleteNamespaceOrDie(testClient, ns, t)

	// NOTE: 该测试不能并行运行，因为它会创建和删除 non-namespaced 对象
	// （PersistentVolume 和 StorageClass）。
	defer testClient.CoreV1().PersistentVolumes().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})
	defer testClient.CoreV1().PersistentVolumeClaims(namespaceName).DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})
	defer testClient.StorageV1().StorageClasses().DeleteCollection(context.TODO(), metav1.DeleteOptions{}, metav1.ListOptions{})

	// 创建非默认 SC（额外的 SC，本测试中的 PVC 都不应使用）。
	nonDefaultSC := storage.StorageClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: storageClassName,
			Annotations: map[string]string{
				util.IsDefaultStorageClassAnnotation: "false",
			},
		},
		Provisioner: provisionerPluginName,
	}
	if _, err := testClient.StorageV1().StorageClasses().Create(context.TODO(), &nonDefaultSC, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create a storage class: %v", err)
	}

	informers.Start(tCtx.Done())
	go binder.Run(tCtx)

	klog.V(2).Infof("TestRetroactiveStorageClassAssignment: start")

	// 1. 验证默认 SC 不存在时，SC 为 "" 的 PV 能与 SC 为 nil 的 PVC 绑定
	//    （确认启用该特性不会破坏原有行为）。
	pv1 := createPVWithStorageClass("pv-1", "/tmp/foo", "5G", "",
		[]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}, v1.PersistentVolumeReclaimRetain)
	_, err := testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv1, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolume: %v", err)
	}

	pvc1 := createPVCWithNilStorageClass("pvc-1", ns.Name, "5G", []v1.PersistentVolumeAccessMode{v1.ReadWriteMany})
	_, err = testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc1, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolumeClaim: %v", err)
	}

	// 等待 controller 完成 volume 的配对。
	waitForPersistentVolumePhase(testClient, pv1.Name, watchPV, v1.VolumeBound)
	t.Log("volume bound")
	waitForPersistentVolumeClaimPhase(testClient, pvc1.Name, ns.Name, watchPVC, v1.ClaimBound)
	t.Log("claim bound")

	pv, err := testClient.CoreV1().PersistentVolumes().Get(context.TODO(), "pv-1", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error getting pv: %v", err)
	}
	if pv.Spec.ClaimRef == nil {
		t.Fatalf("PV %s with \"\" storage class should have been bound to PVC %s that has nil storage class", pv1.Name, pvc1.Name)
	}
	if pv.Spec.ClaimRef.Name != pvc1.Name {
		t.Fatalf("Bind mismatch! Expected %s but got %s", pvc1.Name, pv.Spec.ClaimRef.Name)
	}

	// 2. 验证回溯赋值：先创建 SC 为 nil 的 PVC，再创建默认 SC。
	pvcRetro := createPVCWithNilStorageClass("pvc-provision-noclass", ns.Name, "5G", []v1.PersistentVolumeAccessMode{v1.ReadWriteMany})
	if _, err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvcRetro, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create PVC: %v", err)
	}
	t.Log("claim created")

	// 创建默认 SC。
	defaultSC := storage.StorageClass{
		TypeMeta: metav1.TypeMeta{
			Kind: "StorageClass",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: defaultStorageClassName,
			Annotations: map[string]string{
				util.IsDefaultStorageClassAnnotation: "true",
			},
		},
		Provisioner: provisionerPluginName,
	}
	if _, err := testClient.StorageV1().StorageClasses().Create(context.TODO(), &defaultSC, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create a storage class: %v", err)
	}

	// 验证 SC 已回溯赋值给 PVC。
	if _, ok := waitForPersistentVolumeClaimStorageClass(t, pvcRetro.Name, defaultStorageClassName, watchPVC, 20*time.Second); !ok {
		t.Errorf("Expected claim %s to get a storage class %s assigned retroactively", pvcRetro.Name, defaultStorageClassName)
	}

	waitForPersistentVolumeClaimPhase(testClient, pvcRetro.Name, ns.Name, watchPVC, v1.ClaimBound)

	// 3. 验证 SC 为 nil 的新 claim 仍会绑定到 SC 为 "" 的 PV（若存在），且不会被回溯赋值 SC。
	pv3 := createPVWithStorageClass("pv-3", "/tmp/bar", "5G", "",
		[]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}, v1.PersistentVolumeReclaimRetain)
	_, err = testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv3, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolume: %v", err)
	}
	waitForPersistentVolumePhase(testClient, pv3.Name, watchPV, v1.VolumeAvailable)

	pvc3 := createPVCWithNilStorageClass("pvc-3", ns.Name, "5G", []v1.PersistentVolumeAccessMode{v1.ReadWriteMany})
	if _, err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc3, metav1.CreateOptions{}); err != nil {
		t.Errorf("Failed to create PVC: %v", err)
	}
	t.Log("claim created")

	waitForPersistentVolumeClaimPhase(testClient, pvc3.Name, ns.Name, watchPVC, v1.ClaimBound)

	pvc, err := testClient.CoreV1().PersistentVolumeClaims(ns.Name).Get(context.TODO(), "pvc-3", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error getting pv: %v", err)
	}
	if pvc.Spec.StorageClassName != nil {
		t.Errorf("claim %s should still have nil storage class because it bound to existing PV", pvc.Name)
	}

	// 再创建一个 PV，它应保持未绑定。
	pvUnbound := createPVWithStorageClass("pv-unbound", "/tmp/bar", "5G", "",
		[]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}, v1.PersistentVolumeReclaimRetain)
	_, err = testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pvUnbound, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolume: %v", err)
	}

	waitForPersistentVolumePhase(testClient, pvUnbound.Name, watchPV, v1.VolumeAvailable)

	pv, err = testClient.CoreV1().PersistentVolumes().Get(context.TODO(), "pv-unbound", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error getting pv: %v", err)
	}
	if pv.Spec.ClaimRef != nil {
		t.Fatalf("PV %s shouldn't be bound", pvUnbound.Name)
	}

	// 删除该 PV，避免干扰后续测试。
	testClient.CoreV1().PersistentVolumes().Delete(context.TODO(), pvUnbound.Name, metav1.DeleteOptions{})

	// 4. 验证默认 SC 存在时，SC 为 "" 的 PV 能与 SC 为 "" 的 PVC 绑定。
	// 确保启用该特性及默认 SC 的存在不会破坏此绑定，否则 SC 为 "" 的 PV 将无法绑定。
	pvc4 := createPVC("pvc-4", ns.Name, "5G", []v1.PersistentVolumeAccessMode{v1.ReadWriteMany}, "")
	_, err = testClient.CoreV1().PersistentVolumeClaims(ns.Name).Create(context.TODO(), pvc4, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolumeClaim: %v", err)
	}

	pv4 := createPVWithStorageClass("pv-4", "/tmp/bar", "5G", "",
		[]v1.PersistentVolumeAccessMode{v1.ReadWriteMany}, v1.PersistentVolumeReclaimRetain)
	_, err = testClient.CoreV1().PersistentVolumes().Create(context.TODO(), pv4, metav1.CreateOptions{})
	if err != nil {
		t.Errorf("Failed to create PersistentVolume: %v", err)
	}

	// 等待 controller 完成 volume 的配对。
	waitForPersistentVolumePhase(testClient, pv4.Name, watchPV, v1.VolumeBound)
	t.Log("volume bound")
	waitForPersistentVolumeClaimPhase(testClient, pvc4.Name, ns.Name, watchPVC, v1.ClaimBound)
	t.Log("claim bound")

	pv, err = testClient.CoreV1().PersistentVolumes().Get(context.TODO(), "pv-4", metav1.GetOptions{})
	if err != nil {
		t.Fatalf("Unexpected error getting pv: %v", err)
	}
	if pv.Spec.ClaimRef == nil {
		t.Fatalf("PV %s with \"\" storage class should have been bound to PVC %s that also has \"\" storage class", pv4.Name, pvc4.Name)
	}
	if pv.Spec.ClaimRef.Name != pvc4.Name {
		t.Fatalf("Bind mismatch! Expected PV %s to bind to PVC %s but instead it bound to PVC %s", pv.Name, pvc4.Name, pv.Spec.ClaimRef.Name)
	}
}

func waitForPersistentVolumePhase(client *clientset.Clientset, pvName string, w watch.Interface, phase v1.PersistentVolumePhase) {
	// 先检查 volume 是否已处于目标 phase
	volume, err := client.CoreV1().PersistentVolumes().Get(context.TODO(), pvName, metav1.GetOptions{})
	if err == nil && volume.Status.Phase == phase {
		return
	}

	// 等待 phase 变化
	for {
		event := <-w.ResultChan()
		volume, ok := event.Object.(*v1.PersistentVolume)
		if !ok {
			continue
		}
		if volume.Status.Phase == phase && volume.Name == pvName {
			klog.V(2).Infof("volume %q is %s", volume.Name, phase)
			break
		}
	}
}

func waitForPersistentVolumeClaimPhase(client *clientset.Clientset, claimName, namespace string, w watch.Interface, phase v1.PersistentVolumeClaimPhase) {
	// 先检查 claim 是否已处于目标 phase
	claim, err := client.CoreV1().PersistentVolumeClaims(namespace).Get(context.TODO(), claimName, metav1.GetOptions{})
	if err == nil && claim.Status.Phase == phase {
		return
	}

	// 等待 phase 变化
	for {
		event := <-w.ResultChan()
		claim, ok := event.Object.(*v1.PersistentVolumeClaim)
		if !ok {
			continue
		}
		if claim.Status.Phase == phase && claim.Name == claimName {
			klog.V(2).Infof("claim %q is %s", claim.Name, phase)
			break
		}
	}
}

func waitForAnyPersistentVolumePhase(w watch.Interface, phase v1.PersistentVolumePhase) {
	for {
		event := <-w.ResultChan()
		volume, ok := event.Object.(*v1.PersistentVolume)
		if !ok {
			continue
		}
		if volume.Status.Phase == phase {
			klog.V(2).Infof("volume %q is %s", volume.Name, phase)
			break
		}
	}
}

func waitForSomePersistentVolumeClaimPhase(ctx context.Context, testClient clientset.Interface, namespace string, objCount int, watchPVC watch.Interface, phase v1.PersistentVolumeClaimPhase) error {
	// 调用方无法感知新建的 watch，因此必须由这里自行停止。
	var newWatchPVC watch.Interface
	defer func() {
		if newWatchPVC != nil {
			newWatchPVC.Stop()
		}
	}()

	return wait.ExponentialBackoffWithContext(ctx, retry.DefaultBackoff, func(ctx context.Context) (bool, error) {
		for i := range objCount {
			err := waitForAnyPersistentVolumeClaimPhase(watchPVC, v1.ClaimBound)
			if err != nil {
				klog.Errorf("Failed to wait for a claim (%d/%d) to be bound: %v", i+1, objCount, err)
				klog.Info("Recreating a watch for claims and re-checking the count of bound claims")
				newWatchPVC, err = testClient.CoreV1().PersistentVolumeClaims(namespace).Watch(ctx, metav1.ListOptions{})
				if err != nil {
					return false, err
				}
				watchPVC.Stop()
				watchPVC = newWatchPVC
				// waitForAnyPersistentVolumeClaimPhase 的错误被视为无害，或可通过重建 watch
				// 解决，因此这里通过 ExponentialBackoffWithContext 继续轮询。
				return false, nil
			}
			klog.V(1).Infof("%d claims bound", i+1)
		}
		return true, nil
	})
}

func waitForAnyPersistentVolumeClaimPhase(w watch.Interface, phase v1.PersistentVolumeClaimPhase) error {
	for {
		event, ok := <-w.ResultChan()
		if !ok {
			return fmt.Errorf("watch closed")
		}
		claim, ok := event.Object.(*v1.PersistentVolumeClaim)
		if !ok {
			continue
		}
		if claim.Status.Phase == phase {
			klog.V(2).Infof("claim %q is %s", claim.Name, phase)
			return nil
		}
	}
}

func waitForPersistentVolumeClaimStorageClass(t *testing.T, claimName, scName string, w watch.Interface, duration time.Duration) (*v1.PersistentVolumeClaim, bool) {
	stopTimer := time.NewTimer(duration)
	defer stopTimer.Stop()

	// 等待 storage class 赋值
	for {
		select {
		case event := <-w.ResultChan():
			claim, ok := event.Object.(*v1.PersistentVolumeClaim)
			if ok {
				t.Logf("Watching claim %s", claim.Name)
			} else {
				t.Errorf("Watch closed unexpectedly")
			}
			if claim.Spec.StorageClassName == nil {
				t.Logf("Claim %v does not yet have expected storage class %v", claim.Name, scName)
				continue
			}
			if *claim.Spec.StorageClassName == scName && claim.Name == claimName {
				t.Logf("Claim %s now has expected storage class %s", claim.Name, *claim.Spec.StorageClassName)
				return claim, true
			}
		case <-stopTimer.C:
			return nil, false
		}

	}
}

func createClients(ctx context.Context, namespaceName string, t *testing.T, s *kubeapiservertesting.TestServer, syncPeriod time.Duration) (*clientset.Clientset, *persistentvolumecontroller.PersistentVolumeController, informers.SharedInformerFactory, watch.Interface, watch.Interface) {
	// 提高 QPS 与 Burst：存在创建大量对象的并发测试，默认值太低。
	binderConfig := restclient.CopyConfig(s.ClientConfig)
	binderConfig.QPS = 1000000
	binderConfig.Burst = 1000000
	binderClient := clientset.NewForConfigOrDie(binderConfig)
	testConfig := restclient.CopyConfig(s.ClientConfig)
	testConfig.QPS = 1000000
	testConfig.Burst = 1000000
	testClient := clientset.NewForConfigOrDie(testConfig)

	host := volumetest.NewFakeVolumeHost(t, "/tmp/fake", nil, nil)
	plugin := &volumetest.FakeVolumePlugin{
		PluginName:             provisionerPluginName,
		Host:                   host,
		Config:                 volume.VolumeConfig{},
		LastProvisionerOptions: volume.VolumeOptions{},
		NewAttacherCallCount:   0,
		NewDetacherCallCount:   0,
		Mounters:               nil,
		Unmounters:             nil,
		Attachers:              nil,
		Detachers:              nil,
	}
	plugins := []volume.VolumePlugin{plugin}
	informers := informers.NewSharedInformerFactory(testClient, getSyncPeriod(syncPeriod))
	ctrl, err := persistentvolumecontroller.NewController(
		ctx,
		persistentvolumecontroller.ControllerParameters{
			KubeClient:                binderClient,
			SyncPeriod:                getSyncPeriod(syncPeriod),
			VolumePlugins:             plugins,
			VolumeInformer:            informers.Core().V1().PersistentVolumes(),
			ClaimInformer:             informers.Core().V1().PersistentVolumeClaims(),
			ClassInformer:             informers.Storage().V1().StorageClasses(),
			PodInformer:               informers.Core().V1().Pods(),
			NodeInformer:              informers.Core().V1().Nodes(),
			EnableDynamicProvisioning: true,
		})
	if err != nil {
		t.Fatalf("Failed to construct PersistentVolumes: %v", err)
	}

	watchPV, err := testClient.CoreV1().PersistentVolumes().Watch(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to watch PersistentVolumes: %v", err)
	}
	watchPVC, err := testClient.CoreV1().PersistentVolumeClaims(namespaceName).Watch(context.TODO(), metav1.ListOptions{})
	if err != nil {
		t.Fatalf("Failed to watch PersistentVolumeClaims: %v", err)
	}

	return testClient, ctrl, informers, watchPV, watchPVC
}

func createPV(name, path, cap string, mode []v1.PersistentVolumeAccessMode, reclaim v1.PersistentVolumeReclaimPolicy) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource:        v1.PersistentVolumeSource{HostPath: &v1.HostPathVolumeSource{Path: path}},
			Capacity:                      v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse(cap)},
			AccessModes:                   mode,
			PersistentVolumeReclaimPolicy: reclaim,
		},
	}
}

func createPVWithStorageClass(name, path, cap, scName string, mode []v1.PersistentVolumeAccessMode, reclaim v1.PersistentVolumeReclaimPolicy) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource:        v1.PersistentVolumeSource{HostPath: &v1.HostPathVolumeSource{Path: path}},
			Capacity:                      v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse(cap)},
			AccessModes:                   mode,
			PersistentVolumeReclaimPolicy: reclaim,
			StorageClassName:              scName,
		},
	}
}

func createPVC(name, namespace, cap string, mode []v1.PersistentVolumeAccessMode, class string) *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			Resources:        v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse(cap)}},
			AccessModes:      mode,
			StorageClassName: &class,
		},
	}
}

func createPVCWithNilStorageClass(name, namespace, cap string, mode []v1.PersistentVolumeAccessMode) *v1.PersistentVolumeClaim {
	return &v1.PersistentVolumeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: v1.PersistentVolumeClaimSpec{
			Resources:   v1.VolumeResourceRequirements{Requests: v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse(cap)}},
			AccessModes: mode,
		},
	}
}

func createCSIPV(name, cap string, mode []v1.PersistentVolumeAccessMode, reclaim v1.PersistentVolumeReclaimPolicy) *v1.PersistentVolume {
	return &v1.PersistentVolume{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.PersistentVolumeSpec{
			PersistentVolumeSource:        v1.PersistentVolumeSource{CSI: &v1.CSIPersistentVolumeSource{Driver: "mock-driver", VolumeHandle: "volume-handle"}},
			Capacity:                      v1.ResourceList{v1.ResourceName(v1.ResourceStorage): resource.MustParse(cap)},
			AccessModes:                   mode,
			PersistentVolumeReclaimPolicy: reclaim,
		},
	}
}
