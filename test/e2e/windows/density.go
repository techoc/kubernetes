/*
Copyright 2018 The Kubernetes Authors.

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

package windows

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2emetrics "k8s.io/kubernetes/test/e2e/framework/metrics"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
)

var _ = sigDescribe(feature.Windows, "Density", framework.WithSerial(), framework.WithSlow(), skipUnlessWindows(func() {
	f := framework.NewDefaultFramework("density-test-windows")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	ginkgo.Context("create a batch of pods", func() {
		// TODO(coufon): 这些值比较宽松，应根据基准数据设置更精确的限值
		// 并增加更多测试
		dTests := []densityTest{
			{
				podsNr:   10,
				interval: 0 * time.Millisecond,
				// 单个 pod 启动延迟的百分位限值
				podStartupLimits: e2emetrics.LatencyMetric{
					Perc50: 30 * time.Second,
					Perc90: 54 * time.Second,
					Perc99: 59 * time.Second,
				},
				// 一批 pod 启动延迟的上界
				podBatchStartupLimit: 10 * time.Minute,
			},
		}

		for _, testArg := range dTests {
			desc := fmt.Sprintf("latency/resource should be within limit when create %d pods with %v interval", testArg.podsNr, testArg.interval)
			ginkgo.It(desc, func(ctx context.Context) {
				testArg.createMethod = "batch"
				runDensityBatchTest(ctx, f, testArg)
			})
		}
	})

}))

type densityTest struct {
	// pod 数量
	podsNr int
	// 创建 pod 的间隔（速率控制）
	interval time.Duration
	// 以 'batch' 或 'sequence' 方式创建 pod
	createMethod string
	// API QPS 限制
	APIQPSLimit int
	// 性能限值
	podStartupLimits     e2emetrics.LatencyMetric
	podBatchStartupLimit time.Duration
}

// runDensityBatchTest 运行批量创建 pod 的密度测试
func runDensityBatchTest(ctx context.Context, f *framework.Framework, testArg densityTest) (time.Duration, []e2emetrics.PodLatencyData) {
	const (
		podType = "density_test_pod"
	)
	var (
		mutex      = &sync.Mutex{}
		watchTimes = make(map[string]metav1.Time)
		stopCh     = make(chan struct{})
	)

	// 创建测试 pod 数据结构
	pods := newDensityTestPods(testArg.podsNr, false, imageutils.GetPauseImageName(), podType)

	// controller 监听 pod 状态变化
	controller := newInformerWatchPod(ctx, f, mutex, watchTimes, podType)
	go controller.Run(stopCh)
	defer close(stopCh)

	ginkgo.By("Creating a batch of pods")
	// 它返回 map['pod name']'creation time'，包含创建时间戳
	createTimes := createBatchPodWithRateControl(ctx, f, pods, testArg.interval)

	ginkgo.By("Waiting for all Pods to be observed by the watch...")

	gomega.Eventually(ctx, func() bool {
		return len(watchTimes) == testArg.podsNr
	}, 10*time.Minute, 10*time.Second).Should(gomega.BeTrueBecause("All pods should be observed by the watch."))

	if len(watchTimes) < testArg.podsNr {
		framework.Failf("Timeout reached waiting for all Pods to be observed by the watch.")
	}

	// 分析结果
	var (
		firstCreate metav1.Time
		lastRunning metav1.Time
		init        = true
		e2eLags     = make([]e2emetrics.PodLatencyData, 0)
	)

	for name, create := range createTimes {
		watch, ok := watchTimes[name]
		if !ok {
			framework.Failf("pod %s failed to be observed by the watch", name)
		}

		e2eLags = append(e2eLags,
			e2emetrics.PodLatencyData{Name: name, Latency: watch.Time.Sub(create.Time)})

		if !init {
			if firstCreate.Time.After(create.Time) {
				firstCreate = create
			}
			if lastRunning.Time.Before(watch.Time) {
				lastRunning = watch
			}
		} else {
			init = false
			firstCreate, lastRunning = create, watch
		}
	}

	sort.Sort(e2emetrics.LatencySlice(e2eLags))
	batchLag := lastRunning.Time.Sub(firstCreate.Time)

	deletePodsSync(ctx, f, pods)

	return batchLag, e2eLags
}

// createBatchPodWithRateControl 并发创建一批 pod，每次创建用一个 goroutine。
// 创建之间留有间隔以控制吞吐量
func createBatchPodWithRateControl(ctx context.Context, f *framework.Framework, pods []*v1.Pod, interval time.Duration) map[string]metav1.Time {
	createTimes := make(map[string]metav1.Time)
	for _, pod := range pods {
		createTimes[pod.ObjectMeta.Name] = metav1.Now()
		go e2epod.NewPodClient(f).Create(ctx, pod)
		time.Sleep(interval)
	}
	return createTimes
}

// newInformerWatchPod 创建一个 informer，检查所有 pod 是否都在运行。
func newInformerWatchPod(ctx context.Context, f *framework.Framework, mutex *sync.Mutex, watchTimes map[string]metav1.Time, podType string) cache.Controller {
	ns := f.Namespace.Name
	checkPodRunning := func(p *v1.Pod) {
		mutex.Lock()
		defer mutex.Unlock()
		defer ginkgo.GinkgoRecover()

		if p.Status.Phase == v1.PodRunning {
			if _, found := watchTimes[p.Name]; !found {
				watchTimes[p.Name] = metav1.Now()
			}
		}
	}

	_, controller := cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
				options.LabelSelector = labels.SelectorFromSet(labels.Set{"type": podType}).String()
				obj, err := f.ClientSet.CoreV1().Pods(ns).List(ctx, options)
				return runtime.Object(obj), err
			},
			WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
				options.LabelSelector = labels.SelectorFromSet(labels.Set{"type": podType}).String()
				return f.ClientSet.CoreV1().Pods(ns).Watch(ctx, options)
			},
		},
		&v1.Pod{},
		0,
		cache.ResourceEventHandlerFuncs{
			AddFunc: func(obj interface{}) {
				p, ok := obj.(*v1.Pod)
				if !ok {
					framework.Failf("expected Pod, got %T", obj)
				}
				go checkPodRunning(p)
			},
			UpdateFunc: func(oldObj, newObj interface{}) {
				p, ok := newObj.(*v1.Pod)
				if !ok {
					framework.Failf("expected Pod, got %T", newObj)
				}
				go checkPodRunning(p)
			},
		},
	)
	return controller
}

// newDensityTestPods 为测试创建一组 pod（specification）。
func newDensityTestPods(numPods int, volume bool, imageName, podType string) []*v1.Pod {
	var pods []*v1.Pod

	for range numPods {

		podName := "test-" + string(uuid.NewUUID())
		pod := v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: podName,
				Labels: map[string]string{
					"type": podType,
					"name": podName,
				},
			},
			Spec: v1.PodSpec{
				// 重启策略始终是（默认的）。
				Containers: []v1.Container{
					{
						Image: imageName,
						Name:  podName,
					},
				},
				NodeSelector: map[string]string{
					"kubernetes.io/os": "windows",
				},
			},
		}

		if volume {
			pod.Spec.Containers[0].VolumeMounts = []v1.VolumeMount{
				{MountPath: "/test-volume-mnt", Name: podName + "-volume"},
			}
			pod.Spec.Volumes = []v1.Volume{
				{Name: podName + "-volume", VolumeSource: v1.VolumeSource{EmptyDir: &v1.EmptyDirVolumeSource{}}},
			}
		}

		pods = append(pods, &pod)
	}

	return pods
}

// deletePodsSync 删除一组 pod 并阻塞直到它们消失。
func deletePodsSync(ctx context.Context, f *framework.Framework, pods []*v1.Pod) {
	var wg sync.WaitGroup
	for _, pod := range pods {
		wg.Add(1)
		go func(pod *v1.Pod) {
			defer ginkgo.GinkgoRecover()
			defer wg.Done()

			err := e2epod.NewPodClient(f).Delete(ctx, pod.ObjectMeta.Name, *metav1.NewDeleteOptions(30))
			framework.ExpectNoError(err)

			err = e2epod.WaitForPodNotFoundInNamespace(ctx, f.ClientSet, pod.ObjectMeta.Name, f.Namespace.Name, 10*time.Minute)
			framework.ExpectNoError(err)
		}(pod)
	}
	wg.Wait()
}
