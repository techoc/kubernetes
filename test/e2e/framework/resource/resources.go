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

package resource

import (
	"context"
	"fmt"
	"time"

	"github.com/onsi/ginkgo/v2"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	clientset "k8s.io/client-go/kubernetes"
	scaleclient "k8s.io/client-go/scale"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	testutils "k8s.io/kubernetes/test/utils"
)

const (
	// gc 每秒能删除的对象数量。
	// 单次删除会发出 2 个请求。
	gcThroughput = 10
)

// ScaleResource 将资源扩缩容到指定规模。
func ScaleResource(
	ctx context.Context,
	clientset clientset.Interface,
	scalesGetter scaleclient.ScalesGetter,
	ns, name string,
	size uint,
	wait bool,
	kind schema.GroupKind,
	gvr schema.GroupVersionResource,
) error {
	ginkgo.By(fmt.Sprintf("Scaling %v %s in namespace %s to %d", kind, name, ns, size))
	if err := testutils.ScaleResourceWithRetries(scalesGetter, ns, name, size, gvr); err != nil {
		return fmt.Errorf("error while scaling RC %s to %d replicas: %w", name, size, err)
	}
	if !wait {
		return nil
	}
	return WaitForControlledPodsRunning(ctx, clientset, ns, name, kind)
}

// DeleteResourceAndWaitForGC 只删除指定的资源，然后等待 GC 删除对应的 pod。
func DeleteResourceAndWaitForGC(ctx context.Context, c clientset.Interface, kind schema.GroupKind, ns, name string) error {
	ginkgo.By(fmt.Sprintf("deleting %v %s in namespace %s, will wait for the garbage collector to delete the pods", kind, name, ns))

	rtObject, err := GetRuntimeObjectForKind(ctx, c, kind, ns, name)
	if err != nil {
		if apierrors.IsNotFound(err) {
			framework.Logf("%v %s not found: %v", kind, name, err)
			return nil
		}
		return err
	}
	deleteObject := func() error {
		background := metav1.DeletePropagationBackground
		return testutils.DeleteResource(c, kind, ns, name, metav1.DeleteOptions{PropagationPolicy: &background})
	}
	return deleteObjectAndWaitForGC(ctx, c, rtObject, deleteObject, ns, name, kind.String())
}

// DeleteCustomResourceAndWaitForGC 只删除指定的资源，然后等待 GC 删除对应的 pod。
// 允许提供自定义的资源客户端，例如用于获取 CRD 对象。
func DeleteCustomResourceAndWaitForGC(ctx context.Context, c clientset.Interface, dynamicClient dynamic.Interface, scaleClient scaleclient.ScalesGetter, gvr schema.GroupVersionResource, ns, name string) error {
	ginkgo.By(fmt.Sprintf("deleting %v %s in namespace %s, will wait for the garbage collector to delete the pods", gvr, name, ns))
	resourceClient := dynamicClient.Resource(gvr).Namespace(ns)
	_, err := resourceClient.Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			framework.Logf("%v %s not found: %v", gvr, name, err)
			return nil
		}
		return err
	}
	scaleObj, err := scaleClient.Scales(ns).Get(ctx, gvr.GroupResource(), name, metav1.GetOptions{})
	if err != nil {
		framework.Logf("error while trying to get scale subresource of kind %v with name %v: %v", gvr, name, err)
		return nil
	}
	deleteObject := func() error {
		background := metav1.DeletePropagationBackground
		return resourceClient.Delete(ctx, name, metav1.DeleteOptions{PropagationPolicy: &background})
	}
	return deleteObjectAndWaitForGC(ctx, c, scaleObj, deleteObject, ns, name, gvr.String())
}

func deleteObjectAndWaitForGC(ctx context.Context, c clientset.Interface, rtObject runtime.Object, deleteObject func() error, ns, name, description string) error {
	selector, err := GetSelectorFromRuntimeObject(rtObject)
	if err != nil {
		return err
	}
	replicas, err := GetReplicasFromRuntimeObject(rtObject)
	if err != nil {
		return err
	}

	ps, err := testutils.NewPodStore(c, ns, selector, fields.Everything())
	if err != nil {
		return err
	}

	defer ps.Stop()
	startTime := time.Now()
	if err := testutils.RetryWithExponentialBackOff(func() (bool, error) {
		err := deleteObject()
		if err == nil || apierrors.IsNotFound(err) {
			return true, nil
		}
		return false, fmt.Errorf("failed to delete object with non-retriable error: %w", err)
	}); err != nil {
		return err
	}
	deleteTime := time.Since(startTime)
	framework.Logf("Deleting %v %s took: %v", description, name, deleteTime)

	var interval, timeout time.Duration
	switch {
	case replicas < 100:
		interval = 100 * time.Millisecond
	case replicas < 1000:
		interval = 1 * time.Second
	default:
		interval = 10 * time.Second
	}
	if replicas < 5000 {
		timeout = 10 * time.Minute
	} else {
		timeout = time.Duration(replicas/gcThroughput) * time.Second
		// 现有 gcThroughput 估值偏紧，再放宽一些
		timeout = timeout + 3*time.Minute
	}

	err = waitForPodsInactive(ctx, ps, interval, timeout)
	if err != nil {
		return fmt.Errorf("error while waiting for pods to become inactive %s: %w", name, err)
	}
	terminatePodTime := time.Since(startTime) - deleteTime
	framework.Logf("Terminating %v %s pods took: %v", description, name, terminatePodTime)

	// 在 GCE 中，偶尔会有少量节点因 hostError 消失约 10 分钟。
	// 20 分钟足够重启 VM 并删除 pod。
	err = waitForPodsGone(ctx, ps, interval, 20*time.Minute)
	if err != nil {
		return fmt.Errorf("error while waiting for pods gone %s: %w", name, err)
	}
	return nil
}

// waitForPodsGone 等待 PodStore 中不再有任何 pod。
func waitForPodsGone(ctx context.Context, ps *testutils.PodStore, interval, timeout time.Duration) error {
	var pods []*v1.Pod
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		if pods = ps.List(); len(pods) == 0 {
			return true, nil
		}
		return false, nil
	})

	if wait.Interrupted(err) {
		for _, pod := range pods {
			framework.Logf("ERROR: Pod %q still exists. Node: %q", pod.Name, pod.Spec.NodeName)
		}
		return fmt.Errorf("there are %d pods left. E.g. %q on node %q", len(pods), pods[0].Name, pods[0].Spec.NodeName)
	}
	return err
}

// waitForPodsInactive 等待 PodStore 中不再有活跃 pod。
// 这样能公平比较 DeleteRCAndPods 和 DeleteRCAndWaitForGC 的删除耗时，
// 因为 pod 变为非活跃时 RC controller 就会降低 status.replicas。
func waitForPodsInactive(ctx context.Context, ps *testutils.PodStore, interval, timeout time.Duration) error {
	var activePods []*v1.Pod
	err := wait.PollUntilContextTimeout(ctx, interval, timeout, true, func(ctx context.Context) (bool, error) {
		pods := ps.List()
		activePods = e2epod.FilterActivePods(pods)
		if len(activePods) != 0 {
			return false, nil
		}
		return true, nil
	})

	if wait.Interrupted(err) {
		for _, pod := range activePods {
			framework.Logf("ERROR: Pod %q running on %q is still active", pod.Name, pod.Spec.NodeName)
		}
		return fmt.Errorf("there are %d active pods. E.g. %q on node %q", len(activePods), activePods[0].Name, activePods[0].Spec.NodeName)
	}
	return err
}

// WaitForControlledPodsRunning 最多等待 10 分钟，直到 pod 进入 Running 状态。
func WaitForControlledPodsRunning(ctx context.Context, c clientset.Interface, ns, name string, kind schema.GroupKind) error {
	rtObject, err := GetRuntimeObjectForKind(ctx, c, kind, ns, name)
	if err != nil {
		return err
	}
	selector, err := GetSelectorFromRuntimeObject(rtObject)
	if err != nil {
		return err
	}
	replicas, err := GetReplicasFromRuntimeObject(rtObject)
	if err != nil {
		return err
	}
	err = testutils.WaitForEnoughPodsWithLabelRunning(c, ns, selector, int(replicas))
	if err != nil {
		return fmt.Errorf("Error while waiting for replication controller %s pods to be running: %w", name, err)
	}
	return nil
}

// WaitForControlledPods 最多等待 podListTimeout，获取指定控制器名下的 pod 并返回。
func WaitForControlledPods(ctx context.Context, c clientset.Interface, ns, name string, kind schema.GroupKind) (pods *v1.PodList, err error) {
	rtObject, err := GetRuntimeObjectForKind(ctx, c, kind, ns, name)
	if err != nil {
		return nil, err
	}
	selector, err := GetSelectorFromRuntimeObject(rtObject)
	if err != nil {
		return nil, err
	}
	return e2epod.WaitForPodsWithLabel(ctx, c, ns, selector)
}
