/*
Copyright 2016 The Kubernetes Authors.

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

package pod

import (
	"context"
	"encoding/json"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"sync"
	"time"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/strategicpatch"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	v1core "k8s.io/client-go/kubernetes/typed/core/v1"
	"k8s.io/kubectl/pkg/util/podutils"

	"github.com/onsi/ginkgo/v2"
	ginkgotypes "github.com/onsi/ginkgo/v2/types"
	"github.com/onsi/gomega"

	"k8s.io/kubernetes/test/e2e/framework"
)

const (
	// DefaultPodDeletionTimeout 是删除 Pod 的默认超时时间。
	DefaultPodDeletionTimeout = 3 * time.Minute

	// 容器事件的状态，复制自 k8s.io/kubernetes/pkg/kubelet/events
	killingContainer = "Killing"

	// 容器事件的状态，复制自 k8s.io/kubernetes/pkg/kubelet/events
	failedToCreateContainer = "Failed"

	// 容器事件的状态，复制自 k8s.io/kubernetes/pkg/kubelet/events
	startedContainer = "Started"

	// 复制自 k8s.io/kubernetes/pkg/kubelet/sysctl
	forbiddenReason = "SysctlForbidden"

	// 是哪个测试创建的这个 Pod？
	AnnotationTestOwner = "owner.test"
)

// 全局 flag，便于按测试套件而非按 client 启用特性。
var (
	// GlobalOwnerTracking 控制新创建的 PodClient 是否自动给 Pod 标注
	// 归属测试。归属测试由 "sourcecodepath:linenumber" 标识。
	// 这种标注有助于排查清理不彻底导致 Pod 泄漏的测试。
	// 默认关闭，以最大限度保持向后兼容。
	// 另见：WithOwnerTracking、AnnotationTestOwner
	GlobalOwnerTracking bool
)

// ImagePrePullList 是当前测试套件使用的镜像。应在测试套件中初始化，
// 并在测试套件中预拉取列表里的镜像。目前仅
// node e2e 测试使用。
var ImagePrePullList sets.String

// NewPodClient 是获取 framework 命名空间中 pod client 接口的便利方法，
// 可对 pod spec 应用测试套件特定的变换，例如
// node e2e 的 Pod 调度。
func NewPodClient(f *framework.Framework) *PodClient {
	return &PodClient{
		f:             f,
		PodInterface:  f.ClientSet.CoreV1().Pods(f.Namespace.Name),
		namespace:     f.Namespace.Name,
		ownerTracking: GlobalOwnerTracking,
	}
}

// PodClientNS 是获取其他命名空间中 pod client 接口的便利方法，
// 可对 pod spec 应用测试套件特定的变换，例如
// node e2e 的 Pod 调度。
func PodClientNS(f *framework.Framework, namespace string) *PodClient {
	return &PodClient{
		f:             f,
		PodInterface:  f.ClientSet.CoreV1().Pods(namespace),
		namespace:     namespace,
		ownerTracking: GlobalOwnerTracking,
	}
}

// PodClient 是 pod client 的结构体。
type PodClient struct {
	f *framework.Framework
	v1core.PodInterface
	namespace     string
	ownerTracking bool
}

// WithOwnerTracking 控制是否自动添加注解，记录创建 Pod 的代码位置。
// 排查因清理不彻底而泄漏 Pod 的 e2e 测试（如 e2e_node）时很有帮助。
//
// 这里要浅克隆，避免修改接收者。
// 默认值是*创建 client 时*的 GlobalOwnerTracking 值。
func (c PodClient) WithOwnerTracking(value bool) *PodClient {
	c.ownerTracking = value
	return &c
}

// Create 按 framework 配置创建新 Pod（不等待其启动）。
func (c *PodClient) Create(ctx context.Context, pod *v1.Pod) *v1.Pod {
	ginkgo.GinkgoHelper()
	c.mungeSpec(pod)
	c.setOwnerAnnotation(pod)
	p, err := c.PodInterface.Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "Error creating Pod")
	return p

}

// TryCreate 尝试按给定配置创建新 Pod。
// 设计上在 Pod 创建失败时把错误返回给调用方。
func (c *PodClient) TryCreate(ctx context.Context, pod *v1.Pod) (*v1.Pod, error) {
	ginkgo.GinkgoHelper()
	c.mungeSpec(pod)
	c.setOwnerAnnotation(pod)
	p, err := c.PodInterface.Create(ctx, pod, metav1.CreateOptions{})
	if err != nil {
		return nil, fmt.Errorf("error creating pod %s/%s: %w", pod.Namespace, pod.Name, err)
	}
	return p, nil
}

// CreateSync 按 framework 配置创建新 Pod，并等待其启动、运行且就绪。
func (c *PodClient) CreateSync(ctx context.Context, pod *v1.Pod) *v1.Pod {
	ginkgo.GinkgoHelper()
	p := c.Create(ctx, pod)
	framework.ExpectNoError(WaitTimeoutForPodReadyInNamespace(ctx, c.f.ClientSet, p.Name, c.namespace, framework.PodStartTimeout))
	// Pod 运行且就绪后获取最新的 Pod，因为创建后某些状态可能变化，例如 Pod IP。
	p, err := c.Get(ctx, p.Name, metav1.GetOptions{})
	framework.ExpectNoError(err)
	return p
}

// CreateBatch 批量创建 Pod。先创建所有 Pod，再统一等待。
func (c *PodClient) CreateBatch(ctx context.Context, pods []*v1.Pod) []*v1.Pod {
	ginkgo.GinkgoHelper()
	ps := make([]*v1.Pod, len(pods))
	var wg sync.WaitGroup
	for i, pod := range pods {
		wg.Add(1)
		go func(i int, pod *v1.Pod) {
			defer wg.Done()
			defer ginkgo.GinkgoRecover()
			ps[i] = c.CreateSync(ctx, pod)
		}(i, pod)
	}
	wg.Wait()
	return ps
}

// Update 更新 Pod 对象。遇到冲突会重试，遇到
// 其他 apierror 则抛出错误。name 是 Pod 名，updateFn 是更新
// Pod 对象的函数。
func (c *PodClient) Update(ctx context.Context, name string, updateFn func(pod *v1.Pod)) {
	framework.ExpectNoError(wait.PollUntilContextTimeout(ctx, time.Millisecond*500, time.Second*30, false, func(ctx context.Context) (bool, error) {
		pod, err := c.PodInterface.Get(ctx, name, metav1.GetOptions{})
		if err != nil {
			return false, fmt.Errorf("failed to get pod %q: %w", name, err)
		}
		updateFn(pod)
		_, err = c.PodInterface.Update(ctx, pod, metav1.UpdateOptions{})
		if err == nil {
			framework.Logf("Successfully updated pod %q", name)
			return true, nil
		}
		if apierrors.IsConflict(err) {
			framework.Logf("Conflicting update to pod %q, re-get and re-update: %v", name, err)
			return false, nil
		}
		return false, fmt.Errorf("failed to update pod %q: %w", name, err)
	}))
}

// AddEphemeralContainerSync 给 Pod 添加 EphemeralContainer 并等待其运行。
func (c *PodClient) AddEphemeralContainerSync(ctx context.Context, pod *v1.Pod, ec *v1.EphemeralContainer, timeout time.Duration) error {
	podJS, err := json.Marshal(pod)
	framework.ExpectNoError(err, "error creating JSON for pod %q", FormatPod(pod))

	ecPod := pod.DeepCopy()
	ecPod.Spec.EphemeralContainers = append(ecPod.Spec.EphemeralContainers, *ec)
	ecJS, err := json.Marshal(ecPod)
	framework.ExpectNoError(err, "error creating JSON for pod with ephemeral container %q", FormatPod(pod))

	patch, err := strategicpatch.CreateTwoWayMergePatch(podJS, ecJS, pod)
	framework.ExpectNoError(err, "error creating patch to add ephemeral container %q", FormatPod(pod))

	// 客户端可乐观尝试添加 ephemeral container，以判断 EphemeralContainers 特性是否启用。
	if _, err := c.Patch(ctx, pod.Name, types.StrategicMergePatchType, patch, metav1.PatchOptions{}, "ephemeralcontainers"); err != nil {
		return err
	}

	framework.ExpectNoError(WaitForContainerRunning(ctx, c.f.ClientSet, c.namespace, pod.Name, ec.Name, timeout))
	return nil
}

// FormatPod 以一致、人类可读的格式返回 Pod 字符串，
// 包含 Pod name、namespace 和 UID。
// 这段代码取自 k/k/pkg/kubelet/util/format/pod.go，用于消除
// e2e framework 对 k/k/pkg/kubelet 的依赖。
func FormatPod(pod *v1.Pod) string {
	if pod == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%s_%s(%s)", pod.Name, pod.Namespace, pod.UID)
}

// DeleteSync 删除 Pod 并等待其消失，最长等待 `timeout`。若超时前 Pod 未
// 消失，测试失败。
func (c *PodClient) DeleteSync(ctx context.Context, name string, options metav1.DeleteOptions, timeout time.Duration) {
	err := c.Delete(ctx, name, options)
	if err != nil && !apierrors.IsNotFound(err) {
		framework.Failf("Failed to delete pod %q: %v", name, err)
	}
	framework.ExpectNoError(WaitForPodNotFoundInNamespace(ctx, c.f.ClientSet, name, c.namespace, timeout), "wait for pod %q to disappear", name)
}

// addTestOrigin 添加注解，帮助识别因清理不彻底而错误泄漏 Pod 的测试
func (c *PodClient) setOwnerAnnotation(pod *v1.Pod) {
	if !c.ownerTracking {
		return
	}
	ginkgo.GinkgoHelper()
	location := ginkgotypes.NewCodeLocation(0)
	if pod.Annotations == nil {
		pod.Annotations = make(map[string]string)
	}
	pod.Annotations[AnnotationTestOwner] = fmt.Sprintf("%s:%d", location.FileName, location.LineNumber)
}

// mungeSpec 对 pod spec 应用测试套件特定的变换。
func (c *PodClient) mungeSpec(pod *v1.Pod) {
	if !framework.TestContext.NodeE2E {
		return
	}

	gomega.Expect(pod.Spec.NodeName).To(gomega.Or(gomega.BeZero(), gomega.Equal(framework.TestContext.NodeName)), "Test misconfigured")
	pod.Spec.NodeName = framework.TestContext.NodeName
	// node e2e 不支持默认的 DNSClusterFirst 策略。改为
	// DNSDefault，该策略按节点配置。
	pod.Spec.DNSPolicy = v1.DNSDefault

	// PrepullImages 目前仅适用于 node e2e。集群 e2e 不强制预拉取镜像，
	// 因此不应修改集群 e2e Pod 的 ImagePullPolicy。
	if !framework.TestContext.PrepullImages {
		return
	}
	// 若启用了预拉取，修改 container spec 确保测试期间
	// 不会拉取镜像。
	for i := range pod.Spec.Containers {
		c := &pod.Spec.Containers[i]
		// 若 image pull policy 是 PullNever，确保镜像在预拉取列表中。
		// 注意这里跳过 localhost registry 的镜像，因为它们
		// 通常用于测试私有 registry 拉取，
		// 也不应被预拉取。
		if c.ImagePullPolicy == v1.PullNever && !strings.Contains(c.Image, "localhost") {
			gomega.Expect(ImagePrePullList.Has(c.Image)).To(gomega.BeTrueBecause("Image %q is not in the pre-pull list, consider adding it to PrePulledImages in test/e2e/common/util.go or NodePrePullImageList in test/e2e_node/image_list.go", c.Image))
		}
		// 若 image pull policy 是 PullAlways 或 PullIfNotPresent，镜像无需
		// 在允许列表或预拉取列表中，因为测试本来
		// 就会拉取它。
	}
}

// WaitForSuccess 等待 Pod 成功。
// TODO(random-liu): 把 Pod 等待函数移到本文件
func (c *PodClient) WaitForSuccess(ctx context.Context, name string, timeout time.Duration) {
	gomega.Expect(WaitForPodCondition(ctx, c.f.ClientSet, c.namespace, name, fmt.Sprintf("%s or %s", v1.PodSucceeded, v1.PodFailed), timeout,
		func(pod *v1.Pod) (bool, error) {
			switch pod.Status.Phase {
			case v1.PodFailed:
				return true, fmt.Errorf("pod %q failed with reason: %q, message: %q", name, pod.Status.Reason, pod.Status.Message)
			case v1.PodSucceeded:
				return true, nil
			default:
				return false, nil
			}
		},
	)).To(gomega.Succeed(), "wait for pod %q to succeed", name)
}

// WaitForFinish 等待 Pod 结束运行，无论成功或失败。
func (c *PodClient) WaitForFinish(ctx context.Context, name string, timeout time.Duration) {
	gomega.Expect(WaitForPodCondition(ctx, c.f.ClientSet, c.namespace, name, fmt.Sprintf("%s or %s", v1.PodSucceeded, v1.PodFailed), timeout,
		func(pod *v1.Pod) (bool, error) {
			switch pod.Status.Phase {
			case v1.PodFailed:
				return true, nil
			case v1.PodSucceeded:
				return true, nil
			default:
				return false, nil
			}
		},
	)).To(gomega.Succeed(), "wait for pod %q to finish running", name)
}

// WaitForErrorEventOrSuccess 等待 Pod 成功或出现该 Pod 的错误事件。
func (c *PodClient) WaitForErrorEventOrSuccess(ctx context.Context, pod *v1.Pod) (*v1.Event, error) {
	return c.WaitForErrorEventOrSuccessWithTimeout(ctx, pod, framework.PodStartTimeout)
}

// WaitForErrorEventOrSuccessWithTimeout 在指定时间内等待 Pod 成功或出现该 Pod 的错误事件。
func (c *PodClient) WaitForErrorEventOrSuccessWithTimeout(ctx context.Context, pod *v1.Pod, timeout time.Duration) (*v1.Event, error) {
	var ev *v1.Event
	err := wait.PollUntilContextTimeout(ctx, framework.Poll, timeout, false, func(ctx context.Context) (bool, error) {
		evnts, err := c.f.ClientSet.CoreV1().Events(pod.Namespace).Search(scheme.Scheme, pod)
		if err != nil {
			return false, fmt.Errorf("error in listing events: %w", err)
		}
		for _, e := range evnts.Items {
			switch e.Reason {
			case killingContainer, failedToCreateContainer, forbiddenReason:
				ev = &e
				return true, nil
			case startedContainer:
				return true, nil
			default:
				// 忽略其他所有错误
			}
		}
		return false, nil
	})
	return ev, err
}

// MatchContainerOutput 获取容器输出，并匹配其中预期的 regexp。
func (c *PodClient) MatchContainerOutput(ctx context.Context, name string, containerName string, expectedRegexp string) error {
	f := c.f
	output, err := GetPodLogs(ctx, f.ClientSet, f.Namespace.Name, name, containerName)
	if err != nil {
		return fmt.Errorf("failed to get output for container %q of pod %q", containerName, name)
	}
	regex, err := regexp.Compile(expectedRegexp)
	if err != nil {
		return fmt.Errorf("failed to compile regexp %q: %w", expectedRegexp, err)
	}
	if !regex.MatchString(output) {
		return fmt.Errorf("failed to match regexp %q in output %q", expectedRegexp, output)
	}
	return nil
}

// PodIsReady 判断指定 Pod 是否就绪。
func (c *PodClient) PodIsReady(ctx context.Context, name string) bool {
	pod, err := c.Get(ctx, name, metav1.GetOptions{})
	framework.ExpectNoError(err)
	return podutils.IsPodReady(pod)
}

// RemoveFinalizer 移除 Pod 的 finalizer
func (c *PodClient) RemoveFinalizer(ctx context.Context, podName string, finalizerName string) {
	framework.Logf("Removing pod's %q finalizer: %q", podName, finalizerName)
	c.Update(ctx, podName, func(pod *v1.Pod) {
		pod.ObjectMeta.Finalizers = slices.DeleteFunc(pod.ObjectMeta.Finalizers, func(actual string) bool { return actual == finalizerName })
	})
}
