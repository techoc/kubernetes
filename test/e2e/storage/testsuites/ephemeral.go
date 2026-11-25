/*
Copyright 2019 The Kubernetes Authors.

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

package testsuites

import (
	"context"
	"fmt"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
)

type ephemeralTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

// InitCustomEphemeralTestSuite 使用自定义的测试模式创建 ephemeralTestSuite，
// 该类型实现了 TestSuite 接口。
func InitCustomEphemeralTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &ephemeralTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "ephemeral",
			TestPatterns: patterns,
		},
	}
}

// GenericEphemeralTestPatterns 返回 generic ephemeral inline volume 的测试模式。
func GenericEphemeralTestPatterns() []storageframework.TestPattern {
	genericLateBinding := storageframework.DefaultFsGenericEphemeralVolume
	genericLateBinding.Name += " (late-binding)"
	genericLateBinding.BindingMode = storagev1.VolumeBindingWaitForFirstConsumer

	genericImmediateBinding := storageframework.DefaultFsGenericEphemeralVolume
	genericImmediateBinding.Name += " (immediate-binding)"
	genericImmediateBinding.BindingMode = storagev1.VolumeBindingImmediate

	return []storageframework.TestPattern{
		genericLateBinding,
		genericImmediateBinding,
		storageframework.BlockVolModeGenericEphemeralVolume,
	}
}

// CSIEphemeralTestPatterns 返回 CSI ephemeral inline volume 的测试模式。
func CSIEphemeralTestPatterns() []storageframework.TestPattern {
	return []storageframework.TestPattern{
		storageframework.DefaultFsCSIEphemeralVolume,
	}
}

// AllEphemeralTestPatterns 返回 generic 与 CSI ephemeral inline volume 的所有预定义测试模式。
func AllEphemeralTestPatterns() []storageframework.TestPattern {
	return append(GenericEphemeralTestPatterns(), CSIEphemeralTestPatterns()...)
}

// InitEphemeralTestSuite 使用测试套件的默认模式创建 ephemeralTestSuite，
// 该类型实现了 TestSuite 接口。
func InitEphemeralTestSuite() storageframework.TestSuite {
	return InitCustomEphemeralTestSuite(AllEphemeralTestPatterns())
}

func (p *ephemeralTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return p.tsInfo
}

func (p *ephemeralTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	if pattern.VolMode == v1.PersistentVolumeBlock {
		return checkIfBlockNotSupported(driver)
	}
	return ""
}

func (p *ephemeralTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config *storageframework.PerTestConfig

		testCase *EphemeralTest
		resource *storageframework.VolumeResource
	}
	var (
		dInfo   = driver.GetDriverInfo()
		eDriver storageframework.EphemeralTestDriver
		l       local
	)

	// 注意它还会注册 AfterEach，使 f 在之后不可用。所有使用 f 的代码
	// 都必须放在 It 或 Context 的回调中执行。
	f := framework.NewFrameworkWithCustomTimeouts("ephemeral", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	init := func(ctx context.Context) {
		if pattern.VolType == storageframework.CSIInlineVolume {
			eDriver, _ = driver.(storageframework.EphemeralTestDriver)
		}
		if pattern.VolType == storageframework.GenericEphemeralVolume {
			// GenericEphemeralVolume 已 GA，但测试也可能跑在
			// 尚未启用该特性的旧版本 Kubernetes 上。
			enabled, err := GenericEphemeralVolumesEnabled(ctx, f.ClientSet, f.Timeouts, f.Namespace.Name)
			framework.ExpectNoError(err, "check GenericEphemeralVolume feature")
			if !enabled {
				e2eskipper.Skipf("Cluster doesn't support %q volumes -- skipping", pattern.VolType)
			}
		}
		// driver 可能支持 Topology 能力，这与 VolumeBindingMode immediate 不兼容：
		// 卷可能被立即创建在与工作负载不同的 zone 中。
		if pattern.BindingMode == storagev1.VolumeBindingImmediate && len(dInfo.TopologyKeys) > 0 {
			e2eskipper.Skipf("VolumeBindingMode immediate is not compatible with a multi-topology environment.")
		}

		l = local{}

		if !driver.GetDriverInfo().Capabilities[storageframework.CapOnlineExpansion] {
			pattern.AllowExpansion = false
		}

		// 接下来执行开销较大的测试初始化。
		l.config = driver.PrepareTest(ctx, f)
		l.resource = storageframework.CreateVolumeResource(ctx, driver, l.config, pattern, e2evolume.SizeRange{})

		switch pattern.VolType {
		case storageframework.CSIInlineVolume:
			l.testCase = &EphemeralTest{
				Client:     l.config.Framework.ClientSet,
				Timeouts:   f.Timeouts,
				Namespace:  f.Namespace.Name,
				DriverName: eDriver.GetCSIDriverName(l.config),
				Node:       l.config.ClientNodeSelection,
				GetVolume: func(volumeNumber int) (map[string]string, bool, bool) {
					return eDriver.GetVolume(l.config, volumeNumber)
				},
			}
		case storageframework.GenericEphemeralVolume:
			l.testCase = &EphemeralTest{
				Client:    l.config.Framework.ClientSet,
				Timeouts:  f.Timeouts,
				Namespace: f.Namespace.Name,
				Node:      l.config.ClientNodeSelection,
				VolSource: l.resource.VolSource,
			}
		}
	}

	cleanup := func(ctx context.Context) {
		var cleanUpErrs []error
		cleanUpErrs = append(cleanUpErrs, l.resource.CleanupResource(ctx))
		err := utilerrors.NewAggregate(cleanUpErrs)
		framework.ExpectNoError(err, "while cleaning up")
	}

	ginkgo.It("should create read-only inline ephemeral volume", func(ctx context.Context) {
		if pattern.VolMode == v1.PersistentVolumeBlock {
			e2eskipper.Skipf("raw block volumes cannot be read-only")
		}

		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		l.testCase.ReadOnly = true
		l.testCase.RunningPodCheck = func(ctx context.Context, pod *v1.Pod) interface{} {
			command := "mount | grep /mnt/test | grep ro,"
			if framework.NodeOSDistroIs("windows") {
				// 尝试创建临时文件，并期望创建失败。
				command = "ls /mnt/test* && (touch /mnt/test-0/hello-world || true) && [ ! -f /mnt/test-0/hello-world ]"
			}
			err := e2epod.VerifyExecInPodSucceed(ctx, f, pod, command)
			framework.ExpectNoError(err, "while checking read-only mount")
			return nil
		}
		l.testCase.TestEphemeral(ctx)
	})

	ginkgo.It("should create read/write inline ephemeral volume", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		l.testCase.ReadOnly = false
		l.testCase.RunningPodCheck = func(ctx context.Context, pod *v1.Pod) interface{} {
			command := "mount | grep /mnt/test | grep rw,"
			if framework.NodeOSDistroIs("windows") {
				// 尝试创建临时文件，并期望创建成功。
				command = "ls /mnt/test* && touch /mnt/test-0/hello-world && [ -f /mnt/test-0/hello-world ]"
			}
			if pattern.VolMode == v1.PersistentVolumeBlock {
				command = "if ! [ -b /mnt/test-0 ]; then echo /mnt/test-0 is not a block device; exit 1; fi"
			}
			err := e2epod.VerifyExecInPodSucceed(ctx, f, pod, command)
			framework.ExpectNoError(err, "while checking read/write mount")
			return nil
		}
		l.testCase.TestEphemeral(ctx)
	})

	ginkgo.It("should support expansion of pvcs created for ephemeral pvcs", func(ctx context.Context) {
		if pattern.VolType != storageframework.GenericEphemeralVolume {
			e2eskipper.Skipf("Skipping %s test for expansion", pattern.VolType)
		}

		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		if !driver.GetDriverInfo().Capabilities[storageframework.CapOnlineExpansion] {
			e2eskipper.Skipf("Driver %q does not support online volume expansion - skipping", driver.GetDriverInfo().Name)
		}

		l.testCase.ReadOnly = false
		l.testCase.RunningPodCheck = func(ctx context.Context, pod *v1.Pod) interface{} {
			podName := pod.Name
			framework.Logf("Running volume expansion checks %s", podName)

			outerPodVolumeSpecName := ""
			for i := range pod.Spec.Volumes {
				volume := pod.Spec.Volumes[i]
				if volume.Ephemeral != nil {
					outerPodVolumeSpecName = volume.Name
					break
				}
			}
			pvcName := fmt.Sprintf("%s-%s", podName, outerPodVolumeSpecName)
			pvc, err := f.ClientSet.CoreV1().PersistentVolumeClaims(pod.Namespace).Get(ctx, pvcName, metav1.GetOptions{})
			framework.ExpectNoError(err, "error getting ephemeral pvc")

			ginkgo.By("Expanding current pvc")
			currentPvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
			newSize := currentPvcSize.DeepCopy()
			newSize.Add(resource.MustParse("1Gi"))
			framework.Logf("currentPvcSize %s, requested new size %s", currentPvcSize.String(), newSize.String())

			newPVC, err := ExpandPVCSize(ctx, pvc, newSize, f.ClientSet)
			framework.ExpectNoError(err, "While updating pvc for more size")
			pvc = newPVC
			gomega.Expect(pvc).NotTo(gomega.BeNil())

			pvcSize := pvc.Spec.Resources.Requests[v1.ResourceStorage]
			if pvcSize.Cmp(newSize) != 0 {
				framework.Failf("error updating pvc %s from %s to %s size", pvc.Name, currentPvcSize.String(), newSize.String())
			}

			ginkgo.By("Waiting for cloudprovider resize to finish")
			err = WaitForControllerVolumeResize(ctx, pvc, f.ClientSet, totalResizeWaitPeriod)
			framework.ExpectNoError(err, "While waiting for pvc resize to finish")

			ginkgo.By("Waiting for file system resize to finish")
			pvc, err = WaitForFSResize(ctx, pvc, f.ClientSet)
			framework.ExpectNoError(err, "while waiting for fs resize to finish")

			pvcConditions := pvc.Status.Conditions
			ExpectNoResizeConditions(pvcConditions)
			return nil
		}
		l.testCase.TestEphemeral(ctx)

	})

	ginkgo.It("should support two pods which have the same volume definition", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 如果 driver 只支持只读，就以只读模式测试，否则使用读写模式。
		// 对于 PVC，默认两者均为 false。
		shared := false
		readOnly := false
		if eDriver != nil {
			_, shared, readOnly = eDriver.GetVolume(l.config, 0)
		}

		l.testCase.RunningPodCheck = func(ctx context.Context, pod *v1.Pod) interface{} {
			// 再创建一个使用相同 inline volume 属性的 Pod。
			pod2 := StartInPodWithInlineVolume(ctx, f.ClientSet, f.Namespace.Name, "inline-volume-tester2", e2epod.InfiniteSleepCommand,
				[]v1.VolumeSource{pod.Spec.Volumes[0].VolumeSource},
				readOnly,
				l.testCase.Node)
			framework.ExpectNoError(e2epod.WaitTimeoutForPodRunningInNamespace(ctx, f.ClientSet, pod2.Name, pod2.Namespace, f.Timeouts.PodStartSlow), "waiting for second pod with inline volume")

			// 只有在成功以读写方式挂载、且 Pod 之间不共享卷数据时，
			// 才能验证一个 Pod 中写入的数据确实在另一个 Pod 中不可见。
			if pattern.VolMode != v1.PersistentVolumeBlock && !readOnly && !shared {
				ginkgo.By("writing data in one pod and checking the second does not see it (it should get its own volume)")
				err := e2epod.VerifyExecInPodSucceed(ctx, f, pod, "touch /mnt/test-0/hello-world")
				framework.ExpectNoError(err, "while writing data in first pod")
				err = e2epod.VerifyExecInPodSucceed(ctx, f, pod2, "[ ! -f /mnt/test-0/hello-world ]")
				framework.ExpectNoError(err, "while checking data in second pod")
			}

			// TestEphemeral 要求此函数返回时 Pod 已完全删除，
			// 因此不能延迟这里的清理。
			StopPodAndDependents(ctx, f.ClientSet, f.Timeouts, pod2)

			return nil
		}

		l.testCase.TestEphemeral(ctx)
	})

	ginkgo.It("should support multiple inline ephemeral volumes", func(ctx context.Context) {
		if pattern.BindingMode == storagev1.VolumeBindingImmediate &&
			pattern.VolType == storageframework.GenericEphemeralVolume {
			e2eskipper.Skipf("Multiple generic ephemeral volumes with immediate binding may cause pod startup failures when the volumes get created in separate topology segments.")
		}

		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		l.testCase.NumInlineVolumes = 2
		l.testCase.TestEphemeral(ctx)
	})
}

// EphemeralTest 表示 inline volume 测试所用的参数。
// 并非所有测试都会用到全部参数。
type EphemeralTest struct {
	Client     clientset.Interface
	Timeouts   *framework.TimeoutContext
	Namespace  string
	DriverName string
	VolSource  *v1.VolumeSource
	Node       e2epod.NodeSelection

	// GetVolume 返回指定 inline ephemeral volume 的属性，编号从 #0 开始。
	// 某些测试可能需要多个卷；它们可以相同也可以不同，取决于 driver
	// 支持什么以及想要测试什么。
	//
	// 对每个卷，测试 driver 可以指定其属性、使用相同属性的两个 Pod
	// 是否会共享同一后端存储（即一个 Pod 中的改动在另一个 Pod 中可见），
	// 以及该卷能否以读写方式挂载，还是只能只读挂载。
	GetVolume func(volumeNumber int) (attributes map[string]string, shared bool, readOnly bool)

	// RunningPodCheck 在带有 inline volume 的 Pod 运行期间调用。
	// 可对 Pod 及其卷执行额外检查；返回的数据会传给 StoppedPodCheck。
	RunningPodCheck func(ctx context.Context, pod *v1.Pod) interface{}

	// StoppedPodCheck 在确认 Pod 已消失后调用。
	// 它接收 RunningPodCheck 收集的数据（未定义时为 nil），
	// 可在节点上做进一步检查，例如验证 ephemeral volume 确实已被移除。
	// 具体如何检查取决于 driver，不在通用存储测试套件覆盖范围内。
	StoppedPodCheck func(ctx context.Context, nodeName string, runningPodData interface{})

	// NumInlineVolumes 设置每个 Pod 中 ephemeral inline volume 的数量。
	// 未设置（即零）等同于 1。
	NumInlineVolumes int

	// ReadOnly 限制只能以只读方式挂载。
	ReadOnly bool
}

// TestEphemeral 测试创建带有一个 ephemeral volume 的 Pod。
func (t EphemeralTest) TestEphemeral(ctx context.Context) {
	client := t.Client
	gomega.Expect(client).NotTo(gomega.BeNil(), "EphemeralTest.Client is required")

	ginkgo.By(fmt.Sprintf("checking the requested inline volume exists in the pod running on node %+v", t.Node))

	var volumes []v1.VolumeSource
	numVolumes := t.NumInlineVolumes
	if numVolumes == 0 {
		numVolumes = 1
	}
	for i := 0; i < numVolumes; i++ {
		var volume v1.VolumeSource
		switch {
		case t.GetVolume != nil:
			attributes, _, readOnly := t.GetVolume(i)
			if readOnly && !t.ReadOnly {
				e2eskipper.Skipf("inline ephemeral volume #%d is read-only, but the test needs a read/write volume", i)
			}
			volume = v1.VolumeSource{
				CSI: &v1.CSIVolumeSource{
					Driver:           t.DriverName,
					VolumeAttributes: attributes,
				},
			}
		case t.VolSource != nil:
			volume = *t.VolSource
		default:
			framework.Failf("EphemeralTest has neither GetVolume nor VolSource")
		}
		volumes = append(volumes, volume)
	}
	pod := StartInPodWithInlineVolume(ctx, client, t.Namespace, "inline-volume-tester", e2epod.InfiniteSleepCommand, volumes, t.ReadOnly, t.Node)
	defer func() {
		// 此时 pod 可能已经为 nil。
		StopPodAndDependents(ctx, client, t.Timeouts, pod)
	}()
	framework.ExpectNoError(e2epod.WaitTimeoutForPodRunningInNamespace(ctx, client, pod.Name, pod.Namespace, t.Timeouts.PodStartSlow), "waiting for pod with inline volume")
	runningPod, err := client.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, "get pod")
	actualNodeName := runningPod.Spec.NodeName

	// 对运行中的 Pod 执行检查。
	var runningPodData interface{}
	if t.RunningPodCheck != nil {
		runningPodData = t.RunningPodCheck(ctx, pod)
	}

	StopPodAndDependents(ctx, client, t.Timeouts, pod)
	pod = nil // 避免重复停止。

	// 此时 namespace 中不应残留 PVC。对于 generic ephemeral volume，
	// 一旦出错则可能出现残留。
	pvcs, err := client.CoreV1().PersistentVolumeClaims(t.Namespace).List(ctx, metav1.ListOptions{})
	framework.ExpectNoError(err, "list PVCs")
	gomega.Expect(pvcs.Items).Should(gomega.BeEmpty(), "no dangling PVCs")

	if t.StoppedPodCheck != nil {
		t.StoppedPodCheck(ctx, actualNodeName, runningPodData)
	}
}

// StartInPodWithInlineVolume 在一个 Pod 中启动命令，并将给定的卷挂载到 /mnt/test-<number> 目录。
// 调用方负责检查该 Pod 并在结束后删除它。
func StartInPodWithInlineVolume(ctx context.Context, c clientset.Interface, ns, podName, command string, volumes []v1.VolumeSource, readOnly bool, node e2epod.NodeSelection) *v1.Pod {
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: podName + "-",
			Labels: map[string]string{
				"app": podName,
			},
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "csi-volume-tester",
					Image: e2epod.GetDefaultTestImage(),
					// 注意：/bin/sh 在 agnhost 和 busybox 上都可用。
					Command: []string{"/bin/sh", "-c", command},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
		},
	}
	e2epod.SetNodeSelection(&pod.Spec, node)

	for i, volume := range volumes {
		name := fmt.Sprintf("my-volume-%d", i)
		path := fmt.Sprintf("/mnt/test-%d", i)
		if volume.Ephemeral != nil && volume.Ephemeral.VolumeClaimTemplate.Spec.VolumeMode != nil &&
			*volume.Ephemeral.VolumeClaimTemplate.Spec.VolumeMode == v1.PersistentVolumeBlock {
			pod.Spec.Containers[0].VolumeDevices = append(pod.Spec.Containers[0].VolumeDevices,
				v1.VolumeDevice{
					Name:       name,
					DevicePath: path,
				})
		} else {
			pod.Spec.Containers[0].VolumeMounts = append(pod.Spec.Containers[0].VolumeMounts,
				v1.VolumeMount{
					Name:      name,
					MountPath: path,
					ReadOnly:  readOnly,
				})
		}
		pod.Spec.Volumes = append(pod.Spec.Volumes,
			v1.Volume{
				Name:         name,
				VolumeSource: volume,
			})
	}

	pod, err := c.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "failed to create pod")
	return pod
}

// CSIInlineVolumesEnabled 检查当前集群是否启用了 CSIInlineVolumes feature gate。
// 它尝试创建一个使用该特性的 Pod 来进行判断。
func CSIInlineVolumesEnabled(ctx context.Context, c clientset.Interface, t *framework.TimeoutContext, ns string) (bool, error) {
	return VolumeSourceEnabled(ctx, c, t, ns, v1.VolumeSource{
		CSI: &v1.CSIVolumeSource{
			Driver: "no-such-driver.example.com",
		},
	})
}

// GenericEphemeralVolumesEnabled 检查当前集群是否启用了 GenericEphemeralVolume feature gate。
// 它尝试创建一个使用该特性的 Pod 来进行判断。
func GenericEphemeralVolumesEnabled(ctx context.Context, c clientset.Interface, t *framework.TimeoutContext, ns string) (bool, error) {
	storageClassName := "no-such-storage-class"
	return VolumeSourceEnabled(ctx, c, t, ns, v1.VolumeSource{
		Ephemeral: &v1.EphemeralVolumeSource{
			VolumeClaimTemplate: &v1.PersistentVolumeClaimTemplate{
				Spec: v1.PersistentVolumeClaimSpec{
					StorageClassName: &storageClassName,
					AccessModes:      []v1.PersistentVolumeAccessMode{v1.ReadWriteOnce},
					Resources: v1.VolumeResourceRequirements{
						Requests: v1.ResourceList{
							v1.ResourceStorage: resource.MustParse("1Gi"),
						},
					},
				},
			},
		},
	})
}

// VolumeSourceEnabled 通过尝试创建使用某种 volumeSource 的 Pod，
// 来检查该类型是否已启用。
func VolumeSourceEnabled(ctx context.Context, c clientset.Interface, t *framework.TimeoutContext, ns string, volume v1.VolumeSource) (bool, error) {
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: "inline-volume-",
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  "volume-tester",
					Image: "no-such-registry/no-such-image",
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "my-volume",
							MountPath: "/mnt/test",
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
			Volumes: []v1.Volume{
				{
					Name:         "my-volume",
					VolumeSource: volume,
				},
			},
		},
	}

	pod, err := c.CoreV1().Pods(ns).Create(ctx, pod, metav1.CreateOptions{})

	switch {
	case err == nil:
		// Pod 创建成功，说明该特性受支持。
		StopPodAndDependents(ctx, c, t, pod)
		return true, nil
	case apierrors.IsInvalid(err):
		// 报 "Invalid" 是因为用到了不支持的特性。
		return false, nil
	default:
		// 非预期错误。
		return false, err
	}
}
