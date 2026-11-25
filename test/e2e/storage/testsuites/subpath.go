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

package testsuites

import (
	"context"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/kubernetes/test/e2e/framework"
	e2ekubectl "k8s.io/kubernetes/test/e2e/framework/kubectl"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	storageutils "k8s.io/kubernetes/test/e2e/storage/utils"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

var (
	volumePath      = "/test-volume"
	volumeName      = "test-volume"
	probeVolumePath = "/probe-volume"
	probeFilePath   = probeVolumePath + "/probe-file"
	fileName        = "test-file"
	retryDuration   = 20
)

type subPathTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

// InitCustomSubPathTestSuite 返回一个使用自定义测试模式的 subPathTestSuite，
// 它实现了 TestSuite 接口。
func InitCustomSubPathTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &subPathTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "subPath",
			TestPatterns: patterns,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Mi",
			},
		},
	}
}

// InitSubPathTestSuite 返回一个使用默认测试模式的 subPathTestSuite，
// 它实现了 TestSuite 接口。
func InitSubPathTestSuite() storageframework.TestSuite {
	patterns := []storageframework.TestPattern{
		storageframework.DefaultFsInlineVolume,
		storageframework.DefaultFsPreprovisionedPV,
		storageframework.DefaultFsDynamicPV,
		storageframework.NtfsDynamicPV,
	}
	return InitCustomSubPathTestSuite(patterns)
}

func (s *subPathTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return s.tsInfo
}

func (s *subPathTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	return checkVolTypePatterns(pattern, driver, storageframework.NewVolTypeMap(
		storageframework.PreprovisionedPV,
		storageframework.InlineVolume))
}

func (s *subPathTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config *storageframework.PerTestConfig

		hostExec          storageutils.HostExec
		resource          *storageframework.VolumeResource
		roVolSource       *v1.VolumeSource
		pod               *v1.Pod
		formatPod         *v1.Pod
		subPathDir        string
		filePathInSubpath string
		filePathInVolume  string

		migrationCheck *migrationOpCheck
	}
	var l local

	// 注意它还会注册一个 AfterEach，使 f 失效；所有使用 f 的代码都必须放在 It 或 Context 回调内。
	f := framework.NewFrameworkWithCustomTimeouts("provisioning", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	init := func(ctx context.Context) {
		l = local{}

		// 现在执行开销较大的测试初始化。
		l.config = driver.PrepareTest(ctx, f)
		l.migrationCheck = newMigrationOpCheck(ctx, f.ClientSet, f.ClientConfig(), driver.GetDriverInfo().InTreePluginName)
		testVolumeSizeRange := s.GetTestSuiteInfo().SupportedSizeRange
		l.resource = storageframework.CreateVolumeResource(ctx, driver, l.config, pattern, testVolumeSizeRange)
		l.hostExec = storageutils.NewHostExec(f)

		// 准备 subPath 测试依赖的资源
		volType := pattern.VolType
		switch volType {
		case storageframework.InlineVolume:
			if iDriver, ok := driver.(storageframework.InlineVolumeTestDriver); ok {
				l.roVolSource = iDriver.GetVolumeSource(true, pattern.FsType, l.resource.Volume)
			}
		case storageframework.PreprovisionedPV:
			l.roVolSource = &v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: l.resource.Pvc.Name,
					ReadOnly:  true,
				},
			}
		case storageframework.DynamicPV:
			l.roVolSource = &v1.VolumeSource{
				PersistentVolumeClaim: &v1.PersistentVolumeClaimVolumeSource{
					ClaimName: l.resource.Pvc.Name,
					ReadOnly:  true,
				},
			}
		default:
			framework.Failf("SubPath test doesn't support: %s", volType)
		}

		subPath := f.Namespace.Name
		l.pod = SubpathTestPod(f, subPath, string(volType), l.resource.VolSource, admissionapi.LevelPrivileged)
		e2epod.SetNodeSelection(&l.pod.Spec, l.config.ClientNodeSelection)

		l.formatPod = volumeFormatPod(f, l.resource.VolSource)
		e2epod.SetNodeSelection(&l.formatPod.Spec, l.config.ClientNodeSelection)

		l.subPathDir = filepath.Join(volumePath, subPath)
		l.filePathInSubpath = filepath.Join(volumePath, fileName)
		l.filePathInVolume = filepath.Join(l.subPathDir, fileName)
	}

	cleanup := func(ctx context.Context) {
		var errs []error
		if l.pod != nil {
			ginkgo.By("Deleting pod")
			err := e2epod.DeletePodWithWait(ctx, f.ClientSet, l.pod)
			errs = append(errs, err)
			l.pod = nil
		}

		if l.resource != nil {
			errs = append(errs, l.resource.CleanupResource(ctx))
			l.resource = nil
		}

		framework.ExpectNoError(errors.NewAggregate(errs), "while cleaning up resource")

		if l.hostExec != nil {
			l.hostExec.Cleanup(ctx)
		}

		l.migrationCheck.validateMigrationVolumeOpCounts(ctx)
	}

	driverName := driver.GetDriverInfo().Name

	ginkgo.It("should support non-existent path", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 由 init container 1 在 subPath 内写文件
		setWriteCommand(l.filePathInSubpath, &l.pod.Spec.InitContainers[1])

		// 由 container 1 在 subPath 外读取该文件
		testReadFile(ctx, f, l.filePathInVolume, l.pod, 1)
	})

	ginkgo.It("should support existing directory", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 创建目录
		setInitCommand(l.pod, fmt.Sprintf("mkdir -p %s", l.subPathDir))

		// 由 init container 1 在 subPath 内写文件
		setWriteCommand(l.filePathInSubpath, &l.pod.Spec.InitContainers[1])

		// 由 container 1 在 subPath 外读取该文件
		testReadFile(ctx, f, l.filePathInVolume, l.pod, 1)
	})

	ginkgo.It("should support existing single file [LinuxOnly]", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 在 init container 中创建文件
		setInitCommand(l.pod, fmt.Sprintf("mkdir -p %s; echo \"mount-tester new file\" > %s", l.subPathDir, l.filePathInVolume))

		// 由 container 0 在 subPath 内读取该文件
		testReadFile(ctx, f, l.filePathInSubpath, l.pod, 0)
	})

	ginkgo.It("should support file as subpath [LinuxOnly]", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 在 init container 中创建文件
		setInitCommand(l.pod, fmt.Sprintf("echo %s > %s", f.Namespace.Name, l.subPathDir))

		TestBasicSubpath(ctx, f, f.Namespace.Name, l.pod)
	})

	f.It("should fail if subpath directory is outside the volume", f.WithSlow(), "[LinuxOnly]", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 在卷之外创建 subpath
		var command string
		if framework.NodeOSDistroIs("windows") {
			command = fmt.Sprintf("New-Item -ItemType SymbolicLink -Path %s -value \\Windows", l.subPathDir)
		} else {
			command = fmt.Sprintf("ln -s /bin %s", l.subPathDir)
		}
		setInitCommand(l.pod, command)
		// Pod 应当失败
		testPodFailSubpath(ctx, f, l.pod, false)
	})

	f.It("should fail if subpath file is outside the volume", f.WithSlow(), "[LinuxOnly]", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 在卷之外创建 subpath
		setInitCommand(l.pod, fmt.Sprintf("ln -s /bin/sh %s", l.subPathDir))

		// Pod 应当失败
		testPodFailSubpath(ctx, f, l.pod, false)
	})

	f.It("should fail if non-existent subpath is outside the volume", f.WithSlow(), "[LinuxOnly]", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 在卷之外创建 subpath
		setInitCommand(l.pod, fmt.Sprintf("ln -s /bin/notanexistingpath %s", l.subPathDir))

		// Pod 应当失败
		testPodFailSubpath(ctx, f, l.pod, false)
	})

	f.It("should fail if subpath with backstepping is outside the volume", f.WithSlow(), "[LinuxOnly]", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 在卷之外创建 subpath
		var command string
		if framework.NodeOSDistroIs("windows") {
			command = fmt.Sprintf("New-Item -ItemType SymbolicLink -Path %s -value ..\\", l.subPathDir)
		} else {
			command = fmt.Sprintf("ln -s ../ %s", l.subPathDir)
		}
		setInitCommand(l.pod, command)
		// Pod 应当失败
		testPodFailSubpath(ctx, f, l.pod, false)
	})

	f.It("should support creating multiple subpath from same volumes", f.WithSlow(), func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		subpathDir1 := filepath.Join(volumePath, "subpath1")
		subpathDir2 := filepath.Join(volumePath, "subpath2")
		filepath1 := filepath.Join("/test-subpath1", fileName)
		filepath2 := filepath.Join("/test-subpath2", fileName)
		setInitCommand(l.pod, fmt.Sprintf("mkdir -p %s; mkdir -p %s", subpathDir1, subpathDir2))

		addSubpathVolumeContainer(&l.pod.Spec.Containers[0], v1.VolumeMount{
			Name:      volumeName,
			MountPath: "/test-subpath1",
			SubPath:   "subpath1",
		})
		addSubpathVolumeContainer(&l.pod.Spec.Containers[0], v1.VolumeMount{
			Name:      volumeName,
			MountPath: "/test-subpath2",
			SubPath:   "subpath2",
		})

		// 由 container 0 写入文件并立即读回
		addMultipleWrites(&l.pod.Spec.Containers[0], filepath1, filepath2)
		testMultipleReads(ctx, f, l.pod, 0, filepath1, filepath2)
	})

	f.It("should support restarting containers using directory as subpath", f.WithSlow(), func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 创建目录
		var command string
		command = fmt.Sprintf("mkdir -p %v; touch %v", l.subPathDir, probeFilePath)
		setInitCommand(l.pod, command)
		testPodContainerRestart(ctx, f, l.pod)
	})

	f.It("should support restarting containers using file as subpath", f.WithSlow(), "[LinuxOnly]", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 创建文件
		setInitCommand(l.pod, fmt.Sprintf("touch %v; touch %v", l.subPathDir, probeFilePath))

		testPodContainerRestart(ctx, f, l.pod)
	})

	f.It("should unmount if pod is gracefully deleted while kubelet is down", f.WithDisruptive(), f.WithSlow(), "[LinuxOnly]", func(ctx context.Context) {
		e2eskipper.SkipUnlessSSHKeyPresent()
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		if strings.HasPrefix(driverName, "hostPath") {
			// TODO: #61446 修复后应移除此跳过逻辑
			e2eskipper.Skipf("Driver %s does not support reconstruction, skipping", driverName)
		}

		testSubpathReconstruction(ctx, f, l.hostExec, l.pod, false)
	})

	f.It("should unmount if pod is force deleted while kubelet is down", f.WithDisruptive(), f.WithSlow(), "[LinuxOnly]", func(ctx context.Context) {
		e2eskipper.SkipUnlessSSHKeyPresent()
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		if strings.HasPrefix(driverName, "hostPath") {
			// TODO: #61446 修复后应移除此跳过逻辑
			e2eskipper.Skipf("Driver %s does not support reconstruction, skipping", driverName)
		}

		testSubpathReconstruction(ctx, f, l.hostExec, l.pod, true)
	})

	f.It("should remount stale subpath bind mount after network filesystem disruption [LinuxOnly]",
		f.WithDisruptive(), f.WithSlow(), func(ctx context.Context) {
			init(ctx)
			ginkgo.DeferCleanup(cleanup)

			if strings.HasPrefix(driverName, "hostPath") {
				e2eskipper.Skipf("Driver %s uses a local hostPath volume; stale-mount scenario requires a network filesystem, skipping", driverName)
			}

			testSubpathStaleBindMountRemount(ctx, f, l.pod)
		})

	ginkgo.It("should support readOnly directory specified in the volumeMount", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 创建目录
		setInitCommand(l.pod, fmt.Sprintf("mkdir -p %s", l.subPathDir))

		// 由 init container 2 在卷中写文件
		setWriteCommand(l.filePathInVolume, &l.pod.Spec.InitContainers[2])

		// 由 container 0 在 subPath 内读取该文件
		l.pod.Spec.Containers[0].VolumeMounts[0].ReadOnly = true
		testReadFile(ctx, f, l.filePathInSubpath, l.pod, 0)
	})

	ginkgo.It("should support readOnly file specified in the volumeMount [LinuxOnly]", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 创建文件
		setInitCommand(l.pod, fmt.Sprintf("touch %s", l.subPathDir))

		// 由 init container 2 在卷中写文件
		setWriteCommand(l.subPathDir, &l.pod.Spec.InitContainers[2])

		// 由 container 0 在 subPath 内读取该文件
		l.pod.Spec.Containers[0].VolumeMounts[0].ReadOnly = true
		testReadFile(ctx, f, volumePath, l.pod, 0)
	})

	ginkgo.It("should support existing directories when readOnly specified in the volumeSource", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)
		if l.roVolSource == nil {
			e2eskipper.Skipf("Driver %s on volume type %s doesn't support readOnly source", driverName, pattern.VolType)
		}

		origpod := l.pod.DeepCopy()

		// 创建目录
		setInitCommand(l.pod, fmt.Sprintf("mkdir -p %s", l.subPathDir))

		// 由 init container 1 在 subPath 内写文件
		setWriteCommand(l.filePathInSubpath, &l.pod.Spec.InitContainers[1])

		// 由 container 0 在 subPath 内读取该文件
		testReadFile(ctx, f, l.filePathInSubpath, l.pod, 0)

		// 重置 Pod
		l.pod = origpod

		// 将卷源设置为只读
		l.pod.Spec.Volumes[0].VolumeSource = *l.roVolSource

		// 由 container 0 在 subPath 内读取该文件
		testReadFile(ctx, f, l.filePathInSubpath, l.pod, 0)
	})

	f.It("should verify container cannot write to subpath readonly volumes", f.WithSlow(), func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)
		if l.roVolSource == nil {
			e2eskipper.Skipf("Driver %s on volume type %s doesn't support readOnly source", driverName, pattern.VolType)
		}

		// 在卷仍可写时格式化它
		formatVolume(ctx, f, l.formatPod)

		// 将卷源设置为只读
		l.pod.Spec.Volumes[0].VolumeSource = *l.roVolSource

		// 由 container 0 在卷中写文件
		setWriteCommand(l.subPathDir, &l.pod.Spec.Containers[0])

		// Pod 应当失败
		testPodFailSubpath(ctx, f, l.pod, true)
	})

	// 此测试仅限 Linux：在 Windows 上，一个容器删除目录而另一容器仍在使用时会失败。
	ginkgo.It("should be able to unmount after the subpath directory is deleted [LinuxOnly]", func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		// 将卷所在容器改为 busybox，以便后续 exec
		l.pod.Spec.Containers[1].Image = e2epod.GetDefaultTestImage()
		l.pod.Spec.Containers[1].Command = e2epod.GenerateScriptCmd(e2epod.InfiniteSleepCommand)
		l.pod.Spec.Containers[1].Args = nil

		ginkgo.By(fmt.Sprintf("Creating pod %s", l.pod.Name))
		removeUnusedContainers(l.pod)
		pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, l.pod, metav1.CreateOptions{})
		framework.ExpectNoError(err, "while creating pod")
		ginkgo.DeferCleanup(func(ctx context.Context) error {
			ginkgo.By(fmt.Sprintf("Deleting pod %s", pod.Name))
			return e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
		})

		// 等待 Pod 进入 Running 状态
		err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, f.ClientSet, l.pod.Name, l.pod.Namespace, f.Timeouts.PodStart)
		framework.ExpectNoError(err, "while waiting for pod to be running")

		// 进入挂载卷的容器，删除 subpath 目录
		rmCmd := fmt.Sprintf("rm -r %s", l.subPathDir)
		_, err = podContainerExec(l.pod, 1, rmCmd)
		framework.ExpectNoError(err, "while removing subpath directory")

		// 通过 defer 删除 Pod，并等待其被成功删除
	})

	// TODO: 补充同一磁盘上有两个分区的测试用例
}

// TestBasicSubpath 运行基础的 subpath 测试
func TestBasicSubpath(ctx context.Context, f *framework.Framework, contents string, pod *v1.Pod) {
	TestBasicSubpathFile(ctx, f, contents, pod, volumePath)
}

// TestBasicSubpathFile 运行基础的 subpath 文件测试
func TestBasicSubpathFile(ctx context.Context, f *framework.Framework, contents string, pod *v1.Pod, filepath string) {
	setReadCommand(filepath, &pod.Spec.Containers[0])

	ginkgo.By(fmt.Sprintf("Creating pod %s", pod.Name))
	removeUnusedContainers(pod)
	e2eoutput.TestContainerOutput(ctx, f, "atomic-volume-subpath", pod, 0, []string{contents})

	ginkgo.By(fmt.Sprintf("Deleting pod %s", pod.Name))
	err := e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
	framework.ExpectNoError(err, "while deleting pod")
}

func generateSuffixForPodName(s string) string {
	// Pod 名称必须：
	//   1. 仅由小写字母、数字和 '-' 组成，
	//   2. 首尾均为字母或数字。
	// （例如 'my-name' 或 '123-abc'；校验正则为 '[a-z0-9]([-a-z0-9]*[a-z0-9])?'）
	// 因此后缀按以下步骤生成：
	//   1. 将 [A-Za-z0-9] 之外的字符替换为 "-"，
	//   2. 在末尾追加小写字母或数字（追加 '-[a-z0-9]{4}'），
	//   3. 将整个字符串转为小写。
	re := regexp.MustCompile("[^A-Za-z0-9]")
	return strings.ToLower(fmt.Sprintf("%s-%s", re.ReplaceAllString(s, "-"), rand.String(4)))
}

// SubpathTestPod 返回供 subpath 测试使用的 Pod 规格
func SubpathTestPod(f *framework.Framework, subpath, volumeType string, source *v1.VolumeSource, securityLevel admissionapi.Level) *v1.Pod {
	var (
		suffix          = generateSuffixForPodName(volumeType)
		gracePeriod     = int64(1)
		probeVolumeName = "liveness-probe-volume"
		seLinuxOptions  = &v1.SELinuxOptions{Level: "s0:c0,c1"}
	)

	volumeMount := v1.VolumeMount{Name: volumeName, MountPath: volumePath}
	volumeSubpathMount := v1.VolumeMount{Name: volumeName, MountPath: volumePath, SubPath: subpath}
	probeMount := v1.VolumeMount{Name: probeVolumeName, MountPath: probeVolumePath}

	initSubpathContainer := e2epod.NewAgnhostContainer(
		fmt.Sprintf("test-init-subpath-%s", suffix),
		[]v1.VolumeMount{volumeSubpathMount, probeMount}, nil, "mounttest")
	initSubpathContainer.SecurityContext = e2epod.GenerateContainerSecurityContext(securityLevel)
	initVolumeContainer := e2epod.NewAgnhostContainer(
		fmt.Sprintf("test-init-volume-%s", suffix),
		[]v1.VolumeMount{volumeMount, probeMount}, nil, "mounttest")
	initVolumeContainer.SecurityContext = e2epod.GenerateContainerSecurityContext(securityLevel)
	subpathContainer := e2epod.NewAgnhostContainer(
		fmt.Sprintf("test-container-subpath-%s", suffix),
		[]v1.VolumeMount{volumeSubpathMount, probeMount}, nil, "mounttest")
	subpathContainer.SecurityContext = e2epod.GenerateContainerSecurityContext(securityLevel)
	volumeContainer := e2epod.NewAgnhostContainer(
		fmt.Sprintf("test-container-volume-%s", suffix),
		[]v1.VolumeMount{volumeMount, probeMount}, nil, "mounttest")
	volumeContainer.SecurityContext = e2epod.GenerateContainerSecurityContext(securityLevel)

	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      fmt.Sprintf("pod-subpath-test-%s", suffix),
			Namespace: f.Namespace.Name,
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{
				{
					Name:            fmt.Sprintf("init-volume-%s", suffix),
					Image:           e2epod.GetDefaultTestImage(),
					VolumeMounts:    []v1.VolumeMount{volumeMount, probeMount},
					SecurityContext: e2epod.GenerateContainerSecurityContext(securityLevel),
				},
				initSubpathContainer,
				initVolumeContainer,
			},
			Containers: []v1.Container{
				subpathContainer,
				volumeContainer,
			},
			RestartPolicy:                 v1.RestartPolicyNever,
			TerminationGracePeriodSeconds: &gracePeriod,
			Volumes: []v1.Volume{
				{
					Name:         volumeName,
					VolumeSource: *source,
				},
				{
					Name: probeVolumeName,
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{},
					},
				},
			},
			SecurityContext: e2epod.GeneratePodSecurityContext(nil, seLinuxOptions),
		},
	}
}

func containerIsUnused(container *v1.Container) bool {
	// agnhost 镜像在 command 和 Args 均为 nil，或 Args 仅为 "mounttest" 时不会做任何事；其余配置保持不变
	return container.Image == imageutils.GetE2EImage(imageutils.Agnhost) && container.Command == nil &&
		(container.Args == nil || (len(container.Args) == 1 && container.Args[0] == "mounttest"))
}

// removeUnusedContainers 从 SubpathTestPod 中删除测试不需要的容器。
// 例如测试 subpath 挂载失败时，只需运行一个容器并检查其状态。
func removeUnusedContainers(pod *v1.Pod) {
	initContainers := []v1.Container{}
	containers := []v1.Container{}
	if pod.Spec.InitContainers[0].Command != nil {
		initContainers = append(initContainers, pod.Spec.InitContainers[0])
	}
	for _, ic := range pod.Spec.InitContainers[1:] {
		if !containerIsUnused(&ic) {
			initContainers = append(initContainers, ic)
		}
	}
	containers = append(containers, pod.Spec.Containers[0])
	if !containerIsUnused(&pod.Spec.Containers[1]) {
		containers = append(containers, pod.Spec.Containers[1])
	}
	pod.Spec.InitContainers = initContainers
	pod.Spec.Containers = containers
}

// volumeFormatPod 返回一个不做任何实际工作的 Pod，但会促使插件在首次使用时格式化文件系统
func volumeFormatPod(f *framework.Framework, volumeSource *v1.VolumeSource) *v1.Pod {
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: fmt.Sprintf("volume-prep-%s", f.Namespace.Name),
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    fmt.Sprintf("init-volume-%s", f.Namespace.Name),
					Image:   e2epod.GetDefaultTestImage(),
					Command: e2epod.GenerateScriptCmd("echo nothing"),
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      volumeName,
							MountPath: "/vol",
						},
					},
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
			Volumes: []v1.Volume{
				{
					Name:         volumeName,
					VolumeSource: *volumeSource,
				},
			},
		},
	}
}

func setInitCommand(pod *v1.Pod, command string) {
	pod.Spec.InitContainers[0].Command = e2epod.GenerateScriptCmd(command)
}

func setWriteCommand(file string, container *v1.Container) {
	container.Args = []string{
		"mounttest",
		fmt.Sprintf("--new_file_0644=%v", file),
	}
	// 关于 file_mode 在 Windows 上表现异常，见 issue https://github.com/kubernetes/kubernetes/issues/94237
	// TODO: 问题解决后移除此检查
	if !framework.NodeOSDistroIs("windows") {
		container.Args = append(container.Args, fmt.Sprintf("--file_mode=%v", file))
	}

}

func addSubpathVolumeContainer(container *v1.Container, volumeMount v1.VolumeMount) {
	existingMounts := container.VolumeMounts
	container.VolumeMounts = append(existingMounts, volumeMount)
}

func addMultipleWrites(container *v1.Container, file1 string, file2 string) {
	container.Args = []string{
		"mounttest",
		fmt.Sprintf("--new_file_0644=%v", file1),
		fmt.Sprintf("--new_file_0666=%v", file2),
	}
}

func testMultipleReads(ctx context.Context, f *framework.Framework, pod *v1.Pod, containerIndex int, file1 string, file2 string) {
	ginkgo.By(fmt.Sprintf("Creating pod %s", pod.Name))
	removeUnusedContainers(pod)
	e2eoutput.TestContainerOutput(ctx, f, "multi_subpath", pod, containerIndex, []string{
		"content of file \"" + file1 + "\": mount-tester new file",
		"content of file \"" + file2 + "\": mount-tester new file",
	})
}

func setReadCommand(file string, container *v1.Container) {
	container.Args = []string{
		"mounttest",
		fmt.Sprintf("--file_content_in_loop=%v", file),
		fmt.Sprintf("--retry_time=%d", retryDuration),
	}
}

func testReadFile(ctx context.Context, f *framework.Framework, file string, pod *v1.Pod, containerIndex int) {
	setReadCommand(file, &pod.Spec.Containers[containerIndex])

	ginkgo.By(fmt.Sprintf("Creating pod %s", pod.Name))
	removeUnusedContainers(pod)
	e2eoutput.TestContainerOutput(ctx, f, "subpath", pod, containerIndex, []string{
		"content of file \"" + file + "\": mount-tester new file",
	})

	ginkgo.By(fmt.Sprintf("Deleting pod %s", pod.Name))
	err := e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
	framework.ExpectNoError(err, "while deleting pod")
}

func testPodFailSubpath(ctx context.Context, f *framework.Framework, pod *v1.Pod, allowContainerTerminationError bool) {
	testPodFailSubpathError(ctx, f, pod, "subPath", allowContainerTerminationError)
}

func testPodFailSubpathError(ctx context.Context, f *framework.Framework, pod *v1.Pod, errorMsg string, allowContainerTerminationError bool) {
	ginkgo.By(fmt.Sprintf("Creating pod %s", pod.Name))
	removeUnusedContainers(pod)
	pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "while creating pod")
	ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, pod)
	ginkgo.By("Checking for subpath error in container status")
	err = waitForPodSubpathError(ctx, f, pod, allowContainerTerminationError)
	framework.ExpectNoError(err, "while waiting for subpath failure")
}

func findSubpathContainerName(pod *v1.Pod) string {
	for _, container := range pod.Spec.Containers {
		for _, mount := range container.VolumeMounts {
			if mount.SubPath != "" {
				return container.Name
			}
		}
	}
	return ""
}

func waitForPodSubpathError(ctx context.Context, f *framework.Framework, pod *v1.Pod, allowContainerTerminationError bool) error {
	subpathContainerName := findSubpathContainerName(pod)
	if subpathContainerName == "" {
		return fmt.Errorf("failed to find container that uses subpath")
	}

	waitErr := wait.PollImmediate(framework.Poll, f.Timeouts.PodStart, func() (bool, error) {
		pod, err := f.ClientSet.CoreV1().Pods(pod.Namespace).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, status := range pod.Status.ContainerStatuses {
			// 0 号容器是使用 subpath 的容器
			if status.Name == subpathContainerName {
				switch {
				case status.State.Terminated != nil:
					if status.State.Terminated.ExitCode != 0 && allowContainerTerminationError {
						return true, nil
					}
					return false, fmt.Errorf("subpath container unexpectedly terminated")
				case status.State.Waiting != nil:
					if status.State.Waiting.Reason == "CreateContainerConfigError" &&
						strings.Contains(status.State.Waiting.Message, "subPath") {
						return true, nil
					}
					return false, nil
				default:
					return false, nil
				}
			}
		}
		return false, nil
	})
	if waitErr != nil {
		return fmt.Errorf("error waiting for pod subpath error to occur: %v", waitErr)
	}
	return nil
}

type podContainerRestartHooks struct {
	AddLivenessProbeFunc  func(pod *v1.Pod, probeFilePath string)
	FailLivenessProbeFunc func(pod *v1.Pod, probeFilePath string)
	FixLivenessProbeFunc  func(pod *v1.Pod, probeFilePath string)
}

func (h *podContainerRestartHooks) AddLivenessProbe(pod *v1.Pod, probeFilePath string) {
	if h.AddLivenessProbeFunc != nil {
		h.AddLivenessProbeFunc(pod, probeFilePath)
	}
}

func (h *podContainerRestartHooks) FailLivenessProbe(pod *v1.Pod, probeFilePath string) {
	if h.FailLivenessProbeFunc != nil {
		h.FailLivenessProbeFunc(pod, probeFilePath)
	}
}

func (h *podContainerRestartHooks) FixLivenessProbe(pod *v1.Pod, probeFilePath string) {
	if h.FixLivenessProbeFunc != nil {
		h.FixLivenessProbeFunc(pod, probeFilePath)
	}
}

// testPodContainerRestartWithHooks 验证容器重启后会趋于稳定。
// hooks 在容器重启之间包装各步骤函数。
func testPodContainerRestartWithHooks(ctx context.Context, f *framework.Framework, pod *v1.Pod, hooks *podContainerRestartHooks) {
	pod.Spec.RestartPolicy = v1.RestartPolicyOnFailure

	pod.Spec.Containers[0].Image = e2epod.GetDefaultTestImage()
	pod.Spec.Containers[0].Command = e2epod.GenerateScriptCmd(e2epod.InfiniteSleepCommandWithoutGracefulShutdown)
	pod.Spec.Containers[0].Args = nil
	pod.Spec.Containers[1].Image = e2epod.GetDefaultTestImage()
	pod.Spec.Containers[1].Command = e2epod.GenerateScriptCmd(e2epod.InfiniteSleepCommandWithoutGracefulShutdown)
	pod.Spec.Containers[1].Args = nil
	hooks.AddLivenessProbe(pod, probeFilePath)

	// 启动 Pod
	ginkgo.By(fmt.Sprintf("Creating pod %s", pod.Name))
	removeUnusedContainers(pod)
	pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "while creating pod")
	ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, pod)
	err = e2epod.WaitTimeoutForPodRunningReadyInNamespace(ctx, f.ClientSet, pod.Name, pod.Namespace, f.Timeouts.PodStart)
	framework.ExpectNoError(err, "while waiting for pod to be running and ready")

	ginkgo.By("Failing liveness probe")
	hooks.FailLivenessProbe(pod, probeFilePath)

	// 检查容器是否已重启。所需时间预计短于“删除 Pod 并重新启动 Pod”。
	ginkgo.By("Waiting for container to restart")
	restarts := int32(0)
	err = wait.PollImmediate(10*time.Second, f.Timeouts.PodDelete+f.Timeouts.PodStart, func() (bool, error) {
		pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == pod.Spec.Containers[0].Name {
				framework.Logf("Container %v, restarts: %v", status.Name, status.RestartCount)
				restarts = status.RestartCount
				if restarts > 0 {
					framework.Logf("Container has restart count: %v", restarts)
					return true, nil
				}
			}
		}
		return false, nil
	})
	framework.ExpectNoError(err, "while waiting for container to restart")

	// 修正 liveness probe
	ginkgo.By("Fix liveness probe")
	hooks.FixLivenessProbe(pod, probeFilePath)

	// 等待容器重启趋于稳定。这个时间较难估算；实践中
	// framework.PodStartTimeout = f.Timeouts.PodStart = 5min 太短，
	// 因此现在改用 f.Timeouts.PodStartSlow = 15min。
	ginkgo.By("Waiting for container to stop restarting")
	stableCount := int(0)
	stableThreshold := int(time.Minute / framework.Poll)
	err = wait.PollImmediate(framework.Poll, f.Timeouts.PodStartSlow, func() (bool, error) {
		pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		for _, status := range pod.Status.ContainerStatuses {
			if status.Name == pod.Spec.Containers[0].Name {
				if status.RestartCount == restarts {
					stableCount++
					if stableCount > stableThreshold {
						framework.Logf("Container restart has stabilized")
						return true, nil
					}
				} else {
					restarts = status.RestartCount
					stableCount = 0
					framework.Logf("Container has restart count: %v", restarts)
				}
				break
			}
		}
		return false, nil
	})
	framework.ExpectNoError(err, "while waiting for container to stabilize")
}

// testPodContainerRestart 验证容器重启时能够检测到已有的 subpath 挂载
func testPodContainerRestart(ctx context.Context, f *framework.Framework, pod *v1.Pod) {
	testPodContainerRestartWithHooks(ctx, f, pod, &podContainerRestartHooks{
		AddLivenessProbeFunc: func(p *v1.Pod, probeFilePath string) {
			p.Spec.Containers[0].LivenessProbe = &v1.Probe{
				ProbeHandler: v1.ProbeHandler{
					Exec: &v1.ExecAction{
						Command: []string{"cat", probeFilePath},
					},
				},
				InitialDelaySeconds: 1,
				FailureThreshold:    1,
				PeriodSeconds:       2,
			}
		},
		FailLivenessProbeFunc: func(p *v1.Pod, probeFilePath string) {
			out, err := podContainerExec(p, 1, fmt.Sprintf("rm %v", probeFilePath))
			framework.Logf("Pod exec output: %v", out)
			framework.ExpectNoError(err, "while failing liveness probe")
		},
		FixLivenessProbeFunc: func(p *v1.Pod, probeFilePath string) {
			ginkgo.By("Rewriting the file")
			var writeCmd string
			if framework.NodeOSDistroIs("windows") {
				writeCmd = fmt.Sprintf("echo test-after | Out-File -FilePath %v", probeFilePath)
			} else {
				writeCmd = fmt.Sprintf("echo test-after > %v", probeFilePath)
			}
			out, err := podContainerExec(pod, 1, writeCmd)
			framework.Logf("Pod exec output: %v", out)
			framework.ExpectNoError(err, "while rewriting the probe file")
		},
	})
}

// TestPodContainerRestartWithConfigmapModified 验证 ConfigMap 被修改后容器能重启并趋于稳定。
// 1. 容器正常运行
// 2. 更新 ConfigMap
// 3. 容器重启
// 4. ConfigMap 挂载文件被修改后容器恢复稳定
func TestPodContainerRestartWithConfigmapModified(ctx context.Context, f *framework.Framework, original, modified *v1.ConfigMap) {
	ginkgo.By("Create configmap")
	_, err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Create(ctx, original, metav1.CreateOptions{})
	if err != nil && !apierrors.IsAlreadyExists(err) {
		framework.ExpectNoError(err, "while creating configmap to modify")
	}

	var subpath string
	for k := range original.Data {
		subpath = k
		break
	}
	pod := SubpathTestPod(f, subpath, "configmap", &v1.VolumeSource{ConfigMap: &v1.ConfigMapVolumeSource{LocalObjectReference: v1.LocalObjectReference{Name: original.Name}}}, admissionapi.LevelBaseline)
	pod.Spec.InitContainers[0].Command = e2epod.GenerateScriptCmd(fmt.Sprintf("touch %v", probeFilePath))

	modifiedValue := modified.Data[subpath]
	testPodContainerRestartWithHooks(ctx, f, pod, &podContainerRestartHooks{
		AddLivenessProbeFunc: func(p *v1.Pod, probeFilePath string) {
			p.Spec.Containers[0].LivenessProbe = &v1.Probe{
				ProbeHandler: v1.ProbeHandler{
					Exec: &v1.ExecAction{
						// 期望 probe 文件存在，或 ConfigMap 挂载文件已被修改。
						Command: []string{"sh", "-c", fmt.Sprintf("cat %s || test `cat %s` = '%s'", probeFilePath, volumePath, modifiedValue)},
					},
				},
				InitialDelaySeconds: 1,
				FailureThreshold:    1,
				PeriodSeconds:       2,
			}
		},
		FailLivenessProbeFunc: func(p *v1.Pod, probeFilePath string) {
			out, err := podContainerExec(p, 1, fmt.Sprintf("rm %v", probeFilePath))
			framework.Logf("Pod exec output: %v", out)
			framework.ExpectNoError(err, "while failing liveness probe")
		},
		FixLivenessProbeFunc: func(p *v1.Pod, probeFilePath string) {
			_, err := f.ClientSet.CoreV1().ConfigMaps(f.Namespace.Name).Update(ctx, modified, metav1.UpdateOptions{})
			framework.ExpectNoError(err, "while fixing liveness probe")
		},
	})

}

func testSubpathReconstruction(ctx context.Context, f *framework.Framework, hostExec storageutils.HostExec, pod *v1.Pod, forceDelete bool) {
	// 大体复制自 TestVolumeUnmountsFromDeletedPodWithForceOption()。

	// 该破坏性测试串行运行，因此可缓存所有卷的全局挂载点，并在测试后确认没有泄漏。
	nodeList, err := e2enode.GetReadySchedulableNodes(ctx, f.ClientSet)
	framework.ExpectNoError(err, "while listing schedulable nodes")
	globalMountPointsByNode := make(map[string]sets.String, len(nodeList.Items))
	for _, node := range nodeList.Items {
		globalMountPointsByNode[node.Name] = storageutils.FindVolumeGlobalMountPoints(ctx, hostExec, &node)
	}

	// 切换为 busybox
	pod.Spec.Containers[0].Image = e2epod.GetDefaultTestImage()
	pod.Spec.Containers[0].Command = e2epod.GenerateScriptCmd(e2epod.InfiniteSleepCommand)
	pod.Spec.Containers[0].Args = nil
	pod.Spec.Containers[1].Image = e2epod.GetDefaultTestImage()
	pod.Spec.Containers[1].Command = e2epod.GenerateScriptCmd(e2epod.InfiniteSleepCommand)
	pod.Spec.Containers[1].Args = nil
	// grace period 过短会导致卷管理器来不及清理卷
	gracePeriod := int64(30)
	pod.Spec.TerminationGracePeriodSeconds = &gracePeriod

	ginkgo.By(fmt.Sprintf("Creating pod %s", pod.Name))
	removeUnusedContainers(pod)
	pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "while creating pod")
	err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, f.ClientSet, pod.Name, pod.Namespace, f.Timeouts.PodStart)
	framework.ExpectNoError(err, "while waiting for pod to be running")

	pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, "while getting pod")

	var podNode *v1.Node
	for i := range nodeList.Items {
		if nodeList.Items[i].Name == pod.Spec.NodeName {
			podNode = &nodeList.Items[i]
		}
	}
	gomega.Expect(podNode).ToNot(gomega.BeNil(), "pod node should exist in schedulable nodes")

	storageutils.TestVolumeUnmountsFromDeletedPodWithForceOption(ctx, f.ClientSet, f, pod, forceDelete, true, nil, volumePath)

	if podNode != nil {
		mountPoints := globalMountPointsByNode[podNode.Name]
		mountPointsAfter := storageutils.FindVolumeGlobalMountPoints(ctx, hostExec, podNode)
		s1 := mountPointsAfter.Difference(mountPoints)
		s2 := mountPoints.Difference(mountPointsAfter)
		gomega.Expect(s1).To(gomega.BeEmpty(), "global mount points leaked: %v", s1)
		gomega.Expect(s2).To(gomega.BeEmpty(), "global mount points not found: %v", s2)
	}
}

func formatVolume(ctx context.Context, f *framework.Framework, pod *v1.Pod) {
	ginkgo.By(fmt.Sprintf("Creating pod to format volume %s", pod.Name))
	pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "while creating volume init pod")

	err = e2epod.WaitForPodSuccessInNamespaceTimeout(ctx, f.ClientSet, pod.Name, pod.Namespace, f.Timeouts.PodStart)
	framework.ExpectNoError(err, "while waiting for volume init pod to succeed")

	err = e2epod.DeletePodWithWait(ctx, f.ClientSet, pod)
	framework.ExpectNoError(err, "while deleting volume init pod")
}

func podContainerExec(pod *v1.Pod, containerIndex int, command string) (string, error) {
	if containerIndex > len(pod.Spec.Containers)-1 {
		return "", fmt.Errorf("container not found in pod: index %d", containerIndex)
	}
	var shell string
	var option string
	if framework.NodeOSDistroIs("windows") {
		shell = "powershell"
		option = "/c"
	} else {
		shell = "/bin/sh"
		option = "-c"
	}
	return e2ekubectl.RunKubectl(pod.Namespace, "exec", pod.Name, "--container", pod.Spec.Containers[containerIndex].Name, "--", shell, option, command)
}

// testSubpathStaleBindMountRemount 验证残留 subPath bind mount 的恢复路径。
func testSubpathStaleBindMountRemount(ctx context.Context, f *framework.Framework, pod *v1.Pod) {
	pod.Spec.Containers[0].Image = e2epod.GetDefaultTestImage()
	// ls /data 用于验证 subPath 挂载可用；sleep 让容器保持存活，
	// 以便观察容器崩溃和重启。
	pod.Spec.Containers[0].Command = []string{"sh", "-c",
		fmt.Sprintf("ls %s && trap exit TERM; while true; do sleep 1; done", volumePath)}
	pod.Spec.Containers[0].Args = nil
	gracePeriod := int64(10)
	pod.Spec.TerminationGracePeriodSeconds = &gracePeriod
	pod.Spec.RestartPolicy = v1.RestartPolicyAlways

	ginkgo.By(fmt.Sprintf("Creating pod %s with subPath mount", pod.Name))
	removeUnusedContainers(pod)
	pod, err := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Create(ctx, pod, metav1.CreateOptions{})
	framework.ExpectNoError(err, "creating pod with subPath mount")
	ginkgo.DeferCleanup(e2epod.DeletePodWithWait, f.ClientSet, pod)

	err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, f.ClientSet, pod.Name, pod.Namespace, f.Timeouts.PodStart)
	framework.ExpectNoError(err, "waiting for pod with subPath mount to reach Running")

	pod, err = f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
	framework.ExpectNoError(err, "refreshing pod after Running")

	nodeList, err := e2enode.GetReadySchedulableNodes(ctx, f.ClientSet)
	framework.ExpectNoError(err, "listing schedulable nodes")
	var podNode *v1.Node
	for i := range nodeList.Items {
		if nodeList.Items[i].Name == pod.Spec.NodeName {
			podNode = &nodeList.Items[i]
			break
		}
	}
	gomega.Expect(podNode).NotTo(gomega.BeNil(), "pod node must be in the schedulable node list")

	hostExec := storageutils.NewHostExec(f)
	ginkgo.DeferCleanup(hostExec.Cleanup)

	subpathGlob := fmt.Sprintf("/var/lib/kubelet/pods/%s/volume-subpaths/*/*/*", pod.UID)
	ginkgo.By(fmt.Sprintf("Waiting for and then lazy-unmounting subPath bind mounts: %s", subpathGlob))

	// 等待 bind mount 目标出现。
	var bindTargets string
	gomega.Eventually(ctx, func() string {
		out, _ := hostExec.IssueCommandWithResult(ctx,
			fmt.Sprintf("ls -d %s 2>/dev/null | head -1", subpathGlob), podNode)
		bindTargets = strings.TrimSpace(out)
		return bindTargets
	}, 60*time.Second, 2*time.Second).ShouldNot(gomega.BeEmpty(),
		"subPath bind-mount target must exist within 60s of pod Running")
	framework.Logf("Found subPath bind target: %s", bindTargets)

	// 在 Pod 运行时惰性卸载 subPath bind mount 目标；
	// 容器会因 subPath 挂载消失而崩溃。
	umountCmd := fmt.Sprintf(
		`for mp in $(ls -d %s 2>/dev/null); do umount --lazy "$mp" && echo "unmounted $mp" || true; done`,
		subpathGlob,
	)
	result, err := hostExec.IssueCommandWithResult(ctx, umountCmd, podNode)
	framework.ExpectNoError(err, "lazy-unmounting subPath bind mounts: %s", result)
	framework.Logf("umount result: %s", result)

	// 通过 crictl 停止容器，使 kubelet 重启它并对残留 bind mount
	// 调用 prepareSubpathTarget。
	ginkgo.By("Stopping app container via crictl to trigger restart through stale bind-mount path")
	containerName := pod.Spec.Containers[0].Name
	stopCmd := fmt.Sprintf(
		`cid=$(crictl ps --label io.kubernetes.pod.uid=%s --name %s -q 2>/dev/null | head -1); `+
			`[ -z "$cid" ] && { echo "container not found"; exit 1; }; `+
			`crictl stop "$cid" && echo "stopped $cid"`,
		pod.UID, containerName,
	)
	// crictl stop 偶尔会因 containerd gRPC 客户端的 RST_STREAM / DEADLINE_EXCEEDED 失败。
	// crictl stop 是幂等的，因此可以重试。若 kubelet 已重启容器（RestartCount >= 1），
	// 说明即使 RPC 响应丢失，停止操作实际已生效：此时视为成功并停止重试，避免一直循环到超时。
	var stopResult string
	stopErr := wait.PollUntilContextTimeout(ctx, 5*time.Second, 60*time.Second, true,
		func(ctx context.Context) (bool, error) {
			out, err := hostExec.IssueCommandWithResult(ctx, stopCmd, podNode)
			stopResult = out
			if err == nil {
				return true, nil
			}
			if p, getErr := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{}); getErr == nil {
				for _, cs := range p.Status.ContainerStatuses {
					if cs.Name == containerName && cs.RestartCount >= 1 {
						framework.Logf("crictl stop returned %v, but container already restarted (count=%d); treating as success", err, cs.RestartCount)
						stopResult = "container already restarted"
						return true, nil
					}
				}
			} else {
				framework.Logf("crictl stop failed (will retry): %v — failed to get pod: %v", err, getErr)
			}
			framework.Logf("crictl stop failed (will retry): %v — %s", err, out)
			return false, nil
		})
	framework.ExpectNoError(stopErr, "stopping container via crictl for pod %s: %s", pod.UID, stopResult)
	framework.Logf("crictl stop result: %s", stopResult)

	// 等待容器重启并返回 Running 状态，且重启次数 >= 1。
	ginkgo.By("Waiting for container to restart with a fresh bind mount")
	err = wait.PollUntilContextTimeout(ctx, 3*time.Second, f.Timeouts.PodStart, true,
		func(ctx context.Context) (bool, error) {
			p, getErr := f.ClientSet.CoreV1().Pods(f.Namespace.Name).Get(ctx, pod.Name, metav1.GetOptions{})
			if getErr != nil {
				framework.Logf("Error getting pod: %v", getErr)
				return false, nil
			}
			for _, cs := range p.Status.ContainerStatuses {
				if cs.Name == p.Spec.Containers[0].Name {
					framework.Logf("Container %s: restarts=%d, running=%v",
						cs.Name, cs.RestartCount, cs.State.Running != nil)
					if cs.RestartCount >= 1 && cs.State.Running != nil {
						return true, nil
					}
				}
			}
			return false, nil
		},
	)
	framework.ExpectNoError(err,
		"pod %s container did not restart and reach Running after stale bind mount remount — "+
			"check kubelet logs for prepareSubpathTarget errors",
		pod.Name)
}
