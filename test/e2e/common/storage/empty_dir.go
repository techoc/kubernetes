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

package storage

import (
	"context"
	"fmt"
	"path"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	kubefeatures "k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2epodoutput "k8s.io/kubernetes/test/e2e/framework/pod/output"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
	admissionapi "k8s.io/pod-security-admission/api"
)

const (
	volumePath = "/test-volume"
)

var (
	nonRootUID = int64(1001)
)

var _ = SIGDescribe("EmptyDir volumes", func() {
	f := framework.NewDefaultFramework("emptydir")
	f.NamespacePodSecurityLevel = admissionapi.LevelBaseline

	f.Context("when FSGroup is specified [LinuxOnly]", f.WithNodeConformance(), func() {

		ginkgo.BeforeEach(func() {
			// Windows 不支持 FSGroup SecurityContext 选项。
			e2eskipper.SkipIfNodeOSDistroIs("windows")
		})

		ginkgo.It("new files should be created with FSGroup ownership when container is root", func(ctx context.Context) {
			doTestSetgidFSGroup(ctx, f, 0, v1.StorageMediumMemory)
		})

		ginkgo.It("new files should be created with FSGroup ownership when container is non-root", func(ctx context.Context) {
			doTestSetgidFSGroup(ctx, f, nonRootUID, v1.StorageMediumMemory)
		})

		ginkgo.It("nonexistent volume subPath should have the correct mode and owner using FSGroup", func(ctx context.Context) {
			doTestSubPathFSGroup(ctx, f, nonRootUID, v1.StorageMediumMemory)
		})

		ginkgo.It("files with FSGroup ownership should support (root,0644,tmpfs)", func(ctx context.Context) {
			doTest0644FSGroup(ctx, f, 0, v1.StorageMediumMemory)
		})

		ginkgo.It("volume on default medium should have the correct mode using FSGroup", func(ctx context.Context) {
			doTestVolumeModeFSGroup(ctx, f, 0, v1.StorageMediumDefault)
		})

		ginkgo.It("volume on tmpfs should have the correct mode using FSGroup", func(ctx context.Context) {
			doTestVolumeModeFSGroup(ctx, f, 0, v1.StorageMediumMemory)
		})
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium memory, volume mode default
		描述：创建带 'emptyDir' volume 且 'medium' 为 'Memory' 的 Pod，volume 模式必须为 -rwxrwxrwx，挂载类型为 tmpfs。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限，也不支持 medium = 'Memory'。
	*/
	framework.ConformanceIt("volume on tmpfs should have the correct mode [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTestVolumeMode(ctx, f, 0, v1.StorageMediumMemory)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium memory, volume mode 0644
		描述：创建带 'emptyDir' volume 且 'medium' 为 'Memory'、volume 模式为 0644 的 Pod。volume 模式必须为 -rw-r--r--，挂载类型为 tmpfs，内容必须可读。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限、以 UID / GID 运行，或 medium = 'Memory'。
	*/
	framework.ConformanceIt("should support (root,0644,tmpfs) [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTest0644(ctx, f, 0, v1.StorageMediumMemory)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium memory, volume mode 0666
		描述：创建带 'emptyDir' volume 且 'medium' 为 'Memory'、volume 模式为 0666 的 Pod。volume 模式必须为 -rw-rw-rw-，挂载类型为 tmpfs，内容必须可读。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限、以 UID / GID 运行，或 medium = 'Memory'。
	*/
	framework.ConformanceIt("should support (root,0666,tmpfs) [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTest0666(ctx, f, 0, v1.StorageMediumMemory)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium memory, volume mode 0777
		描述：创建带 'emptyDir' volume 且 'medium' 为 'Memory'、volume 模式为 0777 的 Pod。volume 模式必须为 -rwxrwxrwx，挂载类型为 tmpfs，内容必须可读。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限、以 UID / GID 运行，或 medium = 'Memory'。
	*/
	framework.ConformanceIt("should support (root,0777,tmpfs) [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTest0777(ctx, f, 0, v1.StorageMediumMemory)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium memory, volume mode 0644, non-root user
		描述：创建带 'emptyDir' volume 且 'medium' 为 'Memory'、volume 模式为 0644 的 Pod。volume 挂载到以非 root 用户运行的容器中。volume 模式必须为 -rw-r--r--，挂载类型为 tmpfs，内容必须可读。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限、以 UID / GID 运行，或 medium = 'Memory'。
	*/
	framework.ConformanceIt("should support (non-root,0644,tmpfs) [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTest0644(ctx, f, nonRootUID, v1.StorageMediumMemory)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium memory, volume mode 0666,, non-root user
		描述：创建带 'emptyDir' volume 且 'medium' 为 'Memory'、volume 模式为 0666 的 Pod。volume 挂载到以非 root 用户运行的容器中。volume 模式必须为 -rw-rw-rw-，挂载类型为 tmpfs，内容必须可读。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限、以 UID / GID 运行，或 medium = 'Memory'。
	*/
	framework.ConformanceIt("should support (non-root,0666,tmpfs) [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTest0666(ctx, f, nonRootUID, v1.StorageMediumMemory)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium memory, volume mode 0777, non-root user
		描述：创建带 'emptyDir' volume 且 'medium' 为 'Memory'、volume 模式为 0777 的 Pod。volume 挂载到以非 root 用户运行的容器中。volume 模式必须为 -rwxrwxrwx，挂载类型为 tmpfs，内容必须可读。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限、以 UID / GID 运行，或 medium = 'Memory'。
	*/
	framework.ConformanceIt("should support (non-root,0777,tmpfs) [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTest0777(ctx, f, nonRootUID, v1.StorageMediumMemory)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium default, volume mode default
		描述：创建带 'emptyDir' volume 的 Pod，volume 模式必须为 -rwxrwxrwx，挂载类型为 tmpfs。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限。
	*/
	framework.ConformanceIt("volume on default medium should have the correct mode [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTestVolumeMode(ctx, f, 0, v1.StorageMediumDefault)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium default, volume mode 0644
		描述：创建带 'emptyDir' volume 且 volume 模式为 0644 的 Pod。volume 模式必须为 -rw-r--r--，挂载类型为 tmpfs，内容必须可读。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限或以 UID / GID 运行。
	*/
	framework.ConformanceIt("should support (root,0644,default) [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTest0644(ctx, f, 0, v1.StorageMediumDefault)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium default, volume mode 0666
		描述：创建带 'emptyDir' volume 且 volume 模式为 0666 的 Pod。volume 模式必须为 -rw-rw-rw-，挂载类型为 tmpfs，内容必须可读。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限或以 UID / GID 运行。
	*/
	framework.ConformanceIt("should support (root,0666,default) [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTest0666(ctx, f, 0, v1.StorageMediumDefault)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium default, volume mode 0777
		描述：创建带 'emptyDir' volume 且 volume 模式为 0777 的 Pod。volume 模式必须为 -rwxrwxrwx，挂载类型为 tmpfs，内容必须可读。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限或以 UID / GID 运行。
	*/
	framework.ConformanceIt("should support (root,0777,default) [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTest0777(ctx, f, 0, v1.StorageMediumDefault)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium default, volume mode 0644
		描述：创建带 'emptyDir' volume 且 volume 模式为 0644 的 Pod。volume 挂载到以非 root 用户运行的容器中。volume 模式必须为 -rw-r--r--，挂载类型为 tmpfs，内容必须可读。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限或以 UID / GID 运行。
	*/
	framework.ConformanceIt("should support (non-root,0644,default) [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTest0644(ctx, f, nonRootUID, v1.StorageMediumDefault)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium default, volume mode 0666
		描述：创建带 'emptyDir' volume 且 volume 模式为 0666 的 Pod。volume 挂载到以非 root 用户运行的容器中。volume 模式必须为 -rw-rw-rw-，挂载类型为 tmpfs，内容必须可读。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限或以 UID / GID 运行。
	*/
	framework.ConformanceIt("should support (non-root,0666,default) [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTest0666(ctx, f, nonRootUID, v1.StorageMediumDefault)
	})

	/*
		发布版本：v1.9
		测试名称：EmptyDir, medium default, volume mode 0777
		描述：创建带 'emptyDir' volume 且 volume 模式为 0777 的 Pod。volume 挂载到以非 root 用户运行的容器中。volume 模式必须为 -rwxrwxrwx，挂载类型为 tmpfs，内容必须可读。
		此测试标记为 LinuxOnly，因为 Windows 不支持设置特定文件权限或以 UID / GID 运行。
	*/
	framework.ConformanceIt("should support (non-root,0777,default) [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		doTest0777(ctx, f, nonRootUID, v1.StorageMediumDefault)
	})

	/*
		发布版本：v1.15
		测试名称：EmptyDir, Shared volumes between containers
		描述：创建带 'emptyDir' volume 的 Pod，volume 应在 pod 中的容器之间共享。两个 busybox 镜像容器应共享挂载到 pod 的 volume。
		主容器应等待子容器放下文件，然后访问共享数据。
	*/
	framework.ConformanceIt("pod should support shared volumes between containers", func(ctx context.Context) {
		var (
			volumeName                 = "shared-data"
			busyBoxMainVolumeMountPath = "/usr/share/volumeshare"
			busyBoxSubVolumeMountPath  = "/pod-data"
			busyBoxMainVolumeFilePath  = fmt.Sprintf("%s/shareddata.txt", busyBoxMainVolumeMountPath)
			busyBoxSubVolumeFilePath   = fmt.Sprintf("%s/shareddata.txt", busyBoxSubVolumeMountPath)
			message                    = "Hello from the busy-box sub-container"
			busyBoxMainContainerName   = "busybox-main-container"
			busyBoxSubContainerName    = "busybox-sub-container"
			resultString               = ""
			deletionGracePeriod        = int64(0)
		)

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-sharedvolume-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Volumes: []v1.Volume{
					{
						Name: volumeName,
						VolumeSource: v1.VolumeSource{
							EmptyDir: new(v1.EmptyDirVolumeSource),
						},
					},
				},
				Containers: []v1.Container{
					{
						Name:    busyBoxMainContainerName,
						Image:   imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{"/bin/sh"},
						Args:    []string{"-c", e2epod.InfiniteSleepCommand},
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      volumeName,
								MountPath: busyBoxMainVolumeMountPath,
							},
						},
					},
					{
						Name:    busyBoxSubContainerName,
						Image:   imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{"/bin/sh"},
						Args:    []string{"-c", fmt.Sprintf("echo %s > %s", message, busyBoxSubVolumeFilePath)},
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      volumeName,
								MountPath: busyBoxSubVolumeMountPath,
							},
						},
					},
				},
				TerminationGracePeriodSeconds: &deletionGracePeriod,
				RestartPolicy:                 v1.RestartPolicyNever,
			},
		}

		ginkgo.By("Creating Pod")
		e2epod.NewPodClient(f).Create(ctx, pod)
		framework.ExpectNoError(e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name))

		ginkgo.By("Reading file content from the nginx-container")
		result := e2epod.ExecShellInContainer(f, pod.Name, busyBoxMainContainerName, fmt.Sprintf("cat %s", busyBoxMainVolumeFilePath))
		gomega.Expect(result).To(gomega.Equal(message), "failed to match expected string %s with %s", message, resultString)
	})

	f.Context("EmptyDirVolumeMode [LinuxOnly]", feature.EmptyDirVolumeMode, framework.WithFeatureGate(kubefeatures.EmptyDirVolumeMode), func() {
		ginkgo.It("should set mode 0750 on emptyDir directory", func(ctx context.Context) {
			mode := int32(0o750)
			pod := createEmptyDirModePod(ctx, f, &mode)

			output := e2epod.ExecCommandInContainer(f, pod.Name, "test-container", "/bin/sh", "-c", "stat -c %a /mnt/test-vol")
			gomega.Expect(strings.TrimSpace(output)).To(gomega.Equal("750"))
		})

		ginkgo.It("should set mode 01777 with sticky bit on emptyDir directory", func(ctx context.Context) {
			mode := int32(0o1777)
			pod := createEmptyDirModePod(ctx, f, &mode)

			output := e2epod.ExecCommandInContainer(f, pod.Name, "test-container", "/bin/sh", "-c", "stat -c %a /mnt/test-vol")
			gomega.Expect(strings.TrimSpace(output)).To(gomega.Equal("1777"))
		})

		ginkgo.It("should default to mode 0777 when mode is not set", func(ctx context.Context) {
			pod := createEmptyDirModePod(ctx, f, nil)

			output := e2epod.ExecCommandInContainer(f, pod.Name, "test-container", "/bin/sh", "-c", "stat -c %a /mnt/test-vol")
			gomega.Expect(strings.TrimSpace(output)).To(gomega.Equal("777"))
		})
	})

	/*
		发布版本：v1.20
		测试名称：EmptyDir, Memory backed volume is sized to specified limit
		描述：创建带内存支持的 'emptyDir' volume 的 Pod，其大小应为用户提供的值。
		此测试标记为 LinuxOnly，因为 Windows 支持 emptyDir volume，但不支持内存支持的 emptyDir volume。
	*/
	f.It("pod should support memory backed volumes of specified size [LinuxOnly]", f.WithNodeConformance(), func(ctx context.Context) {
		e2eskipper.SkipIfNodeOSDistroIs("windows")

		var (
			volumeName                 = "shared-data"
			busyBoxMainVolumeMountPath = "/usr/share/volumeshare"
			busyBoxMainContainerName   = "busybox-main-container"
			expectedResult             = "10240" // equal to 10Mi
			deletionGracePeriod        = int64(0)
			sizeLimit                  = resource.MustParse("10Mi")
		)

		pod := &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name: "pod-size-memory-volume-" + string(uuid.NewUUID()),
			},
			Spec: v1.PodSpec{
				Volumes: []v1.Volume{
					{
						Name: volumeName,
						VolumeSource: v1.VolumeSource{
							EmptyDir: &v1.EmptyDirVolumeSource{
								Medium:    v1.StorageMediumMemory,
								SizeLimit: &sizeLimit,
							},
						},
					},
				},
				Containers: []v1.Container{
					{
						Name:    busyBoxMainContainerName,
						Image:   imageutils.GetE2EImage(imageutils.BusyBox),
						Command: []string{"/bin/sh"},
						Args:    []string{"-c", e2epod.InfiniteSleepCommand},
						VolumeMounts: []v1.VolumeMount{
							{
								Name:      volumeName,
								MountPath: busyBoxMainVolumeMountPath,
							},
						},
					},
				},
				TerminationGracePeriodSeconds: &deletionGracePeriod,
				RestartPolicy:                 v1.RestartPolicyNever,
			},
		}

		var err error
		ginkgo.By("Creating Pod")
		pod = e2epod.NewPodClient(f).CreateSync(ctx, pod)

		ginkgo.By("Waiting for the pod running")
		err = e2epod.WaitForPodNameRunningInNamespace(ctx, f.ClientSet, pod.Name, f.Namespace.Name)
		framework.ExpectNoError(err, "failed to deploy pod %s", pod.Name)

		ginkgo.By("Getting the pod")
		pod, err = e2epod.NewPodClient(f).Get(ctx, pod.Name, metav1.GetOptions{})
		framework.ExpectNoError(err, "failed to get pod %s", pod.Name)

		ginkgo.By("Reading empty dir size")
		result := e2epod.ExecShellInContainer(f, pod.Name, busyBoxMainContainerName, fmt.Sprintf("df | grep %s | awk '{print $2}'", busyBoxMainVolumeMountPath))
		gomega.Expect(result).To(gomega.Equal(expectedResult), "failed to match expected string %s with %s", expectedResult, result)
	})
})

const (
	containerName = "test-container"
	volumeName    = "test-volume"
)

func doTestSetgidFSGroup(ctx context.Context, f *framework.Framework, uid int64, medium v1.StorageMedium) {
	var (
		filePath = path.Join(volumePath, "test-file")
		source   = &v1.EmptyDirVolumeSource{Medium: medium}
		pod      = testPodWithVolume(uid, volumePath, source)
	)

	pod.Spec.Containers[0].Args = []string{
		"mounttest",
		fmt.Sprintf("--fs_type=%v", volumePath),
		fmt.Sprintf("--new_file_0660=%v", filePath),
		fmt.Sprintf("--file_perm=%v", filePath),
		fmt.Sprintf("--file_owner=%v", filePath),
	}

	fsGroup := int64(123)
	pod.Spec.SecurityContext.FSGroup = &fsGroup

	msg := fmt.Sprintf("emptydir 0644 on %v", formatMedium(medium))
	out := []string{
		"perms of file \"/test-volume/test-file\": -rw-rw----",
		"content of file \"/test-volume/test-file\": mount-tester new file",
		"owner GID of \"/test-volume/test-file\": 123",
	}
	if medium == v1.StorageMediumMemory {
		out = append(out, "mount type of \"/test-volume\": tmpfs")
	}
	e2epodoutput.TestContainerOutput(ctx, f, msg, pod, 0, out)
}

func doTestSubPathFSGroup(ctx context.Context, f *framework.Framework, uid int64, medium v1.StorageMedium) {
	var (
		subPath = "test-sub"
		source  = &v1.EmptyDirVolumeSource{Medium: medium}
		pod     = testPodWithVolume(uid, volumePath, source)
	)

	pod.Spec.Containers[0].Args = []string{
		"mounttest",
		fmt.Sprintf("--fs_type=%v", volumePath),
		fmt.Sprintf("--file_perm=%v", volumePath),
		fmt.Sprintf("--file_owner=%v", volumePath),
		fmt.Sprintf("--file_mode=%v", volumePath),
	}

	pod.Spec.Containers[0].VolumeMounts[0].SubPath = subPath

	fsGroup := int64(123)
	pod.Spec.SecurityContext.FSGroup = &fsGroup

	msg := fmt.Sprintf("emptydir subpath on %v", formatMedium(medium))
	out := []string{
		"perms of file \"/test-volume\": -rwxrwxrwx",
		"owner UID of \"/test-volume\": 0",
		"owner GID of \"/test-volume\": 123",
		"mode of file \"/test-volume\": dgtrwxrwxrwx",
	}
	if medium == v1.StorageMediumMemory {
		out = append(out, "mount type of \"/test-volume\": tmpfs")
	}
	e2epodoutput.TestContainerOutput(ctx, f, msg, pod, 0, out)
}

func doTestVolumeModeFSGroup(ctx context.Context, f *framework.Framework, uid int64, medium v1.StorageMedium) {
	var (
		source = &v1.EmptyDirVolumeSource{Medium: medium}
		pod    = testPodWithVolume(uid, volumePath, source)
	)

	pod.Spec.Containers[0].Args = []string{
		"mounttest",
		fmt.Sprintf("--fs_type=%v", volumePath),
		fmt.Sprintf("--file_perm=%v", volumePath),
	}

	fsGroup := int64(1001)
	pod.Spec.SecurityContext.FSGroup = &fsGroup

	msg := fmt.Sprintf("emptydir volume type on %v", formatMedium(medium))
	out := []string{
		"perms of file \"/test-volume\": -rwxrwxrwx",
	}
	if medium == v1.StorageMediumMemory {
		out = append(out, "mount type of \"/test-volume\": tmpfs")
	}
	e2epodoutput.TestContainerOutput(ctx, f, msg, pod, 0, out)
}

func doTest0644FSGroup(ctx context.Context, f *framework.Framework, uid int64, medium v1.StorageMedium) {
	var (
		filePath = path.Join(volumePath, "test-file")
		source   = &v1.EmptyDirVolumeSource{Medium: medium}
		pod      = testPodWithVolume(uid, volumePath, source)
	)

	pod.Spec.Containers[0].Args = []string{
		"mounttest",
		fmt.Sprintf("--fs_type=%v", volumePath),
		fmt.Sprintf("--new_file_0644=%v", filePath),
		fmt.Sprintf("--file_perm=%v", filePath),
	}

	fsGroup := int64(123)
	pod.Spec.SecurityContext.FSGroup = &fsGroup

	msg := fmt.Sprintf("emptydir 0644 on %v", formatMedium(medium))
	out := []string{
		"perms of file \"/test-volume/test-file\": -rw-r--r--",
		"content of file \"/test-volume/test-file\": mount-tester new file",
	}
	if medium == v1.StorageMediumMemory {
		out = append(out, "mount type of \"/test-volume\": tmpfs")
	}
	e2epodoutput.TestContainerOutput(ctx, f, msg, pod, 0, out)
}

func doTestVolumeMode(ctx context.Context, f *framework.Framework, uid int64, medium v1.StorageMedium) {
	var (
		source = &v1.EmptyDirVolumeSource{Medium: medium}
		pod    = testPodWithVolume(uid, volumePath, source)
	)

	pod.Spec.Containers[0].Args = []string{
		"mounttest",
		fmt.Sprintf("--fs_type=%v", volumePath),
		fmt.Sprintf("--file_perm=%v", volumePath),
	}

	msg := fmt.Sprintf("emptydir volume type on %v", formatMedium(medium))
	out := []string{
		"perms of file \"/test-volume\": -rwxrwxrwx",
	}
	if medium == v1.StorageMediumMemory {
		out = append(out, "mount type of \"/test-volume\": tmpfs")
	}
	e2epodoutput.TestContainerOutput(ctx, f, msg, pod, 0, out)
}

func doTest0644(ctx context.Context, f *framework.Framework, uid int64, medium v1.StorageMedium) {
	var (
		filePath = path.Join(volumePath, "test-file")
		source   = &v1.EmptyDirVolumeSource{Medium: medium}
		pod      = testPodWithVolume(uid, volumePath, source)
	)

	pod.Spec.Containers[0].Args = []string{
		"mounttest",
		fmt.Sprintf("--fs_type=%v", volumePath),
		fmt.Sprintf("--new_file_0644=%v", filePath),
		fmt.Sprintf("--file_perm=%v", filePath),
	}

	msg := fmt.Sprintf("emptydir 0644 on %v", formatMedium(medium))
	out := []string{
		"perms of file \"/test-volume/test-file\": -rw-r--r--",
		"content of file \"/test-volume/test-file\": mount-tester new file",
	}
	if medium == v1.StorageMediumMemory {
		out = append(out, "mount type of \"/test-volume\": tmpfs")
	}
	e2epodoutput.TestContainerOutput(ctx, f, msg, pod, 0, out)
}

func doTest0666(ctx context.Context, f *framework.Framework, uid int64, medium v1.StorageMedium) {
	var (
		filePath = path.Join(volumePath, "test-file")
		source   = &v1.EmptyDirVolumeSource{Medium: medium}
		pod      = testPodWithVolume(uid, volumePath, source)
	)

	pod.Spec.Containers[0].Args = []string{
		"mounttest",
		fmt.Sprintf("--fs_type=%v", volumePath),
		fmt.Sprintf("--new_file_0666=%v", filePath),
		fmt.Sprintf("--file_perm=%v", filePath),
	}

	msg := fmt.Sprintf("emptydir 0666 on %v", formatMedium(medium))
	out := []string{
		"perms of file \"/test-volume/test-file\": -rw-rw-rw-",
		"content of file \"/test-volume/test-file\": mount-tester new file",
	}
	if medium == v1.StorageMediumMemory {
		out = append(out, "mount type of \"/test-volume\": tmpfs")
	}
	e2epodoutput.TestContainerOutput(ctx, f, msg, pod, 0, out)
}

func doTest0777(ctx context.Context, f *framework.Framework, uid int64, medium v1.StorageMedium) {
	var (
		filePath = path.Join(volumePath, "test-file")
		source   = &v1.EmptyDirVolumeSource{Medium: medium}
		pod      = testPodWithVolume(uid, volumePath, source)
	)

	pod.Spec.Containers[0].Args = []string{
		"mounttest",
		fmt.Sprintf("--fs_type=%v", volumePath),
		fmt.Sprintf("--new_file_0777=%v", filePath),
		fmt.Sprintf("--file_perm=%v", filePath),
	}

	msg := fmt.Sprintf("emptydir 0777 on %v", formatMedium(medium))
	out := []string{
		"perms of file \"/test-volume/test-file\": -rwxrwxrwx",
		"content of file \"/test-volume/test-file\": mount-tester new file",
	}
	if medium == v1.StorageMediumMemory {
		out = append(out, "mount type of \"/test-volume\": tmpfs")
	}
	e2epodoutput.TestContainerOutput(ctx, f, msg, pod, 0, out)
}

func formatMedium(medium v1.StorageMedium) string {
	if medium == v1.StorageMediumMemory {
		return "tmpfs"
	}

	return "node default medium"
}

// testPodWithVolume 创建一个以给定 UID 运行、并在给定路径挂载给定 empty dir source 的 Pod。
// 如果 uid 为 0，Pod 将以其默认用户（root）运行。
func testPodWithVolume(uid int64, path string, source *v1.EmptyDirVolumeSource) *v1.Pod {
	podName := "pod-" + string(uuid.NewUUID())
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: podName,
		},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:  containerName,
					Image: imageutils.GetE2EImage(imageutils.Agnhost),
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      volumeName,
							MountPath: path,
						},
					},
				},
			},
			SecurityContext: &v1.PodSecurityContext{
				SELinuxOptions: &v1.SELinuxOptions{
					Level: "s0",
				},
			},
			RestartPolicy: v1.RestartPolicyNever,
			Volumes: []v1.Volume{
				{
					Name: volumeName,
					VolumeSource: v1.VolumeSource{
						EmptyDir: source,
					},
				},
			},
		},
	}

	if uid != 0 {
		pod.Spec.SecurityContext.RunAsUser = &uid
	}

	return pod
}

func createEmptyDirModePod(ctx context.Context, f *framework.Framework, mode *int32) *v1.Pod {
	podClient := e2epod.NewPodClient(f)
	pod := podClient.CreateSync(ctx, &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: "emptydir-mode-test"},
		Spec: v1.PodSpec{
			Containers: []v1.Container{
				{
					Name:    "test-container",
					Image:   imageutils.GetE2EImage(imageutils.BusyBox),
					Command: []string{"/bin/sleep", "10000"},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      "test-vol",
							MountPath: "/mnt/test-vol",
						},
					},
				},
			},
			Volumes: []v1.Volume{
				{
					Name: "test-vol",
					VolumeSource: v1.VolumeSource{
						EmptyDir: &v1.EmptyDirVolumeSource{
							Mode: mode,
						},
					},
				},
			},
		},
	})

	return pod
}
