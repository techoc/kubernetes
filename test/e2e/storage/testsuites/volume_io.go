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

/*
 * 本测试以不同写入大小对已挂载文件做伪流式写入，检查插件的 VolumeSource 是否正常工作。
 */

package testsuites

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/onsi/ginkgo/v2"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/errors"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
)

// 各文件大小对应测试文件的 SHA256 哈希值。
// 测试文件在 testVolumeIO() 中生成。
// 若文件生成算法发生变化，必须重新计算这些值。
var sha256sum = map[int64]string{
	storageframework.FileSizeSmall:  "aa39c2cbd49a28425c977d229b43e1ff9579ecd98cea7b466982bab04753f2ab",
	storageframework.FileSizeMedium: "5f718f3b7788352e1450c6c14c6051f0298d49ac6af18c5a1ce3cd3b5947de24",
	storageframework.FileSizeLarge:  "11b058d55b088031fbdec33e3e896c032550ff20ca0ed37f76ad1d5d155c5f50",
}

const mountPath = "/opt"

type volumeIOTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

// InitCustomVolumeIOTestSuite 使用自定义的测试模式创建 volumeIOTestSuite，
// 该类型实现了 TestSuite 接口。
func InitCustomVolumeIOTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &volumeIOTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "volumeIO",
			TestPatterns: patterns,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Mi",
			},
		},
	}
}

// InitVolumeIOTestSuite 使用测试套件的默认模式创建 volumeIOTestSuite，
// 该类型实现了 TestSuite 接口。
func InitVolumeIOTestSuite() storageframework.TestSuite {
	patterns := []storageframework.TestPattern{
		storageframework.DefaultFsInlineVolume,
		storageframework.DefaultFsPreprovisionedPV,
		storageframework.DefaultFsDynamicPV,
		storageframework.NtfsDynamicPV,
	}
	return InitCustomVolumeIOTestSuite(patterns)
}

func (t *volumeIOTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return t.tsInfo
}

func (t *volumeIOTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	return checkVolTypePatterns(pattern, driver, storageframework.NewVolTypeMap(
		storageframework.PreprovisionedPV,
		storageframework.InlineVolume))
}

func (t *volumeIOTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config *storageframework.PerTestConfig

		resource *storageframework.VolumeResource

		migrationCheck *migrationOpCheck
	}
	var (
		dInfo = driver.GetDriverInfo()
		l     local
	)

	// 注意它还会注册 AfterEach，使 f 在之后不可用。所有使用 f 的代码
	// 都必须放在 It 或 Context 的回调中执行。
	f := framework.NewFrameworkWithCustomTimeouts("volumeio", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	init := func(ctx context.Context) {
		l = local{}

		// 接下来执行开销较大的测试初始化。
		l.config = driver.PrepareTest(ctx, f)
		l.migrationCheck = newMigrationOpCheck(ctx, f.ClientSet, f.ClientConfig(), dInfo.InTreePluginName)

		testVolumeSizeRange := t.GetTestSuiteInfo().SupportedSizeRange
		l.resource = storageframework.CreateVolumeResource(ctx, driver, l.config, pattern, testVolumeSizeRange)
		if l.resource.VolSource == nil {
			e2eskipper.Skipf("Driver %q does not define volumeSource - skipping", dInfo.Name)
		}

	}

	cleanup := func(ctx context.Context) {
		var errs []error
		if l.resource != nil {
			errs = append(errs, l.resource.CleanupResource(ctx))
			l.resource = nil
		}

		framework.ExpectNoError(errors.NewAggregate(errs), "while cleaning up resource")
		l.migrationCheck.validateMigrationVolumeOpCounts(ctx)
	}

	f.It("should write files of various sizes, verify size, validate content", f.WithSlow(), func(ctx context.Context) {
		init(ctx)
		ginkgo.DeferCleanup(cleanup)

		cs := f.ClientSet
		fileSizes := createFileSizes(dInfo.MaxFileSize)
		testFile := fmt.Sprintf("%s_io_test_%s", dInfo.Name, f.Namespace.Name)
		var fsGroup *int64
		if !framework.NodeOSDistroIs("windows") && dInfo.Capabilities[storageframework.CapFsGroup] {
			fsGroupVal := int64(1234)
			fsGroup = &fsGroupVal
		}
		podSec := v1.PodSecurityContext{
			FSGroup: fsGroup,
		}
		err := testVolumeIO(ctx, f, cs, storageframework.ConvertTestConfig(l.config), *l.resource.VolSource, &podSec, testFile, fileSizes)
		framework.ExpectNoError(err)
	})
}

func createFileSizes(maxFileSize int64) []int64 {
	allFileSizes := []int64{
		storageframework.FileSizeSmall,
		storageframework.FileSizeMedium,
		storageframework.FileSizeLarge,
	}
	fileSizes := []int64{}

	for _, size := range allFileSizes {
		if size <= maxFileSize {
			fileSizes = append(fileSizes, size)
		}
	}

	return fileSizes
}

// 返回插件的客户端 Pod spec，使用 InitContainer 准备文件 I/O 测试环境。
func makePodSpec(config e2evolume.TestConfig, initCmd string, volsrc v1.VolumeSource, podSecContext *v1.PodSecurityContext) *v1.Pod {
	var gracePeriod int64 = 1
	volName := fmt.Sprintf("io-volume-%s", config.Namespace)
	pod := &v1.Pod{
		TypeMeta: metav1.TypeMeta{
			Kind:       "Pod",
			APIVersion: "v1",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name: config.Prefix + "-io-client",
			Labels: map[string]string{
				"role": config.Prefix + "-io-client",
			},
		},
		Spec: v1.PodSpec{
			InitContainers: []v1.Container{
				{
					Name:  config.Prefix + "-io-init",
					Image: e2epod.GetDefaultTestImage(),
					Command: []string{
						"/bin/sh",
						"-c",
						initCmd,
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      volName,
							MountPath: mountPath,
						},
					},
				},
			},
			Containers: []v1.Container{
				{
					Name:  config.Prefix + "-io-client",
					Image: e2epod.GetDefaultTestImage(),
					Command: []string{
						"/bin/sh",
						"-c",
						"sleep 3600", // 保持 Pod 存活，直到被显式删除
					},
					VolumeMounts: []v1.VolumeMount{
						{
							Name:      volName,
							MountPath: mountPath,
						},
					},
				},
			},
			TerminationGracePeriodSeconds: &gracePeriod,
			SecurityContext:               podSecContext,
			Volumes: []v1.Volume{
				{
					Name:         volName,
					VolumeSource: volsrc,
				},
			},
			RestartPolicy: v1.RestartPolicyNever, // init container 失败时应让 Pod 也失败
		},
	}

	e2epod.SetNodeSelection(&pod.Spec, config.ClientNodeSelection)
	return pod
}

// 在 Pod 中用 dd 和 `ddInput` 文件向 `fpath` 写入 `fsize` 字节。
func writeToFile(ctx context.Context, f *framework.Framework, pod *v1.Pod, fpath, ddInput string, fsize int64) error {
	ginkgo.By(fmt.Sprintf("writing %d bytes to test file %s", fsize, fpath))
	loopCnt := fsize / storageframework.MinFileSize
	writeCmd := fmt.Sprintf("i=0; while [ $i -lt %d ]; do dd if=%s bs=%d >>%s 2>/dev/null; let i+=1; done", loopCnt, ddInput, storageframework.MinFileSize, fpath)
	stdout, stderr, err := e2epod.ExecShellInPodWithFullOutput(ctx, f, pod.Name, writeCmd)
	if err != nil {
		return fmt.Errorf("error writing to volume using %q: %s\nstdout: %s\nstderr: %s", writeCmd, err, stdout, stderr)
	}
	return err
}

// 验证测试文件大小和内容均符合预期。
func verifyFile(ctx context.Context, f *framework.Framework, pod *v1.Pod, fpath string, expectSize int64, ddInput string) error {
	ginkgo.By("verifying file size")
	rtnstr, stderr, err := e2epod.ExecShellInPodWithFullOutput(ctx, f, pod.Name, fmt.Sprintf("stat -c %%s %s", fpath))
	if err != nil || rtnstr == "" {
		return fmt.Errorf("unable to get file size via `stat %s`: %v\nstdout: %s\nstderr: %s", fpath, err, rtnstr, stderr)
	}
	size, err := strconv.Atoi(strings.TrimSuffix(rtnstr, "\n"))
	if err != nil {
		return fmt.Errorf("unable to convert string %q to int: %w", rtnstr, err)
	}
	if int64(size) != expectSize {
		return fmt.Errorf("size of file %s is %d, expected %d", fpath, size, expectSize)
	}

	ginkgo.By("verifying file hash")
	rtnstr, stderr, err = e2epod.ExecShellInPodWithFullOutput(ctx, f, pod.Name, fmt.Sprintf("sha256sum %s | cut -d' ' -f1", fpath))
	if err != nil {
		return fmt.Errorf("unable to test file hash via `sha256sum %s`: %v\nstdout: %s\nstderr: %s", fpath, err, rtnstr, stderr)
	}
	actualHash := strings.TrimSuffix(rtnstr, "\n")
	expectedHash, ok := sha256sum[expectSize]
	if !ok {
		return fmt.Errorf("file hash is unknown for file size %d. Was a new file size added to the test suite?",
			expectSize)
	}
	if actualHash != expectedHash {
		return fmt.Errorf("SHA256 hash is incorrect for file %s with size %d. Expected: `%s`; Actual: `%s`",
			fpath, expectSize, expectedHash, actualHash)
	}

	return nil
}

// 删除 `fpath` 以节省宿主机磁盘空间；删除错误仅记录，不视为失败。
func deleteFile(ctx context.Context, f *framework.Framework, pod *v1.Pod, fpath string) {
	ginkgo.By(fmt.Sprintf("deleting test file %s...", fpath))
	stdout, stderr, err := e2epod.ExecShellInPodWithFullOutput(ctx, f, pod.Name, fmt.Sprintf("rm -f %s", fpath))
	if err != nil {
		// 继续即可，卷卸载时测试目录会被删除
		framework.Logf("unable to delete test file %s: %v\nerror ignored, continuing test\nstdout: %s\nstderr: %s", fpath, err, stdout, stderr)
	}
}

// 创建客户端 Pod，并按 `fsizes` 参数指定的大小创建文件，结束后删除客户端 Pod 和新文件。
// 注意：文件名会追加到 "/opt/<Prefix>/<namespace>" 之后，例如 "/opt/nfs/e2e-.../<file>"。
// 注意：podSecContext 参数可传 nil，此时忽略该参数。
// 注意：`fsizes` 中的每个值都会被强制为不小于 `MinFileSize` 且是其整数倍
//
//	bytes.
func testVolumeIO(ctx context.Context, f *framework.Framework, cs clientset.Interface, config e2evolume.TestConfig, volsrc v1.VolumeSource, podSecContext *v1.PodSecurityContext, file string, fsizes []int64) (err error) {
	ddInput := filepath.Join(mountPath, fmt.Sprintf("%s-%s-dd_if", config.Prefix, config.Namespace))
	writeBlk := strings.Repeat("abcdefghijklmnopqrstuvwxyz123456", 32) // 1KiB value
	loopCnt := storageframework.MinFileSize / int64(len(writeBlk))
	// initContainer 的命令，用于创建并填充 dd 的输入文件。目前输入文件为 1MiB；
	// 为避免在 Go 代码中存放 1MiB 的值，改用循环在目标目录中生成该文件。
	initCmd := fmt.Sprintf("i=0; while [ $i -lt %d ]; do echo -n %s >>%s; let i+=1; done", loopCnt, writeBlk, ddInput)

	clientPod := makePodSpec(config, initCmd, volsrc, podSecContext)

	ginkgo.By(fmt.Sprintf("starting %s", clientPod.Name))
	podsNamespacer := cs.CoreV1().Pods(config.Namespace)
	clientPod, err = podsNamespacer.Create(ctx, clientPod, metav1.CreateOptions{})
	if err != nil {
		return fmt.Errorf("failed to create client pod %q: %w", clientPod.Name, err)
	}
	ginkgo.DeferCleanup(func(ctx context.Context) {
		deleteFile(ctx, f, clientPod, ddInput)
		ginkgo.By(fmt.Sprintf("deleting client pod %q...", clientPod.Name))
		e := e2epod.DeletePodWithWait(ctx, cs, clientPod)
		if e != nil {
			framework.Logf("client pod failed to delete: %v", e)
			if err == nil { // delete err is returned if err is not set
				err = e
			}
		} else {
			framework.Logf("sleeping a bit so kubelet can unmount and detach the volume")
			time.Sleep(e2evolume.PodCleanupTimeout)
		}
	})

	err = e2epod.WaitTimeoutForPodRunningInNamespace(ctx, cs, clientPod.Name, clientPod.Namespace, f.Timeouts.PodStart)
	if err != nil {
		return fmt.Errorf("client pod %q not running: %w", clientPod.Name, err)
	}

	// 按传入的大小创建文件，并校验文件大小和内容
	for _, fsize := range fsizes {
		// 文件大小必须是 `MinFileSize` 的整数倍
		if math.Mod(float64(fsize), float64(storageframework.MinFileSize)) != 0 {
			fsize = fsize/storageframework.MinFileSize + storageframework.MinFileSize
		}
		fpath := filepath.Join(mountPath, fmt.Sprintf("%s-%d", file, fsize))
		defer func() {
			deleteFile(ctx, f, clientPod, fpath)
		}()
		if err = writeToFile(ctx, f, clientPod, fpath, ddInput, fsize); err != nil {
			return err
		}
		if err = verifyFile(ctx, f, clientPod, fpath, fsize, ddInput); err != nil {
			return err
		}
	}

	return
}
