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

package testsuites

import (
	"context"
	"fmt"
	"strings"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/component-base/featuregate"
	"k8s.io/kubernetes/pkg/features"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2epod "k8s.io/kubernetes/test/e2e/framework/pod"
	e2evolume "k8s.io/kubernetes/test/e2e/framework/volume"
	storageframework "k8s.io/kubernetes/test/e2e/storage/framework"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"
)

type seLinuxMountTestSuite struct {
	tsInfo storageframework.TestSuiteInfo
}

var (
	// 测试 Pod 使用的 SELinux label。
	seLinuxLabel = &v1.SELinuxOptions{Level: "s0:c0,c1"}
	// 期望的 SELinux mount option，同时匹配 Debian（system_u:object_r:svirt_lxc_net_t）和基于 Fedora 的发行版（system_u:object_r:container_file_t）
	seLinuxMountOptionRE = "context=\"system_u:object_r:[^:]*:s0:c0,c1\""
)

// InitCustomSELinuxMountTestSuite 返回使用自定义 test pattern 实现 TestSuite 接口的 seLinuxMountTestSuite
func InitCustomSELinuxMountTestSuite(patterns []storageframework.TestPattern) storageframework.TestSuite {
	return &seLinuxMountTestSuite{
		tsInfo: storageframework.TestSuiteInfo{
			Name:         "seLinuxMount",
			TestPatterns: patterns,
			SupportedSizeRange: e2evolume.SizeRange{
				Min: "1Mi",
			},
			TestTags: []interface{}{
				feature.SELinux,
				framework.WithFeatureGate(features.SELinuxMountReadWriteOncePod),
				framework.WithFeatureGate(features.SELinuxChangePolicy),
			},
		},
	}
}

// InitSELinuxMountTestSuite 返回使用 testsuite 默认 pattern 实现 TestSuite 接口的 seLinuxMountTestSuite
func InitSELinuxMountTestSuite() storageframework.TestSuite {
	patterns := []storageframework.TestPattern{
		storageframework.DefaultFsDynamicPV,
	}
	return InitCustomSELinuxMountTestSuite(patterns)
}

func (s *seLinuxMountTestSuite) GetTestSuiteInfo() storageframework.TestSuiteInfo {
	return s.tsInfo
}

func (s *seLinuxMountTestSuite) SkipUnsupportedTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) string {
	if !driver.GetDriverInfo().Capabilities[storageframework.CapSELinuxMount] {
		return fmt.Sprintf("Driver %q does not support SELinuxMount", driver.GetDriverInfo().Name)
	}
	return ""
}

func (s *seLinuxMountTestSuite) DefineTests(driver storageframework.TestDriver, pattern storageframework.TestPattern) {
	type local struct {
		config *storageframework.PerTestConfig

		resource *storageframework.VolumeResource
		pod      *v1.Pod
	}
	var l local

	// 注意，它还会注册一个 AfterEach，使 f 变得不可用。
	// 任何使用 f 的代码都必须在 It 或 Context 回调中运行。
	f := framework.NewFrameworkWithCustomTimeouts("selinux-mount", storageframework.GetDriverTimeouts(driver))
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged

	init := func(ctx context.Context, accessMode v1.PersistentVolumeAccessMode) {
		l = local{}
		l.config = driver.PrepareTest(ctx, f)
		testVolumeSizeRange := s.GetTestSuiteInfo().SupportedSizeRange
		l.resource = storageframework.CreateVolumeResourceWithAccessModes(ctx, driver, l.config, pattern, testVolumeSizeRange, []v1.PersistentVolumeAccessMode{accessMode}, nil)
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
	}

	tests := []struct {
		name                     string
		accessMode               v1.PersistentVolumeAccessMode
		seLinuxChangePolicy      *v1.PodSELinuxChangePolicy
		featureGates             []featuregate.Feature
		expectSELinuxMountOption bool
	}{
		{
			name:                     "Pod with RWOP volume and the default policy",
			accessMode:               v1.ReadWriteOncePod,
			seLinuxChangePolicy:      nil, // 意味着使用 mount option
			expectSELinuxMountOption: true,
		},
		{
			name:                     "Pod with RWO volume and mount policy",
			accessMode:               v1.ReadWriteOnce,
			seLinuxChangePolicy:      ptr.To(v1.SELinuxChangePolicyMountOption),
			featureGates:             []featuregate.Feature{features.SELinuxMount},
			expectSELinuxMountOption: true,
		},
		{
			name:                     "Pod with RWOP volume and recursive policy",
			accessMode:               v1.ReadWriteOncePod,
			seLinuxChangePolicy:      ptr.To(v1.SELinuxChangePolicyRecursive),
			expectSELinuxMountOption: false,
		},
		{
			name:                     "Pod with RWO and recursive policy",
			accessMode:               v1.ReadWriteOnce,
			seLinuxChangePolicy:      ptr.To(v1.SELinuxChangePolicyRecursive),
			expectSELinuxMountOption: false,
		},
	}
	for _, test := range tests {
		framework.Context(test.name, func() {
			// 动态拼装 framework.It 的参数，因为 feature gate 是动态变化的
			var args []interface{}
			// 测试名称
			if test.expectSELinuxMountOption {
				args = append(args, "should mount volumes with SELinux mount option")
			} else {
				args = append(args, "should mount volumes without SELinux mount option")
			}
			// feature gate
			for _, fg := range test.featureGates {
				args = append(args, framework.WithFeatureGate(fg))
			}
			// 测试主体
			args = append(args, func(ctx context.Context) {
				init(ctx, test.accessMode)
				ginkgo.DeferCleanup(cleanup)

				ginkgo.By("Creating a pod with PVC")
				podConfig := e2epod.Config{
					NS:                     f.Namespace.Name,
					PVCs:                   []*v1.PersistentVolumeClaim{l.resource.Pvc},
					SeLinuxLabel:           seLinuxLabel,
					NodeSelection:          l.config.ClientNodeSelection,
					PodSELinuxChangePolicy: test.seLinuxChangePolicy,
				}
				var err error
				l.pod, err = e2epod.CreateSecPod(ctx, f.ClientSet, &podConfig, f.Timeouts.PodStart)
				framework.ExpectNoError(err, "while creating the pod")

				ginkgo.By("Waiting for pod to be ready")
				err = e2epod.WaitForPodRunningInNamespace(ctx, f.ClientSet, l.pod)
				framework.ExpectNoError(err, "while waiting for the pod to be ready")

				ginkgo.By("Checking the volume mount options")
				mountOptions, err := getVolumeMountOptions(f, l.pod.Name, "write-pod", "/mnt/volume1")
				framework.ExpectNoError(err, "while getting the mount options")
				framework.Logf("Detected mount options: %s", mountOptions)
				if test.expectSELinuxMountOption {
					gomega.Expect(mountOptions).To(gomega.MatchRegexp(seLinuxMountOptionRE), "-o context mount option should be present")
				} else {
					gomega.Expect(mountOptions).NotTo(gomega.MatchRegexp(seLinuxMountOptionRE), "-o context mount option should not be present")
				}
			})
			framework.It(args...)
		})
	}
}

func getVolumeMountOptions(f *framework.Framework, podName string, containerName string, volumePath string) (string, error) {
	cmd := []string{"cat", "/proc/mounts"}
	stdout, stderr, err := e2epod.ExecCommandInContainerWithFullOutput(f, podName, containerName, cmd...)
	if err != nil {
		// 记录这次调用的所有细节，但不记录会由调用方记录的错误
		framework.Logf("Executed: %s", strings.Join(cmd, " "))
		framework.Logf("stdout: %s", stdout)
		framework.Logf("stderr: %s", stderr)
		return "", err
	}

	for _, line := range strings.Split(stdout, "\n") {
		parts := strings.Fields(line)
		if len(parts) < 4 {
			continue
		}
		mountPath := parts[1]
		if mountPath == volumePath {
			return parts[3], nil
		}
	}
	return "", fmt.Errorf("volume path %s not found in /proc/mounts", volumePath)
}
