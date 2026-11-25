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

package e2enode

import (
	"context"
	"fmt"
	"path/filepath"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/kubernetes/pkg/features"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/pkg/volume/util/fsquota"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	e2eskipper "k8s.io/kubernetes/test/e2e/framework/skipper"
	imageutils "k8s.io/kubernetes/test/utils/image"
	"k8s.io/mount-utils"
	admissionapi "k8s.io/pod-security-admission/api"
	"k8s.io/utils/ptr"

	"github.com/onsi/ginkgo/v2"
)

const (
	LSCIQuotaFeature = features.LocalStorageCapacityIsolationFSQuotaMonitoring
)

func runOneQuotaTest(f *framework.Framework, quotasRequested bool, userNamespacesEnabled bool) {
	evictionTestTimeout := 10 * time.Minute
	sizeLimit := resource.MustParse("100Mi")
	useOverLimit := 101 /* Mb */
	useUnderLimit := 99 /* Mb */

	priority := 0
	if quotasRequested {
		priority = 1
	}
	ginkgo.Context(fmt.Sprintf(testContextFmt, fmt.Sprintf("use quotas for LSCI monitoring (quotas enabled: %v, userNamespacesEnabled: %v)", quotasRequested, userNamespacesEnabled)), func() {
		tempSetCurrentKubeletConfig(f, func(ctx context.Context, initialConfig *kubeletconfig.KubeletConfiguration) {
			defer withFeatureGate(LSCIQuotaFeature, quotasRequested)()
			if !supportsUserNS(ctx, f) {
				e2eskipper.Skipf("runtime does not support user namespaces")
			}
			if quotasRequested && !supportsQuotas(framework.TestContext.KubeletRootDir, userNamespacesEnabled) {
				// 如果底层文件系统未启用配额，将该测试作为正向测试就没有意义。
				// 如果底层文件系统未启用配额，将该测试作为正向测试就没有意义。
				e2eskipper.Skipf("Cannot run LocalStorageCapacityIsolationFSQuotaMonitoring on filesystem without project quota enabled")
			}
			// 将阈值设为 0% 会禁用；非空 map 会覆盖默认值（因 omitempty 而必需）
			initialConfig.EvictionHard = map[string]string{"memory.available": "0%"}

			if initialConfig.FeatureGates == nil {
				initialConfig.FeatureGates = map[string]bool{}
			}
			initialConfig.FeatureGates[string(LSCIQuotaFeature)] = quotasRequested
		})
		runEvictionTest(f, evictionTestTimeout, noPressure, noStarvedResource, logDiskMetrics, []podEvictSpec{
			{
				evictionPriority: priority, // 只有启用配额时，此 Pod 才应因 emptyDir 违规被驱逐
				pod: diskConcealingPod(fmt.Sprintf("emptydir-concealed-disk-over-sizelimit-quotas-%v", quotasRequested), useOverLimit, &v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{SizeLimit: &sizeLimit},
				}, v1.ResourceRequirements{}),
			},
			{
				evictionPriority: 0, // 此 Pod 不应被驱逐，因为用量低于限制（配额测试）
				pod: diskConcealingPod(fmt.Sprintf("emptydir-concealed-disk-under-sizelimit-quotas-%v", quotasRequested), useUnderLimit, &v1.VolumeSource{
					EmptyDir: &v1.EmptyDirVolumeSource{SizeLimit: &sizeLimit},
				}, v1.ResourceRequirements{}),
				prePodCreationModificationFunc: func(ctx context.Context, pod *v1.Pod) {
					// XFS 的推测性预分配可能导致表观用量虚高（参见 isXfs）。
					if isXfs(framework.TestContext.KubeletRootDir) {
						pod.Spec.Containers[0].Command = []string{
							"perl",
							"-e",
							fmt.Sprintf(writeConcealedPodCommand, filepath.Join(volumeMountPath, "file"), 50 /* Mb */),
						}
					}
				},
			},
		})
	})
}

// LocalStorageCapacityIsolationFSQuotaMonitoring 测试是否使用配额
// 而非 du 来监控。方法是创建一个
// Pod，它创建文件、删除文件，然后继续写入数据。如果
// 使用配额监控，就能检测到这个已删除但仍在使用的文件；
// 如果用 du 监控，则检测不到。
var _ = SIGDescribe("LocalStorageCapacityIsolationFSQuotaMonitoring", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.LocalStorageCapacityIsolationQuota, feature.LSCIQuotaMonitoring, feature.UserNamespacesSupport, func() {
	f := framework.NewDefaultFramework("localstorage-quota-monitoring-test")
	f.NamespacePodSecurityLevel = admissionapi.LevelPrivileged
	runOneQuotaTest(f, true, true)
	runOneQuotaTest(f, true, false)
	runOneQuotaTest(f, false, true)
	addAfterEachForCleaningUpPods(f)
})

const (
	writeConcealedPodCommand = `
my $file = "%s.bin";
open OUT, ">$file" || die "Cannot open $file: $!\n";
unlink "$file" || die "Cannot unlink $file: $!\n";
my $a = "a";
foreach (1..20) { $a = "$a$a"; }
foreach (1..%d) { syswrite(OUT, $a); }
sleep 999999;`
)

// 这是测试因隐藏文件中的磁盘空间使用而驱逐 Pod 所需的；shell 没有便捷
// 的方法对隐藏文件执行 I/O，而 busybox 镜像中不含 Perl。
func diskConcealingPod(name string, diskConsumedMB int, volumeSource *v1.VolumeSource, resources v1.ResourceRequirements) *v1.Pod {
	path := ""
	volumeMounts := []v1.VolumeMount{}
	volumes := []v1.Volume{}
	if volumeSource != nil {
		path = volumeMountPath
		volumeMounts = []v1.VolumeMount{{MountPath: volumeMountPath, Name: volumeName}}
		volumes = []v1.Volume{{Name: volumeName, VolumeSource: *volumeSource}}
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("%s-pod", name)},
		Spec: v1.PodSpec{
			HostUsers:     ptr.To(false),
			RestartPolicy: v1.RestartPolicyNever,
			Containers: []v1.Container{
				{
					Image: imageutils.GetE2EImage(imageutils.Perl),
					Name:  fmt.Sprintf("%s-container", name),
					Command: []string{
						"perl",
						"-e",
						fmt.Sprintf(writeConcealedPodCommand, filepath.Join(path, "file"), diskConsumedMB),
					},
					Resources:    resources,
					VolumeMounts: volumeMounts,
				},
			},
			Volumes: volumes,
		},
	}
}

// 不必费心返回错误；如果出问题，
// 直接当作 "no" 处理。
func supportsQuotas(dir string, userNamespacesEnabled bool) bool {
	supportsQuota, err := fsquota.SupportsQuotas(mount.New(""), dir, userNamespacesEnabled)
	return supportsQuota && err == nil
}
