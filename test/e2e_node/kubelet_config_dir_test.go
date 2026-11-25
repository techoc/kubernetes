/*
Copyright 2023 The Kubernetes Authors.

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
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/onsi/ginkgo/v2"
	"github.com/onsi/gomega"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	"k8s.io/kubernetes/test/e2e/feature"
	"k8s.io/kubernetes/test/e2e/framework"
	"k8s.io/utils/cpuset"
)

var _ = SIGDescribe("Kubelet Config", framework.WithSlow(), framework.WithSerial(), framework.WithDisruptive(), feature.KubeletConfigDropInDir, func() {
	f := framework.NewDefaultFramework("kubelet-config-drop-in-dir-test")
	ginkgo.Context("when merging drop-in configs", func() {
		var oldcfg *kubeletconfig.KubeletConfiguration
		ginkgo.BeforeEach(func(ctx context.Context) {
			var err error
			oldcfg, err = getCurrentKubeletConfig(ctx)
			framework.ExpectNoError(err)
		})
		ginkgo.AfterEach(func(ctx context.Context) {
			files, err := filepath.Glob(filepath.Join(framework.TestContext.KubeletConfigDropinDir, "*"+".conf"))
			framework.ExpectNoError(err)
			for _, file := range files {
				err := os.Remove(file)
				framework.ExpectNoError(err)
			}
			updateKubeletConfig(ctx, f, oldcfg, true)
		})
		ginkgo.It("should merge kubelet configs correctly", func(ctx context.Context) {
			// 获取初始 kubelet 配置
			initialConfig, err := getCurrentKubeletConfig(ctx)
			framework.ExpectNoError(err)

			ginkgo.By("Stopping the kubelet")
			restartKubelet := mustStopKubelet(ctx, f)

			configDir := framework.TestContext.KubeletConfigDropinDir

			contents := []byte(`apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
port: 10255
readOnlyPort: 10257
clusterDNS:
- 192.168.1.10
systemReserved:
  memory: 1Gi
authorization:
  mode: Webhook
  webhook:
    cacheAuthorizedTTL: "5m"
    cacheUnauthorizedTTL: "30s"
staticPodURLHeader:
  kubelet-api-support:
  - "Authorization: 234APSDFA"
  - "X-Custom-Header: 123"
  custom-static-pod:
  - "Authorization: 223EWRWER"
  - "X-Custom-Header: 456"
shutdownGracePeriodByPodPriority:
  - priority: 1
    shutdownGracePeriodSeconds: 60
  - priority: 2
    shutdownGracePeriodSeconds: 45
  - priority: 3
    shutdownGracePeriodSeconds: 30
featureGates:
  PodAndContainerStatsFromCRI: true`)
			framework.ExpectNoError(os.WriteFile(filepath.Join(configDir, "10-kubelet.conf"), contents, 0755))
			contents = []byte(`apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
clusterDNS:
- 192.168.1.1
- 192.168.1.5
- 192.168.1.8
port: 8080
cpuManagerReconcilePeriod: 1s
systemReserved:
  memory: 2Gi
authorization:
  mode: Webhook
  webhook:
    cacheAuthorizedTTL: "6m"
    cacheUnauthorizedTTL: "40s"
staticPodURLHeader:
  kubelet-api-support:
  - "Authorization: 8945AFSG1"
  - "X-Custom-Header: 987"
  custom-static-pod:
  - "Authorization: 223EWRWER"
  - "X-Custom-Header: 345"
shutdownGracePeriodByPodPriority:
  - priority: 1
    shutdownGracePeriodSeconds: 19
  - priority: 2
    shutdownGracePeriodSeconds: 41
  - priority: 6
    shutdownGracePeriodSeconds: 30
featureGates:
  KubeletServiceAccountTokenForCredentialProviders: true
  PodAndContainerStatsFromCRI: false
  DynamicResourceAllocation: true`)
			framework.ExpectNoError(os.WriteFile(filepath.Join(configDir, "20-kubelet.conf"), contents, 0755))
			ginkgo.By("Restarting the kubelet")
			restartKubelet(ctx)

			mergedConfig, err := getCurrentKubeletConfig(ctx)
			framework.ExpectNoError(err)

			// 用 expectedConfig 的值替换初始配置中的特定字段
			initialConfig.Port = int32(8080)          // 未被第二个文件覆盖，应保留。
			initialConfig.ReadOnlyPort = int32(10257) // 被第二个文件覆盖。
			if initialConfig.SystemReserved == nil {
				initialConfig.SystemReserved = map[string]string{}
			}
			if initialConfig.ReservedSystemCPUs != "" {
				reservedCPUSet, err := cpuset.Parse(initialConfig.ReservedSystemCPUs)
				if err == nil {
					initialConfig.SystemReserved["cpu"] = strconv.Itoa(reservedCPUSet.Size())
				}
			}
			initialConfig.SystemReserved["memory"] = "2Gi"
			initialConfig.ClusterDNS = []string{"192.168.1.1", "192.168.1.5", "192.168.1.8"} // 被第二个文件中的 slice 覆盖。
			// 此值在 drop-in 中显式设置，确保被保留
			initialConfig.CPUManagerReconcilePeriod = metav1.Duration{Duration: time.Second}
			// 同时，此值未显式设置，但可能被该类型的默认值 0 覆盖。
			// 确保真正的默认值被保留。
			initialConfig.CPUCFSQuotaPeriod = metav1.Duration{Duration: time.Duration(100000000)}
			// 这覆盖带值列表的 map 的情况。
			initialConfig.StaticPodURLHeader = map[string][]string{
				"kubelet-api-support": {"Authorization: 8945AFSG1", "X-Custom-Header: 987"},
				"custom-static-pod":   {"Authorization: 223EWRWER", "X-Custom-Header: 345"},
			}
			// 这覆盖结构体列表中字段被覆盖的情况。
			initialConfig.ShutdownGracePeriodByPodPriority = []kubeletconfig.ShutdownGracePeriodByPodPriority{
				{Priority: 1, ShutdownGracePeriodSeconds: 19},
				{Priority: 2, ShutdownGracePeriodSeconds: 41},
				{Priority: 6, ShutdownGracePeriodSeconds: 30},
			}
			// 这覆盖结构体中字段被覆盖的情况。
			initialConfig.Authorization = kubeletconfig.KubeletAuthorization{
				Mode: "Webhook",
				Webhook: kubeletconfig.KubeletWebhookAuthorization{
					CacheAuthorizedTTL:   metav1.Duration{Duration: time.Duration(6 * time.Minute)},
					CacheUnauthorizedTTL: metav1.Duration{Duration: time.Duration(40 * time.Second)},
				},
			}
			// 这覆盖 map 中字段被覆盖的情况。
			overrides := initialConfig.FeatureGates
			if overrides == nil {
				overrides = map[string]bool{}
			}
			overrides["PodAndContainerStatsFromCRI"] = false
			overrides["DynamicResourceAllocation"] = true
			overrides["KubeletServiceAccountTokenForCredentialProviders"] = true

			// 在某些 CI job 中，由于镜像基于 cgroupv1，`NodeSwap` 被显式禁用，
			// 因此这类 flag 应直接从初始配置中获取
			if _, ok := initialConfig.FeatureGates["NodeSwap"]; ok {
				overrides["NodeSwap"] = initialConfig.FeatureGates["NodeSwap"]
			}
			initialConfig.FeatureGates = overrides
			// 比较预期配置与合并后的配置
			gomega.Expect(initialConfig).To(gomega.BeComparableTo(mergedConfig), "Merged kubelet config does not match the expected configuration.")
		})
		ginkgo.It("should dump the effective KubeletConfiguration to the kubelet log at startup", func(ctx context.Context) {
			configDir := framework.TestContext.KubeletConfigDropinDir

			// 通过 drop-in 配置设置一个独特的非默认值。启动日志
			// 会 dump *生效的*（合并后）KubeletConfiguration，因此该值必须
			// 出现在日志中。
			contents := []byte(`apiVersion: kubelet.config.k8s.io/v1beta1
kind: KubeletConfiguration
eventBurst: 101`)

			dropinConfigPath := filepath.Join(configDir, "10-kubelet.conf")
			framework.ExpectNoError(os.WriteFile(dropinConfigPath, contents, 0755))

			ginkgo.By("Restarting the kubelet")
			restartKubelet := mustStopKubelet(ctx, f)
			restartKubelet(ctx)

			ginkgo.By("Verifying the effective KubeletConfiguration is dumped to the kubelet log")
			// 启动 dump 在 kubelet 启动后不久记录一次。轮询
			// kubelet 日志，以免因日志尚未刷新而不稳定。
			gomega.Eventually(ctx, func() (string, error) {
				kubeletLog, err := os.ReadFile(framework.TestContext.ReportDir + "/kubelet.log")
				if err != nil {
					return "", err
				}
				return string(kubeletLog), nil
			}).WithPolling(5*time.Second).WithTimeout(time.Minute).Should(gomega.And(
				// 生效配置 dump 发出的可 grep 的启动头。
				gomega.ContainSubstring("Effective KubeletConfiguration"),
				// 标记 dump 的值是序列化的 KubeletConfiguration。
				gomega.ContainSubstring("kind: KubeletConfiguration"),
				// 通过上面的 drop-in 配置设置的合并后生效值。
				gomega.ContainSubstring("eventBurst: 101"),
			), "kubelet log should contain the effective KubeletConfiguration with merged values")
		})
	})

})
