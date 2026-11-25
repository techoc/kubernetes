/*
Copyright The Kubernetes Authors.

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

package e2edra

import (
	"strings"
	"testing"
	"time"

	"github.com/onsi/gomega"

	restclient "k8s.io/client-go/rest"
	"k8s.io/dynamic-resource-allocation/resourceslice"
	drautils "k8s.io/kubernetes/test/e2e/dra/utils"
	e2enode "k8s.io/kubernetes/test/e2e/framework/node"
	e2etestfiles "k8s.io/kubernetes/test/e2e/framework/testfiles"
	"k8s.io/kubernetes/test/utils/client-go/ktesting"
	"k8s.io/kubernetes/test/utils/localupcluster"
)

// gateOnFunc 在 feature gate 开启阶段运行，并返回下一阶段的函数。
type gateOnFunc func(tCtx ktesting.TContext, b *drautils.Builder) gateOffFunc

// gateOffFunc 在 feature gate 关闭阶段运行，并返回下一阶段的函数。
type gateOffFunc func(tCtx ktesting.TContext) gateOnAgainFunc

// gateOnAgainFunc 在 feature gate 重新开启后运行。
type gateOnAgainFunc func(tCtx ktesting.TContext)

type getResources func(nodes *drautils.Nodes) map[string]resourceslice.DriverResources

func TestFeatureGateCycle(t *testing.T) { testFeatureGateCycle(ktesting.Init(t)) }

func testFeatureGateCycle(tCtx ktesting.TContext) {
	e2etestfiles.AddFileSource(e2etestfiles.RootFileSource{Root: repoRoot})

	gomega.RegisterFailHandler(func(message string, callerSkip ...int) {
		tCtx.Helper()
		tCtx.Fatal(message)
	})

	envName, dir := currentBinDir()
	if dir == "" {
		tCtx.Fatalf("%s must be set", envName)
	}

	subTests := map[string]struct {
		test                gateOnFunc
		driverResourcesFunc getResources
		// enabledGates 列出整个测试期间保持开启的 feature gate。
		enabledGates []string
		// toggledGates 列出阶段 0 开启、阶段 1 关闭、阶段 2 重新开启的 feature gate。
		toggledGates []string
		// emulatedVersion 非空时会作为 EMULATED_VERSION 传给 local-up-cluster.sh。
		// 设为这些 gate 仍可切换的最后一个版本，这样即使将来版本将其锁定，
		// 测试依然能继续运行。
		emulatedVersion    string
		extraRuntimeConfig string
	}{
		"extended-resource": {
			test:                extendedResourceGateCycle,
			driverResourcesFunc: extendedResourcesDriverResources,
			toggledGates:        []string{"DRAExtendedResource"},
			// 模拟 1.36，使当前二进制中已锁定的 gate 仍可切换。
			// DRAExtendedResource 在 1.36 中可切换。
			emulatedVersion: "1.36",
		},
		"workload-resourceclaims": {
			test:                workloadResourceClaimsGateCycle,
			driverResourcesFunc: workloadResourceClaimsDriverResources,
			enabledGates:        []string{"GenericWorkload"},
			toggledGates:        []string{"DRAWorkloadResourceClaims"},
			extraRuntimeConfig:  "scheduling.k8s.io/v1beta1",
		},
		"compatibility-groups": {
			test:                compatibilityGroupsGateCycle,
			driverResourcesFunc: compatibilityGroupsDriverResources,
			toggledGates:        []string{"DRADeviceCompatibilityGroups"},
			// DRADeviceCompatibilityGroups 自 1.37 起为 alpha 且可切换；
			// 模拟该版本，以便 gate 锁定后测试仍能运行。
			emulatedVersion: "1.37",
		},
	}

	for name, def := range subTests {
		tCtx.Run(name, func(tCtx ktesting.TContext) {
			// ---- 阶段 0：gate 开启 ----
			cluster := localupcluster.New()
			tCtx.CleanupCtx(func(tCtx ktesting.TContext) {
				tCtx.Step("cleanup", cluster.Stop)
			})
			localUpClusterEnv := map[string]string{
				"RUNTIME_CONFIG": localUpClusterRuntimeConfig,
			}
			if def.extraRuntimeConfig != "" {
				localUpClusterEnv["RUNTIME_CONFIG"] += "," + def.extraRuntimeConfig
			}
			if def.emulatedVersion != "" {
				localUpClusterEnv["EMULATED_VERSION"] = def.emulatedVersion
			}
			phase := "phase-0-gate-on"
			cluster.Start(tCtx, phase, dir, localUpClusterEnv, turnFgOn(def.enabledGates)+","+turnFgOn(def.toggledGates))

			restConfig := cluster.LoadConfig(tCtx)
			restConfig.UserAgent = restclient.DefaultKubernetesUserAgent() + " -- dra"
			tCtx = tCtx.WithRESTConfig(restConfig).WithNamespace("default")

			var nodes *drautils.Nodes
			tCtx.Step("wait for node", func(tCtx ktesting.TContext) {
				tCtx.ExpectNoError(e2enode.WaitForAllNodesSchedulable(tCtx, tCtx.Client(), 5*time.Minute))
				nodes = drautils.NewNodesNow(tCtx, 1, 1)
			})

			// 创建 builder
			driver := drautils.NewDriverInstance(tCtx)
			driver.SetNameSuffix(tCtx, name)
			driver.IsLocal = true
			driver.Run(tCtx, "/var/lib/kubelet", nodes, def.driverResourcesFunc(nodes))
			builder := drautils.NewBuilderNow(tCtx, driver)
			builder.SkipCleanup = true

			var gateOffFn gateOffFunc
			tCtx.Run(phase, func(tCtx ktesting.TContext) {
				gateOffFn = def.test(tCtx, builder)
			})

			// ---- 阶段 1：gate 关闭 ----
			phase = "phase-1-gate-off"
			// gate 关闭时，apiserver 会丢弃 driver 重新发布的 ResourceSlice 中的受控字段：
			// kubelet 重启会清空 slice，driver 随之重建，而创建请求中的字段会被剔除。
			// 在 gate 重新开启并重新发布 slice 之前，需要容忍这种预期的字段丢弃。
			driver.SetExpectDroppedFields(true)
			cluster.ToggleFeatureGates(tCtx, phase, turnFgOn(def.enabledGates)+","+turnFgOff(def.toggledGates))
			waitForSlices(tCtx, name, builder, def.driverResourcesFunc(nodes))

			var gateOnAgainFn gateOnAgainFunc
			tCtx.Run(phase, func(tCtx ktesting.TContext) {
				gateOnAgainFn = gateOffFn(tCtx)
			})

			// ---- 阶段 2：gate 重新开启 ----
			phase = "phase-2-gate-on-again"
			cluster.ToggleFeatureGates(tCtx, phase, turnFgOn(def.enabledGates)+","+turnFgOn(def.toggledGates))
			// gate 关闭期间，apiserver 丢弃了受控字段，resourceslice controller
			// 为避免热更新循环，将期望状态锁定为存储中已被剔除字段的结果。
			// 若不重新发布，即便 apiserver 现已接受这些字段，driver 仍会持续发布
			// 不含受控字段的 slice。ToggleFeatureGates 只在 gate 开启的
			// apiserver 就绪后才返回，因此这次重新发布必定到达保留字段的 apiserver。
			driver.PublishResources(tCtx, def.driverResourcesFunc(nodes))
			waitForSlices(tCtx, name, builder, def.driverResourcesFunc(nodes))
			// gate 已重新开启，slice 也已带上受控字段重新发布；
			// 此后任何字段丢弃错误都是真正的 bug。
			driver.SetExpectDroppedFields(false)

			tCtx.Run(phase, func(tCtx ktesting.TContext) {
				gateOnAgainFn(tCtx)
			})
		})
	}
}

// turnFgOn 返回用于开启指定 feature gate 的字符串，例如 "FeatureA=true,FeatureB=true"。
func turnFgOn(featureGates []string) string {
	return turnFg(featureGates, "=true")
}

// turnFgOff 返回用于关闭指定 feature gate 的字符串，例如 "FeatureA=false,FeatureB=false"。
func turnFgOff(featureGates []string) string {
	return turnFg(featureGates, "=false")
}

// turnFg 是 turnFgOn 和 turnFgOff 的公共实现，根据后缀生成开启或关闭指定
// feature gate 的字符串，例如 "FeatureA=true,FeatureB=true" 或 "FeatureA=false,FeatureB=false"。
func turnFg(featureGates []string, suffix string) string {
	if len(featureGates) == 0 {
		return ""
	}
	return strings.Join(featureGates, suffix+",") + suffix
}
