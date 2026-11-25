/*
Copyright 2021 The Kubernetes Authors.

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

package test

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/component-base/featuregate"
	"k8s.io/pod-security-admission/api"
	"k8s.io/pod-security-admission/policy"
)

const (
	// 只测到当前受支持的最新小版本；再往后是未来版本，集群里还不会出现。
	// 从 29 起 restricted 等级开始按 Pod OS 分别提供最小合法夹具，
	// 因为 Linux 和 Windows 的必需字段已经分化。
	newestMinorVersionToTest            = 37
	podOSBasedRestrictionEnabledVersion = 29
)

// Options 保存对已有 apiserver 运行集成测试所需的配置。
type Options struct {
	// ClientConfig 是具有创建、更新、删除命名空间、Pod 及含 Pod 模板对象权限的客户端配置，必填。
	// 测试会向 apiserver 发送 dry-run 请求，权限不足应尽早失败。
	ClientConfig *rest.Config

	// Features 可选，提供已启用的 feature gate 信息，用于跳过依赖 alpha/beta 字段的失败用例。
	// 未设置时运行全部用例；避免把环境门控差异误判为策略失败。
	Features featuregate.FeatureGate

	// EmulationVersion 可选，指定正在模拟的较低次要版本，用于验证版本兼容行为而不启动旧集群。
	EmulationVersion *api.Version

	// CreateNamespace 可选，按给定名称和标签创建命名空间；返回错误会使测试失败。
	// 为空时使用 DefaultCreateNamespace；调用方可借此添加自定义标签或权限断言。
	CreateNamespace func(client kubernetes.Interface, name string, labels map[string]string) (*corev1.Namespace, error)

	// Checks 是待测试的检查及其起始版本；为空时使用 policy.DefaultChecks()。
	// 可传入自定义集合，只运行实验性检查或定位单项规则。
	Checks []policy.Check

	// ExemptClient 可选，提供用于验证豁免客户端行为的接口；当前 Run 尚未读取该字段。
	ExemptClient kubernetes.Interface
	// ExemptNamespaces 可选，列出不应执行 PodSecurity 控制的命名空间；当前 Run 尚未按它跳过测试。
	ExemptNamespaces []string
	// ExemptRuntimeClasses 可选，列出不应执行 PodSecurity 控制的 RuntimeClass；当前仅保留配置入口。
	ExemptRuntimeClasses []string
}

// 报错里打整个 Pod 的 JSON，省得读者再去猜夹具长什么样；序列化失败就当空串处理。
func toJSON(pod *corev1.Pod) string {
	data, _ := json.Marshal(pod)
	return string(data)
}

// checksForLevelAndVersion 返回给定级别和版本适用的检查 ID 集合。
// 检查需格式正确且可传给 policy.NewEvaluator；level 只能为 api.LevelRestricted 或 api.LevelBaseline。
// restricted 包含全部 baseline 检查；privileged 没有需要拒绝的检查，故不纳入。
func checksForLevelAndVersion(checks []policy.Check, level api.Level, version api.Version) ([]policy.CheckID, error) {
	retval := []policy.CheckID{}
	for _, check := range checks {
		if !version.Older(check.Versions[0].MinimumVersion) && (level == check.Level || level == api.LevelRestricted) {
			retval = append(retval, check.ID)
		}
	}
	return retval, nil
}

// computeVersionsToTest 返回检查规则或最小合法 Pod 发生变化的版本，加上固定边界版本。
// 判定采用“版本号不低于规则引入版本”，未变化的中间版本可共享预期，稀疏测试以控制运行时间。
func computeVersionsToTest(t *testing.T, checks []policy.Check) []api.Version {
	seenVersions := map[api.Version]bool{}

	// 纳入所有注册了不同检查规则的版本。
	for _, check := range checks {
		for _, checkVersion := range check.Versions {
			if checkVersion.MinimumVersion.Major() != 1 {
				t.Fatalf("expected major version 1, got %d", checkVersion.MinimumVersion.Major())
			}
			seenVersions[checkVersion.MinimumVersion] = true
		}
	}
	if len(seenVersions) == 0 {
		t.Fatal("no versions defined for checks")
	}

	// 纳入 OS 中立最小合法 Pod 发生变化的版本。
	for _, versionsForLevel := range minimalValidPods {
		for version := range versionsForLevel {
			if version.Major() != 1 {
				t.Fatalf("expected major version 1, got %d", version.Major())
			}
			seenVersions[version] = true
		}
	}

	for _, versionsForLevel := range minimalValidLinuxPods {
		for version := range versionsForLevel {
			if version.Major() != 1 {
				t.Fatalf("expected major version 1, got %d", version.Major())
			}
			seenVersions[version] = true
		}
	}

	for _, versionsForLevel := range minimalValidWindowsPods {
		for version := range versionsForLevel {
			if version.Major() != 1 {
				t.Fatalf("expected major version 1, got %d", version.Major())
			}
			seenVersions[version] = true
		}
	}

	alwaysIncludeVersions := []api.Version{
		// 默认纳入最老版本。
		api.MajorMinorVersion(1, 0),
		api.MajorMinorVersion(1, newestMinorVersionToTest),
	}
	for _, version := range alwaysIncludeVersions {
		seenVersions[version] = true
	}

	versions := []api.Version{}
	for version := range seenVersions {
		versions = append(versions, version)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i].Older(versions[j]) })

	// TODO: 考虑开放测试全部版本的选项，而非只做稀疏采样。

	return versions
}

// 警告头是 apiserver 在客户端回调里异步送上来的，多个请求可能并发追加，
// 所以读写都要同一把锁串行化——否则 Flush 清空的正好是别人刚写进去的那条。
type testWarningHandler struct {
	lock     sync.Mutex
	warnings []string
}

// agent 在这里被忽略：测试只关心警告文本里有没有预期的拒绝原因。
func (t *testWarningHandler) HandleWarningHeader(code int, agent string, warning string) {
	t.lock.Lock()
	defer t.lock.Unlock()
	t.warnings = append(t.warnings, warning)
}

// 取出后立刻清空，保证每个子测试只看自己这段时间里产生的警告。
func (t *testWarningHandler) FlushWarnings() []string {
	t.lock.Lock()
	defer t.lock.Unlock()
	warnings := t.warnings
	t.warnings = nil
	return warnings
}

// Run 针对真实 apiserver 验证指定级别和版本下，通过及失败夹具是否符合预期。
// 每个级别/版本组合使用独立命名空间，避免准入标签变化造成跨用例污染。
func Run(t *testing.T, opts Options) {
	warningHandler := &testWarningHandler{}

	configCopy := rest.CopyConfig(opts.ClientConfig)
	configCopy.WarningHandler = warningHandler
	client, err := kubernetes.NewForConfig(configCopy)
	if err != nil {
		t.Fatalf("error creating client: %v", err)
	}

	if opts.CreateNamespace == nil {
		opts.CreateNamespace = DefaultCreateNamespace
	}
	if len(opts.Checks) == 0 {
		opts.Checks = policy.DefaultChecks()
	}
	// 先本地构建一次 evaluator 做健全性检查：检查集合配错时立刻失败，
	// 免得下面每个子测试都因为同一份坏配置而红一遍。
	_, err = policy.NewEvaluator(opts.Checks, opts.EmulationVersion)
	if err != nil {
		t.Fatalf("invalid checks: %v", err)
	}

	versionsToTest := computeVersionsToTest(t, opts.Checks)

	for _, level := range []api.Level{api.LevelBaseline, api.LevelRestricted} {
		for _, version := range versionsToTest {
			minor := version.Minor()
			// 创建测试名称。
			ns := fmt.Sprintf("podsecurity-%s-1-%d", level, minor)

			// 创建命名空间。
			_, err := opts.CreateNamespace(client, ns, map[string]string{
				api.EnforceLevelLabel:   string(level),
				api.EnforceVersionLabel: fmt.Sprintf("v1.%d", minor),
				api.WarnLevelLabel:      string(level),
				api.WarnVersionLabel:    fmt.Sprintf("v1.%d", minor),
			})
			if err != nil {
				t.Errorf("failed creating namespace %s: %v", ns, err)
				continue
			}
			// 用 Cleanup 而不是 defer：Run 的失败分支包含 continue 或提前返回，
			// 交给测试框架清理才不会被这些分支跳过。
			t.Cleanup(func() {
				client.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
			})

			// 创建 ServiceAccount，避免 Pod 被 serviceaccount admission 拒绝。
			sa, err := client.CoreV1().ServiceAccounts(ns).Create(
				context.Background(),
				&corev1.ServiceAccount{ObjectMeta: metav1.ObjectMeta{Name: "default"}},
				metav1.CreateOptions{},
			)
			if err != nil && !apierrors.IsAlreadyExists(err) {
				t.Errorf("failed creating serviceaccount %s: %v", ns, err)
				continue
			}
			t.Cleanup(func() {
				client.CoreV1().ServiceAccounts(ns).Delete(context.Background(), sa.Name, metav1.DeleteOptions{})
			})

			// 创建 Pod；dry-run 只检查准入结果，不实际启动夹具 Pod。
			createPod := func(t *testing.T, i int, pod *corev1.Pod, expectSuccess bool, expectErrorSubstring string) {
				t.Helper()
				// 深拷贝避免原地修改共享夹具。
				// 同一 fixture 会用于直接创建 Pod 和控制器创建，改名不能污染后续用例。
				pod = pod.DeepCopy()
				// 设置 Pod 名称和 ServiceAccount。
				pod.Name = "test"
				pod.Spec.ServiceAccountName = "default"
				// dry-run 创建。
				_, err := client.CoreV1().Pods(ns).Create(context.Background(), pod, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})
				if !expectSuccess {
					if err == nil {
						t.Errorf("%d: expected error creating %s, got none", i, toJSON(pod))
						return
					}
					// UnknownForbiddenReason 意味着检查逻辑有 bug，不是普通的策略拒绝，
					// 单独报出来才能和普通错误区分开。
					if strings.Contains(err.Error(), policy.UnknownForbiddenReason) {
						t.Errorf("%d: unexpected unknown forbidden reason creating %s: %v", i, toJSON(pod), err)
						return
					}
					if !strings.Contains(err.Error(), expectErrorSubstring) {
						t.Errorf("%d: expected error with substring %q, got %v", i, expectErrorSubstring, err)
						return
					}
				}
				if expectSuccess && err != nil {
					t.Errorf("%d: unexpected error creating %s: %v", i, toJSON(pod), err)
				}
			}

			// 创建控制器；该路径产生警告而非硬拒绝，需与直接创建 Pod 分别验证。
			createController := func(t *testing.T, i int, pod *corev1.Pod, expectSuccess bool, expectError bool, expectErrorSubstring string) {
				t.Helper()

				// 深拷贝避免原地修改共享夹具。
				pod = pod.DeepCopy()
				if pod.Labels == nil {
					pod.Labels = map[string]string{}
				}
				pod.Labels["test"] = "true"

				// 先清空上一次测试残留的警告，避免把它误算到当前这次创建头上。
				warningHandler.FlushWarnings()
				// dry-run 创建。
				deployment := &appsv1.Deployment{
					ObjectMeta: metav1.ObjectMeta{Name: "test"},
					Spec: appsv1.DeploymentSpec{
						Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"test": "true"}},
						Template: corev1.PodTemplateSpec{
							ObjectMeta: pod.ObjectMeta,
							Spec:       pod.Spec,
						},
					},
				}
				_, err := client.AppsV1().Deployments(ns).Create(context.Background(), deployment, metav1.CreateOptions{DryRun: []string{metav1.DryRunAll}})

				failureText := ""
				switch {
				case expectSuccess && expectError:
					t.Error("invalid test combination, cannot expectSuccess and expectError")
					return
				case expectError:
					// 硬错误和警告来源不同，这里先取 err 里的文本，下面统一按子串断言。
					if err == nil {
						t.Errorf("%d: expected error creating controller with %s, got none", i, toJSON(pod))
						return
					}
					failureText = err.Error()
				default:
					if err != nil {
						t.Errorf("%d: unexpected error creating controller with %s: %v", i, toJSON(pod), err)
						return
					}
					failureText = strings.Join(warningHandler.FlushWarnings(), "; ")
				}

				if !expectSuccess {
					if len(failureText) == 0 {
						t.Errorf("%d: expected warnings creating %s, got none", i, toJSON(pod))
						return
					}
					if strings.Contains(failureText, policy.UnknownForbiddenReason) {
						t.Errorf("%d: unexpected unknown forbidden reason creating %s: %v", i, toJSON(pod), failureText)
						return
					}
					if !strings.Contains(failureText, expectErrorSubstring) {
						t.Errorf("%d: expected warning with substring %q, got %v", i, expectErrorSubstring, failureText)
						return
					}
				}

				// 本应通过的夹具若带回目标拒绝原因或 UnknownForbiddenReason，显式报错；其他警告不在此断言范围。
				if expectSuccess && len(failureText) > 0 {
					if (len(expectErrorSubstring) > 0 && strings.Contains(failureText, expectErrorSubstring)) ||
						strings.Contains(failureText, policy.UnknownForbiddenReason) {
						t.Errorf("%d: unexpected warning creating %s: %v", i, toJSON(pod), failureText)
					}
				}
			}

			minimalValidOSNeutralPod, err := GetMinimalValidPod(level, version)
			if err != nil {
				t.Fatal(err)
			}
			var minimalValidLinuxPod, minimalValidWindowsPod *corev1.Pod
			// 只有 restricted 且版本够新才需要分 OS 验证：其它情况 OS 中立的最小 Pod 就够代表。
			if level == api.LevelRestricted && version.Minor() >= podOSBasedRestrictionEnabledVersion {
				minimalValidLinuxPod, err = GetMinimalValidLinuxPod(level, version)
				if err != nil {
					t.Fatal(err)
				}

				minimalValidWindowsPod, err = GetMinimalValidWindowsPod(level, version)
				if err != nil {
					t.Fatal(err)
				}
			}

			t.Run(ns+"_pass_base", func(t *testing.T) {
				// 先证明「什么都不改的最小 Pod」能过，后面逐项改字段才有可比基线。
				createPod(t, 0, minimalValidOSNeutralPod.DeepCopy(), true, "")
				createController(t, 0, minimalValidOSNeutralPod.DeepCopy(), true, false, "")
				if minimalValidLinuxPod != nil && minimalValidWindowsPod != nil {
					// Linux 专属 Pod。
					createPod(t, 0, minimalValidLinuxPod.DeepCopy(), true, "")
					createController(t, 0, minimalValidLinuxPod.DeepCopy(), true, false, "")

					// Windows 专属 Pod。
					createPod(t, 0, minimalValidWindowsPod.DeepCopy(), true, "")
					createController(t, 0, minimalValidWindowsPod.DeepCopy(), true, false, "")
				}

			})

			checkIDs, err := checksForLevelAndVersion(opts.Checks, level, version)
			if err != nil {
				t.Fatal(err)
			}
			if len(checkIDs) == 0 {
				t.Fatal(fmt.Errorf("no checks registered for %s/1.%d", level, minor))
			}
			for _, checkID := range checkIDs {
				checkData, err := getFixtures(fixtureKey{level: level, version: version, check: checkID})
				if err != nil {
					t.Fatal(err)
				}

				t.Run(ns+"_pass_"+string(checkID), func(t *testing.T) {
					for i, pod := range checkData.pass {
						createPod(t, i, pod, true, "")
						createController(t, i, pod, true, false, "")
					}
				})

				// 检查失败用例依赖的 feature gate 是否全部启用。
				// 未启用的 alpha/beta 字段用例应跳过，避免环境差异造成假失败。
				var disabledRequiredFeatures []featuregate.Feature
				for _, f := range checkData.failRequiresFeatures {
					if opts.Features != nil && !opts.Features.Enabled(f) {
						disabledRequiredFeatures = append(disabledRequiredFeatures, f)
					}
				}
				t.Run(ns+"_fail_"+string(checkID), func(t *testing.T) {
					if len(disabledRequiredFeatures) > 0 {
						t.Skipf("features required for failure cases are disabled: %v", disabledRequiredFeatures)
					}
					for i, pod := range checkData.fail {
						createPod(t, i, pod, false, checkData.expectErrorSubstring)
						createController(t, i, pod, false, checkData.failRequiresError, checkData.expectErrorSubstring)
					}
				})
			}
		}
	}
}

// DefaultCreateNamespace 创建仅设置名称和给定标签的命名空间；需要自定义行为时可替换 Options.CreateNamespace。
func DefaultCreateNamespace(client kubernetes.Interface, name string, labels map[string]string) (*corev1.Namespace, error) {
	return client.CoreV1().Namespaces().Create(
		context.Background(),
		&corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		},
		metav1.CreateOptions{},
	)
}
