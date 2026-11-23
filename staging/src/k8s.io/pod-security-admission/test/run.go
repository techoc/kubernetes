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
	// 从 29 起 restricted 等级开始按 Pod OS 分别给最小合法 fixture，
	// 因为 Linux 和 Windows 的必需字段已经分化。
	newestMinorVersionToTest            = 37
	podOSBasedRestrictionEnabledVersion = 29
)

// Options hold configuration for running integration tests against an existing server.
type Options struct {
	// ClientConfig is a client configuration with sufficient permission to create, update, and delete
	// namespaces, pods, and pod-template-containing objects.
	// Required.
	// 测试需要真的往 apiserver 发 dry-run 请求，所以权限不够会在早期就失败。
	ClientConfig *rest.Config

	// Features optionally provides information about which feature gates are enabled.
	// This is used to skip failure cases for negative tests of data in alpha/beta fields.
	// If unset, all testcases are run.
	// 关掉的门控就别拿它的失败用例来考核服务端，否则测的是环境而不是策略。
	Features featuregate.FeatureGate

	// EmulationVersion optionally indicates a different minor version is being emulated.
	// This can lower the effective "latest" version.
	// 用于验证老版本客户端/服务端组合下的判定，不必真起一个老集群。
	EmulationVersion *api.Version

	// CreateNamespace is an optional stub for creating a namespace with the given name and labels.
	// Returning an error fails the test.
	// If nil, DefaultCreateNamespace is used.
	// 留个钩子，让调用方能在建命名空间时加自己的标签或做权限断言。
	CreateNamespace func(client kubernetes.Interface, name string, labels map[string]string) (*corev1.Namespace, error)

	// These are the check ids/starting versions to exercise.
	// If unset, policy.DefaultChecks() are used.
	// 传自定义集合可以只跑实验性检查或某几项，便于定位单个规则的问题。
	Checks []policy.Check

	// ExemptClient is an optional client interface to exercise behavior of an exempt client.
	// 预留给调用方验证豁免身份走的是另一套判定；当前 Run 尚未读取它。
	ExemptClient kubernetes.Interface
	// ExemptNamespaces are optional namespaces not expected to have PodSecurity controls enforced.
	// 同理，目前仅作为选项保留，Run 里还没有按它跳过测试。
	ExemptNamespaces []string
	// ExemptRuntimeClasses are optional runtimeclasses not expected to have PodSecurity controls enforced.
	// 配置侧已有对应豁免项，测试侧留个位置以后串联进来。
	ExemptRuntimeClasses []string
}

// 报错里打整个 Pod 的 JSON，省得读者再去猜 fixture 长什么样；序列化失败就当空串处理。
func toJSON(pod *corev1.Pod) string {
	data, _ := json.Marshal(pod)
	return string(data)
}

// checksForLevelAndVersion returns the set of check IDs that apply when evaluating the given level and version.
// checks are assumed to be well-formed and valid to pass to policy.NewEvaluator().
// level must be api.LevelRestricted or api.LevelBaseline
// Restricted 比 Baseline 更严，所以它也要跑所有 Baseline 的检查；Privileged 没有要拒绝的项，
// 这里直接排除。
func checksForLevelAndVersion(checks []policy.Check, level api.Level, version api.Version) ([]policy.CheckID, error) {
	retval := []policy.CheckID{}
	for _, check := range checks {
		if !version.Older(check.Versions[0].MinimumVersion) && (level == check.Level || level == api.LevelRestricted) {
			retval = append(retval, check.ID)
		}
	}
	return retval, nil
}

// computeVersionsToTest returns all the versions that have distinct checks defined,
// all the versions that have distinct minimal valid pod fixtures defined, and
// any hard-coded versions that should always be tested.
//
// This lets us sparsely test all versions with distinct fixture or policy changes
// without needing to exercise every intermediate version that had no changes.
// 判定是按「版本号 >= 规则引入版本」做的，所以没变过的中间版本共享同一份预期结果，
// 抽出来反而能把测试时间压到可接受范围。
func computeVersionsToTest(t *testing.T, checks []policy.Check) []api.Version {
	seenVersions := map[api.Version]bool{}

	// include all versions we have registered distinct checks for
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

	// include all versions we have registered distinct fixtures for
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
		// include the oldest version by default
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

	// TODO: consider exposing an option to test all versions instead of sparse ones

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

// and ensures pod fixtures expected to pass and fail against that level/version work as expected.
// 整套测试直接打真实 apiserver，所以每个 level/version 组合都建独立命名空间：
// 标签一变，准入判定就跟着变，混在一起会互相污染预期。
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
			// create test name
			ns := fmt.Sprintf("podsecurity-%s-1-%d", level, minor)

			// create namespace
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
			// 用 Cleanup 而不是 defer：Run 里的失败分支是 continue/manual return，
			// 交给测试框架清理才不会被这些分支跳过。
			t.Cleanup(func() {
				client.CoreV1().Namespaces().Delete(context.Background(), ns, metav1.DeleteOptions{})
			})

			// create service account (to allow pod to pass serviceaccount admission)
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

			// create pod
			// dry-run 创建：只看准入结果，不想真把 fixture Pod 跑起来。
			createPod := func(t *testing.T, i int, pod *corev1.Pod, expectSuccess bool, expectErrorSubstring string) {
				t.Helper()
				// avoid mutating original pod fixture
				// 同一个 fixture 会被多个子测试复用，原地改名字会串味，所以先深拷贝。
				pod = pod.DeepCopy()
				// assign pod name and serviceaccount
				pod.Name = "test"
				pod.Spec.ServiceAccountName = "default"
				// dry-run create
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

			// create controller
			// 再走一遍「控制器创建 Pod 模板」的路径，因为那条路径产生的是警告而不是硬拒绝，
			// 和直接建 Pod 的判定分支不一样，必须分别验证。
			createController := func(t *testing.T, i int, pod *corev1.Pod, expectSuccess bool, expectError bool, expectErrorSubstring string) {
				t.Helper()

				// avoid mutating original pod fixture
				pod = pod.DeepCopy()
				if pod.Labels == nil {
					pod.Labels = map[string]string{}
				}
				pod.Labels["test"] = "true"

				// 先清空上一次测试残留的警告，避免把它误算到当前这次创建头上。
				warningHandler.FlushWarnings()
				// dry-run create
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

				// 本该通过的 fixture 却带回了警告，说明策略误伤了合法配置，这里要显式拦下。
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
					// Linux specific pods
					createPod(t, 0, minimalValidLinuxPod.DeepCopy(), true, "")
					createController(t, 0, minimalValidLinuxPod.DeepCopy(), true, false, "")

					// Windows specific pods
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

				// see if any features required for failure cases are disabled
				// 失败用例可能依赖 alpha/beta 字段；门控没开就跳过，而不是报一个假失败。
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

// 默认建命名空间的实现，只设置名字和准入标签；调用方想加自定义标签可以传自己的 stub。
func DefaultCreateNamespace(client kubernetes.Interface, name string, labels map[string]string) (*corev1.Namespace, error) {
	return client.CoreV1().Namespaces().Create(
		context.Background(),
		&corev1.Namespace{
			ObjectMeta: metav1.ObjectMeta{Name: name, Labels: labels},
		},
		metav1.CreateOptions{},
	)
}
