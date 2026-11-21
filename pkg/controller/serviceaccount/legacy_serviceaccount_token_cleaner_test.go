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

package serviceaccount

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	applyv1 "k8s.io/client-go/applyconfigurations/core/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes/fake"
	core "k8s.io/client-go/testing"
	"k8s.io/kubernetes/pkg/controller"
	"k8s.io/kubernetes/pkg/controlplane/controller/legacytokentracking"
	"k8s.io/kubernetes/pkg/serviceaccount"
	testingclock "k8s.io/utils/clock/testing"
)

// configuredConfigMap 构造旧 token 跟踪用的 ConfigMap。
// label 为空表示 ConfigMap 存在但没有 tracked-since 标签，用来覆盖“门禁未开启”的分支：
// 此时清理器不应执行任何写操作。
func configuredConfigMap(label string) *v1.ConfigMap {
	if label == "" {
		return &v1.ConfigMap{
			ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceSystem, Name: legacytokentracking.ConfigMapName},
		}
	}
	return &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Namespace: metav1.NamespaceSystem, Name: legacytokentracking.ConfigMapName},
		Data:       map[string]string{legacytokentracking.ConfigMapDataKey: label},
	}
}

// configuredServiceAccountTokenSecret 构造带可配置标签和时间戳的 ServiceAccount token Secret。
// lastUsedLabel / invalidSinceLabel 允许传入非法日期字符串，用来验证清理器遇到无法解析的标签时
// 会保守跳过（而不是误删仍可能活跃的 token）；deletionTimeString 非空则模拟 Secret 已进入删除流程。
func configuredServiceAccountTokenSecret(lastUsedLabel, invalidSinceLabel, creationTimeString, serviceAccountName, serviceAccountUID, deletionTimeString string) *v1.Secret {
	// 只有显式传入删除时间才设置 DeletionTimestamp，
	// 让“删除中的 Secret 不再处理”这一保护分支可以被单独覆盖。
	var deletionTime *metav1.Time
	if deletionTimeString == "" {
		deletionTime = nil
	} else {
		deletionTime = &metav1.Time{Time: time.Now().UTC()}
	}
	// 解析失败时得到零值时间，正好用于验证异常时间输入的容错行为。
	creationTime, _ := time.Parse(dateFormat, creationTimeString)
	// 标签为空时不写入该键，区分“没有该标签”和“标签值为空”两种语义。
	labels := map[string]string{}
	if lastUsedLabel != "" {
		labels[serviceaccount.LastUsedLabelKey] = lastUsedLabel
	}
	if invalidSinceLabel != "" {
		labels[serviceaccount.InvalidSinceLabelKey] = invalidSinceLabel
	}
	return &v1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:              "token-secret-1",
			Namespace:         "default",
			UID:               "23456",
			ResourceVersion:   "1",
			Labels:            labels,
			CreationTimestamp: metav1.NewTime(creationTime),
			DeletionTimestamp: deletionTime,
			Annotations: map[string]string{
				v1.ServiceAccountNameKey: serviceAccountName,
				v1.ServiceAccountUIDKey:  serviceAccountUID,
			},
		},
		Type: v1.SecretTypeServiceAccountToken,
		Data: map[string][]byte{
			"token":     []byte("ABC"),
			"ca.crt":    []byte("CA Data"),
			"namespace": []byte("default"),
		},
	}
}

// configuredLegacyTokenCleanUpPeriod 以当前时间为基准，计算从 start 至今的时长。
// 测试标签使用固定日期，而清理器用 now 减去该时长来比较，因此用相对时长
// 才能让“超过/未超过清理周期”的断言随时间保持稳定。
func configuredLegacyTokenCleanUpPeriod(start string) time.Duration {
	current := time.Now().UTC()
	startTime, _ := time.Parse(dateFormat, start)
	return current.Sub(startTime)
}

// configuredPod 构造一个 Pod，可选地把目标 Secret 挂载为卷。
// 挂载中的 token 可能仍被工作负载使用，清理器必须保留它，故单独构造该场景。
func configuredPod(withSecretMount bool) *v1.Pod {
	if !withSecretMount {
		return &v1.Pod{
			ObjectMeta: metav1.ObjectMeta{
				Name:      "pod-1",
				Namespace: "default",
			},
		}
	}
	return &v1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "pod-1",
			Namespace: "default",
		},
		Spec: v1.PodSpec{
			Volumes: []v1.Volume{{Name: "foo", VolumeSource: v1.VolumeSource{Secret: &v1.SecretVolumeSource{SecretName: "token-secret-1"}}}},
		},
	}
}

// patchContent 生成清理器为 Secret 打 invalid-since 标签时期望的 merge patch 内容。
// 使用 apply 配置结构并带上 UID 序列化，是为了精确断言补丁内容，防止清理器
// 意外改动其他字段或作用于错误对象。
func patchContent(namespace, name, invalidSince string, uID types.UID) []byte {
	patch, _ := json.Marshal(applyv1.Secret(name, namespace).WithUID(uID).WithLabels(map[string]string{serviceaccount.InvalidSinceLabelKey: invalidSince}))
	return patch
}

// TestLegacyServiceAccountTokenCleanUp 表驱动验证 evaluateSATokens 的完整决策链：
// 从 ConfigMap 门禁、Secret 类型与引用检查，到打 invalid-since 标签和最终删除。
// 每个用例断言 fake client 记录的动作序列，确保“该写时写、不该写时不写”。
func TestLegacyServiceAccountTokenCleanUp(t *testing.T) {
	// LegacyTokenCleanUpPeriod 决定超过多久的 token 会被处理；
	// Existing* 字段预置 informer store，ClientObjects 预置 fake client（如 ConfigMap）；
	// ExpectedActions 是外部可见行为的完整动作序列，任何多余的写操作都会使用例失败。
	testcases := map[string]struct {
		LegacyTokenCleanUpPeriod time.Duration

		ExistingServiceAccount *v1.ServiceAccount
		ExistingSecret         *v1.Secret
		ExistingPod            *v1.Pod
		ClientObjects          []runtime.Object

		ExpectedActions []core.Action
	}{
		// ConfigMap 不存在时门禁未开启，期望只查询 ConfigMap，不做任何清理写操作。
		"configmap does not exist": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-28"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// 缺少 tracked-since 标签同样视为未启用，保护“不误启动清理”的语义。
		"configmap exists, but the configmap does not have tracked-since label": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-28"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// 跟踪时间不足一个清理周期，token 仍太新，不应被处理。
		"configmap exists, the time period since 'tracked-since' is smaller than the CleanUpPeriod": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-29")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-29"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// 标签日期非法时保守跳过，避免解析失败导致误删。
		"configmap exists, the 'tracked-since' cannot be parsed": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-27-1")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-29"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// 非 ServiceAccount token 类型的 Secret 不在清理范围内。
		"secret is not SecretTypeServiceAccountToken type": {
			ExistingSecret:           opaqueSecret(),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-28")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-29"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// 未被 ServiceAccount 引用的 token Secret 不应被清理器接管。
		"secret is not referenced by serviceaccount": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(emptySecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-27")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-29"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// 创建时间晚于跟踪起点，说明是新 token，必须保留。
		"auto-generated secret has a late creation time": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "", "2022-12-30", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-27")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-29"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// 已进入删除流程的 Secret 不再处理，避免重复操作。
		"auto-generated secret has a deletion time": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "", "2022-12-27", "default", "12345", "deleted"),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-27")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-30"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// 最近使用时间很新，说明 token 仍活跃，不应清理。
		"auto-generated secret has a late last-used time": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-30", "", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-27")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-29"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// last-used 标签解析失败时不能假定其过期，保护活跃 token 不被误删。
		"auto-generated secret has a last-used label, but it can not be parsed": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27-1", "", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-27")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-29"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// 找不到引用的 ServiceAccount 时无法确认 token 归属，跳过清理。
		"secret-referenced service account does not exist": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "", "2022-12-27", "default", "12345", ""),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-27")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-29"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// UID 不匹配意味着 Secret 可能已被重建，不应按旧引用处理。
		"secret-referenced service account uid does not match": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "", "2022-12-27", "default", "123456", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-27")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-29"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// ServiceAccount 名称为空无法建立有效引用，保守跳过。
		"secret-referenced service account name is empty": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "", "2022-12-27", "", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-27")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-29"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// 没有 last-used 标签且未被标记无效时，应先打 invalid-since 标签而非直接删除。
		"auto-generated secret does not have 'last-used' label, has not been marked as invalid": {
			ExistingSecret:           configuredServiceAccountTokenSecret("", "", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-28")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-30"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
				core.NewPatchAction(
					schema.GroupVersionResource{Version: "v1", Resource: "secrets"},
					metav1.NamespaceDefault, "token-secret-1",
					types.MergePatchType,
					patchContent(metav1.NamespaceDefault, "token-secret-1", time.Now().UTC().Format(dateFormat), types.UID("23456")),
				),
			},
		},
		// invalid-since 解析失败时重新打标，确保最终一定能走到删除分支。
		"auto-generated secret does not have 'last-used' label, has been marked as invalid, invalid_since label can not be parsed": {
			ExistingSecret:           configuredServiceAccountTokenSecret("", "2022-12-29-1", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-28")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-30"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
				core.NewPatchAction(
					schema.GroupVersionResource{Version: "v1", Resource: "secrets"},
					metav1.NamespaceDefault, "token-secret-1",
					types.MergePatchType,
					patchContent(metav1.NamespaceDefault, "token-secret-1", time.Now().UTC().Format(dateFormat), types.UID("23456")),
				),
			},
		},
		// 标记无效但时间不足一个周期，给客户端留出轮换时间，暂不删除。
		"auto-generated secret does not have 'last-used' label, has been marked as invalid, time period since invalid is less than CleanUpPeriod": {
			ExistingSecret:           configuredServiceAccountTokenSecret("", "2023-01-01", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-28")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-29"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// 标记无效且超过周期后删除，并带 ResourceVersion 前置条件防止并发覆盖。
		"auto-generated secret does not have 'last-used' label, has been marked as invalid, time period since invalid is larger than CleanUpPeriod": {
			ExistingSecret:           configuredServiceAccountTokenSecret("", "2022-12-29", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-28")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2023-01-01"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
				core.NewDeleteActionWithOptions(
					schema.GroupVersionResource{Version: "v1", Resource: "secrets"},
					metav1.NamespaceDefault, "token-secret-1",
					metav1.DeleteOptions{
						Preconditions: &metav1.Preconditions{ResourceVersion: &configuredServiceAccountTokenSecret("", "2022-12-29", "2022-12-27", "default", "12345", "").ResourceVersion},
					}),
			},
		},
		// 被 Pod 挂载的 token 仍在使用，即使过期也不能清理。
		"auto-generated secret is mounted by the pod": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(true),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-28")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-29"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// last-used 已过期但未标记无效，先标记而非立即删除，保证两阶段清理。
		"auto-generated secret has 'last-used' label, the time period since last-used is larger than CleanUpPeriod, secret has not been marked as invalid": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-28")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-30"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
				core.NewPatchAction(
					schema.GroupVersionResource{Version: "v1", Resource: "secrets"},
					metav1.NamespaceDefault, "token-secret-1",
					types.MergePatchType,
					patchContent(metav1.NamespaceDefault, "token-secret-1", time.Now().UTC().Format(dateFormat), types.UID("23456")),
				),
			},
		},
		// 两阶段中第二阶段时间不足，保持只查询不写。
		"auto-generated secret has 'last-used' label, the time period since last-used is larger than CleanUpPeriod, secret has been marked as invalid, time peroid since invalid is less than CleanUpPeriod": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "2023-05-01", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-28")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2022-12-30"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
			},
		},
		// 两阶段均满足后执行删除，验证完整清理路径。
		"auto-generated secret has 'last-used' label, the time period since last-used is larger than CleanUpPeriod, secret has been marked as invalid, time peroid since invalid is larger than CleanUpPeriod": {
			ExistingSecret:           configuredServiceAccountTokenSecret("2022-12-27", "2023-01-05", "2022-12-27", "default", "12345", ""),
			ExistingServiceAccount:   serviceAccount(tokenSecretReferences()),
			ExistingPod:              configuredPod(false),
			ClientObjects:            []runtime.Object{configuredConfigMap("2022-12-28")},
			LegacyTokenCleanUpPeriod: configuredLegacyTokenCleanUpPeriod("2023-05-01"),
			ExpectedActions: []core.Action{
				core.NewGetAction(schema.GroupVersionResource{Version: "v1", Resource: "configmaps"}, metav1.NamespaceSystem, legacytokentracking.ConfigMapName),
				core.NewDeleteActionWithOptions(
					schema.GroupVersionResource{Version: "v1", Resource: "secrets"},
					metav1.NamespaceDefault, "token-secret-1",
					metav1.DeleteOptions{
						Preconditions: &metav1.Preconditions{ResourceVersion: &configuredServiceAccountTokenSecret("", "2023-01-05", "2022-12-27", "default", "12345", "").ResourceVersion},
					}),
			},
		},
	}

	for k, tc := range testcases {
		t.Run(k, func(t *testing.T) {
			// Secret 既要在 fake client 中（供 patch/delete 动作生效），
			// 也要在 informer store 中（供控制器读取），因此两处都加入。
			tc.ClientObjects = append(tc.ClientObjects, tc.ExistingSecret)
			client := fake.NewSimpleClientset(tc.ClientObjects...)

			informers := informers.NewSharedInformerFactory(client, controller.NoResyncPeriodFunc())
			secretInformer := informers.Core().V1().Secrets()
			saInformer := informers.Core().V1().ServiceAccounts()
			podInformer := informers.Core().V1().Pods()
			// 直接操作 informer 的本地 store 预置对象，避免启动 informer 带来的异步时序不确定性。
			secrets := secretInformer.Informer().GetStore()
			serviceAccounts := saInformer.Informer().GetStore()
			pods := podInformer.Informer().GetStore()
			options := LegacySATokenCleanerOptions{
				SyncInterval:  30 * time.Second,
				CleanUpPeriod: tc.LegacyTokenCleanUpPeriod,
			}
			cleaner, _ := NewLegacySATokenCleaner(saInformer, secretInformer, podInformer, client, testingclock.NewFakeClock(time.Now().UTC()), options)

			if tc.ExistingServiceAccount != nil {
				serviceAccounts.Add(tc.ExistingServiceAccount)
			}
			if tc.ExistingPod != nil {
				pods.Add(tc.ExistingPod)
			}
			secrets.Add(tc.ExistingSecret)

			ctx := t.Context()
			// 同步调用被测逻辑，随后用 fake client 记录的动作作为“外部可见行为”的断言依据。
			cleaner.evaluateSATokens(ctx)

			actions := client.Actions()
			// 先断言动作总数：多一次写操作（例如误删）同样是缺陷，必须被捕获。
			if len(actions) != len(tc.ExpectedActions) {
				t.Fatalf("got %d actions, wanted %d actions", len(actions), len(tc.ExpectedActions))
			}
			for i, action := range actions {
				if len(tc.ExpectedActions) < i+1 {
					t.Errorf("%s: %d unexpected actions: %+v", k, len(actions)-len(tc.ExpectedActions), actions[i:])
					break
				}
				expectedAction := tc.ExpectedActions[i]
				// 再逐条比对动作类型与参数，确保写操作作用于正确的对象和补丁内容。
				if !reflect.DeepEqual(expectedAction, action) {
					t.Errorf("got action %#v, wanted %v", action, expectedAction)
				}
			}
		})
	}
}
