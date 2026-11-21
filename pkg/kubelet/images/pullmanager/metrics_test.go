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

package pullmanager

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/component-base/metrics/legacyregistry"
	metricstestutil "k8s.io/component-base/metrics/testutil"
	kubeletconfig "k8s.io/kubernetes/pkg/kubelet/apis/config"
	containertest "k8s.io/kubernetes/pkg/kubelet/container/testing"
	"k8s.io/kubernetes/test/utils/ktesting"
)

// TestFSPullRecordsMetrics 验证磁盘与内存拉取记录相关指标：
// 写入/删除记录、重复写入不重复计数，以及缓存超过容量后的使用率统计。
func TestFSPullRecordsMetrics(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	tempDir := t.TempDir()
	legacyregistry.Reset()
	defer legacyregistry.Reset()

	fsAccessor, err := NewFSPullRecordsAccessor(logger, tempDir)
	if err != nil {
		t.Fatal(err)
	}

	// 缓存容量设为 5 和 10，便于构造超出容量后的百分比指标。
	const pullIntentsCacheSize, pulledRecordsCacheSize, stripedSetLocksSize int32 = 5, 10, 1
	inMemoryAccessor := NewCachedPullRecordsAccessor(logger, fsAccessor, pullIntentsCacheSize, pulledRecordsCacheSize, stripedSetLocksSize)
	// 初始时磁盘与内存指标都应为 0。

	cmpFSIntents(t, 0)
	cmpMemIntents(t, 0)
	require.NoError(t, inMemoryAccessor.WriteImagePullIntent(logger, "test-image:latest"))
	cmpFSIntents(t, 1)
	cmpMemIntents(t, 20)

	// 重复写入同一记录不应增加计数。
	require.NoError(t, inMemoryAccessor.WriteImagePullIntent(logger, "test-image:latest"))
	require.NoError(t, inMemoryAccessor.WriteImagePullIntent(logger, "test-image:latest"))
	cmpFSIntents(t, 1)
	cmpMemIntents(t, 20)

	// 增加更多记录后计数应同步增长。
	require.NoError(t, inMemoryAccessor.WriteImagePullIntent(logger, "test-image:v1"))
	require.NoError(t, inMemoryAccessor.WriteImagePullIntent(logger, "test-image:v1.1"))
	cmpFSIntents(t, 3)
	cmpMemIntents(t, 60)

	cmpFSPulledRecords(t, 0)
	cmpMemPulledRecords(t, 0)
	require.NoError(t, inMemoryAccessor.WriteImagePulledRecord(logger, &kubeletconfig.ImagePulledRecord{
		ImageRef:        "test-image-latest-ref",
		LastUpdatedTime: metav1.NewTime(time.Now()),
	}))
	cmpFSPulledRecords(t, 1)
	cmpMemPulledRecords(t, 10)

	// 重复写入同一记录不应增加计数。
	require.NoError(t, inMemoryAccessor.WriteImagePulledRecord(logger, &kubeletconfig.ImagePulledRecord{
		ImageRef:        "test-image-latest-ref",
		LastUpdatedTime: metav1.NewTime(time.Now()),
	}))
	require.NoError(t, inMemoryAccessor.WriteImagePulledRecord(logger, &kubeletconfig.ImagePulledRecord{
		ImageRef:        "test-image-latest-ref",
		LastUpdatedTime: metav1.NewTime(time.Now()),
	}))
	cmpFSPulledRecords(t, 1)
	cmpMemPulledRecords(t, 10)

	// 增加更多记录后计数应同步增长。
	require.NoError(t, inMemoryAccessor.WriteImagePulledRecord(logger, &kubeletconfig.ImagePulledRecord{
		ImageRef:        "test-image-v1-ref",
		LastUpdatedTime: metav1.NewTime(time.Now()),
	}))
	require.NoError(t, inMemoryAccessor.WriteImagePulledRecord(logger, &kubeletconfig.ImagePulledRecord{
		ImageRef:        "test-image-v1.1-ref",
		LastUpdatedTime: metav1.NewTime(time.Now()),
	}))
	require.NoError(t, inMemoryAccessor.WriteImagePulledRecord(logger, &kubeletconfig.ImagePulledRecord{
		ImageRef:        "test-image-v1.2-ref",
		LastUpdatedTime: metav1.NewTime(time.Now()),
	}))
	cmpFSPulledRecords(t, 4)
	cmpMemPulledRecords(t, 40)

	// 确认拉取记录的增删不会影响 pull intent 计数。
	cmpFSIntents(t, 3)
	cmpMemIntents(t, 60)

	// 删除 intent 后计数应减少；重复删除是幂等的。
	require.NoError(t, inMemoryAccessor.DeleteImagePullIntent(logger, "test-image:latest"))
	cmpFSIntents(t, 2)
	cmpMemIntents(t, 40)

	require.NoError(t, inMemoryAccessor.DeleteImagePullIntent(logger, "test-image:latest"))
	require.NoError(t, inMemoryAccessor.DeleteImagePullIntent(logger, "test-image:latest"))
	cmpFSIntents(t, 2)
	cmpMemIntents(t, 40)

	require.NoError(t, inMemoryAccessor.DeleteImagePullIntent(logger, "test-image:v1"))
	require.NoError(t, inMemoryAccessor.DeleteImagePullIntent(logger, "test-image:v1.1"))
	cmpFSIntents(t, 0)
	cmpMemIntents(t, 0)

	// 确认 intent 的删除不会影响 pulled record 计数。
	cmpFSPulledRecords(t, 4)
	cmpMemPulledRecords(t, 40)

	// 删除 pulled record 后计数应减少。
	require.NoError(t, inMemoryAccessor.DeleteImagePulledRecord(logger, "test-image-v1.1-ref"))
	cmpFSPulledRecords(t, 3)
	cmpMemPulledRecords(t, 30)

	require.NoError(t, inMemoryAccessor.DeleteImagePulledRecord(logger, "test-image-v1.1-ref"))
	require.NoError(t, inMemoryAccessor.DeleteImagePulledRecord(logger, "test-image-v1.1-ref"))
	cmpFSPulledRecords(t, 3)
	cmpMemPulledRecords(t, 30)

	require.NoError(t, inMemoryAccessor.DeleteImagePulledRecord(logger, "test-image-v1.2-ref"))
	require.NoError(t, inMemoryAccessor.DeleteImagePulledRecord(logger, "test-image-v1-ref"))
	require.NoError(t, inMemoryAccessor.DeleteImagePulledRecord(logger, "test-image-latest-ref"))
	cmpFSPulledRecords(t, 0)
	cmpMemPulledRecords(t, 0)

	require.NoError(t, inMemoryAccessor.DeleteImagePulledRecord(logger, "test-image-v1-ref"))
	require.NoError(t, inMemoryAccessor.DeleteImagePulledRecord(logger, "test-image-latest-ref"))
	cmpFSPulledRecords(t, 0)
	cmpMemPulledRecords(t, 0)

	// 写入 20 条记录，超过缓存容量以触发使用率百分比统计。
	for i := range 20 {
		require.NoError(t, inMemoryAccessor.WriteImagePullIntent(logger, fmt.Sprintf("test-image-%d:latest", i)))
		require.NoError(t, inMemoryAccessor.WriteImagePulledRecord(logger, &kubeletconfig.ImagePulledRecord{
			ImageRef:        fmt.Sprintf("test-image-v%d-ref", i),
			LastUpdatedTime: metav1.NewTime(time.Now()),
		}))
	}
	cmpFSIntents(t, 20)
	cmpFSPulledRecords(t, 20)
	cmpMemIntents(t, 100)
	cmpMemPulledRecords(t, 100)

	// 从缓存中移除最新记录，验证百分比随之下降。
	require.NoError(t, inMemoryAccessor.DeleteImagePullIntent(logger, "test-image-19:latest"))
	cmpFSIntents(t, 19)
	cmpMemIntents(t, 80)

	require.NoError(t, inMemoryAccessor.DeleteImagePulledRecord(logger, "test-image-v19-ref"))
	cmpFSPulledRecords(t, 19)
	cmpMemPulledRecords(t, 90)

}

// TestMustAttemptPullMetrics 验证 MustAttemptImagePull 的结果指标：
// 错误、凭据记录命中、策略豁免、必须认证等标签分别计数。
func TestMustAttemptPullMetrics(t *testing.T) {
	logger, ctx := ktesting.NewTestContext(t)

	tempDir := t.TempDir()
	pulledDir := filepath.Join(tempDir, "image_manager", "pulled")
	legacyregistry.Reset()
	defer legacyregistry.Reset()

	fsAccessor, err := NewFSPullRecordsAccessor(logger, tempDir)
	if err != nil {
		t.Fatal(err)
	}

	fakeRuntime := &containertest.FakeRuntime{}
	imageManager, err := NewImagePullManager(ctx,
		fsAccessor,
		&NeverVerifyAllowlistedImages{absoluteURLs: sets.New("docker.io/testing/policyexempt")},
		fakeRuntime,
		1,
	)
	require.NoError(t, err)

	// 预置 pulled 记录文件，模拟已有拉取历史。
	teeTestData(t, pulledDir, "pulled", []string{
		"sha256-e766e4624f9bc4d3847d6c5b470d9d5362cc8d0c250bd9572daf95278e044263",
		"sha256-a2eace2182b24cdbbb730798e47b10709b9ef5e0f0c1624a3bc06c8ca987727a",
		"sha256-f4058727984875eb66ddbf289f7013096d4cbaa167e591fbcb2e447e36391c1f",
		"sha256-b3c0cc4278800b03a308ceb2611161430df571ca733122f0a40ac8b9792a9064",
	})

	// 按结果标签累计期望的 must-pull 检查次数，最后与指标快照整体比较。
	expectedMetrics := make(map[string]int)
	testMustAttemptImagePull(t, true, imageManager, ctx, "docker.io/testing/broken", "testbrokenrecord", nil)
	expectedMetrics[string(checkResultError)]++
	cmpMustAttemptPullMetrics(t, expectedMetrics)

	testMustAttemptImagePull(t, true, imageManager, ctx, "docker.io/testing/broken", "testbrokenrecord", nil)
	testMustAttemptImagePull(t, true, imageManager, ctx, "docker.io/testing/broken", "testbrokenrecord", nil)
	expectedMetrics[string(checkResultError)] += 2

	testMustAttemptImagePull(t, false, imageManager, ctx, "docker.io/testing/test", "testimage-anonpull", nil)
	expectedMetrics[string(checkResultCredentialRecordFound)]++
	cmpMustAttemptPullMetrics(t, expectedMetrics)

	testMustAttemptImagePull(t, false, imageManager, ctx, "docker.io/testing/test", "testimage-anonpull", nil)
	testMustAttemptImagePull(t, false, imageManager, ctx, "docker.io/testing/test", "testimage-anonpull", nil)
	testMustAttemptImagePull(t, false, imageManager, ctx, "docker.io/testing/test", "testimage-anonpull", nil)
	expectedMetrics[string(checkResultCredentialRecordFound)] += 3
	cmpMustAttemptPullMetrics(t, expectedMetrics)

	testMustAttemptImagePull(t, false, imageManager, ctx, "docker.io/testing/policyexempt", "policyallowed", nil)
	expectedMetrics[string(checkResultCredentialPolicyAllowed)]++
	cmpMustAttemptPullMetrics(t, expectedMetrics)

	testMustAttemptImagePull(t, false, imageManager, ctx, "docker.io/testing/policyexempt", "policyallowed", nil)
	expectedMetrics[string(checkResultCredentialPolicyAllowed)]++
	cmpMustAttemptPullMetrics(t, expectedMetrics)

	testMustAttemptImagePull(t, true, imageManager, ctx, "docker.io/testing/norecords", "somewhatunknown", nil)
	expectedMetrics[string(checkResultMustAuthenticate)]++
	cmpMustAttemptPullMetrics(t, expectedMetrics)

	testMustAttemptImagePull(t, false, imageManager, ctx, "docker.io/testing/test", "testimageref",
		func() ([]kubeletconfig.ImagePullSecret, *kubeletconfig.ImagePullServiceAccount, error) {
			return []kubeletconfig.ImagePullSecret{
				{UID: "testsecretuid", Namespace: "default", Name: "pull-secret", CredentialHash: "testsecrethash"},
			}, nil, nil
		})
	expectedMetrics[string(checkResultCredentialRecordFound)]++
	cmpMustAttemptPullMetrics(t, expectedMetrics)
}

// testMustAttemptImagePull 调用 MustAttemptImagePull 并断言返回值。
func testMustAttemptImagePull(t *testing.T, expected bool, f *PullManager, ctx context.Context, image, imageRef string, getPodCredentials GetPodCredentials) {
	mustPull, err := f.MustAttemptImagePull(ctx, image, imageRef, getPodCredentials)
	require.NoError(t, err)
	require.Equal(t, expected, mustPull)
}

// cmpFSIntents 采集并比较磁盘上的 pull intent 数量指标。
func cmpFSIntents(t *testing.T, expected uint) {
	t.Helper()
	const metricFormat = `
# HELP kubelet_imagemanager_ondisk_pullintents [ALPHA] Number of ImagePullIntents stored on disk.
# TYPE kubelet_imagemanager_ondisk_pullintents gauge
kubelet_imagemanager_ondisk_pullintents %d
`

	err := metricstestutil.GatherAndCompare(
		legacyregistry.DefaultGatherer, strings.NewReader(fmt.Sprintf(metricFormat, expected)), "kubelet_imagemanager_ondisk_pullintents",
	)
	if err != nil {
		t.Errorf("failed to gather metrics: %v", err)
	}
}

// cmpFSPulledRecords 采集并比较磁盘上的 pulled record 数量指标。
func cmpFSPulledRecords(t *testing.T, expected uint) {
	t.Helper()
	const metricFormat = `
# HELP kubelet_imagemanager_ondisk_pulledrecords [ALPHA] Number of ImagePulledRecords stored on disk.
# TYPE kubelet_imagemanager_ondisk_pulledrecords gauge
kubelet_imagemanager_ondisk_pulledrecords %d
`

	err := metricstestutil.GatherAndCompare(
		legacyregistry.DefaultGatherer, strings.NewReader(fmt.Sprintf(metricFormat, expected)), "kubelet_imagemanager_ondisk_pulledrecords",
	)
	if err != nil {
		t.Errorf("failed to gather metrics: %v", err)
	}
}

// cmpMemIntents 采集并比较内存 pull intent 缓存使用率指标。
func cmpMemIntents(t *testing.T, expected uint) {
	t.Helper()
	const metricFormat = `
# HELP kubelet_imagemanager_inmemory_pullintents_usage_percent [ALPHA] The ImagePullIntents in-memory cache usage in percent.
# TYPE kubelet_imagemanager_inmemory_pullintents_usage_percent gauge
kubelet_imagemanager_inmemory_pullintents_usage_percent %d
`

	err := metricstestutil.GatherAndCompare(
		legacyregistry.DefaultGatherer, strings.NewReader(fmt.Sprintf(metricFormat, expected)), "kubelet_imagemanager_inmemory_pullintents_usage_percent",
	)
	if err != nil {
		t.Errorf("failed to gather metrics: %v", err)
	}
}

// cmpMemPulledRecords 采集并比较内存 pulled record 缓存使用率指标。
func cmpMemPulledRecords(t *testing.T, expected uint) {
	t.Helper()
	const metricFormat = `
# HELP kubelet_imagemanager_inmemory_pulledrecords_usage_percent [ALPHA] The ImagePulledRecords in-memory cache usage in percent.
# TYPE kubelet_imagemanager_inmemory_pulledrecords_usage_percent gauge
kubelet_imagemanager_inmemory_pulledrecords_usage_percent %d
`

	err := metricstestutil.GatherAndCompare(
		legacyregistry.DefaultGatherer, strings.NewReader(fmt.Sprintf(metricFormat, expected)), "kubelet_imagemanager_inmemory_pulledrecords_usage_percent",
	)
	if err != nil {
		t.Errorf("failed to gather metrics: %v", err)
	}
}

// cmpMustAttemptPullMetrics 按 result 标签拼接期望文本，并与计数指标比较。
func cmpMustAttemptPullMetrics(t *testing.T, labelMap map[string]int) {
	t.Helper()
	const metricFormat = `
# HELP kubelet_imagemanager_image_mustpull_checks_total [ALPHA] Counter for how many times kubelet checked whether credentials need to be re-verified to access an image
# TYPE kubelet_imagemanager_image_mustpull_checks_total counter
`
	expected := metricFormat
	for label, val := range labelMap {
		expected += fmt.Sprintf("kubelet_imagemanager_image_mustpull_checks_total{result=\"%s\"} %d\n", label, val)
	}

	err := metricstestutil.GatherAndCompare(
		legacyregistry.DefaultGatherer, strings.NewReader(expected), "kubelet_imagemanager_image_mustpull_checks_total",
	)
	if err != nil {
		t.Errorf("failed to gather metrics: %v", err)
	}
}
