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

package kuberuntime

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/client-go/util/flowcontrol"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/credentialprovider"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/images"
	imagepullmanager "k8s.io/kubernetes/pkg/kubelet/images/pullmanager"
	"k8s.io/kubernetes/test/utils/ktesting"
	"k8s.io/utils/ptr"
)

// TestPullImage 验证匿名拉取镜像成功时返回镜像引用、凭证为空，且镜像出现在列表中。
func TestPullImage(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	imageRef, creds, err := fakeManager.PullImage(tCtx, kubecontainer.ImageSpec{Image: "busybox"}, nil, nil)
	assert.NoError(t, err)
	assert.Equal(t, "busybox", imageRef)
	assert.Nil(t, creds) // 此次为匿名拉取，不应返回凭证

	// 拉取成功后，镜像应出现在本地镜像列表中。
	images, err := fakeManager.ListImages(tCtx)
	assert.NoError(t, err)
	assert.Len(t, images, 1)
	assert.Equal(t, []string{"busybox"}, images[0].RepoTags)
}

// TestPullImageWithError 验证底层镜像服务拉取失败时错误向上返回，且不会留下镜像记录。
func TestPullImageWithError(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	fakeImageService.InjectError("PullImage", fmt.Errorf("test-error"))
	imageRef, creds, err := fakeManager.PullImage(tCtx, kubecontainer.ImageSpec{Image: "busybox"}, nil, nil)
	assert.Error(t, err)
	assert.Equal(t, "", imageRef)
	assert.Nil(t, creds)

	images, err := fakeManager.ListImages(tCtx)
	assert.NoError(t, err)
	assert.Empty(t, images)
}

// TestListImages 验证 ListImages 返回的镜像 ID 集合与镜像服务中设置的完全一致。
func TestListImages(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	images := []string{"1111", "2222", "3333"}
	expected := sets.New[string](images...)
	fakeImageService.SetFakeImages(images)

	actualImages, err := fakeManager.ListImages(tCtx)
	assert.NoError(t, err)
	// 用集合比较，避免依赖镜像返回顺序。
	actual := sets.New[string]()
	for _, i := range actualImages {
		actual.Insert(i.ID)
	}

	assert.Equal(t, sets.List(expected), sets.List(actual))
}

// TestListImagesPinnedField 验证 ListImages 正确透传每个镜像的 Pinned 标记。
func TestListImagesPinnedField(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	imagesPinned := map[string]bool{
		"1111": false,
		"2222": true,
		"3333": false,
	}
	imageList := []string{}
	for image, pinned := range imagesPinned {
		fakeImageService.SetFakeImagePinned(image, pinned)
		imageList = append(imageList, image)
	}
	fakeImageService.SetFakeImages(imageList)

	actualImages, err := fakeManager.ListImages(tCtx)
	assert.NoError(t, err)
	for _, image := range actualImages {
		assert.Equal(t, imagesPinned[image.ID], image.Pinned)
	}
}

// TestListImagesWithError 验证镜像服务列出镜像失败时返回错误且不返回镜像列表。
func TestListImagesWithError(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	fakeImageService.InjectError("ListImages", fmt.Errorf("test-failure"))

	actualImages, err := fakeManager.ListImages(tCtx)
	assert.Error(t, err)
	assert.Nil(t, actualImages)
}

// TestGetImageRef 验证本地已存在镜像时 GetImageRef 返回正确的镜像引用。
func TestGetImageRef(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	image := "busybox"
	fakeImageService.SetFakeImages([]string{image})
	imageRef, err := fakeManager.GetImageRef(tCtx, kubecontainer.ImageSpec{Image: image})
	assert.NoError(t, err)
	assert.Equal(t, image, imageRef)
}

// TestGetImageRefReturnsImageIdNotRepoDigest 验证 GetImageRef 返回 Image.Id 而非 RepoDigests，
// 以保证基于内容的去重：同一镜像从不同仓库拉取时应被视为同一镜像，
// 避免在拉取记录查询中产生误判。
func TestGetImageRefReturnsImageIdNotRepoDigest(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
	require.NoError(t, err)

	// 模拟同时设置了 Id 与 RepoDigests 的镜像。
	// 现实场景中，同一镜像内容从不同仓库拉取会得到相同的 Id，但 RepoDigests 不同。
	imageID := "sha256:abcd1234"
	repoDigest1 := "registry1.io/myimage@sha256:abcd1234"
	repoDigest2 := "registry2.io/myimage@sha256:abcd1234"

	fakeImageService.Lock()
	fakeImageService.Images["myimage"] = &runtimeapi.Image{
		Id:          imageID,
		RepoDigests: []string{repoDigest1, repoDigest2},
		RepoTags:    []string{"myimage:latest"},
		Size:        1000,
	}
	fakeImageService.Unlock()

	// GetImageRef 应返回基于内容的 Image.Id，而不是基于位置的 RepoDigest。
	imageRef, err := fakeManager.GetImageRef(tCtx, kubecontainer.ImageSpec{Image: "myimage"})
	require.NoError(t, err)
	assert.Equal(t, imageID, imageRef, "GetImageRef should return Image.Id for content-based deduplication")
}

// TestImageSize 验证 GetImageSize 返回镜像服务中设置的镜像大小。
func TestImageSize(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	const imageSize = uint64(64)
	fakeImageService.SetFakeImageSize(imageSize)
	image := "busybox"
	fakeImageService.SetFakeImages([]string{image})
	actualSize, err := fakeManager.GetImageSize(tCtx, kubecontainer.ImageSpec{Image: image})
	assert.NoError(t, err)
	assert.Equal(t, imageSize, actualSize)
}

// TestGetImageRefImageNotAvailableLocally 验证本地不存在镜像时 GetImageRef 返回空引用且不报错。
func TestGetImageRefImageNotAvailableLocally(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	image := "busybox"

	imageRef, err := fakeManager.GetImageRef(tCtx, kubecontainer.ImageSpec{Image: image})
	assert.NoError(t, err)

	imageNotAvailableLocallyRef := ""
	assert.Equal(t, imageNotAvailableLocallyRef, imageRef)
}

// TestGetImageRefWithError 验证查询镜像状态失败时 GetImageRef 返回错误与空引用。
func TestGetImageRefWithError(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	image := "busybox"

	fakeImageService.InjectError("ImageStatus", fmt.Errorf("test-error"))

	imageRef, err := fakeManager.GetImageRef(tCtx, kubecontainer.ImageSpec{Image: image})
	assert.Error(t, err)
	assert.Equal(t, "", imageRef)
}

// TestRemoveImage 验证 RemoveImage 会真正删除本地镜像。
func TestRemoveImage(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	_, _, err = fakeManager.PullImage(tCtx, kubecontainer.ImageSpec{Image: "busybox"}, nil, nil)
	assert.NoError(t, err)
	assert.Len(t, fakeImageService.Images, 1)

	err = fakeManager.RemoveImage(tCtx, kubecontainer.ImageSpec{Image: "busybox"})
	assert.NoError(t, err)
	assert.Empty(t, fakeImageService.Images)
}

// TestRemoveImageNoOpIfImageNotLocal 验证删除本地不存在的镜像时应视为成功（空操作）。
func TestRemoveImageNoOpIfImageNotLocal(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	err = fakeManager.RemoveImage(tCtx, kubecontainer.ImageSpec{Image: "busybox"})
	assert.NoError(t, err)
}

// TestRemoveImageWithError 验证删除镜像失败时返回错误，且镜像仍保留在本地。
func TestRemoveImageWithError(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	_, _, err = fakeManager.PullImage(tCtx, kubecontainer.ImageSpec{Image: "busybox"}, nil, nil)
	assert.NoError(t, err)
	assert.Len(t, fakeImageService.Images, 1)

	fakeImageService.InjectError("RemoveImage", fmt.Errorf("test-failure"))

	err = fakeManager.RemoveImage(tCtx, kubecontainer.ImageSpec{Image: "busybox"})
	assert.Error(t, err)
	assert.Len(t, fakeImageService.Images, 1)
}

// TestImageStats 验证 ImageStats 汇总所有镜像大小得到总存储字节数。
func TestImageStats(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	const imageSize = 64
	fakeImageService.SetFakeImageSize(imageSize)
	images := []string{"1111", "2222", "3333"}
	fakeImageService.SetFakeImages(images)

	actualStats, err := fakeManager.ImageStats(tCtx)
	assert.NoError(t, err)
	expectedStats := &kubecontainer.ImageStats{TotalStorageBytes: imageSize * uint64(len(images))}
	assert.Equal(t, expectedStats, actualStats)
}

// TestImageStatsWithError 验证列出镜像失败时 ImageStats 返回错误且不返回统计结果。
func TestImageStatsWithError(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	fakeImageService.InjectError("ListImages", fmt.Errorf("test-failure"))

	actualImageStats, err := fakeManager.ImageStats(tCtx)
	assert.Error(t, err)
	assert.Nil(t, actualImageStats)
}

// TestPullWithSecrets 验证从 Pod Secret 与内置 keyring 中选择镜像拉取凭证的优先级：
// Secret 中匹配的凭证应覆盖内置 keyring。
func TestPullWithSecrets(t *testing.T) {
	tCtx := ktesting.Init(t)
	logger := tCtx.Logger()
	// 认证值等价于： "username":"passed-user","password":"passed-password"
	dockerCfg := map[string]map[string]string{"index.docker.io/v1/": {"email": "passed-email", "auth": "cGFzc2VkLXVzZXI6cGFzc2VkLXBhc3N3b3Jk"}}
	dockercfgContent, err := json.Marshal(dockerCfg)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	dockerConfigJSON := map[string]map[string]map[string]string{"auths": dockerCfg}
	dockerConfigJSONContent, err := json.Marshal(dockerConfigJSON)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}

	podSandboxConfig := &runtimeapi.PodSandboxConfig{
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      "testpod",
			Namespace: "testpod-ns",
			Uid:       "testpod-uid",
		},
	}

	tests := map[string]struct {
		imageName           string
		passedSecrets       []v1.Secret
		builtInDockerConfig credentialprovider.DockerConfig
		expectedAuth        *runtimeapi.AuthConfig
	}{
		"no matching secrets": {
			"ubuntu:latest",
			[]v1.Secret{},
			credentialprovider.DockerConfig(map[string]credentialprovider.DockerConfigEntry{}),
			nil,
		},
		"default keyring secrets": {
			"ubuntu:latest",
			[]v1.Secret{},
			credentialprovider.DockerConfig(map[string]credentialprovider.DockerConfigEntry{
				"index.docker.io/v1/": {Username: "built-in", Password: "password", Provider: nil},
			}),
			&runtimeapi.AuthConfig{Username: "built-in", Password: "password"},
		},
		"default keyring secrets unused": {
			"ubuntu:latest",
			[]v1.Secret{},
			credentialprovider.DockerConfig(map[string]credentialprovider.DockerConfigEntry{
				"extraneous": {Username: "built-in", Password: "password", Provider: nil},
			}),
			nil,
		},
		"builtin keyring secrets, but use passed": {
			"ubuntu:latest",
			[]v1.Secret{{Type: v1.SecretTypeDockercfg, Data: map[string][]byte{v1.DockerConfigKey: dockercfgContent}}},
			credentialprovider.DockerConfig(map[string]credentialprovider.DockerConfigEntry{
				"index.docker.io/v1/": {Username: "built-in", Password: "password", Provider: nil},
			}),
			&runtimeapi.AuthConfig{Username: "passed-user", Password: "passed-password"},
		},
		"builtin keyring secrets, but use passed with new docker config": {
			"ubuntu:latest",
			[]v1.Secret{{Type: v1.SecretTypeDockerConfigJson, Data: map[string][]byte{v1.DockerConfigJsonKey: dockerConfigJSONContent}}},
			credentialprovider.DockerConfig(map[string]credentialprovider.DockerConfigEntry{
				"index.docker.io/v1/": {Username: "built-in", Password: "password", Provider: nil},
			}),
			&runtimeapi.AuthConfig{Username: "passed-user", Password: "passed-password"},
		},
	}
	for description, test := range tests {
		builtInKeyRing := &credentialprovider.BasicDockerKeyring{}
		builtInKeyRing.Add(nil, test.builtInDockerConfig)

		// 每个用例重建 fake manager 与拉取记录，避免凭证和拉取状态在用例间串扰。
		_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
		require.NoError(t, err)

		fsRecordAccessor, err := imagepullmanager.NewFSPullRecordsAccessor(logger, t.TempDir())
		if err != nil {
			t.Fatal("failed to setup an file pull records accessor")
		}

		const intentsRecordsCacheSize, pulledRecordsCacheSize, stripedLocksSetSize = 50, 100, 10
		memCacheRecordsAccessor := imagepullmanager.NewCachedPullRecordsAccessor(
			logger,
			fsRecordAccessor,
			intentsRecordsCacheSize,
			pulledRecordsCacheSize,
			stripedLocksSetSize,
		)

		imagePullManager, err := imagepullmanager.NewImagePullManager(tCtx, memCacheRecordsAccessor, imagepullmanager.AlwaysVerifyImagePullPolicy(), fakeManager, 10)
		if err != nil {
			t.Fatal("failed to setup an image pull manager")
		}

		fakeManager.imagePuller = images.NewImageManager(
			fakeManager.recorder,
			builtInKeyRing,
			fakeManager,
			imagePullManager,
			flowcontrol.NewBackOff(time.Second, 300*time.Second),
			false,
			ptr.To[int32](0), // 不限制并行镜像拉取数，
			0,                // 将 QPS 设为 0 以禁用镜像拉取限流，
			0,
			&fakePodPullingTimeRecorder{},
		)

		_, _, err = fakeManager.imagePuller.EnsureImageExists(tCtx, nil, makeTestPod("testpod", "testpod-ns", "testpod-uid", []v1.Container{}), test.imageName, test.passedSecrets, podSandboxConfig, "", v1.PullAlways)
		require.NoError(t, err)
		fakeImageService.AssertImagePulledWithAuth(t, &runtimeapi.ImageSpec{Image: test.imageName, Annotations: make(map[string]string)}, test.expectedAuth, description)
	}
}

// TestPullWithSecretsWithError 验证无效 Secret 或拉取失败时 EnsureImageExists 返回错误且不留下镜像。
func TestPullWithSecretsWithError(t *testing.T) {
	tCtx := ktesting.Init(t)
	logger := tCtx.Logger()

	dockerCfg := map[string]map[string]map[string]string{
		"auths": {
			"index.docker.io/v1/": {
				"email": "passed-email",
				"auth":  "cGFzc2VkLXVzZXI6cGFzc2VkLXBhc3N3b3Jk",
			},
		},
	}

	dockerConfigJSON, err := json.Marshal(dockerCfg)
	if err != nil {
		t.Fatal(err)
	}

	podSandboxConfig := &runtimeapi.PodSandboxConfig{
		Metadata: &runtimeapi.PodSandboxMetadata{
			Name:      "testpod",
			Namespace: "testpod-ns",
			Uid:       "testpod-uid",
		},
	}

	for _, test := range []struct {
		name              string
		imageName         string
		passedSecrets     []v1.Secret
		shouldInjectError bool
	}{
		{
			name:          "invalid docker secret",
			imageName:     "ubuntu:latest",
			passedSecrets: []v1.Secret{{Type: v1.SecretTypeDockercfg, Data: map[string][]byte{v1.DockerConfigKey: []byte("invalid")}}},
		},
		{
			name:      "secret provided, pull failed",
			imageName: "ubuntu:latest",
			passedSecrets: []v1.Secret{
				{Type: v1.SecretTypeDockerConfigJson, Data: map[string][]byte{v1.DockerConfigKey: dockerConfigJSON}},
			},
			shouldInjectError: true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			// 每个用例重建 fake manager，避免错误注入与拉取记录状态相互影响。
			_, fakeImageService, fakeManager, err := createTestRuntimeManager(tCtx)
			assert.NoError(t, err)

			if test.shouldInjectError {
				fakeImageService.InjectError("PullImage", fmt.Errorf("test-error"))
			}

			fsRecordAccessor, err := imagepullmanager.NewFSPullRecordsAccessor(logger, t.TempDir())
			if err != nil {
				t.Fatal("failed to setup an file pull records accessor")
			}

			const intentsRecordsCacheSize, pulledRecordsCacheSize, stripedLocksSetSize = 50, 100, 10
			memCacheRecordsAccessor := imagepullmanager.NewCachedPullRecordsAccessor(
				logger,
				fsRecordAccessor,
				intentsRecordsCacheSize,
				pulledRecordsCacheSize,
				stripedLocksSetSize,
			)

			imagePullManager, err := imagepullmanager.NewImagePullManager(tCtx, memCacheRecordsAccessor, imagepullmanager.AlwaysVerifyImagePullPolicy(), fakeManager, 10)
			if err != nil {
				t.Fatal("failed to setup an image pull manager")
			}

			fakeManager.imagePuller = images.NewImageManager(
				fakeManager.recorder,
				&credentialprovider.BasicDockerKeyring{},
				fakeManager,
				imagePullManager,
				flowcontrol.NewBackOff(time.Second, 300*time.Second),
				false,
				ptr.To[int32](0), // 不限制并行镜像拉取数，
				0,                // 将 QPS 设为 0 以禁用镜像拉取限流，
				0,
				&fakePodPullingTimeRecorder{},
			)

			imageRef, _, err := fakeManager.imagePuller.EnsureImageExists(tCtx, nil, makeTestPod("testpod", "testpod-ns", "testpod-uid", []v1.Container{}), test.imageName, test.passedSecrets, podSandboxConfig, "", v1.PullAlways)
			assert.Error(t, err)
			assert.Equal(t, "", imageRef)

			images, err := fakeManager.ListImages(tCtx)
			assert.NoError(t, err)
			assert.Empty(t, images)
		})
	}
}

// TestPullThenListWithAnnotations 验证拉取镜像时携带的注解在后续 ListImages 中原样保留。
func TestPullThenListWithAnnotations(t *testing.T) {
	tCtx := ktesting.Init(t)
	_, _, fakeManager, err := createTestRuntimeManager(tCtx)
	assert.NoError(t, err)

	// 注解是镜像 spec 的一部分，拉取后应在列表中原样读回。
	imageSpec := kubecontainer.ImageSpec{
		Image: "12345",
		Annotations: []kubecontainer.Annotation{
			{Name: "kubernetes.io/runtimehandler", Value: "handler_name"},
		},
	}

	_, _, err = fakeManager.PullImage(tCtx, imageSpec, nil, nil)
	assert.NoError(t, err)

	images, err := fakeManager.ListImages(tCtx)
	assert.NoError(t, err)
	assert.Len(t, images, 1)
	assert.Equal(t, images[0].Spec, imageSpec)
}
