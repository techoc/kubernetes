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

package testing

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"

	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
)

// FakeImageService 是 CRI ImageManagerService 的假实现（mock），用于单元测试中模拟镜像服务。
// 它不真正与容器运行时通信，而是通过内存中的 map 保存镜像、通过 Called 记录方法调用、
// 通过 Errors 注入预设错误，从而让测试可以精确控制服务的行为。
type FakeImageService struct {
	// 嵌入互斥锁，保证下面所有可变字段在并发访问时的线程安全。
	sync.Mutex

	// FakeImageSize 是构造假镜像时统一使用的镜像大小（字节）。
	FakeImageSize uint64
	// Called 按顺序记录被调到的方法名，供测试断言调用发生及调用顺序。
	Called []string
	// Errors 按方法名保存待注入的错误队列，用于模拟 RPC 失败。
	Errors map[string][]error
	// Images 保存当前"本地已有"的镜像，键为镜像名。
	Images map[string]*runtimeapi.Image
	// Pinned 记录每个镜像是否被固定（pinned），固定镜像不会被 GC 清理。
	Pinned map[string]bool

	// pulledImages 记录每次 PullImage 调用时的镜像规格和认证配置，供断言拉取行为。
	pulledImages []*pulledImage

	// FakeFilesystemUsage 是 ImageFsInfo 返回的镜像文件系统用量信息。
	FakeFilesystemUsage []*runtimeapi.FilesystemUsage
	// FakeContainerFilesystemUsage 是 ImageFsInfo 返回的容器文件系统用量信息。
	FakeContainerFilesystemUsage []*runtimeapi.FilesystemUsage
}

// SetFakeImages 用一组镜像名初始化假镜像列表，覆盖已有的 Images。
// 每个镜像会生成一个不带注解的 ImageSpec，并据此构造假镜像。
func (r *FakeImageService) SetFakeImages(images []string) {
	// 加锁保护共享字段 Images。
	r.Lock()
	defer r.Unlock()

	// 重建 map，丢弃之前保存的所有镜像。
	r.Images = make(map[string]*runtimeapi.Image)
	// 逐个镜像名构造假镜像并存入 map。
	for _, image := range images {
		r.Images[image] = r.makeFakeImage(
			&runtimeapi.ImageSpec{
				Image:       image,
				Annotations: make(map[string]string)})
	}
}

// SetFakeImagesWithAnnotations 与 SetFakeImages 类似，但允许为每个镜像指定完整的 ImageSpec（含注解），
// 便于测试依赖镜像注解的逻辑。
func (r *FakeImageService) SetFakeImagesWithAnnotations(imageSpecs []*runtimeapi.ImageSpec) {
	r.Lock()
	defer r.Unlock()

	// 同样先清空已有镜像。
	r.Images = make(map[string]*runtimeapi.Image)
	// 以 ImageSpec 中的镜像名为键保存构造出的假镜像。
	for _, imageSpec := range imageSpecs {
		r.Images[imageSpec.Image] = r.makeFakeImage(imageSpec)
	}
}

// SetFakeImageSize 设置构造假镜像时使用的统一镜像大小，影响后续所有 makeFakeImage 的结果。
func (r *FakeImageService) SetFakeImageSize(size uint64) {
	r.Lock()
	defer r.Unlock()

	r.FakeImageSize = size
}

// SetFakeImagePinned 设置单个镜像的 pinned（固定）标记。
// 若 Pinned map 尚未初始化，会先创建它。
func (r *FakeImageService) SetFakeImagePinned(image string, pinned bool) {
	r.Lock()
	defer r.Unlock()

	// 延迟初始化 map，避免对 nil map 赋值导致 panic。
	if r.Pinned == nil {
		r.Pinned = make(map[string]bool)
	}
	r.Pinned[image] = pinned
}

// SetFakeFilesystemUsage 设置 ImageFsInfo 返回的镜像文件系统用量。
func (r *FakeImageService) SetFakeFilesystemUsage(usage []*runtimeapi.FilesystemUsage) {
	r.Lock()
	defer r.Unlock()

	r.FakeFilesystemUsage = usage
}

// SetFakeContainerFilesystemUsage 设置 ImageFsInfo 返回的容器文件系统用量。
func (r *FakeImageService) SetFakeContainerFilesystemUsage(usage []*runtimeapi.FilesystemUsage) {
	r.Lock()
	defer r.Unlock()

	r.FakeContainerFilesystemUsage = usage
}

// NewFakeImageService 创建一个初始化好的 FakeImageService：
// Called、Errors、Images 三个字段都被初始化为空集合，避免调用方遇到 nil map 或 nil 切片。
func NewFakeImageService() *FakeImageService {
	return &FakeImageService{
		Called: make([]string, 0),
		Errors: make(map[string][]error),
		Images: make(map[string]*runtimeapi.Image),
	}
}

// makeFakeImage 根据给定的 ImageSpec 构造一个假的 runtimeapi.Image 对象。
// 镜像 ID 直接使用镜像名（真实运行时会随机生成，这里为方便测试而简化），
// 大小取自 FakeImageSize，并同步 Pinned 标记。
func (r *FakeImageService) makeFakeImage(image *runtimeapi.ImageSpec) *runtimeapi.Image {
	return &runtimeapi.Image{
		Id:       image.Image,
		Size:     r.FakeImageSize,
		Spec:     image,
		RepoTags: []string{image.Image},
		Pinned:   r.Pinned[image.Image],
	}
}

// stringInSlice 判断字符串 s 是否存在于切片 list 中，存在返回 true，否则返回 false。
func stringInSlice(s string, list []string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}

	return false
}

// InjectError 向指定方法名（f）的错误队列追加一个错误，用于模拟该方法下一次调用失败。
// 多次调用可注入多个错误，popError 会按先进先出的顺序逐个取出。
func (r *FakeImageService) InjectError(f string, err error) {
	r.Lock()
	defer r.Unlock()
	r.Errors[f] = append(r.Errors[f], err)
}

// popError 取出并返回方法 f 对应的下一个待注入错误。
// 注意：调用方必须先持有锁（本函数自身不加锁），因为每组错误是按调用顺序消费的。
// 若 f 没有注入错误或队列已空，返回 nil，表示该方法应正常执行。
func (r *FakeImageService) popError(f string) error {
	// Errors 未初始化时视为没有错误。
	if r.Errors == nil {
		return nil
	}
	errs := r.Errors[f]
	// 队列为空，说明没有更多错误需要注入。
	if len(errs) == 0 {
		return nil
	}
	// 取出队首错误，并把剩余队列写回 map，保证下次调用拿到下一个错误。
	err, errs := errs[0], errs[1:]
	r.Errors[f] = errs
	return err
}

// ListImages 列出假镜像。若之前通过 InjectError 为 ListImages 注入了错误，则返回该错误。
// 若 filter 指定了镜像名，只返回 RepoTags 中包含该名字的镜像。
func (r *FakeImageService) ListImages(_ context.Context, filter *runtimeapi.ImageFilter) ([]*runtimeapi.Image, error) {
	r.Lock()
	defer r.Unlock()

	// 记录本次调用，供测试断言。
	r.Called = append(r.Called, "ListImages")
	// 消费预设错误，若有则立即返回。
	if err := r.popError("ListImages"); err != nil {
		return nil, err
	}

	// 先准备一个非 nil 的空切片，保证即使没有镜像也返回 [] 而不是 nil。
	images := make([]*runtimeapi.Image, 0)
	for _, img := range r.Images {
		// 仅当 filter 明确指定镜像名时才做过滤，且要求该名字出现在 RepoTags 中。
		if filter != nil && filter.Image != nil {
			if !stringInSlice(filter.Image.Image, img.RepoTags) {
				continue
			}
		}

		images = append(images, img)
	}
	return images, nil
}

// ImageStatus 返回指定镜像的状态（这里直接从 Images map 中取镜像对象）。
// verbose 参数在该假实现中不影响结果。
func (r *FakeImageService) ImageStatus(_ context.Context, image *runtimeapi.ImageSpec, verbose bool) (*runtimeapi.ImageStatusResponse, error) {
	r.Lock()
	defer r.Unlock()

	r.Called = append(r.Called, "ImageStatus")
	if err := r.popError("ImageStatus"); err != nil {
		return nil, err
	}

	// 用镜像名查表；若镜像不存在，取到的是 nil，不会有额外的错误。
	return &runtimeapi.ImageStatusResponse{Image: r.Images[image.Image]}, nil
}

// PullImage 模拟拉取镜像：记录本次拉取的镜像与认证配置，并在本地镜像表中加入该镜像。
// 真实运行时会为镜像生成随机 ID，这里直接用镜像名作为 ID 以简化测试。
func (r *FakeImageService) PullImage(_ context.Context, image *runtimeapi.ImageSpec, auth *runtimeapi.AuthConfig, podSandboxConfig *runtimeapi.PodSandboxConfig) (string, error) {
	r.Lock()
	defer r.Unlock()

	r.Called = append(r.Called, "PullImage")
	if err := r.popError("PullImage"); err != nil {
		return "", err
	}

	// 记录拉取时的镜像规格和认证信息，供 AssertImagePulledWithAuth 断言。
	r.pulledImages = append(r.pulledImages, &pulledImage{imageSpec: image, authConfig: auth})
	imageID := image.Image
	// 只有当镜像尚不存在时才创建假镜像，避免覆盖已有镜像。
	if _, ok := r.Images[imageID]; !ok {
		r.Images[imageID] = r.makeFakeImage(image)
	}

	return imageID, nil
}

// RemoveImage 从假镜像表中删除指定镜像；若注入了错误则返回该错误。
func (r *FakeImageService) RemoveImage(_ context.Context, image *runtimeapi.ImageSpec) error {
	r.Lock()
	defer r.Unlock()

	r.Called = append(r.Called, "RemoveImage")
	if err := r.popError("RemoveImage"); err != nil {
		return err
	}

	// 直接从 map 中删除键；若镜像不存在，delete 是安全的空操作。
	delete(r.Images, image.Image)

	return nil
}

// ImageFsInfo 返回镜像文件系统与容器文件系统的用量信息，这些数据由
// SetFakeFilesystemUsage / SetFakeContainerFilesystemUsage 预先设置。
func (r *FakeImageService) ImageFsInfo(_ context.Context) (*runtimeapi.ImageFsInfoResponse, error) {
	r.Lock()
	defer r.Unlock()

	r.Called = append(r.Called, "ImageFsInfo")
	if err := r.popError("ImageFsInfo"); err != nil {
		return nil, err
	}

	// 用预设的用量数据组装响应。
	return &runtimeapi.ImageFsInfoResponse{
		ImageFilesystems:     r.FakeFilesystemUsage,
		ContainerFilesystems: r.FakeContainerFilesystemUsage,
	}, nil
}

// AssertImagePulledWithAuth 断言之前是否用给定的认证配置拉取过指定镜像。
// 它在 pulledImages 记录中查找匹配的项；若找不到，则通过 testify 的 assert 报告失败，
// failMsg 为断言失败时输出自定义消息。
func (r *FakeImageService) AssertImagePulledWithAuth(t *testing.T, image *runtimeapi.ImageSpec, auth *runtimeapi.AuthConfig, failMsg string) {
	r.Lock()
	defer r.Unlock()
	expected := &pulledImage{imageSpec: image, authConfig: auth}
	assert.Contains(t, r.pulledImages, expected, failMsg)
}

// pulledImage 记录一次 PullImage 调用时的完整上下文，
// 包含镜像规格（imageSpec）和使用的认证配置（authConfig），用于断言拉取行为。
type pulledImage struct {
	imageSpec  *runtimeapi.ImageSpec
	authConfig *runtimeapi.AuthConfig
}

// Close 模拟关闭客户端连接；假实现中仅记录调用并处理可能注入的错误。
func (r *FakeImageService) Close(_ context.Context) error {
	r.Lock()
	defer r.Unlock()

	r.Called = append(r.Called, "Close")
	if err := r.popError("Close"); err != nil {
		return err
	}

	return nil
}
