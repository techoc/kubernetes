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
	"time"

	"k8s.io/klog/v2"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
)

// GetPodCredentials 惰性查找并返回一组拉取凭据。
type GetPodCredentials func() ([]kubeletconfiginternal.ImagePullSecret, *kubeletconfiginternal.ImagePullServiceAccount, error)

// ImagePullManager 维护已拉取镜像以及仍在拉取中的镜像状态。
// 它需要记录 kubelet 当前正在拉取的镜像，以便在拉取失败时
// 判断是否应删除对应的 "pulling" 记录。
type ImagePullManager interface {
	// RecordPullIntent 记录拉取镜像的意图，应在实际拉取前调用。
	//
	// 每次拉取镜像前都必须调用 RecordPullIntent()，且每次调用必须
	// 与一次 RecordImagePulled()/RecordImagePullFailed() 调用严格对应。
	//
	// image 是 Pod 容器 image 字段的内容。
	RecordPullIntent(logger klog.Logger, image string) error
	// RecordImagePulled 记录镜像已使用给定凭据成功拉取。
	//
	// credentials 不能为 nil，且必须在 .KubernetesSecrets 中恰好包含一个
	// Kubernetes Secret 坐标，或把 .NodePodsAccessible 设为 true。
	//
	// image 是 Pod 容器 image 字段的内容。
	RecordImagePulled(ctx context.Context, image, imageRef string, credentials *kubeletconfiginternal.ImagePullCredentials)
	// RecordImagePullFailed 在镜像拉取失败时调用。
	//
	// 内部会把该镜像的引用计数减一；计数归零时删除对应的拉取意图记录。
	//
	// image 是 Pod 容器 image 字段的内容。
	RecordImagePullFailed(ctx context.Context, image string)
	// MustAttemptImagePull 评估 image 对应的拉取策略；若策略要求校验，
	// 则检查内部缓存中是否存在使用给定凭据拉取该镜像的记录，
	// 或该镜像是否可被节点上的任意 Pod 访问，
	// 或是否可被指定的 ServiceAccount 访问。
	//
	// 在评估完与凭据无关的策略部分后，才调用 getPodCredentials 获取凭据。
	//
	// getPodCredentials 返回错误时，返回 true 和该错误。
	//
	// 策略要求校验且缓存中找不到拉取记录时，返回 true 和 nil。
	//
	// image 是 Pod 容器 image 字段的内容。
	MustAttemptImagePull(ctx context.Context, image, imageRef string, getPodCredentials GetPodCredentials) (bool, error)
	// PruneUnknownRecords 删除缓存中 imageRef 不在 imageList 里的
	// ImagePulledRecord，前提是该记录的最后更新时间早于 until。
	//
	// 该方法只应由 kubelet 的镜像垃圾回收器调用。
	// until 是在向 CRI 请求 imageList 之前创建的时间戳。
	PruneUnknownRecords(ctx context.Context, imageList []string, until time.Time)
}

// PullRecordsAccessor 为 ImagePullIntents/ImagePulledRecords 提供统一访问接口，
// 与底层数据库实现无关。
type PullRecordsAccessor interface {
	// ListImagePullIntents 列出数据库中的全部 ImagePullIntent。
	// 无法解码的 ImagePullIntent 不会出现在结果中。
	// 读取数据库失败时返回 nil 和错误。
	//
	// 列举过程中出现部分错误时可能返回部分成功：
	// 返回成功读取的记录列表以及聚合后的错误。
	ListImagePullIntents() ([]*kubeletconfiginternal.ImagePullIntent, error)
	// ImagePullIntentExists 返回给定镜像是否存在有效的 ImagePullIntent。
	ImagePullIntentExists(image string) (bool, error)
	// WriteImagePullIntent 把镜像的拉取意图记录写入数据库。
	WriteImagePullIntent(logger klog.Logger, image string) error
	// DeleteImagePullIntent 从数据库删除镜像的拉取意图记录。
	DeleteImagePullIntent(logger klog.Logger, image string) error

	// ListImagePulledRecords 列出数据库中的 ImagePulledRecord。
	// 无法解码的记录会被忽略。
	// 读取数据库失败时返回错误。
	//
	// 列举过程中出现部分错误时可能返回部分成功：
	// 返回成功读取的记录列表以及聚合后的错误。
	ListImagePulledRecords() ([]*kubeletconfiginternal.ImagePulledRecord, error)
	// GetImagePulledRecord 获取指定 imageRef 的 ImagePulledRecord。
	// 若 imageRef 对应文件存在但内容无法解码，
	// 返回 exists=true，err 为解码错误。
	GetImagePulledRecord(imageRef string) (record *kubeletconfiginternal.ImagePulledRecord, exists bool, err error)
	// WriteImagePulledRecord 把 ImagePulledRecord 写入数据库。
	WriteImagePulledRecord(logger klog.Logger, record *kubeletconfiginternal.ImagePulledRecord) error
	// DeleteImagePulledRecord 从数据库删除 imageRef 对应的 ImagePulledRecord。
	DeleteImagePulledRecord(logger klog.Logger, imageRef string) error
}
