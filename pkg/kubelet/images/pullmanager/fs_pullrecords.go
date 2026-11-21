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
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/klog/v2"
	kubeletconfigv1beta1 "k8s.io/kubelet/config/v1beta1"
	kubeletconfiginternal "k8s.io/kubernetes/pkg/kubelet/apis/config"
	kubeletconfigint1alpha1 "k8s.io/kubernetes/pkg/kubelet/apis/config/v1alpha1"
	kubeletconfigint1beta1 "k8s.io/kubernetes/pkg/kubelet/apis/config/v1beta1"
)

const (
	// cacheFilesSHA256Prefix 是记录文件名前缀，表示文件名由镜像的 SHA256 摘要生成。
	cacheFilesSHA256Prefix = "sha256-"
	// tmpFilesSuffix 是写入过程中临时文件的后缀，用于保证写入的原子性。
	tmpFilesSuffix = ".tmp"
)

// encodeVersion 是记录序列化时使用的 API 版本。
var encodeVersion = kubeletconfigv1beta1.SchemeGroupVersion

var _ PullRecordsAccessor = &fsPullRecordsAccessor{}

// fsPullRecordsAccessor 使用文件系统读写 ImagePullIntent/ImagePulledRecord 记录。
type fsPullRecordsAccessor struct {
	pullingDir string
	pulledDir  string

	encoder runtime.Encoder
	decoder runtime.Decoder
}

// NewFSPullRecordsAccessor 返回一个以文件系统为后端存储的
// ImagePullIntent/ImagePulledRecord 访问器。
func NewFSPullRecordsAccessor(logger klog.Logger, kubeletDir string) (PullRecordsAccessor, error) {
	kubeletConfigEncoder, kubeletConfigDecoder, err := createKubeletConfigSchemeEncoderDecoder()
	if err != nil {
		return nil, err
	}

	accessor := &fsPullRecordsAccessor{
		pullingDir: filepath.Join(kubeletDir, "image_manager", "pulling"),
		pulledDir:  filepath.Join(kubeletDir, "image_manager", "pulled"),

		encoder: kubeletConfigEncoder,
		decoder: kubeletConfigDecoder,
	}

	// 目录权限收紧为 0700：记录中可能包含镜像引用等敏感信息，仅 kubelet 自身可访问。
	if err := os.MkdirAll(accessor.pullingDir, 0700); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(accessor.pulledDir, 0700); err != nil {
		return nil, err
	}

	// 启动时把旧版本格式的记录迁移到当前版本。
	accessor.recordsVersionMigration(logger)
	return NewMeteringRecordsAccessor(accessor, fsPullIntentsSize, fsPulledRecordsSize), nil
}

// recordsVersionMigration 将 pulling/pulled 目录中旧版本的记录重写为当前版本。
func (f *fsPullRecordsAccessor) recordsVersionMigration(logger klog.Logger) {
	err := processDirFiles(f.pullingDir,
		func(filePath string, fileContent []byte) error {
			intent, isCurrentVersion, err := decodeIntent(f.decoder, fileContent)
			if err != nil {
				return fmt.Errorf("failed to decode ImagePullIntent from file %q: %w", filePath, err)
			}
			// 仅当文件不是当前版本时才需要重写，避免无谓的磁盘写入。
			if !isCurrentVersion {
				if err := f.WriteImagePullIntent(logger, intent.Image); err != nil {
					return fmt.Errorf("failed to migrate ImagePullIntent for image %q to the current version: %w", intent.Image, err)
				}
			}
			return nil
		})
	if err != nil {
		logger.Error(err, "Error migrating image pull intents")
	}
	err = processDirFiles(f.pulledDir,
		func(filePath string, fileContent []byte) error {
			pullRecord, isCurrentVersion, err := decodePulledRecord(f.decoder, fileContent)
			if err != nil {
				return fmt.Errorf("failed to decode ImagePulledRecord from file %q: %w", filePath, err)
			}
			if !isCurrentVersion {
				if err := f.WriteImagePulledRecord(logger, pullRecord); err != nil {
					return fmt.Errorf("failed to migrate ImagePulledRecord for image ref %q to the current version: %w", pullRecord.ImageRef, err)
				}
			}
			return nil
		})

	if err != nil {
		logger.Error(err, "Error migrating image pulled records")
	}
}

// WriteImagePullIntent 持久化一条镜像拉取意图记录。
func (f *fsPullRecordsAccessor) WriteImagePullIntent(logger klog.Logger, image string) error {
	intent := kubeletconfiginternal.ImagePullIntent{
		Image: image,
	}

	intentBytes := bytes.NewBuffer([]byte{})
	if err := f.encoder.Encode(&intent, intentBytes); err != nil {
		return err
	}

	return writeFile(f.pullingDir, cacheFilename(image), intentBytes.Bytes())
}

// ListImagePullIntents 列出所有已持久化的镜像拉取意图。
func (f *fsPullRecordsAccessor) ListImagePullIntents() ([]*kubeletconfiginternal.ImagePullIntent, error) {
	var intents []*kubeletconfiginternal.ImagePullIntent
	// 遍历 pulling 目录收集全部拉取意图记录。
	err := processDirFiles(f.pullingDir,
		func(filePath string, fileContent []byte) error {
			intent, _, err := decodeIntent(f.decoder, fileContent)
			if err != nil {
				return fmt.Errorf("failed to deserialize content of file %q into ImagePullIntent: %w", filePath, err)
			}
			intents = append(intents, intent)

			return nil
		})
	return intents, err
}

// ImagePullIntentExists 判断指定镜像是否存在拉取意图记录。
func (f *fsPullRecordsAccessor) ImagePullIntentExists(image string) (bool, error) {
	intentRecordPath := filepath.Join(f.pullingDir, cacheFilename(image))
	intentBytes, err := os.ReadFile(intentRecordPath)
	if os.IsNotExist(err) {
		return false, nil
	} else if err != nil {
		return false, err
	}

	// 文件名由镜像摘要生成，但仍需校验内容中的镜像名，
	// 防止哈希碰撞或文件被替换导致误判。
	intent, _, err := decodeIntent(f.decoder, intentBytes)
	if err != nil {
		return false, err
	}

	return intent.Image == image, nil
}

// DeleteImagePullIntent 删除指定镜像的拉取意图记录；记录不存在时视为成功。
func (f *fsPullRecordsAccessor) DeleteImagePullIntent(logger klog.Logger, image string) error {
	err := os.Remove(filepath.Join(f.pullingDir, cacheFilename(image)))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// GetImagePulledRecord 获取指定镜像引用的拉取记录。
// 第二个返回值表示记录文件是否存在，即使内容无效。
func (f *fsPullRecordsAccessor) GetImagePulledRecord(imageRef string) (*kubeletconfiginternal.ImagePulledRecord, bool, error) {
	recordBytes, err := os.ReadFile(filepath.Join(f.pulledDir, cacheFilename(imageRef)))
	if os.IsNotExist(err) {
		return nil, false, nil
	} else if err != nil {
		return nil, false, err
	}

	pulledRecord, _, err := decodePulledRecord(f.decoder, recordBytes)
	if err != nil {
		return nil, true, err
	}
	// 校验记录中的镜像引用与请求一致，防止哈希碰撞导致返回错误记录。
	if pulledRecord.ImageRef != imageRef {
		return nil, false, nil
	}
	return pulledRecord, true, err
}

// ListImagePulledRecords 列出所有已持久化的镜像拉取记录。
func (f *fsPullRecordsAccessor) ListImagePulledRecords() ([]*kubeletconfiginternal.ImagePulledRecord, error) {
	var pullRecords []*kubeletconfiginternal.ImagePulledRecord
	err := processDirFiles(f.pulledDir,
		func(filePath string, fileContent []byte) error {
			pullRecord, _, err := decodePulledRecord(f.decoder, fileContent)
			if err != nil {
				return fmt.Errorf("failed to deserialize content of file %q into ImagePulledRecord: %w", filePath, err)
			}
			pullRecords = append(pullRecords, pullRecord)
			return nil
		})

	return pullRecords, err
}

// WriteImagePulledRecord 持久化一条镜像拉取记录。
func (f *fsPullRecordsAccessor) WriteImagePulledRecord(logger klog.Logger, pulledRecord *kubeletconfiginternal.ImagePulledRecord) error {
	recordBytes := bytes.NewBuffer([]byte{})
	if err := f.encoder.Encode(pulledRecord, recordBytes); err != nil {
		return fmt.Errorf("failed to serialize ImagePulledRecord: %w", err)
	}

	return writeFile(f.pulledDir, cacheFilename(pulledRecord.ImageRef), recordBytes.Bytes())
}

// DeleteImagePulledRecord 删除指定镜像引用的拉取记录；记录不存在时视为成功。
func (f *fsPullRecordsAccessor) DeleteImagePulledRecord(logger klog.Logger, imageRef string) error {
	err := os.Remove(filepath.Join(f.pulledDir, cacheFilename(imageRef)))
	if os.IsNotExist(err) {
		return nil
	}
	return err
}

// intentsSize 返回 pulling 目录中有效缓存文件的数量。
func (f *fsPullRecordsAccessor) intentsSize() (uint, error) {
	intentsCount, err := countCacheFiles(f.pullingDir)
	if err != nil {
		return 0, err
	}
	return intentsCount, nil
}

// pulledRecordsSize 返回 pulled 目录中有效缓存文件的数量。
func (f *fsPullRecordsAccessor) pulledRecordsSize() (uint, error) {
	pulledRecordsCount, err := countCacheFiles(f.pulledDir)
	if err != nil {
		return 0, err
	}
	return pulledRecordsCount, nil
}

// countCacheFiles 统计目录中有效缓存文件的数量。
func countCacheFiles(dirName string) (uint, error) {
	// 分批读取目录项，避免一次性加载大量条目占用过多内存。
	const readBatch = 20

	var cacheFilesCount uint
	dir, err := os.Open(dirName)
	if err != nil {
		return 0, fmt.Errorf("failed to open directory %q: %w", dirName, err)
	}
	// 只读打开的目录关闭出错是安全的，可以忽略。
	defer func() { _ = dir.Close() }()

	for {
		entries, err := dir.ReadDir(readBatch)
		for _, entry := range entries {
			if !isValidCacheFile(entry) {
				continue
			}
			cacheFilesCount++
		}
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return 0, fmt.Errorf("failed to list all entries in directory %q: %w", dirName, err)
		}
	}
	return cacheFilesCount, nil
}

// cacheFilename 根据镜像字符串的 SHA256 摘要生成记录文件名，
// 避免镜像名中的特殊字符直接出现在文件名中。
func cacheFilename(image string) string {
	return fmt.Sprintf("%s%x", cacheFilesSHA256Prefix, sha256.Sum256([]byte(image)))
}

// isValidCacheFile 判断目录项是否为有效缓存文件：
// 不是目录，且文件名符合前缀要求且不是临时文件。
func isValidCacheFile(fileInfo os.DirEntry) bool {
	if fileInfo.IsDir() {
		return false
	}

	filename := fileInfo.Name()
	return strings.HasPrefix(filename, cacheFilesSHA256Prefix) && !strings.HasSuffix(filename, tmpFilesSuffix)
}

// writeFile 将 content 写入目录 dir 下名为 filename 的文件。
// 通过“先写临时文件、成功后重命名”的方式保证写入的原子性，
// 避免进程崩溃时留下半写文件。
func writeFile(dir, filename string, content []byte) error {
	// 目标目录不存在时先创建。
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create directory %q: %w", dir, err)
	}

	targetPath := filepath.Join(dir, filename)
	tmpPath := targetPath + tmpFilesSuffix
	if err := os.WriteFile(tmpPath, content, 0600); err != nil {
		_ = os.Remove(tmpPath) // 文件可能已被部分写入，尝试删除以保持目录干净。
		return fmt.Errorf("failed to create temporary file %q: %w", tmpPath, err)
	}

	// 重命名失败时清理临时文件。
	if err := os.Rename(tmpPath, targetPath); err != nil {
		_ = os.Remove(tmpPath) // 尝试清理。
		return err
	}
	return nil
}

// processDirFiles 读取指定目录中的文件，并对每个文件执行 fileAction。
func processDirFiles(dirName string, fileAction func(filePath string, fileContent []byte) error) error {
	var walkErrors []error
	err := filepath.WalkDir(dirName, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			walkErrors = append(walkErrors, err)
			return nil
		}

		if path == dirName {
			return nil
		}

		if d.IsDir() {
			// 不进入子目录：记录只存放在目录第一层。
			return filepath.SkipDir
		}

		// 跳过非本组件写入的文件与临时文件。
		if filename := d.Name(); !strings.HasPrefix(filename, cacheFilesSHA256Prefix) || strings.HasSuffix(filename, tmpFilesSuffix) {
			return nil
		}

		fileContent, err := os.ReadFile(path)
		if err != nil {
			walkErrors = append(walkErrors, fmt.Errorf("failed to read %q: %w", path, err))
			return nil
		}

		if err := fileAction(path, fileContent); err != nil {
			walkErrors = append(walkErrors, err)
			return nil
		}

		return nil
	})
	if err != nil {
		walkErrors = append(walkErrors, err)
	}

	return utilerrors.NewAggregate(walkErrors)
}

// createKubeletConfigSchemeEncoderDecoder 为内部与 alpha 版 kubelet 配置 API
// 创建严格编码的 encoder 与 decoder。
func createKubeletConfigSchemeEncoderDecoder() (runtime.Encoder, runtime.Decoder, error) {
	codecs, info, err := getKubeletConfigSerializerInfo()
	if err != nil {
		return nil, nil, err
	}
	return codecs.EncoderForVersion(info.Serializer, encodeVersion), codecs.UniversalDecoder(), nil
}

// getKubeletConfigSerializerInfo 注册 kubelet 配置相关 API 并返回序列化器信息。
func getKubeletConfigSerializerInfo() (*serializer.CodecFactory, *runtime.SerializerInfo, error) {
	const mediaType = runtime.ContentTypeJSON

	scheme := runtime.NewScheme()
	if err := kubeletconfigint1beta1.AddToScheme(scheme); err != nil {
		return nil, nil, err
	}
	if err := kubeletconfigint1alpha1.AddToScheme(scheme); err != nil {
		return nil, nil, err
	}
	if err := kubeletconfiginternal.AddToScheme(scheme); err != nil {
		return nil, nil, err
	}

	// 启用严格模式：遇到未知字段直接报错，防止记录格式悄悄漂移。
	codecs := serializer.NewCodecFactory(scheme, serializer.EnableStrict)

	info, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), mediaType)
	if !ok {
		return nil, nil, fmt.Errorf("unable to locate encoder -- %q is not a supported media type", mediaType)
	}
	return &codecs, &info, nil
}

// decodeIntent 解码镜像拉取意图并转换为内部 API 版本。
// decoder 必须已注册 kubelet 配置 API 的各个 schema。
//
// 返回值中的对象为内部表示，布尔值表示该记录是否已是当前已知的最新版本。
func decodeIntent(d runtime.Decoder, objBytes []byte) (*kubeletconfiginternal.ImagePullIntent, bool, error) {
	obj, gvk, err := d.Decode(objBytes, nil, nil)
	if err != nil {
		return nil, false, err
	}

	intentObj, ok := obj.(*kubeletconfiginternal.ImagePullIntent)
	if !ok {
		return nil, false, fmt.Errorf("failed to convert object to *ImagePullIntent: %T", obj)
	}

	// 通过解码出的 GVK 版本判断记录是否需要迁移。
	isLatestVersion := gvk.Version == encodeVersion.Version
	return intentObj, isLatestVersion, nil
}

// decodePulledRecord 解码镜像拉取记录并转换为内部 API 版本。
// decoder 必须已注册 kubelet 配置 API 的各个 schema。
//
// 返回值中的对象为内部表示，布尔值表示该记录是否已是当前已知的最新版本。
func decodePulledRecord(d runtime.Decoder, objBytes []byte) (*kubeletconfiginternal.ImagePulledRecord, bool, error) {
	obj, gvk, err := d.Decode(objBytes, nil, nil)
	if err != nil {
		return nil, false, err
	}

	pulledRecord, ok := obj.(*kubeletconfiginternal.ImagePulledRecord)
	if !ok {
		return nil, false, fmt.Errorf("failed to convert object to *ImagePulledRecord: %T", obj)
	}

	// 通过解码出的 GVK 版本判断记录是否需要迁移。
	isLatestVersion := gvk.Version == encodeVersion.Version
	return pulledRecord, isLatestVersion, nil
}
