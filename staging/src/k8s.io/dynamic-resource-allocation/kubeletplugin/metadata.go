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

package kubeletplugin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	cdispec "tags.cncf.io/container-device-interface/specs-go"

	resourceapi "k8s.io/api/resource/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/dynamic-resource-allocation/api/metadata"
	"k8s.io/dynamic-resource-allocation/api/metadata/install"
	"k8s.io/dynamic-resource-allocation/devicemetadata"
	"k8s.io/dynamic-resource-allocation/resourceclaim"
)

// metadataScheme 同时注册了内部版本与各外部版本的元数据类型；metadataCodecFactory 由它生成编解码器。
// metadataSerializer 保存 JSON 序列化器；每个写入器再用 newMetadataWriter 按目标版本创建各自的编码器。
var (
	metadataScheme       = install.NewScheme()
	metadataCodecFactory = runtimeserializer.NewCodecFactory(metadataScheme)
	metadataSerializer   runtime.Serializer
)

// init 从编解码工厂中查找 JSON 媒体类型对应的序列化器并保存其 PrettySerializer。
func init() {
	// 查找 JSON 媒体类型对应的序列化器信息。
	info, ok := runtime.SerializerInfoForMediaType(metadataCodecFactory.SupportedMediaTypes(), runtime.ContentTypeJSON)
	// 未注册 JSON 序列化器属于配置错误，直接 panic。
	if !ok {
		panic("no JSON serializer registered for metadata scheme")
	}
	// 使用带缩进的序列化器，便于生成可读的元数据文件。
	metadataSerializer = info.PrettySerializer
}

// decodeFirstMetadata 从元数据 JSON 流中解码第一个兼容对象为内部 DeviceMetadata 类型。
func decodeFirstMetadata(data []byte) (*metadata.DeviceMetadata, error) {
	// 准备接收解码结果的内部对象。
	var dm metadata.DeviceMetadata
	// 使用流式解码器跳过不兼容的版本，只取第一个可识别的对象。
	if err := devicemetadata.DecodeMetadataFromStream(json.NewDecoder(bytes.NewReader(data)), &dm); err != nil {
		return nil, err
	}
	return &dm, nil
}

// 以下常量定义元数据文件在宿主机上的存放子目录、默认 CDI 目录及文件权限。
const (
	// metadataSubDir 是插件数据目录下存放元数据文件的子目录名。
	metadataSubDir = "dra-device-metadata"

	// DefaultCDIDir 是 CDI 规范文件的默认目录。
	DefaultCDIDir = "/var/run/cdi"

	// metadataFilePerms 与 metadataDirPerms 分别是元数据文件与目录的权限位。
	metadataFilePerms = os.FileMode(0644)
	metadataDirPerms  = os.FileMode(0755)
)

// claimRef 把 ResourceClaim 的身份信息与 Pod 级别引用打包在一起，
// 以便在写入路径中逐层传递，避免函数参数列表过长。
type claimRef struct {
	// namespace、name、uid 分别标识 claim 的命名空间、名称与 UID；
	// podClaimName 是资源声明模板对应的 Pod 级别声明名（若存在）。
	namespace    string
	name         string
	uid          types.UID
	podClaimName *string
}

// newClaimRef 从 ResourceClaim 构造 claimRef；若带有 PodResourceClaimAnnotation 注解则记录 Pod 级别声明名。
func newClaimRef(claim *resourceapi.ResourceClaim) claimRef {
	// 先填入 claim 的身份字段。
	ref := claimRef{
		namespace: claim.Namespace,
		name:      claim.Name,
		uid:       claim.UID,
	}
	// 存在 Pod 资源声明注解时，记录其声明名。
	if v, ok := claim.Annotations[resourceapi.PodResourceClaimAnnotation]; ok {
		ref.podClaimName = &v
	}
	return ref
}

// metadataWriter 负责为设备元数据特性写入元数据文件与 CDI 规范。
// 文件格式与容器内路径布局见 k8s.io/dynamic-resource-allocation/api/metadata。
type metadataWriter struct {
	// driverName 是驱动名；pluginDataDir 是插件数据目录；cdiDir 是 CDI 规范目录；
	// versions 是目标 API 版本列表；encoders 是按版本创建的编码器；
	// fileOps 是可替换的文件操作接口，便于测试。
	driverName    string
	pluginDataDir string
	cdiDir        string
	versions      []schema.GroupVersion
	encoders      map[schema.GroupVersion]runtime.Encoder
	fileOps       MetadataFileOperations
}

// newMetadataWriter 创建写入器，为每个指定版本准备编码器；不支持的版本被静默跳过，
// 但如果请求的版本全部不受支持则返回错误。
func newMetadataWriter(driverName, pluginDataDir, cdiDir string, versions []schema.GroupVersion, fileOps MetadataFileOperations) (*metadataWriter, error) {
	// 记录实际受支持的版本。
	var supported []schema.GroupVersion
	// 按请求数量预分配编码器映射。
	encoders := make(map[schema.GroupVersion]runtime.Encoder, len(versions))
	// 逐个版本检查是否已在 scheme 中注册。
	for _, gv := range versions {
		// 未注册的版本直接跳过。
		if !metadataScheme.IsVersionRegistered(gv) {
			continue
		}
		// 受支持版本加入列表。
		supported = append(supported, gv)
		// 为该版本创建专用编码器。
		encoders[gv] = metadataCodecFactory.EncoderForVersion(metadataSerializer, gv)
	}
	// 一个版本都不支持时无法写入任何内容，直接报错。
	if len(supported) == 0 {
		// TODO(KEP#5304)：目前没有默认版本，驱动应通过 EnableDeviceMetadata 的 MetadataVersions 选项显式指定版本。
		return nil, fmt.Errorf("none of the requested metadata API versions %v are supported", versions)
	}
	// 用筛选后的版本与编码器构造写入器。
	return &metadataWriter{
		driverName:    driverName,
		pluginDataDir: pluginDataDir,
		cdiDir:        cdiDir,
		versions:      supported,
		encoders:      encoders,
		fileOps:       fileOps,
	}, nil
}

// processPreparedClaim 为已准备的 ResourceClaim 中所有请求写入元数据文件与 CDI 规范。
// 返回“基础请求名 -> 应注入 gRPC 响应的 CDI 设备 ID”的映射。
func (w *metadataWriter) processPreparedClaim(
	claim *resourceapi.ResourceClaim,
	devices []Device,
) (map[string]string, error) {
	// 提取 claim 的身份信息与 Pod 级别引用。
	ref := newClaimRef(claim)

	// 先把设备按请求名分组：一个请求可能对应多台设备。
	preparedDevicesByRequest := make(map[string][]Device)
	// 遍历已准备的设备。
	for _, dev := range devices {
		// 同一台设备可能被多个请求引用。
		for _, reqName := range dev.Requests {
			preparedDevicesByRequest[reqName] = append(preparedDevicesByRequest[reqName], dev)
		}
	}

	// 保存每个基础请求对应的 CDI 设备 ID。
	cdiDeviceIDs := make(map[string]string)

	// 逐请求写入元数据与 CDI 规范。
	for requestRef, devs := range preparedDevicesByRequest {
		// 去掉子请求后缀得到基础请求名，用于文件路径与映射键。
		baseReq := resourceclaim.BaseRequestRef(requestRef)

		// 写入该请求的元数据文件。
		if err := w.writeMetadataFile(ref, baseReq, requestRef, devs); err != nil {
			return nil, fmt.Errorf("write metadata for request %q: %w", requestRef, err)
		}

		// 写入把元数据文件绑定挂载进容器的 CDI 规范。
		cdiID, err := w.writeCDISpec(ref, claim, baseReq)
		if err != nil {
			return nil, fmt.Errorf("write CDI spec for request %q: %w", requestRef, err)
		}

		// 记录该基础请求对应的 CDI 设备 ID。
		cdiDeviceIDs[baseReq] = cdiID
	}

	return cdiDeviceIDs, nil
}

// cleanupClaim 删除某个 claim 的全部元数据文件与 CDI 规范；没有任何文件时视为无操作。
func (w *metadataWriter) cleanupClaim(claimNamespace, claimName string, claimUID types.UID) error {
	// 先定位该 claim 的元数据目录。
	metadataDir := w.claimMetadataDir(claimNamespace, claimName)
	// 递归删除整个目录，目录不存在不算错误。
	if err := w.fileOps.RemoveAll(metadataDir); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove metadata directory %s: %w", metadataDir, err)
	}

	// 按“驱动名_metadata_CLAIMUID_*.json”模式匹配该 claim 的 CDI 规范。
	pattern := filepath.Join(w.cdiDir, fmt.Sprintf("%s_metadata_%s_*.json", w.driverName, string(claimUID)))
	// 列出所有匹配的 CDI 规范文件。
	matches, err := w.fileOps.Glob(pattern)
	if err != nil {
		return fmt.Errorf("glob CDI specs %s: %w", pattern, err)
	}
	// 逐个删除，忽略文件不存在的情况。
	for _, cdiPath := range matches {
		if err := w.fileOps.Remove(cdiPath); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("remove CDI spec %s: %w", cdiPath, err)
		}
	}

	return nil
}

// updateRequestMetadata 覆写某个请求的元数据文件：读取现有流取得当前 generation，加一后写回。
// 供驱动在 NRI 钩子中、于 PrepareResourceClaims 之后才拿到设备细节（例如网络信息）时使用。
// requestName 可以带子请求名（如 "gpu/high-memory"），文件路径使用基础请求名。
func (w *metadataWriter) updateRequestMetadata(
	ref claimRef,
	requestName string,
	devices []Device,
) error {
	// 文件路径使用基础请求名。
	baseReq := resourceclaim.BaseRequestRef(requestName)
	// 定位已有的元数据文件。
	filePath := w.metadataFilePath(ref.namespace, ref.name, baseReq)

	// 读取现有元数据。
	existing, err := w.fileOps.ReadFile(filePath)
	// 文件不存在表示该 claim 尚未准备这个请求。
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("request %q not found in prepared claim %s/%s", requestName, ref.namespace, ref.name)
		}
		return fmt.Errorf("read metadata file: %w", err)
	}

	// 解码已有元数据，以取得 generation 与 Pod 声明名。
	prev, err := decodeFirstMetadata(existing)
	if err != nil {
		return fmt.Errorf("decode existing metadata: %w", err)
	}

	// 沿用已有的 Pod 声明名，避免更新时丢失。
	ref.podClaimName = prev.PodClaimName

	// generation 递增。
	generation := prev.Generation + 1
	// 用新的设备信息重建元数据。
	dm := w.buildDeviceMetadata(ref, generation, requestName, devices)

	// 按配置版本重新编码为 JSON 流。
	data, err := w.encodeMetadataStream(dm)
	if err != nil {
		return err
	}

	// 覆写元数据文件。
	if err := w.fileOps.WriteFile(filePath, data, metadataFilePerms); err != nil {
		return fmt.Errorf("write metadata file: %w", err)
	}

	return nil
}

// writeMetadataFile 为一个请求写入单个元数据文件：内容是按每个配置版本编码后拼接的 JSON 流。
// baseRequestName 决定文件路径；requestRef（可含子请求名）记录在元数据内容中。
func (w *metadataWriter) writeMetadataFile(
	ref claimRef,
	baseRequestName string,
	requestRef string,
	devices []Device,
) error {
	// 首次写入，generation 从 1 开始。
	dm := w.buildDeviceMetadata(ref, 1, requestRef, devices)

	// 编码为多版本 JSON 流。
	data, err := w.encodeMetadataStream(dm)
	if err != nil {
		return err
	}

	// 计算宿主机上的文件路径。
	filePath := w.metadataFilePath(ref.namespace, ref.name, baseRequestName)
	// 先创建父目录。
	if err := w.fileOps.MkdirAll(filepath.Dir(filePath), metadataDirPerms); err != nil {
		return fmt.Errorf("create metadata directory: %w", err)
	}

	// 写入文件。
	if err := w.fileOps.WriteFile(filePath, data, metadataFilePerms); err != nil {
		return fmt.Errorf("write metadata file: %w", err)
	}

	return nil
}

// buildDeviceMetadata 从 claim 信息与设备数据构造内部 DeviceMetadata。
// 内部类型与版本无关，序列化时由各版本编码器负责转换到目标 API 版本。
func (w *metadataWriter) buildDeviceMetadata(
	ref claimRef,
	generation int64,
	requestName string,
	devices []Device,
) *metadata.DeviceMetadata {
	// 预分配设备列表。
	metadataDevices := make([]metadata.Device, 0, len(devices))
	// 逐台设备转换。
	for _, dev := range devices {
		// 驱动名、池名与设备名直接取自设备本身。
		d := metadata.Device{
			Driver: w.driverName,
			Pool:   dev.PoolName,
			Name:   dev.DeviceName,
		}
		// 存在额外元数据时复制属性与网络数据。
		if dev.Metadata != nil {
			// 属性映射需把键转换为带命名空间的 QualifiedName。
			if dev.Metadata.Attributes != nil {
				// 预分配属性映射。
				d.Attributes = make(map[resourceapi.QualifiedName]resourceapi.DeviceAttribute, len(dev.Metadata.Attributes))
				// 逐个复制属性。
				for k, v := range dev.Metadata.Attributes {
					d.Attributes[resourceapi.QualifiedName(k)] = v
				}
			}
			// 复制网络数据。
			d.NetworkData = dev.Metadata.NetworkData
		}
		// 加入结果列表。
		metadataDevices = append(metadataDevices, d)
	}

	// 组装元数据对象：ObjectMeta 携带 claim 身份与 generation，Requests 只含当前请求。
	return &metadata.DeviceMetadata{
		ObjectMeta: metav1.ObjectMeta{
			Name:       ref.name,
			Namespace:  ref.namespace,
			UID:        ref.uid,
			Generation: generation,
		},
		PodClaimName: ref.podClaimName,
		Requests: []metadata.DeviceMetadataRequest{
			{
				Name:    requestName,
				Devices: metadataDevices,
			},
		},
	}
}

// encodeMetadataStream 按每个配置版本各编码一次内部对象，并把结果拼接成一个 JSON 流。
func (w *metadataWriter) encodeMetadataStream(dm *metadata.DeviceMetadata) ([]byte, error) {
	// 用缓冲区拼接各版本的编码结果。
	var buf bytes.Buffer
	// 逐版本编码。
	for _, gv := range w.versions {
		// 使用该版本专用的编码器。
		data, err := runtime.Encode(w.encoders[gv], dm)
		if err != nil {
			return nil, fmt.Errorf("encode metadata for %s: %w", gv, err)
		}
		// 追加该版本的 JSON 数据。
		buf.Write(data)
		// 每个版本后追加换行，作为流中对象之间的分隔。
		buf.WriteByte('\n')
	}
	return buf.Bytes(), nil
}

// writeCDISpec 写入一份 CDI 规范，把元数据文件绑定挂载到容器中，并返回 CDI 设备 ID。
// 容器内路径布局见 k8s.io/dynamic-resource-allocation/api/metadata。
func (w *metadataWriter) writeCDISpec(
	ref claimRef,
	claim *resourceapi.ResourceClaim,
	requestName string,
) (string, error) {
	// CDI 设备名由 claim UID 与请求名拼接，保证唯一。
	deviceName := string(claim.UID) + "_" + requestName

	// 构造 CDI 规范：一个设备，带一个只读绑定挂载。
	spec := &cdispec.Spec{
		Kind: w.driverName + "/metadata",
		Devices: []cdispec.Device{
			{
				Name: deviceName,
				ContainerEdits: cdispec.ContainerEdits{
					Mounts: []*cdispec.Mount{
						{
							HostPath:      w.metadataFilePath(claim.Namespace, claim.Name, requestName),
							ContainerPath: w.containerFilePath(ref, requestName),
							Options:       []string{"ro", "bind"},
						},
					},
				},
			},
		},
	}

	// 根据规范内容计算所需的最低 CDI 版本。
	minVersion, err := cdispec.MinimumRequiredVersion(spec)
	if err != nil {
		return "", fmt.Errorf("determine CDI spec version: %w", err)
	}
	// 写入计算得到的最低版本。
	spec.Version = minVersion

	// 序列化为带缩进的 JSON，便于阅读。
	data, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		return "", fmt.Errorf("marshal CDI spec: %w", err)
	}

	// 确保 CDI 目录存在。
	if err := w.fileOps.MkdirAll(w.cdiDir, metadataDirPerms); err != nil {
		return "", fmt.Errorf("create CDI directory: %w", err)
	}

	// 计算 CDI 规范文件的路径。
	cdiPath := w.cdiSpecFilePath(claim.UID, requestName)
	// 写入 CDI 规范文件。
	if err := w.fileOps.WriteFile(cdiPath, data, metadataFilePerms); err != nil {
		return "", fmt.Errorf("write CDI spec file: %w", err)
	}

	// 返回“驱动名/种类=设备名”形式的 CDI 设备 ID。
	return w.driverName + "/metadata=" + deviceName, nil
}

// 下面是路径计算辅助函数。
// claimMetadataDir 返回某个 claim 元数据文件在宿主机上的目录：
// {pluginDataDir}/dra-device-metadata/{claimNs}_{claimName}/。
func (w *metadataWriter) claimMetadataDir(claimNs, claimName string) string {
	return filepath.Join(w.pluginDataDir, metadataSubDir, claimNs+"_"+claimName)
}

// metadataFilePath 返回某个请求元数据文件在宿主机上的路径：
// {pluginDataDir}/dra-device-metadata/{claimNs}_{claimName}/{requestName}/metadata.json。
func (w *metadataWriter) metadataFilePath(claimNs, claimName, requestName string) string {
	return filepath.Join(w.claimMetadataDir(claimNs, claimName), requestName, "metadata.json")
}

// cdiSpecFilePath 返回 CDI 规范文件路径：
// {cdiDir}/{driverName}_metadata_{claimUID}_{requestName}.json。
func (w *metadataWriter) cdiSpecFilePath(claimUID types.UID, requestName string) string {
	return filepath.Join(w.cdiDir, fmt.Sprintf("%s_metadata_%s_%s.json", w.driverName, string(claimUID), requestName))
}

// containerFilePath 返回元数据文件在容器内的路径；
// 来自资源声明模板的请求走 ResourceClaimTemplates 路径，普通请求走 ResourceClaims 路径。
func (w *metadataWriter) containerFilePath(ref claimRef, requestName string) string {
	// 带有 Pod 声明名时使用模板路径。
	if ref.podClaimName != nil && *ref.podClaimName != "" {
		return metadata.ResourceClaimTemplateFilePath(w.driverName, *ref.podClaimName, requestName)
	}
	return metadata.ResourceClaimFilePath(w.driverName, ref.name, requestName)
}

// UpdateRequestMetadata 覆写已准备 claim 中某个请求的元数据文件，并递增 generation。
// 适用于设备属性（如 IP 地址、接口名）只有在 Pod 沙箱创建后才可知的驱动：
// PrepareResourceClaims 时先返回不带 Metadata 的设备，信息就绪后再调用本方法。
// devices 应与当初 PrepareResult 中的设备一致，只是填上了 Metadata 字段。
func (d *Helper) UpdateRequestMetadata(
	ctx context.Context,
	claimNamespace, claimName string,
	claimUID types.UID,
	requestName string,
	devices []Device,
) error {
	// 未启用设备元数据特性时直接报错。
	if d.metadataWriter == nil {
		return fmt.Errorf("device metadata is not enabled")
	}
	// 用调用方提供的身份信息构造 claimRef（Pod 声明名会从已有文件继承）。
	ref := claimRef{
		namespace: claimNamespace,
		name:      claimName,
		uid:       claimUID,
	}
	// 委托写入器执行更新。
	return d.metadataWriter.updateRequestMetadata(ref, requestName, devices)
}
