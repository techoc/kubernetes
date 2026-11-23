/*
Copyright 2019 The Kubernetes Authors.

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

package plugins

import (
	"fmt"
	"regexp"
	"strings"

	v1 "k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog/v2"
)

const (
	// AzureFileDriverName is the name of the CSI driver for Azure File
	// AzureFileDriverName 是 Azure File 对应的 CSI 驱动名称，翻译后的 PV 会引用它。
	AzureFileDriverName = "file.csi.azure.com"
	// AzureFileInTreePluginName is the name of the intree plugin for Azure file
	// AzureFileInTreePluginName 是树内（in-tree）Azure File 插件的名称。
	AzureFileInTreePluginName = "kubernetes.io/azure-file"

	// separator 是 volume handle 中用于分隔各字段的字符（#）。
	// volumeIDTemplate 是 volume handle 的格式模板，依次填入资源组、存储账户、文件共享名、PV 名称和 Secret 命名空间。
	separator        = "#"
	volumeIDTemplate = "%s#%s#%s#%s#%s"
	// Parameter names defined in azure file CSI driver, refer to
	// https://github.com/kubernetes-sigs/azurefile-csi-driver/blob/master/docs/driver-parameters.md
	// 下面这组常量是 Azure File CSI 驱动约定的 VolumeAttributes 键名：
	// shareNameField 表示文件共享名，secretNameField 表示存储账户 Secret 名称，
	// secretNamespaceField 表示 Secret 所在命名空间，secretNameTemplate 是默认 Secret 名称的模板，
	// defaultSecretNamespace 是未指定命名空间时使用的默认值，
	// resourceGroupAnnotation 是记录资源组信息的 PV annotation 键。
	shareNameField          = "sharename"
	secretNameField         = "secretname"
	secretNamespaceField    = "secretnamespace"
	secretNameTemplate      = "azure-storage-account-%s-secret"
	defaultSecretNamespace  = "default"
	resourceGroupAnnotation = "kubernetes.io/azure-file-resource-group"
)

// 编译期断言：确保 azureFileCSITranslator 完整实现 InTreePlugin 接口。
var _ InTreePlugin = &azureFileCSITranslator{}

// secretNameFormatRE 用于从默认的 Secret 名称中反解出存储账户名。
var secretNameFormatRE = regexp.MustCompile(`azure-storage-account-(.+)-secret`)

// azureFileCSITranslator handles translation of PV spec from In-tree
// Azure File to CSI Azure File and vice versa
// azureFileCSITranslator 负责在树内 Azure File 与 CSI Azure File 之间双向翻译 PV 配置。
type azureFileCSITranslator struct{}

// NewAzureFileCSITranslator returns a new instance of azureFileTranslator
// NewAzureFileCSITranslator 返回一个 Azure File 翻译器实例。
func NewAzureFileCSITranslator() InTreePlugin {
	return &azureFileCSITranslator{}
}

// TranslateInTreeStorageClassToCSI translates InTree Azure File storage class parameters to CSI storage class
// TranslateInTreeStorageClassToCSI 翻译 StorageClass 参数；Azure File 无需转换，因此原样返回。
func (t *azureFileCSITranslator) TranslateInTreeStorageClassToCSI(logger klog.Logger, sc *storage.StorageClass) (*storage.StorageClass, error) {
	return sc, nil
}

// TranslateInTreeInlineVolumeToCSI takes a Volume with AzureFile set from in-tree
// and converts the AzureFile source to a CSIPersistentVolumeSource
// TranslateInTreeInlineVolumeToCSI 把 Pod 中内联的 AzureFile 卷转换为等价的 CSI PV。
func (t *azureFileCSITranslator) TranslateInTreeInlineVolumeToCSI(logger klog.Logger, volume *v1.Volume, podNamespace string) (*v1.PersistentVolume, error) {
	// 内联卷必须显式带有 AzureFile 来源，否则无法翻译。
	if volume == nil || volume.AzureFile == nil {
		return nil, fmt.Errorf("volume is nil or Azure File not defined on volume")
	}

	azureSource := volume.AzureFile
	// 优先从 Secret 名称中解析存储账户名，以生成包含账户信息的 volume handle。
	accountName, err := getStorageAccountName(azureSource.SecretName)
	if err != nil {
		// 名称不符合默认模板时退化为使用原始 Secret 名，保证翻译仍可进行。
		logger.V(5).Info("getStorageAccountName returned with error", "secretName", azureSource.SecretName, "err", err)
		accountName = azureSource.SecretName
	}

	// 内联卷没有 SecretNamespace 字段，默认落到 default，Pod 有命名空间时改用 Pod 的命名空间。
	secretNamespace := defaultSecretNamespace
	if podNamespace != "" {
		secretNamespace = podNamespace
	}
	// 资源组在内联卷中无从得知，留空；最终 handle 形如 "#account#share#volume#namespace"。
	volumeID := fmt.Sprintf(volumeIDTemplate, "", accountName, azureSource.ShareName, volume.Name, secretNamespace)

	// 构造一个仅存在于内存中的 PV：名字必须唯一，因为它会作为 staging 路径的一部分。
	var (
		pv = &v1.PersistentVolume{
			ObjectMeta: metav1.ObjectMeta{
				// Must be unique as it is used as the unique part of the staging path
				// 直接用 volume handle 作为名字，保证同一个卷的 staging 路径稳定且唯一。
				Name: volumeID,
			},
			Spec: v1.PersistentVolumeSpec{
				PersistentVolumeSource: v1.PersistentVolumeSource{
					CSI: &v1.CSIPersistentVolumeSource{
						// 驱动名、handle、只读标志和共享名是 Azure File CSI 插件需要的核心参数。
						Driver:           AzureFileDriverName,
						VolumeHandle:     volumeID,
						ReadOnly:         azureSource.ReadOnly,
						VolumeAttributes: map[string]string{shareNameField: azureSource.ShareName},
						// 挂载时需要 Secret 提供存储账户凭据，故通过 NodeStageSecretRef 传递。
						NodeStageSecretRef: &v1.SecretReference{
							// Secret 名称沿用 in-tree 的配置，命名空间取上面算出的结果。
							Name:      azureSource.SecretName,
							Namespace: secretNamespace,
						},
					},
				},
				// Azure Files 支持多节点读写，因此固定使用 ReadWriteMany。
				AccessModes: []v1.PersistentVolumeAccessMode{v1.ReadWriteMany},
			},
		}
	)

	// 返回构造好的内存 PV，不落盘。
	return pv, nil
}

// TranslateInTreePVToCSI takes a PV with AzureFile set from in-tree
// and converts the AzureFile source to a CSIPersistentVolumeSource
// TranslateInTreePVToCSI 把树内 Azure File PV 原地改造成 CSI PV。
func (t *azureFileCSITranslator) TranslateInTreePVToCSI(logger klog.Logger, pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	// 只接受带有 AzureFile 来源的 PV。
	if pv == nil || pv.Spec.AzureFile == nil {
		return nil, fmt.Errorf("pv is nil or Azure File source not defined on pv")
	}

	azureSource := pv.Spec.PersistentVolumeSource.AzureFile
	// 从 Secret 名称里提取存储账户名；解析失败时保留原字符串，避免丢失信息。
	accountName, err := getStorageAccountName(azureSource.SecretName)
	if err != nil {
		logger.V(5).Info("getStorageAccountName returned with error", "secretName", azureSource.SecretName, "err", err)
		accountName = azureSource.SecretName
	}
	// 资源组不在 AzureFile 来源里，只能从 PV 的 annotation 中恢复。
	resourceGroup := ""
	if pv.ObjectMeta.Annotations != nil {
		if v, ok := pv.ObjectMeta.Annotations[resourceGroupAnnotation]; ok {
			resourceGroup = v
		}
	}

	// Secret is required when mounting a volume but pod presence cannot be assumed - we should not try to read pod now.
	// 挂载阶段需要知道 Secret 所在的命名空间，但此时不能假设 Pod 已存在。
	namespace := ""
	// Try to read SecretNamespace from source pv.
	// 优先使用 PV 来源中显式声明的 SecretNamespace。
	if azureSource.SecretNamespace != nil {
		namespace = *azureSource.SecretNamespace
	} else {
		// Try to read namespace from ClaimRef which should be always present.
		// 否则回退到 ClaimRef 的命名空间（静态制备的 PV 通常都有 ClaimRef）。
		if pv.Spec.ClaimRef != nil {
			namespace = pv.Spec.ClaimRef.Namespace
		}
	}

	// 两处都拿不到命名空间就只能报错，否则 CSI 插件无法找到凭据。
	if len(namespace) == 0 {
		return nil, fmt.Errorf("could not find a secret namespace in PersistentVolumeSource or ClaimRef")
	}

	// 把资源组、账户、共享名、PV 名和命名空间拼成驱动可识别的 volume handle。
	volumeID := fmt.Sprintf(volumeIDTemplate, resourceGroup, accountName, azureSource.ShareName, pv.ObjectMeta.Name, namespace)

	var (
		// refer to https://github.com/kubernetes-sigs/azurefile-csi-driver/blob/master/docs/driver-parameters.md
		// 生成 CSI 来源：驱动名与 node stage 阶段使用的 Secret 引用。
		csiSource = &v1.CSIPersistentVolumeSource{
			Driver: AzureFileDriverName,
			// Secret 名称与命名空间直接沿用 in-tree 的配置。
			NodeStageSecretRef: &v1.SecretReference{
				// 这里沿用原来的 Secret 名，命名空间用上面解析出的结果。
				Name:      azureSource.SecretName,
				Namespace: namespace,
			},
			// 只读标志、VolumeAttributes 和 handle 完成剩余字段的映射。
			ReadOnly:         azureSource.ReadOnly,
			VolumeAttributes: map[string]string{shareNameField: azureSource.ShareName},
			VolumeHandle:     volumeID,
		}
	)

	// 先清除旧来源再挂上 CSI 来源，保证同一时刻只有一个 PersistentVolumeSource。
	pv.Spec.PersistentVolumeSource.AzureFile = nil
	pv.Spec.PersistentVolumeSource.CSI = csiSource

	return pv, nil
}

// TranslateCSIPVToInTree takes a PV with CSIPersistentVolumeSource set and
// translates the Azure File CSI source to a AzureFile source.
// TranslateCSIPVToInTree 是反向翻译：把 CSI Azure File PV 还原成树内 AzureFile PV，用于回滚。
func (t *azureFileCSITranslator) TranslateCSIPVToInTree(pv *v1.PersistentVolume) (*v1.PersistentVolume, error) {
	// 必须是 CSI 来源的 PV。
	if pv == nil || pv.Spec.CSI == nil {
		return nil, fmt.Errorf("pv is nil or CSI source not defined on pv")
	}
	csiSource := pv.Spec.CSI

	// refer to https://github.com/kubernetes-sigs/azurefile-csi-driver/blob/master/docs/driver-parameters.md
	// 先建立空的 AzureFile 来源，只读标志直接继承。
	azureSource := &v1.AzureFilePersistentVolumeSource{
		ReadOnly: csiSource.ReadOnly,
	}

	// CSI 侧把共享名等信息放在 VolumeAttributes 中；键名大小写不统一，故统一转小写后匹配。
	for k, v := range csiSource.VolumeAttributes {
		switch strings.ToLower(k) {
		case shareNameField:
			azureSource.ShareName = v
		case secretNameField:
			azureSource.SecretName = v
		case secretNamespaceField:
			// 命名空间在树内结构中是指针类型，需要单独取地址。
			ns := v
			azureSource.SecretNamespace = &ns
		}
	}

	// Secret 引用（若存在）比 VolumeAttributes 更权威，直接用它覆盖 Secret 名与命名空间。
	resourceGroup := ""
	if csiSource.NodeStageSecretRef != nil && csiSource.NodeStageSecretRef.Name != "" {
		azureSource.SecretName = csiSource.NodeStageSecretRef.Name
		azureSource.SecretNamespace = &csiSource.NodeStageSecretRef.Namespace
	}
	// 共享名或 Secret 名缺失时，只能从 volume handle 中反解；同时可恢复资源组。
	if azureSource.ShareName == "" || azureSource.SecretName == "" {
		rg, storageAccount, fileShareName, _, err := getFileShareInfo(csiSource.VolumeHandle)
		if err != nil {
			return nil, err
		}
		// 只补缺失的字段，已有的显式配置优先。
		if azureSource.ShareName == "" {
			azureSource.ShareName = fileShareName
		}
		if azureSource.SecretName == "" {
			azureSource.SecretName = fmt.Sprintf(secretNameTemplate, storageAccount)
		}
		resourceGroup = rg
	}

	// 树内插件要求 SecretNamespace 非空，缺失时用 default 兜底。
	if azureSource.SecretNamespace == nil {
		ns := defaultSecretNamespace
		azureSource.SecretNamespace = &ns
	}

	// 切换来源字段。
	pv.Spec.CSI = nil
	pv.Spec.AzureFile = azureSource
	// 资源组信息在树内模型中放在 annotation 里，不存在时先初始化 map。
	if pv.ObjectMeta.Annotations == nil {
		pv.ObjectMeta.Annotations = map[string]string{}
	}
	if resourceGroup != "" {
		pv.ObjectMeta.Annotations[resourceGroupAnnotation] = resourceGroup
	}

	return pv, nil
}

// CanSupport tests whether the plugin supports a given volume
// specification from the API.  The spec pointer should be considered
// const.
// CanSupport 判断该 PV 是否为树内 Azure File 卷。
func (t *azureFileCSITranslator) CanSupport(pv *v1.PersistentVolume) bool {
	return pv != nil && pv.Spec.AzureFile != nil
}

// CanSupportInline tests whether the plugin supports a given inline volume
// specification from the API.  The spec pointer should be considered
// const.
// CanSupportInline 判断内联 Volume 是否为 Azure File 类型。
func (t *azureFileCSITranslator) CanSupportInline(volume *v1.Volume) bool {
	return volume != nil && volume.AzureFile != nil
}

// GetInTreePluginName returns the name of the intree plugin driver
// GetInTreePluginName 返回树内插件名称。
func (t *azureFileCSITranslator) GetInTreePluginName() string {
	return AzureFileInTreePluginName
}

// GetCSIPluginName returns the name of the CSI plugin
// GetCSIPluginName 返回 CSI 驱动名称。
func (t *azureFileCSITranslator) GetCSIPluginName() string {
	return AzureFileDriverName
}

// RepairVolumeHandle 原样返回 volume handle；Azure File 的 handle 不含需要从节点推断的字段。
func (t *azureFileCSITranslator) RepairVolumeHandle(volumeHandle, nodeID string) (string, error) {
	return volumeHandle, nil
}

// get file share info according to volume id, e.g.
// input: "rg#f5713de20cde511e8ba4900#pvc-file-dynamic-17e43f84-f474-11e8-acd0-000d3a00df41#diskname.vhd"
// output: rg, f5713de20cde511e8ba4900, pvc-file-dynamic-17e43f84-f474-11e8-acd0-000d3a00df41, diskname.vhd
// getFileShareInfo 按 # 拆分 volume handle，提取资源组、存储账户、共享名和可选的磁盘名。
func getFileShareInfo(id string) (string, string, string, string, error) {
	segments := strings.Split(id, separator)
	// 至少需要三段（资源组、账户、共享名）才能满足反向翻译需要。
	if len(segments) < 3 {
		return "", "", "", "", fmt.Errorf("error parsing volume id: %q, should at least contain two #", id)
	}
	var diskName string
	// 第四段是可选的磁盘名，缺失时返回空字符串。
	if len(segments) > 3 {
		diskName = segments[3]
	}
	return segments[0], segments[1], segments[2], diskName, nil
}

// get storage account name from secret name
// getStorageAccountName 从形如 azure-storage-account-<账户>-secret 的名称中提取存储账户名。
func getStorageAccountName(secretName string) (string, error) {
	matches := secretNameFormatRE.FindStringSubmatch(secretName)
	// 必须恰好匹配到一个捕获组，否则说明不是默认的 Secret 命名格式。
	if len(matches) != 2 {
		return "", fmt.Errorf("could not get account name from %s, correct format: %s", secretName, secretNameFormatRE)
	}
	return matches[1], nil
}
