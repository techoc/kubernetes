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

package componentconfigs

import (
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/validation/field"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	outputapiv1alpha3 "k8s.io/kubernetes/cmd/kubeadm/app/apis/output/v1alpha3"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/config/strict"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// handler 是包内类型，负责组件配置工厂及通用功能。
// 每个组件配置组应恰好拥有一个 handler 静态实例。
type handler struct {
	// GroupVersion 保存该处理器的组名和首选版本。
	GroupVersion schema.GroupVersion

	// AddToScheme 指向将 GV 类型添加到 Scheme 的函数。
	AddToScheme func(*runtime.Scheme) error

	// CreateEmpty 返回未进行任何默认化处理的空 kubeadmapi.ComponentConfig。
	CreateEmpty func() kubeadmapi.ComponentConfig

	// fromCluster 应从集群中的 ConfigMap 加载组件配置。
	// 不要直接调用，请使用 FromCluster。
	fromCluster func(*handler, clientset.Interface, *kubeadmapi.ClusterConfiguration) (kubeadmapi.ComponentConfig, error)
}

// FromDocumentMap 在文档映射中查找属于该处理器组的文档。
// 找到后创建新的组件配置并加载这些文档。
// 未找到文档时不返回错误。
func (h *handler) FromDocumentMap(docmap kubeadmapi.DocumentMap) (kubeadmapi.ComponentConfig, error) {
	for gvk := range docmap {
		if gvk.Group == h.GroupVersion.Group {
			cfg := h.CreateEmpty()
			if err := cfg.Unmarshal(docmap); err != nil {
				return nil, err
			}
			// 将从文档映射成功加载的所有配置视为用户提供。
			cfg.SetUserSupplied(true)
			return cfg, nil
		}
	}
	return nil, nil
}

// fromConfigMap 是工具函数，加载 ConfigMap 中某个键的值，并通过 h.FromDocumentMap() 完成解析。
// 仅供组件配置支持实现使用，不要在该场景之外调用。
func (h *handler) fromConfigMap(client clientset.Interface, cmName, cmKey string, mustExist bool) (kubeadmapi.ComponentConfig, error) {
	configMap, err := apiclient.GetConfigMapWithShortRetry(client, metav1.NamespaceSystem, cmName)
	if err != nil {
		if !mustExist && (apierrors.IsNotFound(err) || apierrors.IsForbidden(err)) {
			klog.Warningf("Warning: No %s config is loaded. Continuing without it: %v", h.GroupVersion, err)
			return nil, nil
		}
		return nil, err
	}

	configData, ok := configMap.Data[cmKey]
	if !ok {
		return nil, errors.Errorf("unexpected error when reading %s ConfigMap: %s key value pair missing", cmName, cmKey)
	}

	gvkmap, err := kubeadmutil.SplitConfigDocuments([]byte(configData))
	if err != nil {
		return nil, err
	}

	// 校验和匹配时，假定配置由 kubeadm 生成。
	generatedConfig := VerifyConfigMapSignature(configMap)

	componentCfg, err := h.FromDocumentMap(gvkmap)
	if err != nil {
		// 如果配置由 kubeadm 生成且遇到 UnsupportedConfigVersionError，则跳过加载。
		// 这会强制使用当前版本的生成默认值，相当于用当前版本重新生成配置。
		if _, ok := err.(*UnsupportedConfigVersionError); ok && generatedConfig {
			return nil, nil
		}
		return nil, err
	}

	if componentCfg != nil {
		componentCfg.SetUserSupplied(!generatedConfig)
	}

	return componentCfg, nil
}

// FromCluster 从集群中的 ConfigMap 加载组件配置。
func (h *handler) FromCluster(clientset clientset.Interface, clusterCfg *kubeadmapi.ClusterConfiguration) (kubeadmapi.ComponentConfig, error) {
	return h.fromCluster(h, clientset, clusterCfg)
}

// known 保存已知的组件配置处理器；新增组件配置时在此添加。
var known = []*handler{
	&kubeProxyHandler,
	&kubeletHandler,
}

// configBase 是所有组件配置实现的基础类型。
type configBase struct {
	// GroupVersion 保存继承配置支持的 GroupVersion。
	GroupVersion schema.GroupVersion

	// userSupplied 表示配置是否由用户提供（校验和无效）。
	userSupplied bool
}

// IsUserSupplied 报告配置来源，供默认化和校验逻辑区分用户配置与 kubeadm 生成配置。
func (cb *configBase) IsUserSupplied() bool {
	return cb.userSupplied
}

// SetUserSupplied 更新来源标记；文档加载视为用户提供，集群配置按校验和判定。
func (cb *configBase) SetUserSupplied(userSupplied bool) {
	cb.userSupplied = userSupplied
}

// DeepCopyInto 复制基础配置状态；当前字段均为值类型。
func (cb *configBase) DeepCopyInto(other *configBase) {
	*other = *cb
}

func cloneBytes(in []byte) []byte {
	out := make([]byte, len(in))
	copy(out, in)
	return out
}

// Marshal 是工具函数，供组件配置支持实现使用，按正确的组和版本将 runtime.Object 序列化为 YAML。
func (cb *configBase) Marshal(object runtime.Object) ([]byte, error) {
	return kubeadmutil.MarshalToYamlForCodecs(object, cb.GroupVersion, Codecs)
}

// Unmarshal 尝试从文档映射中反序列化 runtime.Object；未找到对象时不返回错误。
// 如果组匹配但版本不匹配，则返回错误，提示用户手动转换。
func (cb *configBase) Unmarshal(from kubeadmapi.DocumentMap, into runtime.Object) error {
	for gvk, yaml := range from {
		// 组不同则忽略。
		if gvk.Group != cb.GroupVersion.Group {
			continue
		}

		if gvk.Version != cb.GroupVersion.Version {
			return &UnsupportedConfigVersionError{
				OldVersion:     gvk.GroupVersion(),
				CurrentVersion: cb.GroupVersion,
				Document:       cloneBytes(yaml),
			}
		}

		// 输出严格校验错误的警告。
		if err := strict.VerifyUnmarshalStrict([]*runtime.Scheme{Scheme}, gvk, yaml); err != nil {
			klog.Warning(err.Error())
		}

		// 由于每个组件配置仅支持单一 kind，此处可以直接解码。
		return runtime.DecodeInto(Codecs.UniversalDecoder(), yaml, into)
	}

	return nil
}

// ensureInitializedComponentConfigs 是工具函数，在可能写入 ClusterConfiguration 前初始化 ComponentConfigMap。
func ensureInitializedComponentConfigs(clusterCfg *kubeadmapi.ClusterConfiguration) {
	if clusterCfg.ComponentConfigs == nil {
		clusterCfg.ComponentConfigs = kubeadmapi.ComponentConfigMap{}
	}
}

// Default 在给定 ClusterConfiguration 中设置完成默认化的组件配置。
func Default(clusterCfg *kubeadmapi.ClusterConfiguration, localAPIEndpoint *kubeadmapi.APIEndpoint, nodeRegOpts *kubeadmapi.NodeRegistrationOptions) {
	ensureInitializedComponentConfigs(clusterCfg)

	for _, handler := range known {
		// 组件配置存在时直接默认化；否则先创建再默认化。
		group := handler.GroupVersion.Group
		if componentCfg, ok := clusterCfg.ComponentConfigs[group]; ok {
			componentCfg.Default(clusterCfg, localAPIEndpoint, nodeRegOpts)
		} else {
			componentCfg := handler.CreateEmpty()
			componentCfg.Default(clusterCfg, localAPIEndpoint, nodeRegOpts)
			clusterCfg.ComponentConfigs[group] = componentCfg
		}
	}
}

// FetchFromCluster 尝试从各自的 ConfigMap 获取所有已知组件配置，并存入给定 ClusterConfiguration。
func FetchFromCluster(clusterCfg *kubeadmapi.ClusterConfiguration, client clientset.Interface) error {
	ensureInitializedComponentConfigs(clusterCfg)

	for _, handler := range known {
		componentCfg, err := handler.FromCluster(client, clusterCfg)
		if err != nil {
			return err
		}

		if componentCfg != nil {
			clusterCfg.ComponentConfigs[handler.GroupVersion.Group] = componentCfg
		}
	}

	return nil
}

// FetchFromDocumentMap 尝试从文档映射加载所有已知组件配置，并存入给定 ClusterConfiguration。
func FetchFromDocumentMap(clusterCfg *kubeadmapi.ClusterConfiguration, docmap kubeadmapi.DocumentMap) error {
	ensureInitializedComponentConfigs(clusterCfg)

	for _, handler := range known {
		componentCfg, err := handler.FromDocumentMap(docmap)
		if err != nil {
			return err
		}

		if componentCfg != nil {
			clusterCfg.ComponentConfigs[handler.GroupVersion.Group] = componentCfg
		}
	}

	return nil
}

// GetVersionStates 返回 ComponentConfigVersionState 切片，
// 描述在集群中识别的所有受支持组件配置组。
func GetVersionStates(clusterCfg *kubeadmapi.ClusterConfiguration, client clientset.Interface) ([]outputapiv1alpha3.ComponentConfigVersionState, error) {
	// 不修改 clusterCfg，因此创建工作副本。
	// 同时移除已默认化的组件配置。
	scratchClusterCfg := clusterCfg.DeepCopy()
	scratchClusterCfg.ComponentConfigs = kubeadmapi.ComponentConfigMap{}

	err := FetchFromCluster(scratchClusterCfg, client)
	if err != nil {
		// 这似乎是实际错误，因此在此终止。
		return nil, err
	}

	results := []outputapiv1alpha3.ComponentConfigVersionState{}
	for _, handler := range known {
		group := handler.GroupVersion.Group
		if _, ok := scratchClusterCfg.ComponentConfigs[group]; ok {
			// 正常加载的组件配置，用户无需手动升级。
			results = append(results, outputapiv1alpha3.ComponentConfigVersionState{
				Group:            group,
				CurrentVersion:   handler.GroupVersion.Version, // kubeadm 目前每个 API 组仅支持一个版本。
				PreferredVersion: handler.GroupVersion.Version, // 因此两个版本可以相同。
			})
		} else {
			// 配置可能不存在（用户未安装插件），也可能是 kubeadm 生成但不受支持的配置，
			// 因而被跳过以便自动重新生成；用户无需执行任何操作。
			results = append(results, outputapiv1alpha3.ComponentConfigVersionState{
				Group:            group,
				PreferredVersion: handler.GroupVersion.Version,
			})
		}
	}

	return results, nil
}

// Validate 是校验 ClusterConfiguration 中已加载组件配置的占位函数。
// TODO：研究该函数能否改为通过 CLI 校验组件配置。
func Validate(clusterCfg *kubeadmapi.ClusterConfiguration) field.ErrorList {
	return field.ErrorList{}
}
