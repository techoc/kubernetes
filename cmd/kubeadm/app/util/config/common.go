/*
Copyright 2018 The Kubernetes Authors.

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

// Package config 包含管理 kubeadm 配置 API 的工具。
package config

import (
	"bytes"
	"fmt"
	"net"
	"reflect"
	"strings"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	netutil "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/version"
	apimachineryversion "k8s.io/apimachinery/pkg/version"
	componentversion "k8s.io/component-base/version"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmscheme "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/scheme"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	"k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/componentconfigs"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// LoadOrDefaultConfigurationOptions 保存 LoadOrDefaultConfiguration 的通用选项。
type LoadOrDefaultConfigurationOptions struct {
	// AllowExperimental 表示是否允许使用实验性或仍在开发中的 API。
	AllowExperimental bool
	// SkipCRIDetect 表示未提供 CRI socket 时是否跳过 CRI socket 检测。
	SkipCRIDetect bool
}

// MarshalKubeadmConfigObject 序列化 kubeadm scheme 中注册的对象。
// 对象为 InitConfiguration 或 ClusterConfiguration 时执行额外处理。
func MarshalKubeadmConfigObject(obj runtime.Object, gv schema.GroupVersion) ([]byte, error) {
	switch internalcfg := obj.(type) {
	case *kubeadmapi.InitConfiguration:
		return MarshalInitConfigurationToBytes(internalcfg, gv)
	default:
		return kubeadmutil.MarshalToYamlForCodecs(obj, gv, kubeadmscheme.Codecs)
	}
}

// validateSupportedVersion 检查给定 GroupVersion 是否属于已不支持或已弃用的旧 GV；属于则返回错误。
func validateSupportedVersion(gvk schema.GroupVersionKind, allowDeprecated, allowExperimental bool) error {
	// 当前及未来的支持矩阵如下：
	// v1.10 及更早：v1alpha1
	// v1.11：v1alpha1 只读，仅写入 v1alpha2 配置
	// v1.12：v1alpha2 只读，仅写入 v1alpha3 配置；使用 v1alpha1 报错
	// v1.13：v1alpha3 只读，仅写入 v1beta1 配置；使用 v1alpha1 或 v1alpha2 报错
	// v1.14：v1alpha3 仅转换，仅写入 v1beta1 配置；使用 v1alpha1 或 v1alpha2 报错
	// v1.15：v1beta1 只读，仅写入 v1beta2 配置；使用 v1alpha1、v1alpha2 或 v1alpha3 报错
	// v1.22：v1beta2 只读，仅写入 v1beta3 配置；使用 v1beta1 及更早版本报错
	// v1.27：仅支持 v1beta3；使用 v1beta2 及更早版本报错
	// v1.31：v1beta3 只读，仅写入 v1beta4；使用更早 API 报错
	// v1.37：仅支持 v1beta4；使用 v1beta3 及更早版本报错
	oldKnownAPIVersions := map[string]string{
		"kubeadm.k8s.io/v1alpha1": "v1.11",
		"kubeadm.k8s.io/v1alpha2": "v1.12",
		"kubeadm.k8s.io/v1alpha3": "v1.14",
		"kubeadm.k8s.io/v1beta1":  "v1.15",
		"kubeadm.k8s.io/v1beta2":  "v1.22",
		"kubeadm.k8s.io/v1beta3":  "v1.36",
	}

	// 实验性 API 在正式发布前保留在此处，仅在明确允许时使用。
	experimentalAPIVersions := map[string]struct{}{
		"kubeadm.k8s.io/v1": {},
	}

	// 已弃用 API 在移除前仍受支持，但会发出警告。
	deprecatedAPIVersions := map[string]struct{}{}

	gvString := gvk.GroupVersion().String()

	if useKubeadmVersion := oldKnownAPIVersions[gvString]; useKubeadmVersion != "" {
		return errors.Errorf("your configuration file uses an old API spec: %q (kind: %q). Please use kubeadm %s instead and run 'kubeadm config migrate --old-config old-config-file --new-config new-config-file', which will write the new, similar spec using a newer API version.", gvString, gvk.Kind, useKubeadmVersion)
	}

	if _, present := deprecatedAPIVersions[gvString]; present && !allowDeprecated {
		klog.Warningf("your configuration file uses a deprecated API spec: %q (kind: %q). Please use 'kubeadm config migrate --old-config old-config-file --new-config new-config-file', which will write the new, similar spec using a newer API version.", gvString, gvk.Kind)
	}

	if _, present := experimentalAPIVersions[gvString]; present && !allowExperimental {
		return errors.Errorf("experimental API spec: %q (kind: %q) is not allowed. You can use the --%s flag if the command supports it.", gvString, gvk.Kind, options.AllowExperimentalAPI)
	}

	return nil
}

// NormalizeKubernetesVersion 解析版本标签，按 CI 构建要求设置备用镜像仓库，
// 并验证版本不低于 kubeadm SetInitDynamicDefaults 支持的最低版本。
func NormalizeKubernetesVersion(cfg *kubeadmapi.ClusterConfiguration) error {
	isCIVersion := kubeadmutil.KubernetesIsCIVersion(cfg.KubernetesVersion)

	// 请求的是自动 CI 构建，因此核心镜像使用 KubernetesCI Image Repository。
	if isCIVersion && cfg.ImageRepository == kubeadmapiv1.DefaultImageRepository {
		cfg.CIImageRepository = constants.DefaultCIImageRepository
	}

	// 解析并验证版本参数，同时解析可能的 CI 版本标签。
	ver, err := kubeadmutil.KubernetesReleaseVersion(cfg.KubernetesVersion)
	if err != nil {
		return err
	}

	// 请求的是自动 CI 构建，因此将 CIKubernetesVersion 标记为 `ci/<resolved-version>`。
	if isCIVersion {
		cfg.CIKubernetesVersion = fmt.Sprintf("%s%s", constants.CIKubernetesVersionPrefix, ver)
	}

	cfg.KubernetesVersion = ver

	// 解析给定的 Kubernetes 版本，并确保其高于最低支持版本。
	k8sVersion, err := version.ParseSemantic(cfg.KubernetesVersion)
	if err != nil {
		return errors.Wrapf(err, "couldn't parse Kubernetes version %q", cfg.KubernetesVersion)
	}

	// Kubernetes 发布期间，主分支中的 kubeadm 版本可能已是 1.23.0-pre，而 1.22.0 尚未发布。
	// 此时 MinimumControlPlaneVersion 按 kubeadm 版本减 1 计算为 1.22，但 k8sVersion 仍为从
	// "stable" 标记获取的 1.21.x，因而无法通过校验。此情形仅显示警告。
	mcpVersion := constants.MinimumControlPlaneVersion
	versionInfo := componentversion.Get()
	if isKubeadmPrereleaseVersion(&versionInfo, k8sVersion, mcpVersion) {
		klog.V(1).Infof("WARNING: tolerating control plane version %s as a pre-release version", cfg.KubernetesVersion)

		return nil
	}
	// 非预发布版本按正常流程校验。
	if k8sVersion.LessThan(mcpVersion) {
		return errors.Errorf("this version of kubeadm only supports deploying clusters with the control plane version >= %s. Current version: %s",
			mcpVersion, cfg.KubernetesVersion)
	}
	return nil
}

// LowercaseSANs 可强制将所有 SAN 转为小写，以通过 IsDNS1123Subdomain 校验。
func LowercaseSANs(sans []string) {
	for i, san := range sans {
		lowercase := strings.ToLower(san)
		if lowercase != san {
			klog.V(1).Infof("lowercasing SAN %q to %q", san, lowercase)
			sans[i] = lowercase
		}
	}
}

// VerifyAPIServerBindAddress 检查 API 服务器的绑定地址是否为 0.0.0.0；该地址无效，不应使用。
func VerifyAPIServerBindAddress(address string) error {
	ip := netutils.ParseIPSloppy(address)
	if ip == nil {
		return errors.Errorf("cannot parse IP address: %s", address)
	}
	// 某些网络虽有默认路由，但接口仅使用链路本地地址（如 RFC5549 所述）。
	// 回环接口上通常能找到匹配的全局单播地址，因此 kubeadm 允许指定 address=Loopback。
	// 注意：SetAPIEndpointDynamicDefaults 随后会将 loopback 转换为有效地址。
	if ip.IsLoopback() {
		return nil
	}
	if !ip.IsGlobalUnicast() {
		return errors.Errorf("cannot use %q as the bind address for the API Server", address)
	}
	return nil
}

// ChooseAPIServerBindAddress 包装 netutil.ResolveBindAddress，
// 并处理找不到默认路由、无法取得 API 服务器 IP 的情况。
func ChooseAPIServerBindAddress(bindAddress net.IP) (net.IP, error) {
	ip, err := netutil.ResolveBindAddress(bindAddress)
	if err != nil {
		if netutil.IsNoRoutesError(err) {
			klog.Warningf("WARNING: could not obtain a bind address for the API Server: %v; using: %s", err, constants.DefaultAPIServerBindAddress)
			defaultIP := netutils.ParseIPSloppy(constants.DefaultAPIServerBindAddress)
			if defaultIP == nil {
				return nil, errors.Errorf("cannot parse default IP address: %s", constants.DefaultAPIServerBindAddress)
			}
			return defaultIP, nil
		}
		return nil, err
	}
	if bindAddress != nil && !bindAddress.IsUnspecified() && !reflect.DeepEqual(ip, bindAddress) {
		klog.Warningf("WARNING: overriding requested API server bind address: requested %q, actual %q", bindAddress, ip)
	}
	return ip, nil
}

// validateKnownGVKs 检查给定 GVK 列表是否已注册在 kubeadm 或组件配置 scheme 中。
func validateKnownGVKs(gvks []schema.GroupVersionKind) error {
	var unknown []schema.GroupVersionKind

	schemes := []*runtime.Scheme{
		kubeadmscheme.Scheme,
		componentconfigs.Scheme,
	}

	for _, gvk := range gvks {
		var scheme *runtime.Scheme

		// 跳过已知的旧 GV，避免其触发错误；函数仅对从未注册过的 GV 返回错误。
		if err := validateSupportedVersion(gvk, true, true); err != nil {
			continue
		}

		for _, s := range schemes {
			if _, err := s.New(gvk); err == nil {
				scheme = s
				break
			}
		}
		if scheme == nil {
			unknown = append(unknown, gvk)
		}
	}

	if len(unknown) > 0 {
		return errors.Errorf("unknown configuration APIs: %#v", unknown)
	}

	return nil
}

// MigrateOldConfig 将旧配置字节切片迁移为新配置字节切片，仅迁移 kubeadm 类型。
func MigrateOldConfig(oldConfig []byte, allowExperimental bool, mutators migrateMutators) ([]byte, error) {
	newConfig := [][]byte{}

	if mutators == nil {
		mutators = defaultMigrateMutators()
	}

	gvkmap, err := kubeadmutil.SplitConfigDocuments(oldConfig)
	if err != nil {
		return []byte{}, err
	}

	gvks := []schema.GroupVersionKind{}
	for gvk := range gvkmap {
		gvks = append(gvks, gvk)
	}

	if err := validateKnownGVKs(gvks); err != nil {
		return []byte{}, err
	}

	gv := kubeadmapiv1.SchemeGroupVersion
	// 必要时将 GV 更新为实验性版本。
	if allowExperimental {
		gv = kubeadmapiv1.SchemeGroupVersion
	}
	// 若配置中存在 InitConfiguration 和 ClusterConfiguration，则迁移它们。
	if kubeadmutil.GroupVersionKindsHasInitConfiguration(gvks...) || kubeadmutil.GroupVersionKindsHasClusterConfiguration(gvks...) {
		o, err := documentMapToInitConfiguration(gvkmap, true, allowExperimental, true, false)
		if err != nil {
			return []byte{}, err
		}
		if err := mutators.mutate([]any{o}); err != nil {
			return []byte{}, err
		}
		b, err := MarshalKubeadmConfigObject(o, gv)
		if err != nil {
			return []byte{}, err
		}
		newConfig = append(newConfig, b)
	}

	// 若存在 JoinConfiguration，则迁移它。
	if kubeadmutil.GroupVersionKindsHasJoinConfiguration(gvks...) {
		o, err := documentMapToJoinConfiguration(gvkmap, true, allowExperimental, true, false)
		if err != nil {
			return []byte{}, err
		}
		if err := mutators.mutate([]any{o}); err != nil {
			return []byte{}, err
		}
		b, err := MarshalKubeadmConfigObject(o, gv)
		if err != nil {
			return []byte{}, err
		}
		newConfig = append(newConfig, b)
	}

	// 若存在 ResetConfiguration，则迁移它。
	if kubeadmutil.GroupVersionKindsHasResetConfiguration(gvks...) {
		o, err := documentMapToResetConfiguration(gvkmap, true, allowExperimental, true, false)
		if err != nil {
			return []byte{}, err
		}
		if err := mutators.mutate([]any{o}); err != nil {
			return []byte{}, err
		}
		b, err := MarshalKubeadmConfigObject(o, gv)
		if err != nil {
			return []byte{}, err
		}
		newConfig = append(newConfig, b)
	}

	// 若存在 UpgradeConfiguration，则迁移它。
	if kubeadmutil.GroupVersionKindsHasUpgradeConfiguration(gvks...) {
		o, err := documentMapToUpgradeConfiguration(gvkmap, true, allowExperimental, true)
		if err != nil {
			return []byte{}, err
		}
		if err := mutators.mutate([]any{o}); err != nil {
			return []byte{}, err
		}
		b, err := MarshalKubeadmConfigObject(o, gv)
		if err != nil {
			return []byte{}, err
		}
		newConfig = append(newConfig, b)
	}

	return bytes.Join(newConfig, []byte(constants.YAMLDocumentSeparator)), nil
}

// ValidateConfig 接收 kubeadm 配置字节切片，转换为内部类型并校验。
func ValidateConfig(config []byte, allowDeprecated, allowExperimental bool) error {
	gvkmap, err := kubeadmutil.SplitConfigDocuments(config)
	if err != nil {
		return err
	}

	gvks := []schema.GroupVersionKind{}
	for gvk := range gvkmap {
		gvks = append(gvks, gvk)
	}

	if err := validateKnownGVKs(gvks); err != nil {
		return err
	}

	// 若配置中存在 InitConfiguration 和 ClusterConfiguration，则校验它们。
	if kubeadmutil.GroupVersionKindsHasInitConfiguration(gvks...) || kubeadmutil.GroupVersionKindsHasClusterConfiguration(gvks...) {
		if _, err := documentMapToInitConfiguration(gvkmap, allowDeprecated, allowExperimental, true, true); err != nil {
			return err
		}
	}

	// 若存在 JoinConfiguration，则校验它。
	if kubeadmutil.GroupVersionKindsHasJoinConfiguration(gvks...) {
		if _, err := documentMapToJoinConfiguration(gvkmap, allowDeprecated, allowExperimental, true, true); err != nil {
			return err
		}
	}

	// 若存在 ResetConfiguration，则校验它。
	if kubeadmutil.GroupVersionKindsHasResetConfiguration(gvks...) {
		if _, err := documentMapToResetConfiguration(gvkmap, allowDeprecated, allowExperimental, true, true); err != nil {
			return err
		}
	}

	// 若存在 UpgradeConfiguration，则校验它。
	if kubeadmutil.GroupVersionKindsHasUpgradeConfiguration(gvks...) {
		if _, err := documentMapToUpgradeConfiguration(gvkmap, allowDeprecated, allowExperimental, true); err != nil {
			return err
		}
	}

	return nil
}

// isKubeadmPrereleaseVersion 判断 kubeadm 是否为预发布版本，且最低控制平面版本是否为给定
// k8sVersion 的 N+2 MINOR 版本。
func isKubeadmPrereleaseVersion(versionInfo *apimachineryversion.Info, k8sVersion, mcpVersion *version.Version) bool {
	if len(versionInfo.Major) != 0 { // if len(versionInfo.Major) != 0 { // 确保组件版本已填充
		kubeadmVersion := version.MustParseSemantic(versionInfo.String())
		if len(kubeadmVersion.PreRelease()) != 0 { // if len(kubeadmVersion.PreRelease()) != 0 { // 仅处理 kubeadm 二进制为预发布版本的情况
			// 将 k8s MINOR 版本加一后，若不早于 MCP 版本则返回 true。
			v := k8sVersion.WithMinor(k8sVersion.Minor() + 1)
			if comp, _ := v.Compare(mcpVersion.String()); comp != -1 {
				return true
			}
		}
	}
	return false
}

// prepareStaticVariables 从给定配置提取值并保存，供多个包使用。
func prepareStaticVariables(config any) {
	switch c := config.(type) {
	case *kubeadmapi.InitConfiguration:
		kubeadmapi.SetActiveTimeouts(c.Timeouts)
	case *kubeadmapi.JoinConfiguration:
		kubeadmapi.SetActiveTimeouts(c.Timeouts)
	case *kubeadmapi.ResetConfiguration:
		kubeadmapi.SetActiveTimeouts(c.Timeouts)
	case *kubeadmapi.UpgradeConfiguration:
		kubeadmapi.SetActiveTimeouts(c.Timeouts)
	}
}

// migrateMutator 可修改配置对象切片；修改原地执行，不复制对象。
type migrateMutator struct {
	in         []any
	mutateFunc func(in []any) error
}

// migrateMutators 保存已注册的 mutator。
type migrateMutators []migrateMutator

// mutate 在已注册的 mutator 中查找合适的实现来修改配置对象。
func (mutators migrateMutators) mutate(in []any) error {
	var mutator *migrateMutator
	for idx, m := range mutators {
		if len(m.in) != len(in) {
			continue
		}
		inputMatch := true
		for idx := range m.in {
			if reflect.TypeOf(m.in[idx]) != reflect.TypeOf(in[idx]) {
				inputMatch = false
				break
			}
		}
		if inputMatch {
			mutator = &mutators[idx]
			break
		}
	}
	if mutator == nil {
		return errors.Errorf("could not find a mutator for input: %#v", in)
	}
	return mutator.mutateFunc(in)
}

// addEmpty 为给定输入添加空的迁移 mutator。
func (mutators *migrateMutators) addEmpty(in []any) {
	mutator := migrateMutator{
		in:         in,
		mutateFunc: func(in []any) error { return nil },
	}
	*mutators = append(*mutators, mutator)
}

// defaultMutators 返回已知配置对象的默认 mutator 列表。
// 迁移期间需要修改的字段必须在此定义。
func defaultMigrateMutators() migrateMutators {
	return defaultEmptyMigrateMutators()
}

// defaultEmptyMigrateMutators 返回已知类型的空 mutator 列表。
func defaultEmptyMigrateMutators() migrateMutators {
	mutators := &migrateMutators{}

	mutators.addEmpty([]any{(*kubeadmapi.InitConfiguration)(nil)})
	mutators.addEmpty([]any{(*kubeadmapi.JoinConfiguration)(nil)})
	mutators.addEmpty([]any{(*kubeadmapi.ResetConfiguration)(nil)})
	mutators.addEmpty([]any{(*kubeadmapi.UpgradeConfiguration)(nil)})

	return *mutators
}
