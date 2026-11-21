/*
Copyright 2021 The Kubernetes Authors.

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

package pod

import (
	"context"
	"fmt"
	"os"
	"strings"

	apiequality "k8s.io/apimachinery/pkg/api/equality"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	nodeapi "k8s.io/kubernetes/pkg/api/node"
	pvcutil "k8s.io/kubernetes/pkg/api/persistentvolumeclaim"
	api "k8s.io/kubernetes/pkg/apis/core"
	"k8s.io/kubernetes/pkg/apis/core/pods"
	"k8s.io/kubernetes/pkg/features"
)

// GetWarningsForPod 为 Pod 对象生成警告信息。
func GetWarningsForPod(ctx context.Context, pod, oldPod *api.Pod) []string {
	if pod == nil {
		return nil
	}

	// 顶层 Pod 没有字段路径前缀，传入 nil；更新场景拆出旧 spec/meta 供对比。
	var (
		oldSpec *api.PodSpec
		oldMeta *metav1.ObjectMeta
	)
	if oldPod != nil {
		oldSpec = &oldPod.Spec
		oldMeta = &oldPod.ObjectMeta
	}
	return warningsForPodSpecAndMeta(nil, &pod.Spec, &pod.ObjectMeta, oldSpec, oldMeta)
}

// GetWarningsForPodTemplate 为 PodTemplateSpec（被 Job/Deployment 等资源内嵌）生成警告。
// fieldPath 是模板在父资源中的路径，用于让警告指向正确的字段位置。
func GetWarningsForPodTemplate(ctx context.Context, fieldPath *field.Path, podTemplate, oldPodTemplate *api.PodTemplateSpec) []string {
	if podTemplate == nil {
		return nil
	}

	var (
		oldSpec *api.PodSpec
		oldMeta *metav1.ObjectMeta
	)
	if oldPodTemplate != nil {
		oldSpec = &oldPodTemplate.Spec
		oldMeta = &oldPodTemplate.ObjectMeta
	}
	return warningsForPodSpecAndMeta(fieldPath, &podTemplate.Spec, &podTemplate.ObjectMeta, oldSpec, oldMeta)
}

// deprecatedAnnotations 是 Pod 上已失效的注解及其替代字段说明。
var deprecatedAnnotations = []struct {
	key     string
	prefix  string
	message string
}{
	{
		key:     `scheduler.alpha.kubernetes.io/critical-pod`,
		message: `non-functional in v1.16+; use the "priorityClassName" field instead`,
	},
	{
		key:     `security.alpha.kubernetes.io/sysctls`,
		message: `non-functional in v1.11+; use the "sysctls" field instead`,
	},
	{
		key:     `security.alpha.kubernetes.io/unsafe-sysctls`,
		message: `non-functional in v1.11+; use the "sysctls" field instead`,
	},
}

// warningsForPodSpecAndMeta 执行 Pod 的所有警告检查。
// fieldPath 为 nil 表示顶层 Pod；非 nil 表示模板（如 Job 的 spec.template）。
// oldPodSpec/oldMeta 仅用于更新场景的对比。
func warningsForPodSpecAndMeta(fieldPath *field.Path, podSpec *api.PodSpec, meta *metav1.ObjectMeta, oldPodSpec *api.PodSpec, oldMeta *metav1.ObjectMeta) []string {
	var warnings []string

	// ---- 节点选择器 / 亲和性 / 拓扑分布中的弃用节点标签 ----
	for k := range podSpec.NodeSelector {
		if msg, deprecated := nodeapi.GetNodeLabelDeprecatedMessage(k); deprecated {
			warnings = append(warnings, fmt.Sprintf("%s: %s", fieldPath.Child("spec", "nodeSelector").Key(k), msg))
		}
	}
	if podSpec.Affinity != nil && podSpec.Affinity.NodeAffinity != nil {
		n := podSpec.Affinity.NodeAffinity
		// requiredDuringScheduling 的 nodeSelectorTerms：只检查 key 是否弃用。
		if n.RequiredDuringSchedulingIgnoredDuringExecution != nil {
			termFldPath := fieldPath.Child("spec", "affinity", "nodeAffinity", "requiredDuringSchedulingIgnoredDuringExecution", "nodeSelectorTerms")
			for i, term := range n.RequiredDuringSchedulingIgnoredDuringExecution.NodeSelectorTerms {
				warnings = append(warnings, nodeapi.GetWarningsForNodeSelectorTerm(term, false, termFldPath.Index(i))...)
			}
		}
		// preferredDuringScheduling 的 preference：同时检查 key 弃用和 value 合法性。
		preferredFldPath := fieldPath.Child("spec", "affinity", "nodeAffinity", "preferredDuringSchedulingIgnoredDuringExecution")
		for i, term := range n.PreferredDuringSchedulingIgnoredDuringExecution {
			warnings = append(warnings, nodeapi.GetWarningsForNodeSelectorTerm(term.Preference, true, preferredFldPath.Index(i).Child("preference"))...)
		}
	}
	for i, t := range podSpec.TopologySpreadConstraints {
		if msg, deprecated := nodeapi.GetNodeLabelDeprecatedMessage(t.TopologyKey); deprecated {
			warnings = append(warnings, fmt.Sprintf(
				"%s: %s is %s",
				fieldPath.Child("spec", "topologySpreadConstraints").Index(i).Child("topologyKey"),
				t.TopologyKey,
				msg,
			))
		}

		// labelSelector 为 nil 表示“不匹配任何 Pod”，通常是配置失误。
		if t.LabelSelector == nil {
			warnings = append(warnings, fmt.Sprintf("%s: a null labelSelector results in matching no pod", fieldPath.Child("spec", "topologySpreadConstraints").Index(i).Child("labelSelector")))
		}
	}

	// ---- 已弃用的注解 ----
	for _, deprecated := range deprecatedAnnotations {
		if _, exists := meta.Annotations[deprecated.key]; exists {
			warnings = append(warnings, fmt.Sprintf("%s: %s", fieldPath.Child("metadata", "annotations").Key(deprecated.key), deprecated.message))
		}
		if len(deprecated.prefix) > 0 {
			// 对带前缀的注解，同类注解只需报一次警告。
			for k := range meta.Annotations {
				if strings.HasPrefix(k, deprecated.prefix) {
					warnings = append(warnings, fmt.Sprintf("%s: %s", fieldPath.Child("metadata", "annotations").Key(k), deprecated.message))
					break
				}
			}
		}
	}

	// ---- 已弃用/移除的 volume 插件 ----
	for i, v := range podSpec.Volumes {
		if v.PhotonPersistentDisk != nil {
			warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.11, non-functional in v1.16+", fieldPath.Child("spec", "volumes").Index(i).Child("photonPersistentDisk")))
		}
		if v.GitRepo != nil {
			// GitRepo 卷已弃用，v1.33+ 默认禁用；根据特性开关给出不同提示。
			if !utilfeature.DefaultFeatureGate.Enabled(features.GitRepoVolumeDriver) {
				warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.11, and disabled by default in v1.33+", fieldPath.Child("spec", "volumes").Index(i).Child("gitRepo")))
			} else {
				warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.11", fieldPath.Child("spec", "volumes").Index(i).Child("gitRepo")))
			}
		}
		if v.ScaleIO != nil {
			warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.16, non-functional in v1.22+", fieldPath.Child("spec", "volumes").Index(i).Child("scaleIO")))
		}
		if v.Flocker != nil {
			warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.22, non-functional in v1.25+", fieldPath.Child("spec", "volumes").Index(i).Child("flocker")))
		}
		if v.StorageOS != nil {
			warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.22, non-functional in v1.25+", fieldPath.Child("spec", "volumes").Index(i).Child("storageOS")))
		}
		if v.Quobyte != nil {
			warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.22, non-functional in v1.25+", fieldPath.Child("spec", "volumes").Index(i).Child("quobyte")))
		}
		if v.Glusterfs != nil {
			warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.25, non-functional in v1.26+", fieldPath.Child("spec", "volumes").Index(i).Child("glusterfs")))
		}
		if v.Ephemeral != nil && v.Ephemeral.VolumeClaimTemplate != nil {
			// 临时卷里内嵌了 PVC 模板，复用 PVC 的存储容量检查。
			warnings = append(warnings, pvcutil.GetWarningsForPersistentVolumeClaimSpec(fieldPath.Child("spec", "volumes").Index(i).Child("ephemeral").Child("volumeClaimTemplate").Child("spec"), v.Ephemeral.VolumeClaimTemplate.Spec)...)
		}
		if v.CephFS != nil {
			warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.28, non-functional in v1.31+", fieldPath.Child("spec", "volumes").Index(i).Child("cephfs")))
		}
		if v.RBD != nil {
			warnings = append(warnings, fmt.Sprintf("%s: deprecated in v1.28, non-functional in v1.31+", fieldPath.Child("spec", "volumes").Index(i).Child("rbd")))
		}
	}

	// 卷里的 key 映射到重叠的虚拟路径会导致文件互相覆盖。
	if overlaps := warningsForOverlappingVirtualPaths(podSpec.Volumes); len(overlaps) > 0 {
		warnings = append(warnings, overlaps...)
	}

	// ---- 重复的 hostAliases IP（历史 issue #91670, #58477）----
	if len(podSpec.HostAliases) > 1 {
		items := sets.New[string]()
		for i, item := range podSpec.HostAliases {
			if items.Has(item.IP) {
				warnings = append(warnings, fmt.Sprintf("%s: duplicate ip %q", fieldPath.Child("spec", "hostAliases").Index(i).Child("ip"), item.IP))
			} else {
				items.Insert(item.IP)
			}
		}
	}

	// ---- 重复的 imagePullSecrets 名称（#91629, #58477）----
	if len(podSpec.ImagePullSecrets) > 1 {
		items := sets.New[string]()
		for i, item := range podSpec.ImagePullSecrets {
			if items.Has(item.Name) {
				warnings = append(warnings, fmt.Sprintf("%s: duplicate name %q", fieldPath.Child("spec", "imagePullSecrets").Index(i).Child("name"), item.Name))
			} else {
				items.Insert(item.Name)
			}
		}
	}
	// 空名称没有意义，通常是漏填（#99454）。
	for i, item := range podSpec.ImagePullSecrets {
		if len(item.Name) == 0 {
			warnings = append(warnings, fmt.Sprintf("%s: invalid empty name %q", fieldPath.Child("spec", "imagePullSecrets").Index(i).Child("name"), item.Name))
		}
	}

	// ---- 小数字节值（#79950, #49442, #18538）----
	// Quantity 内部以毫为单位，对 1000 取余可检测小数字节。
	if value, ok := podSpec.Overhead[api.ResourceMemory]; ok && value.MilliValue()%int64(1000) != int64(0) {
		warnings = append(warnings, fmt.Sprintf("%s: fractional byte value %q is invalid, must be an integer", fieldPath.Child("spec", "overhead").Key(string(api.ResourceMemory)), value.String()))
	}
	if value, ok := podSpec.Overhead[api.ResourceEphemeralStorage]; ok && value.MilliValue()%int64(1000) != int64(0) {
		warnings = append(warnings, fmt.Sprintf("%s: fractional byte value %q is invalid, must be an integer", fieldPath.Child("spec", "overhead").Key(string(api.ResourceEphemeralStorage)), value.String()))
	}

	// ---- seccomp / AppArmor：弃用注解没有配套字段时告警 ----
	if podSpec.SecurityContext == nil || podSpec.SecurityContext.SeccompProfile == nil {
		if _, exists := meta.Annotations[api.SeccompPodAnnotationKey]; exists {
			warnings = append(warnings, fmt.Sprintf(`%s: non-functional in v1.27+; use the "seccompProfile" field instead`, fieldPath.Child("metadata", "annotations").Key(api.SeccompPodAnnotationKey)))
		}
	}
	var podAppArmorProfile *api.AppArmorProfile
	if podSpec.SecurityContext != nil {
		podAppArmorProfile = podSpec.SecurityContext.AppArmorProfile
	}

	// 遍历普通容器和 sidecar 容器，逐容器检查。
	pods.VisitContainersWithPath(podSpec, fieldPath.Child("spec"), func(c *api.Container, p *field.Path) bool {
		// 容器级 seccomp 注解。
		if c.SecurityContext == nil || c.SecurityContext.SeccompProfile == nil {
			if _, exists := meta.Annotations[api.SeccompContainerAnnotationKeyPrefix+c.Name]; exists {
				warnings = append(warnings, fmt.Sprintf(`%s: non-functional in v1.27+; use the "seccompProfile" field instead`, fieldPath.Child("metadata", "annotations").Key(api.SeccompContainerAnnotationKeyPrefix+c.Name)))
			}
		}

		// AppArmor 注解：Pod 对象本身的告警走 applyAppArmorVersionSkew，
		// 这里只处理 PodTemplate（fieldPath != nil）场景。
		isPodTemplate := fieldPath != nil
		hasAppArmorField := c.SecurityContext != nil && c.SecurityContext.AppArmorProfile != nil
		if isPodTemplate && !hasAppArmorField {
			key := api.DeprecatedAppArmorAnnotationKeyPrefix + c.Name
			if annotation, exists := meta.Annotations[key]; exists {
				// 若注解与 Pod 级 profile 一致，autoscaled 字段已覆盖它，无需再告警。
				if podAppArmorProfile == nil || !apiequality.Semantic.DeepEqual(podAppArmorProfile, ApparmorFieldForAnnotation(annotation)) {
					warnings = append(warnings, fmt.Sprintf(`%s: deprecated since v1.30; use the "appArmorProfile" field instead`, fieldPath.Child("metadata", "annotations").Key(key)))
				}
			}
		}

		// 容器的小数字节资源值。
		if value, ok := c.Resources.Limits[api.ResourceMemory]; ok && value.MilliValue()%int64(1000) != int64(0) {
			warnings = append(warnings, fmt.Sprintf("%s: fractional byte value %q is invalid, must be an integer", p.Child("resources", "limits").Key(string(api.ResourceMemory)), value.String()))
		}
		if value, ok := c.Resources.Requests[api.ResourceMemory]; ok && value.MilliValue()%int64(1000) != int64(0) {
			warnings = append(warnings, fmt.Sprintf("%s: fractional byte value %q is invalid, must be an integer", p.Child("resources", "requests").Key(string(api.ResourceMemory)), value.String()))
		}
		if value, ok := c.Resources.Limits[api.ResourceEphemeralStorage]; ok && value.MilliValue()%int64(1000) != int64(0) {
			warnings = append(warnings, fmt.Sprintf("%s: fractional byte value %q is invalid, must be an integer", p.Child("resources", "limits").Key(string(api.ResourceEphemeralStorage)), value.String()))
		}
		if value, ok := c.Resources.Requests[api.ResourceEphemeralStorage]; ok && value.MilliValue()%int64(1000) != int64(0) {
			warnings = append(warnings, fmt.Sprintf("%s: fractional byte value %q is invalid, must be an integer", p.Child("resources", "requests").Key(string(api.ResourceEphemeralStorage)), value.String()))
		}

		// 重复的 env 名称（#86163, #93266, #58477）：
		// 后面重复的定义可能覆盖前面的值，使用 apply 时旧值可能被丢弃。
		if len(c.Env) > 1 {
			items := sets.New[string]()
			for i, item := range c.Env {
				if items.Has(item.Name) {
					bad := false
					ref := fmt.Sprintf("$(%s)", item.Name) // 对该名称的变量引用形式
					// 新值用 valueFrom 替换旧值时，旧值会丢失。
					if item.ValueFrom != nil {
						bad = true
					}
					// X="$(X)" 这种自引用是危险的。
					if item.Value == ref {
						bad = true
					}
					// 新值不包含对旧值的引用（如 X="abc" 后 X="$(X)123"），旧值丢失。
					if !strings.Contains(item.Value, ref) {
						bad = true
					}
					if bad {
						warnings = append(warnings, fmt.Sprintf("%s: hides previous definition of %q, which may be dropped when using apply", p.Child("env").Index(i), item.Name))
					}
				} else {
					items.Insert(item.Name)
				}
			}
		}
		return true
	})

	// ---- 跨容器端口冲突检查 ----
	type portBlock struct {
		field *field.Path
		port  api.ContainerPort
	}

	// 按 "containerPort/protocol" 聚合所有容器的端口，再逐组检查冲突。
	allPorts := map[string][]portBlock{}
	pods.VisitContainersWithPath(podSpec, fieldPath.Child("spec"), func(c *api.Container, fldPath *field.Path) bool {
		for i, port := range c.Ports {
			if port.HostIP != "" && port.HostPort == 0 {
				warnings = append(warnings, fmt.Sprintf("%s: hostIP set without hostPort: %+v",
					fldPath.Child("ports").Index(i), port))
			}
			k := fmt.Sprintf("%d/%s", port.ContainerPort, port.Protocol)
			if others, found := allPorts[k]; found {
				// 同协议+容器端口存在其他定义，逐一判断是否为真正冲突。
				for _, other := range others {
					if port.HostIP == other.port.HostIP && port.HostPort == other.port.HostPort {
						// 完全相同的定义（正常情况下校验会过滤，未指定时除外）。
						warnings = append(warnings, fmt.Sprintf("%s: duplicate port definition with %s", fldPath.Child("ports").Index(i), other.field))
					} else if port.HostPort == 0 || other.port.HostPort == 0 {
						// HostPort=0 与任意值都重叠：冗余但危险性低。
						warnings = append(warnings, fmt.Sprintf("%s: overlapping port definition with %s", fldPath.Child("ports").Index(i), other.field))
					} else if a, b := port.HostIP == "", other.port.HostIP == ""; port.HostPort == other.port.HostPort && ((a || b) && !(a && b)) {
						// HostPort 相同但只有一个指定了 HostIP，行为未定义。
						warnings = append(warnings, fmt.Sprintf("%s: dangerously ambiguous port definition with %s", fldPath.Child("ports").Index(i), other.field))
					}
				}
				allPorts[k] = append(allPorts[k], portBlock{field: fldPath.Child("ports").Index(i), port: port})
			} else {
				allPorts[k] = []portBlock{{field: fldPath.Child("ports").Index(i), port: port}}
			}
		}
		return true
	})

	// 端口名称重复：Service/probe 按名称选端口时会选到第一个。
	allPortsNames := map[string]*field.Path{}
	pods.VisitContainersWithPath(podSpec, fieldPath.Child("spec"), func(c *api.Container, fldPath *field.Path) bool {
		for i, port := range c.Ports {
			if port.Name != "" {
				if other, found := allPortsNames[port.Name]; found {
					warnings = append(warnings, fmt.Sprintf("%s: duplicate port name %q with %s, services and probes that select ports by name will use %s", fldPath.Child("ports").Index(i), port.Name, other, other))
				} else {
					allPortsNames[port.Name] = fldPath.Child("ports").Index(i)
				}
			}
		}
		return true
	})

	// 负的 terminationGracePeriodSeconds 会被当作 1 处理。
	if podSpec.TerminationGracePeriodSeconds != nil && *podSpec.TerminationGracePeriodSeconds < 0 {
		warnings = append(warnings, fmt.Sprintf("%s: must be >= 0; negative values are invalid and will be treated as 1", fieldPath.Child("spec", "terminationGracePeriodSeconds")))
	}

	// Pod 亲和性/反亲和性中 nil labelSelector 同样意味着不匹配任何 Pod。
	if podSpec.Affinity != nil {
		if affinity := podSpec.Affinity.PodAffinity; affinity != nil {
			warnings = append(warnings, warningsForPodAffinityTerms(affinity.RequiredDuringSchedulingIgnoredDuringExecution, fieldPath.Child("spec", "affinity", "podAffinity", "requiredDuringSchedulingIgnoredDuringExecution"))...)
			warnings = append(warnings, warningsForWeightedPodAffinityTerms(affinity.PreferredDuringSchedulingIgnoredDuringExecution, fieldPath.Child("spec", "affinity", "podAffinity", "preferredDuringSchedulingIgnoredDuringExecution"))...)
		}
		if affinity := podSpec.Affinity.PodAntiAffinity; affinity != nil {
			warnings = append(warnings, warningsForPodAffinityTerms(affinity.RequiredDuringSchedulingIgnoredDuringExecution, fieldPath.Child("spec", "affinity", "podAntiAffinity", "requiredDuringSchedulingIgnoredDuringExecution"))...)
			warnings = append(warnings, warningsForWeightedPodAffinityTerms(affinity.PreferredDuringSchedulingIgnoredDuringExecution, fieldPath.Child("spec", "affinity", "podAntiAffinity", "preferredDuringSchedulingIgnoredDuringExecution"))...)
		}
	}

	// 已弃用的 IP 地址格式（如 IPv4 的前导零写法）。
	if podSpec.DNSConfig != nil {
		for i, ns := range podSpec.DNSConfig.Nameservers {
			warnings = append(warnings, validation.GetWarningsForIP(fieldPath.Child("spec", "dnsConfig", "nameservers").Index(i), ns)...)
		}
	}
	for i, hostAlias := range podSpec.HostAliases {
		warnings = append(warnings, validation.GetWarningsForIP(fieldPath.Child("spec", "hostAliases").Index(i).Child("ip"), hostAlias.IP)...)
	}

	return warnings
}

// warningsForPodAffinityTerms 检查 requiredDuringScheduling 的亲和性项。
func warningsForPodAffinityTerms(terms []api.PodAffinityTerm, fieldPath *field.Path) []string {
	var warnings []string
	for i, t := range terms {
		if t.LabelSelector == nil {
			warnings = append(warnings, fmt.Sprintf("%s: a null labelSelector results in matching no pod", fieldPath.Index(i).Child("labelSelector")))
		}
	}
	return warnings
}

// warningsForWeightedPodAffinityTerms 检查 preferredDuringScheduling 的带权亲和性项。
func warningsForWeightedPodAffinityTerms(terms []api.WeightedPodAffinityTerm, fieldPath *field.Path) []string {
	var warnings []string
	for i, t := range terms {
		// labelSelector 为空意味着“不匹配任何 Pod”，通常是配置失误。
		if t.PodAffinityTerm.LabelSelector == nil {
			warnings = append(warnings, fmt.Sprintf("%s: a null labelSelector results in matching no pod", fieldPath.Index(i).Child("podAffinityTerm", "labelSelector")))
		}
	}
	return warnings
}

// warningsForOverlappingVirtualPaths 检查单个 ConfigMap/Secret/DownwardAPI/Projected 卷内
// 是否存在映射到相同虚拟路径的情况。
// 同一个卷把不同的 key 映射到同一路径会导致后注册的值覆盖前者；
// 若一个路径是另一个路径的父目录，则会出现 "is directory" 或 "file exists" 错误。
// 示例：
//
//	configMap:
//	  name: myconfig
//	  items:
//	    - key: key1
//	      path: path
//	    - key: key2
//	      path: path/path2
func warningsForOverlappingVirtualPaths(volumes []api.Volume) []string {
	var warnings []string

	mkWarn := func(volName, volDesc, body string) string {
		return fmt.Sprintf("volume %q (%s): overlapping paths: %s", volName, volDesc, body)
	}

	for _, v := range volumes {
		// 四类卷各自内部的 key→path 映射都要检查。
		if v.ConfigMap != nil && v.ConfigMap.Items != nil {
			overlaps := checkVolumeMappingForOverlap(extractPaths(v.ConfigMap.Items, ""))
			for _, ol := range overlaps {
				warnings = append(warnings, mkWarn(v.Name, fmt.Sprintf("ConfigMap %q", v.ConfigMap.Name), ol))
			}
		}

		if v.Secret != nil && v.Secret.Items != nil {
			overlaps := checkVolumeMappingForOverlap(extractPaths(v.Secret.Items, ""))
			for _, ol := range overlaps {
				warnings = append(warnings, mkWarn(v.Name, fmt.Sprintf("Secret %q", v.Secret.SecretName), ol))
			}
		}

		if v.DownwardAPI != nil && v.DownwardAPI.Items != nil {
			overlaps := checkVolumeMappingForOverlap(extractPathsDownwardAPI(v.DownwardAPI.Items, ""))
			for _, ol := range overlaps {
				warnings = append(warnings, mkWarn(v.Name, "DownwardAPI", ol))
			}
		}

		if v.Projected != nil {
			var allPaths []pathAndSource

			for _, source := range v.Projected.Sources {
				// 空的 projection 没有任何内容，单独提示。
				if source == (api.VolumeProjection{}) {
					warnings = append(warnings, fmt.Sprintf("volume %q (Projected) has no sources provided", v.Name))
					continue
				}

				// 按 projection 类型取出各自的路径列表，并标注来源以便报错时定位。
				var sourcePaths []pathAndSource
				switch {
				case source.ConfigMap != nil && source.ConfigMap.Items != nil:
					sourcePaths = extractPaths(source.ConfigMap.Items, fmt.Sprintf("ConfigMap %q", source.ConfigMap.Name))
				case source.Secret != nil && source.Secret.Items != nil:
					sourcePaths = extractPaths(source.Secret.Items, fmt.Sprintf("Secret %q", source.Secret.Name))
				case source.DownwardAPI != nil && source.DownwardAPI.Items != nil:
					sourcePaths = extractPathsDownwardAPI(source.DownwardAPI.Items, "DownwardAPI")
				case source.ServiceAccountToken != nil:
					sourcePaths = []pathAndSource{{source.ServiceAccountToken.Path, "ServiceAccountToken"}}
				case source.ClusterTrustBundle != nil:
					// Name 和 SignerName 二选一，取非空的那个作为来源描述。
					name := ""
					if source.ClusterTrustBundle.Name != nil {
						name = *source.ClusterTrustBundle.Name
					} else {
						name = *source.ClusterTrustBundle.SignerName
					}
					sourcePaths = []pathAndSource{{source.ClusterTrustBundle.Path, fmt.Sprintf("ClusterTrustBundle %q", name)}}
				case source.PodCertificate != nil:
					// PodCertificate 可能产出多个文件，每个路径都要检查。
					sourcePaths = []pathAndSource{}
					if len(source.PodCertificate.CertificateChainPath) != 0 {
						sourcePaths = append(sourcePaths, pathAndSource{source.PodCertificate.CertificateChainPath, "PodCertificate chain"})
					}
					if len(source.PodCertificate.KeyPath) != 0 {
						sourcePaths = append(sourcePaths, pathAndSource{source.PodCertificate.KeyPath, "PodCertificate key"})
					}
					if len(source.PodCertificate.CredentialBundlePath) != 0 {
						sourcePaths = append(sourcePaths, pathAndSource{source.PodCertificate.CredentialBundlePath, "PodCertificate credential bundle"})
					}
				}

				if len(sourcePaths) == 0 {
					continue
				}

				// 与其他 projection 已累计的路径做交叉检查。
				for _, ps := range sourcePaths {
					ps.path = strings.TrimRight(ps.path, string(os.PathSeparator))
					if collisions := checkForOverlap(allPaths, ps); len(collisions) > 0 {
						for _, c := range collisions {
							warnings = append(warnings, mkWarn(v.Name, "Projected", fmt.Sprintf("%s with %s", ps.String(), c.String())))
						}
					}
					allPaths = append(allPaths, ps)
				}
			}
		}
	}
	return warnings
}

// pathAndSource 记录一个虚拟路径及其来源，便于生成更有信息量的告警。
type pathAndSource struct {
	path   string
	source string
}

func (ps pathAndSource) String() string {
	if ps.source != "" {
		return fmt.Sprintf("%q (%s)", ps.path, ps.source)
	}
	return fmt.Sprintf("%q", ps.path)
}

// extractPaths 把 ConfigMap/Secret 的 KeyToPath 映射提取为路径列表。
func extractPaths(mapping []api.KeyToPath, source string) []pathAndSource {
	result := make([]pathAndSource, 0, len(mapping))

	for _, v := range mapping {
		result = append(result, pathAndSource{v.Path, source})
	}
	return result
}

// extractPathsDownwardAPI 把 DownwardAPI 的 VolumeFile 映射提取为路径列表。
func extractPathsDownwardAPI(mapping []api.DownwardAPIVolumeFile, source string) []pathAndSource {
	result := make([]pathAndSource, 0, len(mapping))

	for _, v := range mapping {
		result = append(result, pathAndSource{v.Path, source})
	}
	return result
}

// checkVolumeMappingForOverlap 检查同一个卷内的路径是否互相重叠。
func checkVolumeMappingForOverlap(paths []pathAndSource) []string {
	pathSeparator := string(os.PathSeparator)
	var warnings []string
	var allPaths []pathAndSource

	for _, ps := range paths {
		// 去掉尾部斜杠后比较，避免 "a/" 与 "a" 被当成不同路径。
		ps.path = strings.TrimRight(ps.path, pathSeparator)
		if collisions := checkForOverlap(allPaths, ps); len(collisions) > 0 {
			for _, c := range collisions {
				warnings = append(warnings, fmt.Sprintf("%s with %s", ps.String(), c.String()))
			}
		}
		allPaths = append(allPaths, ps)
	}

	return warnings
}

// checkForOverlap 判断 needle 与 haystack 中哪些路径重叠：
// 完全相同，或一个是另一个的父目录。
// 这里固定用 "/" 而不是 os.PathSeparator，因为检查运行在 API server 中，
// 与操作系统无关。
func checkForOverlap(haystack []pathAndSource, needle pathAndSource) []pathAndSource {
	pathSeparator := `/`

	// 空路径不参与比较（它等价于卷根目录，不代表具体文件）。
	if needle.path == "" {
		return nil
	}

	var result []pathAndSource
	for _, item := range haystack {
		switch {
		case item.path == "":
			continue
		case item == needle:
			// 完全相同。
			result = append(result, item)
		case strings.HasPrefix(item.path+pathSeparator, needle.path+pathSeparator):
			// needle 是 item 的父目录。
			result = append(result, item)
		case strings.HasPrefix(needle.path+pathSeparator, item.path+pathSeparator):
			// item 是 needle 的父目录。
			result = append(result, item)
		}
	}

	return result
}
