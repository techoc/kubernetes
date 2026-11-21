/*
Copyright 2017 The Kubernetes Authors.

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

package validation

import (
	"fmt"
	"strings"

	"k8s.io/apimachinery/pkg/api/validate/content"
	apimachineryvalidation "k8s.io/apimachinery/pkg/api/validation"
	unversionedvalidation "k8s.io/apimachinery/pkg/apis/meta/v1/validation"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	utilfeature "k8s.io/apiserver/pkg/util/feature"
	api "k8s.io/kubernetes/pkg/apis/core"
	apivalidation "k8s.io/kubernetes/pkg/apis/core/validation"
	"k8s.io/kubernetes/pkg/apis/networking"
	"k8s.io/kubernetes/pkg/features"
	netutils "k8s.io/utils/net"
	"k8s.io/utils/ptr"
)

const (
	annotationIngressClass       = "kubernetes.io/ingress.class"
	maxLenIngressClassController = 250
)

var (
	supportedPathTypes = sets.NewString(
		string(networking.PathTypeExact),
		string(networking.PathTypePrefix),
		string(networking.PathTypeImplementationSpecific),
	)
	invalidPathSequences = []string{"//", "/./", "/../", "%2f", "%2F"}
	invalidPathSuffixes  = []string{"/..", "/."}

	supportedIngressClassParametersReferenceScopes = sets.NewString(
		networking.IngressClassParametersReferenceScopeNamespace,
		networking.IngressClassParametersReferenceScopeCluster,
	)
)

// NetworkPolicyValidationOptions 控制 NetworkPolicy 校验的兼容性开关。
// AllowInvalidLabelValueInSelector 允许 selector 中使用非法的 label value，兼容历史遗留对象。
// AllowCIDRsEvenIfInvalid 列出即使非法也要放行的旧对象 CIDR，避免历史对象无法更新。
type NetworkPolicyValidationOptions struct {
	AllowInvalidLabelValueInSelector bool
	AllowCIDRsEvenIfInvalid          []string
}

// ValidateNetworkPolicyName 检查给定的 NetworkPolicy 名称是否合法。
func ValidateNetworkPolicyName(name string, prefix bool) []string {
	return apimachineryvalidation.NameIsDNSSubdomain(name, prefix)
}

// ValidateNetworkPolicyPort 校验 NetworkPolicyPort 的协议和端口范围。
func ValidateNetworkPolicyPort(port *networking.NetworkPolicyPort, portPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if port.Protocol != nil && *port.Protocol != api.ProtocolTCP && *port.Protocol != api.ProtocolUDP && *port.Protocol != api.ProtocolSCTP {
		allErrs = append(allErrs, field.NotSupported(portPath.Child("protocol"), *port.Protocol, []string{string(api.ProtocolTCP), string(api.ProtocolUDP), string(api.ProtocolSCTP)}))
	}
	if port.Port != nil {
		// 数字端口需要校验数值本身，并检查端口区间的上下界关系。
		if port.Port.Type == intstr.Int {
			for _, msg := range validation.IsValidPortNum(int(port.Port.IntVal)) {
				allErrs = append(allErrs, field.Invalid(portPath.Child("port"), port.Port.IntVal, msg))
			}
			if port.EndPort != nil {
				if *port.EndPort < port.Port.IntVal {
					allErrs = append(allErrs, field.Invalid(portPath.Child("endPort"), *port.EndPort, "must be greater than or equal to `port`"))
				}
				for _, msg := range validation.IsValidPortNum(int(*port.EndPort)) {
					allErrs = append(allErrs, field.Invalid(portPath.Child("endPort"), *port.EndPort, msg))
				}
			}
		} else {
			if port.EndPort != nil {
				allErrs = append(allErrs, field.Invalid(portPath.Child("endPort"), *port.EndPort, "may not be specified when `port` is non-numeric"))
			}
			for _, msg := range validation.IsValidPortName(port.Port.StrVal) {
				allErrs = append(allErrs, field.Invalid(portPath.Child("port"), port.Port.StrVal, msg))
			}
		}
	} else {
		if port.EndPort != nil {
			allErrs = append(allErrs, field.Invalid(portPath.Child("endPort"), *port.EndPort, "may not be specified when `port` is not specified"))
		}
	}

	return allErrs
}

// ValidateNetworkPolicyPeer 校验 NetworkPolicyPeer，只允许指定一种 peer 来源。
func ValidateNetworkPolicyPeer(peer *networking.NetworkPolicyPeer, opts NetworkPolicyValidationOptions, peerPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	numPeers := 0
	labelSelectorValidationOpts := unversionedvalidation.LabelSelectorValidationOptions{
		AllowInvalidLabelValueInSelector: opts.AllowInvalidLabelValueInSelector,
	}

	if peer.PodSelector != nil {
		numPeers++
		allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(peer.PodSelector, labelSelectorValidationOpts, peerPath.Child("podSelector"))...)
	}
	if peer.NamespaceSelector != nil {
		numPeers++
		allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(peer.NamespaceSelector, labelSelectorValidationOpts, peerPath.Child("namespaceSelector"))...)
	}
	if peer.IPBlock != nil {
		numPeers++
		allErrs = append(allErrs, ValidateIPBlock(peer.IPBlock, peerPath.Child("ipBlock"), opts)...)
	}

	// peer 必须至少指定一个来源；ipBlock 不能与 pod/namespace selector 混用，
	// 否则无法明确该规则到底匹配哪些流量。
	if numPeers == 0 {
		allErrs = append(allErrs, field.Required(peerPath, "must specify a peer"))
	} else if numPeers > 1 && peer.IPBlock != nil {
		allErrs = append(allErrs, field.Forbidden(peerPath, "may not specify both ipBlock and another peer"))
	}

	return allErrs
}

// ValidateNetworkPolicySpec 校验 NetworkPolicy 的 podSelector、ingress/egress 规则和 policyTypes。
func ValidateNetworkPolicySpec(spec *networking.NetworkPolicySpec, opts NetworkPolicyValidationOptions, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	labelSelectorValidationOpts := unversionedvalidation.LabelSelectorValidationOptions{
		AllowInvalidLabelValueInSelector: opts.AllowInvalidLabelValueInSelector,
	}
	allErrs = append(allErrs, unversionedvalidation.ValidateLabelSelector(
		&spec.PodSelector,
		labelSelectorValidationOpts,
		fldPath.Child("podSelector"),
	)...)

	// 分别校验入站和出站规则中的端口与 peer。
	for i, ingress := range spec.Ingress {
		ingressPath := fldPath.Child("ingress").Index(i)
		for i, port := range ingress.Ports {
			portPath := ingressPath.Child("ports").Index(i)
			allErrs = append(allErrs, ValidateNetworkPolicyPort(&port, portPath)...)
		}
		for i, from := range ingress.From {
			fromPath := ingressPath.Child("from").Index(i)
			allErrs = append(allErrs, ValidateNetworkPolicyPeer(&from, opts, fromPath)...)
		}
	}
	// 出站规则与入站规则结构相同，同样需要逐条校验端口和 peer。
	for i, egress := range spec.Egress {
		egressPath := fldPath.Child("egress").Index(i)
		for i, port := range egress.Ports {
			portPath := egressPath.Child("ports").Index(i)
			allErrs = append(allErrs, ValidateNetworkPolicyPort(&port, portPath)...)
		}
		for i, to := range egress.To {
			toPath := egressPath.Child("to").Index(i)
			allErrs = append(allErrs, ValidateNetworkPolicyPeer(&to, opts, toPath)...)
		}
	}
	// policyTypes 最多两种，且只能取 Ingress/Egress，避免重复或无意义的声明。
	allowed := sets.NewString(string(networking.PolicyTypeIngress), string(networking.PolicyTypeEgress))
	if len(spec.PolicyTypes) > len(allowed) {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("policyTypes"), &spec.PolicyTypes, "may not specify more than two policyTypes"))
		return allErrs
	}
	for i, pType := range spec.PolicyTypes {
		policyPath := fldPath.Child("policyTypes").Index(i)
		if !allowed.Has(string(pType)) {
			allErrs = append(allErrs, field.NotSupported(policyPath, pType, []string{string(networking.PolicyTypeIngress), string(networking.PolicyTypeEgress)}))
		}
	}
	return allErrs
}

// ValidateNetworkPolicy 校验 NetworkPolicy 对象的元数据和 spec。
func ValidateNetworkPolicy(np *networking.NetworkPolicy, opts NetworkPolicyValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&np.ObjectMeta, true, ValidateNetworkPolicyName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateNetworkPolicySpec(&np.Spec, opts, field.NewPath("spec"))...)
	return allErrs
}

// ValidationOptionsForNetworking 根据旧对象生成校验选项，为历史遗留的非法 selector 保留兼容通道。
func ValidationOptionsForNetworking(new, old *networking.NetworkPolicy) NetworkPolicyValidationOptions {
	opts := NetworkPolicyValidationOptions{
		AllowInvalidLabelValueInSelector: false,
	}
	if old != nil {
		labelSelectorValidationOpts := unversionedvalidation.LabelSelectorValidationOptions{
			AllowInvalidLabelValueInSelector: opts.AllowInvalidLabelValueInSelector,
		}
		if len(unversionedvalidation.ValidateLabelSelector(&old.Spec.PodSelector, labelSelectorValidationOpts, nil)) > 0 {
			opts.AllowInvalidLabelValueInSelector = true
		}
	}
	return opts
}

// ValidateNetworkPolicyUpdate 校验 NetworkPolicy 的更新，兼容旧对象中已存在的非法 CIDR。
func ValidateNetworkPolicyUpdate(update, old *networking.NetworkPolicy, opts NetworkPolicyValidationOptions) field.ErrorList {
	// 收集旧对象中已经存在的所有 CIDR；这些值即使非法也必须在更新时被放行，
	// 详见 ValidateIPBlock 的说明。
	var existingCIDRs []string
	for _, ingress := range old.Spec.Ingress {
		for _, peer := range ingress.From {
			if peer.IPBlock != nil {
				existingCIDRs = append(existingCIDRs, peer.IPBlock.CIDR)
				existingCIDRs = append(existingCIDRs, peer.IPBlock.Except...)
			}
		}
	}
	for _, egress := range old.Spec.Egress {
		for _, peer := range egress.To {
			if peer.IPBlock != nil {
				existingCIDRs = append(existingCIDRs, peer.IPBlock.CIDR)
				existingCIDRs = append(existingCIDRs, peer.IPBlock.Except...)
			}
		}
	}
	opts.AllowCIDRsEvenIfInvalid = existingCIDRs

	allErrs := field.ErrorList{}
	allErrs = append(allErrs, apivalidation.ValidateObjectMetaUpdate(&update.ObjectMeta, &old.ObjectMeta, field.NewPath("metadata"))...)
	allErrs = append(allErrs, ValidateNetworkPolicySpec(&update.Spec, opts, field.NewPath("spec"))...)
	return allErrs
}

// ValidateIPBlock 校验 IPBlock 的 cidr 和 except 字段。
//
// 如果某个已经存在的 CIDR 非法或不安全，但本次更新并未改动它，就必须继续放行它，
// 否则旧对象将无法更新。但用户可能以任意方式修改策略（增删规则、增删 peer、
// 增删 ipBlock.except 等），无法可靠判断当前看到的 CIDR 是新增的还是原有的，
// 因此统一放行旧 NetworkPolicy 中出现过的 CIDR，不再重新校验它们。
func ValidateIPBlock(ipb *networking.IPBlock, fldPath *field.Path, opts NetworkPolicyValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if ipb.CIDR == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("cidr"), "").MarkCoveredByDeclarative())
		return allErrs
	}
	allErrs = append(allErrs, apivalidation.IsValidCIDRForLegacyField(fldPath.Child("cidr"), ipb.CIDR, opts.AllowCIDRsEvenIfInvalid)...)
	_, cidrIPNet, err := netutils.ParseCIDRSloppy(ipb.CIDR)
	if err != nil {
		// 解析失败意味着上面的校验已经报错，无需重复添加错误。
		return allErrs
	}

	for i, exceptCIDRStr := range ipb.Except {
		exceptPath := fldPath.Child("except").Index(i)
		allErrs = append(allErrs, apivalidation.IsValidCIDRForLegacyField(exceptPath, exceptCIDRStr, opts.AllowCIDRsEvenIfInvalid)...)
		_, exceptCIDR, err := netutils.ParseCIDRSloppy(exceptCIDRStr)
		if err != nil {
			// 解析失败意味着上面的校验已经报错，跳过后续的子集检查。
			continue
		}

		cidrMaskLen, _ := cidrIPNet.Mask.Size()
		exceptMaskLen, _ := exceptCIDR.Mask.Size()
		// except 必须是 cidr 的真子集：不仅要被包含，掩码还必须更长。
		if !cidrIPNet.Contains(exceptCIDR.IP) || cidrMaskLen >= exceptMaskLen {
			allErrs = append(allErrs, field.Invalid(exceptPath, exceptCIDRStr, "must be a strict subset of `cidr`"))
		}
	}
	return allErrs
}

// ValidateIngressName 校验给定名称能否用作 Ingress 名称。
var ValidateIngressName = apimachineryvalidation.NameIsDNSSubdomain

// IngressValidationOptions 覆盖 HTTP PathType 从 beta 到 GA 过渡期的兼容性选项。
type IngressValidationOptions struct {
	// AllowInvalidSecretName 是否允许 spec.tls[*].secretName 使用非法的 Secret 名称。
	AllowInvalidSecretName bool

	// AllowInvalidWildcardHostRule 是否允许通配符 hostname 的规则中包含非法规则值。
	AllowInvalidWildcardHostRule bool

	// AllowRelaxedServiceNameValidation 是否放宽后端 Service 名称校验，
	// 改用 apimachineryvalidation.NameIsDNSLabel。
	AllowRelaxedServiceNameValidation bool
}

// validateIngress 执行 Ingress 创建和更新时共用的校验逻辑。
func validateIngress(ingress *networking.Ingress, opts IngressValidationOptions) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&ingress.ObjectMeta, true, ValidateIngressName, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateIngressSpec(&ingress.Spec, field.NewPath("spec"), opts)...)
	return allErrs
}

// ValidateIngressCreate validates Ingresses on create.
func ValidateIngressCreate(ingress *networking.Ingress) field.ErrorList {
	allErrs := field.ErrorList{}
	opts := IngressValidationOptions{
		AllowInvalidSecretName:            false,
		AllowInvalidWildcardHostRule:      false,
		AllowRelaxedServiceNameValidation: allowRelaxedServiceNameValidation(nil),
	}
	allErrs = append(allErrs, validateIngress(ingress, opts)...)
	// 旧注解和新字段同时存在时必须一致，避免控制器从两者中读到不同的 IngressClass。
	annotationVal, annotationIsSet := ingress.Annotations[annotationIngressClass]
	if annotationIsSet && ingress.Spec.IngressClassName != nil && annotationVal != *ingress.Spec.IngressClassName {
		annotationPath := field.NewPath("annotations").Child(annotationIngressClass)
		allErrs = append(allErrs, field.Invalid(annotationPath, annotationVal, "must match `ingressClassName` when both are specified"))
	}
	return allErrs
}

// ValidateIngressUpdate 校验 Ingress 的更新，并根据旧对象放宽历史遗留数据的校验。
func ValidateIngressUpdate(ingress, oldIngress *networking.Ingress) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&ingress.ObjectMeta, &oldIngress.ObjectMeta, field.NewPath("metadata"))
	opts := IngressValidationOptions{
		AllowInvalidSecretName:            allowInvalidSecretName(oldIngress),
		AllowInvalidWildcardHostRule:      allowInvalidWildcardHostRule(oldIngress),
		AllowRelaxedServiceNameValidation: allowRelaxedServiceNameValidation(oldIngress),
	}

	allErrs = append(allErrs, validateIngress(ingress, opts)...)
	return allErrs
}

// validateIngressTLS 校验 Ingress TLS 配置的 host 和 secretName。
func validateIngressTLS(spec *networking.IngressSpec, fldPath *field.Path, opts IngressValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	// TODO: Perform a more thorough validation of spec.TLS.Hosts that takes
	// the wildcard spec from RFC 6125 into account.
	for tlsIndex, itls := range spec.TLS {
		for i, host := range itls.Hosts {
			// 带通配符的 host 使用专门的校验规则，合法的前提是仅包含一段前缀通配符。
			if strings.Contains(host, "*") {
				for _, msg := range validation.IsWildcardDNS1123Subdomain(host) {
					allErrs = append(allErrs, field.Invalid(fldPath.Index(tlsIndex).Child("hosts").Index(i), host, msg))
				}
				continue
			}
			for _, msg := range validation.IsDNS1123Subdomain(host) {
				allErrs = append(allErrs, field.Invalid(fldPath.Index(tlsIndex).Child("hosts").Index(i), host, msg))
			}
		}

		if !opts.AllowInvalidSecretName {
			for _, msg := range validateTLSSecretName(itls.SecretName) {
				allErrs = append(allErrs, field.Invalid(fldPath.Index(tlsIndex).Child("secretName"), itls.SecretName, msg))
			}
		}
	}

	return allErrs
}

// ValidateIngressSpec 校验 IngressSpec 的默认后端、规则、TLS 和 IngressClassName。
func ValidateIngressSpec(spec *networking.IngressSpec, fldPath *field.Path, opts IngressValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	// 规则和默认后端不能同时为空，否则 Ingress 无法路由任何请求。
	if len(spec.Rules) == 0 && spec.DefaultBackend == nil {
		errMsg := fmt.Sprintf("either `%s` or `rules` must be specified", "defaultBackend")
		allErrs = append(allErrs, field.Invalid(fldPath, spec.Rules, errMsg))
	}
	if spec.DefaultBackend != nil {
		allErrs = append(allErrs, validateIngressBackend(spec.DefaultBackend, fldPath.Child("defaultBackend"), opts)...)
	}
	if len(spec.Rules) > 0 {
		allErrs = append(allErrs, validateIngressRules(spec.Rules, fldPath.Child("rules"), opts)...)
	}
	if len(spec.TLS) > 0 {
		allErrs = append(allErrs, validateIngressTLS(spec, fldPath.Child("tls"), opts)...)
	}
	if spec.IngressClassName != nil {
		for _, msg := range ValidateIngressClassName(*spec.IngressClassName, false) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("ingressClassName"), *spec.IngressClassName, msg))
		}
	}
	return allErrs
}

// ValidateIngressStatusUpdate 校验 Ingress status 更新，只检查元数据和负载均衡状态。
func ValidateIngressStatusUpdate(ingress, oldIngress *networking.Ingress) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&ingress.ObjectMeta, &oldIngress.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, ValidateIngressLoadBalancerStatus(&ingress.Status.LoadBalancer, &oldIngress.Status.LoadBalancer, field.NewPath("status", "loadBalancer"))...)
	return allErrs
}

// ValidateIngressLoadBalancerStatus 校验 Ingress 负载均衡状态的 IP 和 hostname。
func ValidateIngressLoadBalancerStatus(status, oldStatus *networking.IngressLoadBalancerStatus, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 旧状态中的 IP 即使不符合当前校验规则也要放行，否则控制器无法写回状态。
	var existingIngressIPs []string
	if oldStatus != nil {
		existingIngressIPs = make([]string, 0, len(oldStatus.Ingress))
		for _, ingress := range oldStatus.Ingress {
			if len(ingress.IP) > 0 {
				existingIngressIPs = append(existingIngressIPs, ingress.IP)
			}
		}
	}

	for i, ingress := range status.Ingress {
		idxPath := fldPath.Child("ingress").Index(i)
		if len(ingress.IP) > 0 {
			allErrs = append(allErrs, apivalidation.IsValidIPForLegacyField(idxPath.Child("ip"), ingress.IP, existingIngressIPs)...)
		}
		if len(ingress.Hostname) > 0 {
			for _, msg := range validation.IsDNS1123Subdomain(ingress.Hostname) {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("hostname"), ingress.Hostname, msg))
			}
			// hostname 必须是 DNS 名称，IP 地址应填写到 ip 字段。
			if isIP := (netutils.ParseIPSloppy(ingress.Hostname) != nil); isIP {
				allErrs = append(allErrs, field.Invalid(idxPath.Child("hostname"), ingress.Hostname, "must be a DNS name, not an IP address"))
			}
		}
	}
	return allErrs
}

// validateIngressRules 逐条校验 Ingress 规则，包括 host 合法性和规则值。
func validateIngressRules(ingressRules []networking.IngressRule, fldPath *field.Path, opts IngressValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	for i, ih := range ingressRules {
		wildcardHost := false
		if len(ih.Host) > 0 {
			// Ingress 的 host 按 DNS 名称解释，不能直接写 IP。
			if isIP := (netutils.ParseIPSloppy(ih.Host) != nil); isIP {
				allErrs = append(allErrs, field.Invalid(fldPath.Index(i).Child("host"), ih.Host, "must be a DNS name, not an IP address"))
			}
			// TODO: Ports and ips are allowed in the host part of a url
			// according to RFC 3986, consider allowing them.
			if strings.Contains(ih.Host, "*") {
				for _, msg := range validation.IsWildcardDNS1123Subdomain(ih.Host) {
					allErrs = append(allErrs, field.Invalid(fldPath.Index(i).Child("host"), ih.Host, msg))
				}
				wildcardHost = true
			} else {
				for _, msg := range validation.IsDNS1123Subdomain(ih.Host) {
					allErrs = append(allErrs, field.Invalid(fldPath.Index(i).Child("host"), ih.Host, msg))
				}
			}
		}

		// 兼容开关仅在通配符 host 的规则上生效，其余情况必须严格校验。
		if !wildcardHost || !opts.AllowInvalidWildcardHostRule {
			allErrs = append(allErrs, validateIngressRuleValue(&ih.IngressRuleValue, fldPath.Index(i), opts)...)
		}
	}
	return allErrs
}

// validateIngressRuleValue 校验 Ingress 规则的值，当前只有 HTTP 一种类型。
func validateIngressRuleValue(ingressRule *networking.IngressRuleValue, fldPath *field.Path, opts IngressValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if ingressRule.HTTP != nil {
		allErrs = append(allErrs, validateHTTPIngressRuleValue(ingressRule.HTTP, fldPath.Child("http"), opts)...)
	}
	return allErrs
}

// validateHTTPIngressRuleValue 校验 HTTP 规则至少有一条 path，并逐条校验路径。
func validateHTTPIngressRuleValue(httpIngressRuleValue *networking.HTTPIngressRuleValue, fldPath *field.Path, opts IngressValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(httpIngressRuleValue.Paths) == 0 {
		allErrs = append(allErrs, field.Required(fldPath.Child("paths"), ""))
	}
	for i, path := range httpIngressRuleValue.Paths {
		allErrs = append(allErrs, validateHTTPIngressPath(&path, fldPath.Child("paths").Index(i), opts)...)
	}
	return allErrs
}

// validateHTTPIngressPath 校验单条 HTTP 路径的类型、路径格式和后端。
func validateHTTPIngressPath(path *networking.HTTPIngressPath, fldPath *field.Path, opts IngressValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	// pathType 从 GA 开始是必填字段，缺省时无法推断匹配语义。
	if path.PathType == nil {
		return append(allErrs, field.Required(fldPath.Child("pathType"), "pathType must be specified"))
	}

	switch *path.PathType {
	case networking.PathTypeExact, networking.PathTypePrefix:
		// Exact/Prefix 类型要求路径是规范的绝对路径，防止出现绕过匹配的路径写法。
		if !strings.HasPrefix(path.Path, "/") {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("path"), path.Path, "must be an absolute path"))
		}
		if len(path.Path) > 0 {
			for _, invalidSeq := range invalidPathSequences {
				if strings.Contains(path.Path, invalidSeq) {
					allErrs = append(allErrs, field.Invalid(fldPath.Child("path"), path.Path, fmt.Sprintf("must not contain '%s'", invalidSeq)))
				}
			}

			for _, invalidSuff := range invalidPathSuffixes {
				if strings.HasSuffix(path.Path, invalidSuff) {
					allErrs = append(allErrs, field.Invalid(fldPath.Child("path"), path.Path, fmt.Sprintf("cannot end with '%s'", invalidSuff)))
				}
			}
		}
	case networking.PathTypeImplementationSpecific:
		if len(path.Path) > 0 {
			if !strings.HasPrefix(path.Path, "/") {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("path"), path.Path, "must be an absolute path"))
			}
		}
	default:
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("pathType"), *path.PathType, supportedPathTypes.List()))
	}
	allErrs = append(allErrs, validateIngressBackend(&path.Backend, fldPath.Child("backend"), opts)...)
	return allErrs
}

// validateIngressBackend 校验 Ingress 后端：resource 和 service 只能指定一个，端口名和数字也只能指定一个。
func validateIngressBackend(backend *networking.IngressBackend, fldPath *field.Path, opts IngressValidationOptions) field.ErrorList {
	allErrs := field.ErrorList{}

	hasResourceBackend := backend.Resource != nil
	hasServiceBackend := backend.Service != nil

	switch {
	case hasResourceBackend && hasServiceBackend:
		return append(allErrs, field.Invalid(fldPath, "", "cannot set both resource and service backends"))
	case hasResourceBackend:
		allErrs = append(allErrs, validateIngressTypedLocalObjectReference(backend.Resource, fldPath.Child("resource"))...)
	case hasServiceBackend:

		if len(backend.Service.Name) == 0 {
			allErrs = append(allErrs, field.Required(fldPath.Child("service", "name"), ""))
		} else {

			validationFunc := apivalidation.ValidateServiceName
			// 兼容开关允许历史上合法的 DNS label 名称继续使用。
			if opts.AllowRelaxedServiceNameValidation {
				validationFunc = apimachineryvalidation.NameIsDNSLabel
			}

			for _, msg := range validationFunc(backend.Service.Name, false) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("service", "name"), backend.Service.Name, msg))
			}
		}

		hasPortName := len(backend.Service.Port.Name) > 0
		hasPortNumber := backend.Service.Port.Number != 0
		if hasPortName && hasPortNumber {
			allErrs = append(allErrs, field.Invalid(fldPath, "", "cannot set both port name & port number"))
		} else if hasPortName {
			for _, msg := range validation.IsValidPortName(backend.Service.Port.Name) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("service", "port", "name"), backend.Service.Port.Name, msg))
			}
		} else if hasPortNumber {
			for _, msg := range validation.IsValidPortNum(int(backend.Service.Port.Number)) {
				allErrs = append(allErrs, field.Invalid(fldPath.Child("service", "port", "number"), backend.Service.Port.Number, msg))
			}
		} else {
			allErrs = append(allErrs, field.Required(fldPath, "port name or number is required"))
		}
	default:
		allErrs = append(allErrs, field.Invalid(fldPath, "", "resource or service backend is required"))
	}
	return allErrs
}

// ValidateIngressClassName 校验给定名称能否用作 IngressClass 名称。
var ValidateIngressClassName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateIngressClass 校验 IngressClass 资源的元数据和 spec。
func ValidateIngressClass(ingressClass *networking.IngressClass) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&ingressClass.ObjectMeta, false, ValidateIngressClassName, field.NewPath("metadata"))
	allErrs = append(allErrs, validateIngressClassSpec(&ingressClass.Spec, field.NewPath("spec"))...)
	return allErrs
}

// ValidateIngressClassUpdate 校验 IngressClass 更新，其中 controller 字段不可变。
func ValidateIngressClassUpdate(newIngressClass, oldIngressClass *networking.IngressClass) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&newIngressClass.ObjectMeta, &oldIngressClass.ObjectMeta, field.NewPath("metadata"))
	allErrs = append(allErrs, validateIngressClassSpecUpdate(&newIngressClass.Spec, &oldIngressClass.Spec, field.NewPath("spec"))...)
	allErrs = append(allErrs, ValidateIngressClass(newIngressClass)...)
	return allErrs
}

// validateIngressClassSpec 校验 IngressClassSpec 的 controller 和 parameters 字段。
func validateIngressClassSpec(spec *networking.IngressClassSpec, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(spec.Controller) > maxLenIngressClassController {
		allErrs = append(allErrs, field.TooLong(fldPath.Child("controller"), "" /*unused*/, maxLenIngressClassController))
	}
	allErrs = append(allErrs, validation.IsDomainPrefixedPath(fldPath.Child("controller"), spec.Controller)...)
	allErrs = append(allErrs, validateIngressClassParametersReference(spec.Parameters, fldPath.Child("parameters"))...)
	return allErrs
}

// validateIngressClassSpecUpdate 确保 IngressClassSpec 更新合法。
func validateIngressClassSpecUpdate(newSpec, oldSpec *networking.IngressClassSpec, fldPath *field.Path) field.ErrorList {
	// controller 决定由哪个 ingress 控制器实现该类，修改会导致已有 Ingress 行为突变，因此不可变。
	return apivalidation.ValidateImmutableField(newSpec.Controller, oldSpec.Controller, fldPath.Child("controller"))
}

// validateIngressTypedLocalObjectReference 校验 TypedLocalObjectReference 类型的 parameters 字段。
func validateIngressTypedLocalObjectReference(params *api.TypedLocalObjectReference, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if params == nil {
		return allErrs
	}

	if params.APIGroup != nil {
		for _, msg := range validation.IsDNS1123Subdomain(*params.APIGroup) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("apiGroup"), *params.APIGroup, msg))
		}
	}

	if params.Kind == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("kind"), ""))
	} else {
		for _, msg := range content.IsPathSegmentName(params.Kind) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("kind"), params.Kind, msg))
		}
	}

	if params.Name == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), ""))
	} else {
		for _, msg := range content.IsPathSegmentName(params.Name) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), params.Name, msg))
		}
	}

	return allErrs
}

// validateIngressClassParametersReference 校验 IngressClassParametersReference 的字段。
// parameters 之前是 `TypedLocalObjectReference` 类型，使用
// `validateIngressTypedLocalObjectReference()` 校验；本函数在其基础上扩展了
// 命名空间作用域引用新增字段的校验。
func validateIngressClassParametersReference(params *networking.IngressClassParametersReference, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if params == nil {
		return allErrs
	}

	if params.APIGroup != nil {
		for _, msg := range validation.IsDNS1123Subdomain(*params.APIGroup) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("apiGroup"), *params.APIGroup, msg))
		}
	}

	if params.Kind == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("kind"), "").MarkCoveredByDeclarative())
	} else {
		for _, msg := range content.IsPathSegmentName(params.Kind) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("kind"), params.Kind, msg))
		}
	}

	if params.Name == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), "").MarkCoveredByDeclarative())
	} else {
		for _, msg := range content.IsPathSegmentName(params.Name) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), params.Name, msg))
		}
	}

	// scope 决定参数是集群级还是命名空间级引用，必须显式指定以避免歧义。
	if params.Scope == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("scope"), ""))
		return allErrs
	}

	scope := ptr.Deref(params.Scope, "")

	if !supportedIngressClassParametersReferenceScopes.Has(scope) {
		allErrs = append(allErrs, field.NotSupported(fldPath.Child("scope"), scope,
			supportedIngressClassParametersReferenceScopes.List()))
	} else {
		// 命名空间级引用必须给出 namespace，并校验其名称合法性。
		if scope == networking.IngressClassParametersReferenceScopeNamespace {
			if params.Namespace == nil {
				allErrs = append(allErrs, field.Required(fldPath.Child("namespace"), "`parameters.scope` is set to 'Namespace'"))
			} else {
				for _, msg := range apivalidation.ValidateNamespaceName(*params.Namespace, false) {
					allErrs = append(allErrs, field.Invalid(fldPath.Child("namespace"), *params.Namespace, msg))
				}
			}
		}

		// 集群级引用不应携带 namespace，否则语义相互矛盾。
		if scope == networking.IngressClassParametersReferenceScopeCluster && params.Namespace != nil {
			allErrs = append(allErrs, field.Forbidden(fldPath.Child("namespace"), "`parameters.scope` is set to 'Cluster'"))
		}
	}

	return allErrs
}

// allowInvalidSecretName 判断旧 Ingress 中是否已经存在非法的 TLS secretName。
func allowInvalidSecretName(oldIngress *networking.Ingress) bool {
	if oldIngress != nil {
		for _, tls := range oldIngress.Spec.TLS {
			if len(validateTLSSecretName(tls.SecretName)) > 0 {
				// 为兼容已持久化的旧对象，即使 secretName 非法也必须允许更新。
				return true
			}
		}
	}
	return false
}

// validateTLSSecretName 校验 TLS secret 名称，空名称表示不引用 secret，视为合法。
func validateTLSSecretName(name string) []string {
	if len(name) == 0 {
		return nil
	}
	return apivalidation.ValidateSecretName(name, false)
}

// allowInvalidWildcardHostRule 判断旧 Ingress 中是否已经存在非法的通配符 host 规则。
func allowInvalidWildcardHostRule(oldIngress *networking.Ingress) bool {
	if oldIngress != nil {
		for _, rule := range oldIngress.Spec.Rules {
			if strings.Contains(rule.Host, "*") && len(validateIngressRuleValue(&rule.IngressRuleValue, nil, IngressValidationOptions{})) > 0 {
				// 为兼容历史遗留的非法数据，继续允许这类规则存在。
				return true
			}
		}
	}
	return false
}

// allowRelaxedServiceNameValidation 判断是否放宽后端 Service 名称校验。
// 若特性门控开启，或旧对象中已有仅满足 DNSLabel 而不满足 DNS1035Label 的名称，则放宽校验。
func allowRelaxedServiceNameValidation(oldIngress *networking.Ingress) bool {
	// 特性门控开启时直接放宽校验，无需再检查旧对象。
	if utilfeature.DefaultFeatureGate.Enabled(features.RelaxedServiceNameValidation) {
		return true
	}
	// 没有旧对象时按严格模式处理，避免误放行非法名称。
	if oldIngress == nil {
		return false
	}

	// 先检查默认后端中的 Service 名称。
	if oldIngress.Spec.DefaultBackend != nil && oldIngress.Spec.DefaultBackend.Service != nil {
		serviceName := oldIngress.Spec.DefaultBackend.Service.Name
		// 名称不满足 DNS1035Label 但满足 DNSLabel 时，允许它作为 Ingress 的 Service 名称，
		// 以免历史对象因校验收紧而无法更新。
		dnsLabelValidationErrors := apimachineryvalidation.NameIsDNSLabel(serviceName, false)
		dns1035LabelValidationErrors := apimachineryvalidation.NameIsDNS1035Label(serviceName, false)
		if len(dnsLabelValidationErrors) == 0 && len(dns1035LabelValidationErrors) > 0 {
			return true
		}
	}

	// 特性门控未开启时，再检查旧 Ingress 各条规则中的 Service 名称。
	for _, rule := range oldIngress.Spec.Rules {
		if rule.HTTP == nil {
			continue
		}
		for _, path := range rule.HTTP.Paths {
			if path.Backend.Service == nil {
				continue
			}
			serviceName := path.Backend.Service.Name
			// 同样只放行满足 DNSLabel 但不满足 DNS1035Label 的历史名称。
			dnsLabelValidationErrors := apimachineryvalidation.NameIsDNSLabel(serviceName, false)
			dns1035LabelValidationErrors := apimachineryvalidation.NameIsDNS1035Label(serviceName, false)
			if len(dnsLabelValidationErrors) == 0 && len(dns1035LabelValidationErrors) > 0 {
				return true
			}
		}
	}
	return false
}

// ValidateIPAddressName 校验名称是 IP 地址的十进制表示。
// IPAddress 不支持生成名称，因此不考虑 prefix 参数。
func ValidateIPAddressName(name string, prefix bool) []string {
	var errs []string

	allErrs := validation.IsValidIP(&field.Path{}, name)
	// 这里把 IsValidIP 返回的 field.Error 还原成字符串，
	// 因为调用方还会再把它转换回 field.Error。
	for _, err := range allErrs {
		errs = append(errs, err.Detail)
	}
	return errs
}

// ValidateIPAddress 校验 IPAddress 对象的元数据和 parentRef。
func ValidateIPAddress(ipAddress *networking.IPAddress) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&ipAddress.ObjectMeta, false, ValidateIPAddressName, field.NewPath("metadata"))
	errs := validateIPAddressParentReference(ipAddress.Spec.ParentRef, field.NewPath("spec"))
	allErrs = append(allErrs, errs...)
	return allErrs

}

// validateIPAddressParentReference 校验 IPAddress 的 ParentReference 存在且各字段合法。
func validateIPAddressParentReference(params *networking.ParentReference, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if params == nil {
		allErrs = append(allErrs, field.Required(fldPath.Child("parentRef"), "").MarkCoveredByDeclarative())
		return allErrs
	}

	fldPath = fldPath.Child("parentRef")
	// group 理论上是必填的，但 Service 所在的 Core group 用空字符串表示，因此不能强制校验非空。
	if params.Group != "" {
		for _, msg := range validation.IsDNS1123Subdomain(params.Group) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("group"), params.Group, msg))
		}
	}

	// resource 必填，用于指明父资源类型。
	if params.Resource == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("resource"), "").MarkCoveredByDeclarative())
	} else {
		for _, msg := range content.IsPathSegmentName(params.Resource) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("resource"), params.Resource, msg))
		}
	}

	// name 必填，用于定位具体的父资源。
	if params.Name == "" {
		allErrs = append(allErrs, field.Required(fldPath.Child("name"), "").MarkCoveredByDeclarative())
	} else {
		for _, msg := range content.IsPathSegmentName(params.Name) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("name"), params.Name, msg))
		}
	}

	// namespace 可选，仅在父资源为命名空间级对象时填写。
	if params.Namespace != "" {
		for _, msg := range content.IsPathSegmentName(params.Namespace) {
			allErrs = append(allErrs, field.Invalid(fldPath.Child("namespace"), params.Namespace, msg))
		}
	}
	return allErrs
}

// ValidateIPAddressUpdate 校验 IPAddress 更新，其中 spec.parentRef 不可变。
func ValidateIPAddressUpdate(update, old *networking.IPAddress) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, apivalidation.ValidateObjectMetaUpdate(&update.ObjectMeta, &old.ObjectMeta, field.NewPath("metadata"))...)
	allErrs = append(allErrs, apivalidation.ValidateImmutableField(update.Spec.ParentRef, old.Spec.ParentRef, field.NewPath("spec").Child("parentRef")).MarkCoveredByDeclarative().WithOrigin("immutable")...)
	return allErrs
}

// ValidateServiceCIDRName 校验 ServiceCIDR 名称是否为合法的 DNS 子域。
var ValidateServiceCIDRName = apimachineryvalidation.NameIsDNSSubdomain

// ValidateServiceCIDR 校验 ServiceCIDR 对象的元数据和 CIDR 列表。
func ValidateServiceCIDR(cidrConfig *networking.ServiceCIDR) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMeta(&cidrConfig.ObjectMeta, false, ValidateServiceCIDRName, field.NewPath("metadata"))
	allErrs = append(allErrs, validateServiceCIDRSpec(&cidrConfig.Spec, field.NewPath("spec", "cidrs"))...)
	return allErrs
}

// validateServiceCIDRSpec 校验 CIDR 列表最多两个值，且不能重复同一地址族。
func validateServiceCIDRSpec(cidrConfigSpec *networking.ServiceCIDRSpec, fieldPath *field.Path) field.ErrorList {
	var allErrs field.ErrorList
	if len(cidrConfigSpec.CIDRs) == 0 {
		allErrs = append(allErrs, field.Required(fieldPath, "at least one CIDR required"))
		return allErrs
	}

	if len(cidrConfigSpec.CIDRs) > 2 {
		allErrs = append(allErrs, field.Invalid(fieldPath, cidrConfigSpec, "may only hold up to 2 values"))
		return allErrs
	}

	for i, cidr := range cidrConfigSpec.CIDRs {
		allErrs = append(allErrs, validation.IsValidCIDR(fieldPath.Index(i), cidr)...)
	}

	// 双栈场景下每种地址族最多一个 CIDR，重复的地址族没有意义。
	if len(cidrConfigSpec.CIDRs) == 2 &&
		netutils.IPFamilyOfCIDRString(cidrConfigSpec.CIDRs[0]) == netutils.IPFamilyOfCIDRString(cidrConfigSpec.CIDRs[1]) &&
		netutils.IPFamilyOfCIDRString(cidrConfigSpec.CIDRs[0]) != netutils.IPFamilyUnknown {
		allErrs = append(allErrs, field.Invalid(fieldPath, cidrConfigSpec.CIDRs, "may specify no more than one IP for each IP family, i.e 192.168.0.0/24 and 2001:db8::/64"))
	}

	return allErrs
}

// ValidateServiceCIDRUpdate 校验 ServiceCIDR 更新，只允许从单栈扩展为双栈。
func ValidateServiceCIDRUpdate(update, old *networking.ServiceCIDR) field.ErrorList {
	var allErrs field.ErrorList
	allErrs = append(allErrs, apivalidation.ValidateObjectMetaUpdate(&update.ObjectMeta, &old.ObjectMeta, field.NewPath("metadata"))...)
	switch {
	// CIDR 数量不变时，每一个 CIDR 都不允许修改。
	case len(old.Spec.CIDRs) == len(update.Spec.CIDRs):
		for i, ip := range old.Spec.CIDRs {
			if ip != update.Spec.CIDRs[i] {
				allErrs = append(allErrs, field.Invalid(field.NewPath("spec").Child("cidrs").Index(i), update.Spec.CIDRs[i], apimachineryvalidation.FieldImmutableErrorMsg))
			}
		}
		// 从单栈扩展为双栈时允许新增一个 CIDR。
		// ref: https://issues.k8s.io/131261
	case len(old.Spec.CIDRs) == 1 && len(update.Spec.CIDRs) == 2:
		// 已有的 CIDR 仍然不允许修改，只能追加另一个地址族的 CIDR。
		if update.Spec.CIDRs[0] != old.Spec.CIDRs[0] {
			allErrs = append(allErrs, field.Invalid(field.NewPath("spec").Child("cidrs").Index(0), update.Spec.CIDRs[0], apimachineryvalidation.FieldImmutableErrorMsg))
		}
		// 按规格重新校验整个 CIDR 列表，确保新增的 CIDR 合法。
		allErrs = append(allErrs, validateServiceCIDRSpec(&update.Spec, field.NewPath("spec", "cidrs"))...)

		// 其余变更（如删除 CIDR 或改成两个以上）一律不允许。
	default:
		allErrs = append(allErrs, field.Invalid(field.NewPath("spec").Child("cidrs"), update.Spec.CIDRs, apimachineryvalidation.FieldImmutableErrorMsg))
	}
	return allErrs
}

// ValidateServiceCIDRStatusUpdate 校验 ServiceCIDR status 更新，只检查元数据。
func ValidateServiceCIDRStatusUpdate(update, old *networking.ServiceCIDR) field.ErrorList {
	allErrs := apivalidation.ValidateObjectMetaUpdate(&update.ObjectMeta, &old.ObjectMeta, field.NewPath("metadata"))
	return allErrs
}
