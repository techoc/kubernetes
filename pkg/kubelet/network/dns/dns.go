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

package dns

import (
	"context"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strings"

	v1 "k8s.io/api/core/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	utilvalidation "k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/tools/record"
	runtimeapi "k8s.io/cri-api/pkg/apis/runtime/v1"
	"k8s.io/kubernetes/pkg/apis/core/validation"
	kubecontainer "k8s.io/kubernetes/pkg/kubelet/container"
	"k8s.io/kubernetes/pkg/kubelet/util/format"

	"k8s.io/klog/v2"
	utilio "k8s.io/utils/io"
	utilnet "k8s.io/utils/net"
)

// defaultDNSOptions 是未显式指定 DNS options 时使用的默认值。
var (
	// 默认的 DNS options 字符串。
	defaultDNSOptions = []string{"ndots:5"}
)

// podDNSType 描述 pod 需要使用的 DNS 配置类型。
type podDNSType int

// podDNSType 的各取值。
const (
	// podDNSCluster 表示使用集群 DNS。
	podDNSCluster podDNSType = iota
	// podDNSHost 表示使用宿主机 DNS 配置。
	podDNSHost
	// podDNSNone 表示不配置 DNS。
	podDNSNone
)

// 读取 resolv.conf 时的硬性上限。
const (
	// maxResolvConfLength 限制读取 resolv.conf 的最大字节数，防止超大文件拖垮 kubelet。
	maxResolvConfLength = 10 * 1 << 20 // 10MB
)

// Configurer 负责在启动 pod 时为其配置 DNS 解析器。
type Configurer struct {
	recorder         record.EventRecorderLogger
	getHostDNSConfig func(klog.Logger, string) (*runtimeapi.DNSConfig, error)
	nodeRef          *v1.ObjectReference
	nodeIPs          []net.IP

	// 非 nil 时，作为容器使用的 DNS 服务器列表。
	clusterDNS []net.IP
	// 非空时，作为容器 DNS 搜索域使用。
	ClusterDomain string
	// 用于生成容器 DNS 解析配置文件的模板路径（宿主机上的 resolv.conf）。
	// 可与 clusterDomain、clusterDNS 配合使用。
	ResolverConfig string
}

// NewConfigurer 返回一个用于启动 pod 的 DNS 配置器。
func NewConfigurer(recorder record.EventRecorderLogger, nodeRef *v1.ObjectReference, nodeIPs []net.IP, clusterDNS []net.IP, clusterDomain, resolverConfig string) *Configurer {
	return &Configurer{
		recorder:         recorder,
		getHostDNSConfig: getHostDNSConfig,
		nodeRef:          nodeRef,
		nodeIPs:          nodeIPs,
		clusterDNS:       clusterDNS,
		ClusterDomain:    clusterDomain,
		ResolverConfig:   resolverConfig,
	}
}

// omitDuplicates 去除字符串列表中的重复项，保留首次出现的顺序。
func omitDuplicates(strs []string) []string {
	// 用 map 记录已出现过的字符串，实现 O(1) 的去重判断。
	uniqueStrs := make(map[string]bool)

	var ret []string
	for _, str := range strs {
		if !uniqueStrs[str] {
			ret = append(ret, str)
			uniqueStrs[str] = true
		}
	}
	return ret
}

// formDNSSearchFitsLimits 裁剪 DNS 搜索路径列表，使其不超过内核/校验允许的上限；
// 一旦发生裁剪，会通过事件和日志报告被省略的搜索路径。
func (c *Configurer) formDNSSearchFitsLimits(logger klog.Logger, composedSearch []string, pod *v1.Pod) []string {
	limitsExceeded := false

	maxDNSSearchPaths, maxDNSSearchListChars := validation.MaxDNSSearchPaths, validation.MaxDNSSearchListChars

	if len(composedSearch) > maxDNSSearchPaths {
		composedSearch = composedSearch[:maxDNSSearchPaths]
		limitsExceeded = true
	}

	// 部分 DNS 解析器（如 glibc 2.28）在搜索路径超过 255 字符时会直接 abort()，必须过滤掉这类路径。
	l := 0
	for _, search := range composedSearch {
		if len(search) > utilvalidation.DNS1123SubdomainMaxLength {
			limitsExceeded = true
			continue
		}
		composedSearch[l] = search
		l++
	}
	composedSearch = composedSearch[:l]

	if resolvSearchLineStrLen := len(strings.Join(composedSearch, " ")); resolvSearchLineStrLen > maxDNSSearchListChars {
		cutDomainsNum := 0
		cutDomainsLen := 0
		for i := len(composedSearch) - 1; i >= 0; i-- {
			cutDomainsLen += len(composedSearch[i]) + 1
			cutDomainsNum++

			if (resolvSearchLineStrLen - cutDomainsLen) <= maxDNSSearchListChars {
				break
			}
		}

		composedSearch = composedSearch[:(len(composedSearch) - cutDomainsNum)]
		limitsExceeded = true
	}

	if limitsExceeded {
		err := fmt.Errorf("Search Line limits were exceeded, some search paths have been omitted, the applied search line is: %s", strings.Join(composedSearch, " "))
		c.recorder.WithLogger(logger).Event(pod, v1.EventTypeWarning, "DNSConfigForming", err.Error())
		logger.Error(err, "Search Line limits exceeded")
	}
	return composedSearch
}

// formDNSNameserversFitsLimits 裁剪 DNS 服务器列表，使其不超过允许的最大数量，
// 超出上限时通过事件和日志报告被省略的服务器。
func (c *Configurer) formDNSNameserversFitsLimits(logger klog.Logger, nameservers []string, pod *v1.Pod) []string {
	if len(nameservers) > validation.MaxDNSNameservers {
		nameservers = nameservers[0:validation.MaxDNSNameservers]
		err := fmt.Errorf("Nameserver limits were exceeded, some nameservers have been omitted, the applied nameserver line is: %s", strings.Join(nameservers, " "))
		c.recorder.WithLogger(logger).Event(pod, v1.EventTypeWarning, "DNSConfigForming", err.Error())
		logger.Error(err, "Nameserver limits exceeded")
	}
	return nameservers
}

// formDNSConfigFitsLimits 对 DNS 配置中的服务器和搜索路径分别做上限裁剪。
func (c *Configurer) formDNSConfigFitsLimits(logger klog.Logger, dnsConfig *runtimeapi.DNSConfig, pod *v1.Pod) *runtimeapi.DNSConfig {
	dnsConfig.Servers = c.formDNSNameserversFitsLimits(logger, dnsConfig.Servers, pod)
	dnsConfig.Searches = c.formDNSSearchFitsLimits(logger, dnsConfig.Searches, pod)
	return dnsConfig
}

// generateSearchesForDNSClusterFirst 在宿主机搜索路径前追加集群内部搜索域
// （命名空间.svc.集群域、svc.集群域、集群域），并去除重复项。
func (c *Configurer) generateSearchesForDNSClusterFirst(hostSearch []string, pod *v1.Pod) []string {
	if c.ClusterDomain == "" {
		return hostSearch
	}

	// 搜索域从具体到宽泛排列，保证 pod 内的短域名优先解析到集群内部服务。
	nsSvcDomain := fmt.Sprintf("%s.svc.%s", pod.Namespace, c.ClusterDomain)
	svcDomain := fmt.Sprintf("svc.%s", c.ClusterDomain)
	clusterSearch := []string{nsSvcDomain, svcDomain, c.ClusterDomain}

	// 去重后再拼接宿主机搜索路径，避免同一域名重复出现浪费解析次数。
	return omitDuplicates(append(clusterSearch, hostSearch...))
}

// CheckLimitsForResolvConf 检查宿主机 resolv.conf 的搜索路径数量与长度是否超出限制。
func (c *Configurer) CheckLimitsForResolvConf(logger klog.Logger) {
	f, err := os.Open(c.ResolverConfig)
	if err != nil {
		c.recorder.WithLogger(logger).Event(c.nodeRef, v1.EventTypeWarning, "CheckLimitsForResolvConf", err.Error())
		logger.V(4).Info("Check limits for resolv.conf failed at file open", "err", err)
		return
	}
	defer f.Close()

	_, hostSearch, _, err := parseResolvConf(f)
	if err != nil {
		c.recorder.WithLogger(logger).Event(c.nodeRef, v1.EventTypeWarning, "CheckLimitsForResolvConf", err.Error())
		logger.V(4).Info("Check limits for resolv.conf failed at parse resolv.conf", "err", err)
		return
	}

	domainCountLimit, maxDNSSearchListChars := validation.MaxDNSSearchPaths, validation.MaxDNSSearchListChars

	// 集群域会额外占用 3 个搜索路径名额，先从上限中扣除，保证最终搜索列表不超限。
	if c.ClusterDomain != "" {
		domainCountLimit -= 3
	}

	if len(hostSearch) > domainCountLimit {
		log := fmt.Sprintf("Resolv.conf file '%s' contains search line consisting of more than %d domains!", c.ResolverConfig, domainCountLimit)
		c.recorder.WithLogger(logger).Event(c.nodeRef, v1.EventTypeWarning, "CheckLimitsForResolvConf", log)
		logger.V(4).Info("Check limits for resolv.conf failed", "eventlog", log)
		return
	}

	for _, search := range hostSearch {
		if len(search) > utilvalidation.DNS1123SubdomainMaxLength {
			log := fmt.Sprintf("Resolv.conf file %q contains a search path which length is more than allowed %d chars!", c.ResolverConfig, utilvalidation.DNS1123SubdomainMaxLength)
			c.recorder.WithLogger(logger).Event(c.nodeRef, v1.EventTypeWarning, "CheckLimitsForResolvConf", log)
			logger.V(4).Info("Check limits for resolv.conf failed", "eventlog", log)
			return
		}
	}

	if len(strings.Join(hostSearch, " ")) > maxDNSSearchListChars {
		log := fmt.Sprintf("Resolv.conf file '%s' contains search line which length is more than allowed %d chars!", c.ResolverConfig, maxDNSSearchListChars)
		c.recorder.WithLogger(logger).Event(c.nodeRef, v1.EventTypeWarning, "CheckLimitsForResolvConf", log)
		logger.V(4).Info("Check limits for resolv.conf failed", "eventlog", log)
		return
	}
}

// parseResolvConf 从给定的 reader 读取 resolv.conf 内容，解析出 nameserver、search 和 options，
// 解析过程中的行级错误会聚合后返回。
func parseResolvConf(reader io.Reader) (nameservers []string, searches []string, options []string, err error) {
	file, err := utilio.ReadAtMost(reader, maxResolvConfLength)
	if err != nil {
		return nil, nil, nil, err
	}

	// "nameserver 1.2.3.4" 形式的行会累积为多个服务器。
	nameservers = []string{}

	// "search example.com" 形式的行会覆盖之前的搜索路径，以最后一行为准。
	searches = []string{}

	// "option ndots:5 attempts:2" 形式的行会覆盖之前的选项，以最后一行为准。
	// 每个选项作为数组中的一个元素记录。
	options = []string{}

	var allErrors []error
	lines := strings.Split(string(file), "\n")
	for l := range lines {
		trimmed := strings.TrimSpace(lines[l])
		if strings.HasPrefix(trimmed, "#") {
			continue
		}
		fields := strings.Fields(trimmed)
		if len(fields) == 0 {
			continue
		}
		if fields[0] == "nameserver" {
			if len(fields) >= 2 {
				nameservers = append(nameservers, fields[1])
			} else {
				allErrors = append(allErrors, fmt.Errorf("nameserver list is empty "))
			}
		}
		if fields[0] == "search" {
			// 对 search 字段做规范化，使带/不带结尾点号的同一域名只计数一次，
			// 避免触发搜索路径的校验上限。
			searches = []string{}
			for _, s := range fields[1:] {
				if s != "." {
					searches = append(searches, strings.TrimSuffix(s, "."))
				}
			}
		}
		if fields[0] == "options" {
			options = appendOptions(options, fields[1:]...)
		}
	}

	return nameservers, searches, options, utilerrors.NewAggregate(allErrors)
}

// getDNSConfig 读取类似 resolv.conf 的文件并返回其中的 DNS 配置项。
// 当 resolverConfigFile 为空字符串时返回一个空的 DNSConfig。
func getDNSConfig(logger klog.Logger, resolverConfigFile string) (*runtimeapi.DNSConfig, error) {
	var hostDNS, hostSearch, hostOptions []string
	// 解析宿主机的 DNS 配置。
	if resolverConfigFile != "" {
		f, err := os.Open(resolverConfigFile)
		if err != nil {
			logger.Error(err, "Could not open resolv conf file.")
			return nil, err
		}
		defer f.Close()

		hostDNS, hostSearch, hostOptions, err = parseResolvConf(f)
		if err != nil {
			err := fmt.Errorf("Encountered error while parsing resolv conf file. Error: %w", err)
			logger.Error(err, "Could not parse resolv conf file.")
			return nil, err
		}
	}
	return &runtimeapi.DNSConfig{
		Servers:  hostDNS,
		Searches: hostSearch,
		Options:  hostOptions,
	}, nil
}

// getPodDNSType 根据 pod 的 DNSPolicy 判定其 DNS 配置类型。
func getPodDNSType(pod *v1.Pod) (podDNSType, error) {
	dnsPolicy := pod.Spec.DNSPolicy
	switch dnsPolicy {
	case v1.DNSNone:
		return podDNSNone, nil
	case v1.DNSClusterFirstWithHostNet:
		return podDNSCluster, nil
	case v1.DNSClusterFirst:
		if !kubecontainer.IsHostNetworkPod(pod) {
			return podDNSCluster, nil
		}
		// 主机网络 pod 无法使用 ClusterFirst 的集群 DNS，回退为使用宿主机 DNS 配置。
		fallthrough
	case v1.DNSDefault:
		return podDNSHost, nil
	}
	// 正常情况下 kube-apiserver 已经拒绝非法的 DNSPolicy，这里不应被触发。
	return podDNSCluster, fmt.Errorf("invalid DNSPolicy=%v", dnsPolicy)
}

// mergeDNSOptions 合并 DNS options。出现重复时，PodDNSConfigOption 提供的条目会覆盖已有条目。
func mergeDNSOptions(existingDNSConfigOptions []string, dnsConfigOptions []v1.PodDNSConfigOption) []string {
	// 先以选项名为 key 建立映射，便于按名字覆盖重复项。
	optionsMap := make(map[string]string)
	for _, op := range existingDNSConfigOptions {
		if index := strings.Index(op, ":"); index != -1 {
			optionsMap[op[:index]] = op[index+1:]
		} else {
			optionsMap[op] = ""
		}
	}
	for _, op := range dnsConfigOptions {
		if op.Value != nil {
			optionsMap[op.Name] = *op.Value
		} else {
			optionsMap[op.Name] = ""
		}
	}
	// 重新把合并结果转换为字符串数组，保证返回格式与入参一致。
	options := []string{}
	for opName, opValue := range optionsMap {
		op := opName
		if opValue != "" {
			op = op + ":" + opValue
		}
		options = append(options, op)
	}
	return options
}

// appendOptions 把新选项追加到已有列表，但不添加重复项。
// 无论新选项出现在新行还是同一行，都会覆盖之前同名列的选项。
func appendOptions(options []string, newOption ...string) []string {
	// 以选项名为 key 建映射，后写覆盖先写，实现去重且以新值为准。
	var optionMap = make(map[string]string)
	for _, option := range options {
		optName := strings.Split(option, ":")[0]
		optionMap[optName] = option
	}
	for _, option := range newOption {
		optName := strings.Split(option, ":")[0]
		optionMap[optName] = option
	}

	options = []string{}
	for _, v := range optionMap {
		options = append(options, v)
	}
	return options
}

// appendDNSConfig 把 PodDNSConfig 提供的 DNS 服务器、搜索路径和选项追加到已有 DNS 配置中，
// 重复项会被合并。要求 existingDNSConfig 和 dnsConfig 均不为 nil。
func appendDNSConfig(existingDNSConfig *runtimeapi.DNSConfig, dnsConfig *v1.PodDNSConfig) *runtimeapi.DNSConfig {
	existingDNSConfig.Servers = omitDuplicates(append(existingDNSConfig.Servers, dnsConfig.Nameservers...))
	existingDNSConfig.Searches = omitDuplicates(append(existingDNSConfig.Searches, dnsConfig.Searches...))
	existingDNSConfig.Options = mergeDNSOptions(existingDNSConfig.Options, dnsConfig.Options)
	return existingDNSConfig
}

// GetPodDNS 返回 pod 最终生效的 DNS 配置：先取宿主机配置，再按 DNSPolicy 调整，
// 最后叠加 pod 的自定义 DNSConfig 并做上限裁剪。
func (c *Configurer) GetPodDNS(ctx context.Context, pod *v1.Pod) (*runtimeapi.DNSConfig, error) {
	logger := klog.FromContext(ctx)
	dnsConfig, err := c.getHostDNSConfig(logger, c.ResolverConfig)
	if err != nil {
		return nil, err
	}

	dnsType, err := getPodDNSType(pod)
	if err != nil {
		logger.Error(err, "Failed to get DNS type for pod. Falling back to DNSClusterFirst policy.", "pod", klog.KObj(pod))
		dnsType = podDNSCluster
	}
	switch dnsType {
	case podDNSNone:
		// DNSNone 策略不配置任何 DNS，因此以空配置为基准。
		dnsConfig = &runtimeapi.DNSConfig{}
	case podDNSCluster:
		if len(c.clusterDNS) != 0 {
			// ClusterFirst 策略下容器只配置集群 DNS 服务器，由集群 DNS 自身负责
			// 向外部服务器转发无法解析的查询。
			dnsConfig.Servers = []string{}
			for _, ip := range c.clusterDNS {
				dnsConfig.Servers = append(dnsConfig.Servers, ip.String())
			}
			dnsConfig.Searches = c.generateSearchesForDNSClusterFirst(dnsConfig.Searches, pod)
			dnsConfig.Options = defaultDNSOptions
			break
		}
		// 未配置 clusterDNS 时无法按 ClusterFirst 创建 pod，记录事件并回退到 Default 策略。
		nodeErrorMsg := fmt.Sprintf("kubelet does not have ClusterDNS IP configured and cannot create Pod using %q policy. Falling back to %q policy.", v1.DNSClusterFirst, v1.DNSDefault)
		c.recorder.WithLogger(logger).Eventf(c.nodeRef, v1.EventTypeWarning, "MissingClusterDNS", "%s", nodeErrorMsg)
		c.recorder.WithLogger(logger).Eventf(pod, v1.EventTypeWarning, "MissingClusterDNS", "pod: %q. %s", format.Pod(pod), nodeErrorMsg)
		// 回退到 DNSDefault。
		fallthrough
	case podDNSHost:
		// 解析器配置为空时，用本机回环地址覆盖 docker 默认行为（默认读 /etc/resolv.conf），
		// 等价于按 bind 文档"未指定 nameserver 时使用本机"的行为，从而禁用外部 DNS 查询。
		if c.ResolverConfig == "" {
			for _, nodeIP := range c.nodeIPs {
				if utilnet.IsIPv6(nodeIP) {
					dnsConfig.Servers = append(dnsConfig.Servers, "::1")
				} else {
					dnsConfig.Servers = append(dnsConfig.Servers, "127.0.0.1")
				}
			}
			if len(dnsConfig.Servers) == 0 {
				dnsConfig.Servers = append(dnsConfig.Servers, "127.0.0.1")
			}
			dnsConfig.Searches = []string{"."}
		}
	}

	if pod.Spec.DNSConfig != nil {
		dnsConfig = appendDNSConfig(dnsConfig, pod.Spec.DNSConfig)
	}
	return c.formDNSConfigFitsLimits(logger, dnsConfig, pod), nil
}

// SetupDNSinContainerizedMounter 用 kubelet 的 ClusterDNS 替换 containerized-mounter 根文件系统
// 中 /etc/resolv.conf 的 nameserver；若配置了 ResolverConfig，还会追加其搜索路径。
func (c *Configurer) SetupDNSinContainerizedMounter(logger klog.Logger, mounterPath string) {
	resolvePath := filepath.Join(strings.TrimSuffix(mounterPath, "/mounter"), "rootfs", "etc", "resolv.conf")
	dnsString := ""
	for _, dns := range c.clusterDNS {
		dnsString = dnsString + fmt.Sprintf("nameserver %s\n", dns)
	}
	// 只替换 nameserver 部分，搜索路径从宿主机 resolv.conf 中读取并追加，
	// 保证容器化挂载器仍能用集群内的域名解析。
	if c.ResolverConfig != "" {
		f, err := os.Open(c.ResolverConfig)
		if err != nil {
			logger.Error(err, "Could not open resolverConf file")
		} else {
			defer f.Close()
			_, hostSearch, _, err := parseResolvConf(f)
			if err != nil {
				logger.Error(err, "Error for parsing the resolv.conf file")
			} else {
				dnsString = dnsString + "search"
				for _, search := range hostSearch {
					dnsString = dnsString + fmt.Sprintf(" %s", search)
				}
				dnsString = dnsString + "\n"
			}
		}
	}
	if err := os.WriteFile(resolvePath, []byte(dnsString), 0600); err != nil {
		logger.Error(err, "Could not write dns nameserver in the file", "path", resolvePath)
	}
}
