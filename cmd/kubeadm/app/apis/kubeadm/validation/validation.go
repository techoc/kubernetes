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

package validation

import (
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/distribution/reference"
	"github.com/spf13/pflag"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/apimachinery/pkg/util/validation/field"
	bootstrapapi "k8s.io/cluster-bootstrap/token/api"
	bootstraputil "k8s.io/cluster-bootstrap/token/util"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"

	bootstraptokenv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/bootstraptoken/v1"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmapiv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm/v1beta4"
	kubeadmcmdoptions "k8s.io/kubernetes/cmd/kubeadm/app/cmd/options"
	"k8s.io/kubernetes/cmd/kubeadm/app/componentconfigs"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/features"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
)

// ValidateInitConfiguration 校验 InitConfiguration 对象并收集所有错误。
func ValidateInitConfiguration(c *kubeadm.InitConfiguration) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateNodeRegistrationOptions(&c.NodeRegistration, field.NewPath("nodeRegistration"))...)
	allErrs = append(allErrs, ValidateBootstrapTokens(c.BootstrapTokens, field.NewPath("bootstrapTokens"))...)
	allErrs = append(allErrs, ValidateClusterConfiguration(&c.ClusterConfiguration)...)
	// TODO(Arvinderpal): 双栈实现后更新 advertiseAddress 校验。
	allErrs = append(allErrs, ValidateAPIEndpoint(&c.LocalAPIEndpoint, field.NewPath("localAPIEndpoint"))...)
	allErrs = append(allErrs, ValidateCertificateKey(c.CertificateKey, field.NewPath("certificateKey"))...)
	return allErrs
}

// ValidateClusterConfiguration 校验 ClusterConfiguration 对象并收集所有错误。
func ValidateClusterConfiguration(c *kubeadm.ClusterConfiguration) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateDNS(&c.DNS, field.NewPath("dns"))...)
	allErrs = append(allErrs, ValidateNetworking(c, field.NewPath("networking"))...)
	allErrs = append(allErrs, ValidateAPIServer(&c.APIServer, field.NewPath("apiServer"))...)
	allErrs = append(allErrs, ValidateControllerManager(&c.ControllerManager, field.NewPath("controllerManager"))...)
	allErrs = append(allErrs, ValidateScheduler(&c.Scheduler, field.NewPath("scheduler"))...)
	allErrs = append(allErrs, ValidateAbsolutePath(c.CertificatesDir, field.NewPath("certificatesDir"))...)
	allErrs = append(allErrs, ValidateFeatureGates(c.FeatureGates, field.NewPath("featureGates"))...)
	allErrs = append(allErrs, ValidateHostPort(c.ControlPlaneEndpoint, field.NewPath("controlPlaneEndpoint"))...)
	allErrs = append(allErrs, ValidateImageRepository(c.ImageRepository, field.NewPath("imageRepository"))...)
	allErrs = append(allErrs, ValidateEtcd(&c.Etcd, field.NewPath("etcd"))...)
	allErrs = append(allErrs, ValidateEncryptionAlgorithm(c.EncryptionAlgorithm, field.NewPath("encryptionAlgorithm"))...)
	allErrs = append(allErrs, componentconfigs.Validate(c)...)
	for _, certError := range ValidateCertValidity(c) {
		klog.Warningf("WARNING: %s", certError.Error())
	}
	return allErrs
}

// ValidateAPIServer 校验 APIServer 对象并收集所有错误。
func ValidateAPIServer(a *kubeadm.APIServer, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateCertSANs(a.CertSANs, fldPath.Child("certSANs"))...)
	allErrs = append(allErrs, ValidateExtraArgs(a.ExtraArgs, fldPath.Child("extraArgs"))...)
	return allErrs
}

// ValidateControllerManager 校验控制器管理器对象并收集所有错误。
func ValidateControllerManager(a *kubeadm.ControlPlaneComponent, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateExtraArgs(a.ExtraArgs, fldPath.Child("extraArgs"))...)
	return allErrs
}

// ValidateScheduler 校验调度器对象并收集所有错误。
func ValidateScheduler(a *kubeadm.ControlPlaneComponent, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateExtraArgs(a.ExtraArgs, fldPath.Child("extraArgs"))...)
	return allErrs
}

// ValidateJoinConfiguration 校验节点配置并收集所有错误。
func ValidateJoinConfiguration(c *kubeadm.JoinConfiguration) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateDiscovery(&c.Discovery, field.NewPath("discovery"))...)
	allErrs = append(allErrs, ValidateNodeRegistrationOptions(&c.NodeRegistration, field.NewPath("nodeRegistration"))...)
	allErrs = append(allErrs, ValidateJoinControlPlane(c.ControlPlane, field.NewPath("controlPlane"))...)

	if !isAbs(c.CACertPath) || !strings.HasSuffix(c.CACertPath, ".crt") {
		allErrs = append(allErrs, field.Invalid(field.NewPath("caCertPath"), c.CACertPath, "the ca certificate path must be an absolute path"))
	}
	return allErrs
}

// ValidateJoinControlPlane 校验加入控制平面的配置并收集所有错误。
func ValidateJoinControlPlane(c *kubeadm.JoinControlPlane, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if c != nil {
		allErrs = append(allErrs, ValidateAPIEndpoint(&c.LocalAPIEndpoint, fldPath.Child("localAPIEndpoint"))...)
		allErrs = append(allErrs, ValidateCertificateKey(c.CertificateKey, fldPath.Child("certificateKey"))...)
	}
	return allErrs
}

// ValidateNodeRegistrationOptions 校验 NodeRegistrationOptions 对象并收集所有错误。
func ValidateNodeRegistrationOptions(nro *kubeadm.NodeRegistrationOptions, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if len(nro.Name) == 0 {
		allErrs = append(allErrs, field.Required(fldPath, "--node-name or .nodeRegistration.name in the config file is a required value. It seems like this value couldn't be automatically detected in your environment, please specify the desired value using the CLI or config file."))
	} else {
		nameFldPath := fldPath.Child("name")
		for _, err := range validation.IsDNS1123Subdomain(nro.Name) {
			allErrs = append(allErrs, field.Invalid(nameFldPath, nro.Name, err))
		}
	}
	allErrs = append(allErrs, ValidateSocketPath(nro.CRISocket, fldPath.Child("criSocket"))...)
	allErrs = append(allErrs, ValidateExtraArgs(nro.KubeletExtraArgs, fldPath.Child("kubeletExtraArgs"))...)
	allErrs = append(allErrs, ValidateImagePullPolicy(nro.ImagePullPolicy, fldPath.Child("imagePullPolicy"))...)
	// TODO: 未来可考虑使用 pkg/apis/core/validation 中的 validateNodeTaints() 等逻辑校验 .Taints。
	return allErrs
}

// ValidateDiscovery 校验发现相关配置并收集所有错误。
func ValidateDiscovery(d *kubeadm.Discovery, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if d.BootstrapToken == nil && d.File == nil {
		allErrs = append(allErrs, field.Invalid(fldPath, "", "bootstrapToken or file must be set"))
	}

	if d.BootstrapToken != nil && d.File != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, "", "bootstrapToken and file cannot both be set"))
	}

	if d.BootstrapToken != nil {
		allErrs = append(allErrs, ValidateDiscoveryBootstrapToken(d.BootstrapToken, fldPath.Child("bootstrapToken"))...)
		allErrs = append(allErrs, ValidateToken(d.TLSBootstrapToken, fldPath.Child("tlsBootstrapToken"))...)
	}

	if d.File != nil {
		allErrs = append(allErrs, ValidateDiscoveryFile(d.File, fldPath.Child("file"))...)
		if len(d.TLSBootstrapToken) != 0 {
			allErrs = append(allErrs, ValidateToken(d.TLSBootstrapToken, fldPath.Child("tlsBootstrapToken"))...)
		}
	}

	return allErrs
}

// ValidateDiscoveryBootstrapToken 校验基于 Bootstrap Token 的发现配置并收集所有错误。
func ValidateDiscoveryBootstrapToken(b *kubeadm.BootstrapTokenDiscovery, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(b.APIServerEndpoint) == 0 {
		allErrs = append(allErrs, field.Required(fldPath, "APIServerEndpoint is not set"))
	}

	if len(b.CACertHashes) == 0 && !b.UnsafeSkipCAVerification {
		allErrs = append(allErrs, field.Invalid(fldPath.Child("caCertHashes"), "", "using token-based discovery without caCertHashes can be unsafe. Set unsafeSkipCAVerification as true in your kubeadm config file or pass --discovery-token-unsafe-skip-ca-verification flag to continue"))
	}

	allErrs = append(allErrs, ValidateToken(b.Token, fldPath.Child(kubeadmcmdoptions.TokenStr))...)
	allErrs = append(allErrs, ValidateDiscoveryTokenAPIServer(b.APIServerEndpoint, fldPath.Child("apiServerEndpoint"))...)

	return allErrs
}

// ValidateDiscoveryFile 校验文件发现配置并收集所有错误。
func ValidateDiscoveryFile(f *kubeadm.FileDiscovery, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	allErrs = append(allErrs, ValidateDiscoveryKubeConfigPath(f.KubeConfigPath, fldPath.Child("kubeConfigPath"))...)

	return allErrs
}

// ValidateDiscoveryTokenAPIServer 校验用于发现的 API 服务器令牌。
func ValidateDiscoveryTokenAPIServer(apiServer string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	_, _, err := net.SplitHostPort(apiServer)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, apiServer, err.Error()))
	}
	return allErrs
}

// ValidateDiscoveryKubeConfigPath 校验发现文件的位置。
func ValidateDiscoveryKubeConfigPath(discoveryFile string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	u, err := url.Parse(discoveryFile)
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, discoveryFile, "not a valid HTTPS URL or a file on disk"))
		return allErrs
	}

	if u.Scheme == "" {
		// 不带 scheme 的 URI 按文件处理。
		if _, err := os.Stat(discoveryFile); os.IsNotExist(err) {
			allErrs = append(allErrs, field.Invalid(fldPath, discoveryFile, "not a valid HTTPS URL or a file on disk"))
		}
		return allErrs
	}

	if u.Scheme != "https" {
		allErrs = append(allErrs, field.Invalid(fldPath, discoveryFile, "if a URL is used, the scheme must be https"))
	}
	return allErrs
}

// ValidateBootstrapTokens 校验 BootstrapToken 对象切片并收集所有错误。
func ValidateBootstrapTokens(bts []bootstraptokenv1.BootstrapToken, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for i, bt := range bts {
		btPath := fldPath.Child(fmt.Sprintf("%d", i))
		allErrs = append(allErrs, ValidateToken(bt.Token.String(), btPath.Child(kubeadmcmdoptions.TokenStr))...)
		allErrs = append(allErrs, ValidateTokenUsages(bt.Usages, btPath.Child(kubeadmcmdoptions.TokenUsages))...)
		allErrs = append(allErrs, ValidateTokenGroups(bt.Usages, bt.Groups, btPath.Child(kubeadmcmdoptions.TokenGroups))...)

		if bt.Expires != nil && bt.TTL != nil {
			allErrs = append(allErrs, field.Invalid(btPath, "", "the BootstrapToken .TTL and .Expires fields are mutually exclusive"))
		}
	}
	return allErrs
}

// ValidateToken 校验 Bootstrap Token 并收集所有错误。
func ValidateToken(token string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if !bootstraputil.IsValidBootstrapToken(token) {
		allErrs = append(allErrs, field.Invalid(fldPath, token, "the bootstrap token is invalid"))
	}

	return allErrs
}

// ValidateTokenGroups 校验令牌组并收集所有错误。
func ValidateTokenGroups(usages []string, groups []string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 只有用于身份验证时，设置组才有意义。
	usagesSet := sets.New(usages...)
	usageAuthentication := strings.TrimPrefix(bootstrapapi.BootstrapTokenUsageAuthentication, bootstrapapi.BootstrapTokenUsagePrefix)
	if len(groups) > 0 && !usagesSet.Has(usageAuthentication) {
		allErrs = append(allErrs, field.Invalid(fldPath, groups, fmt.Sprintf("token groups cannot be specified unless --usages includes %q", usageAuthentication)))
	}

	// 校验所有额外组名。
	for _, group := range groups {
		if err := bootstraputil.ValidateBootstrapGroupName(group); err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath, groups, err.Error()))
		}
	}

	return allErrs
}

// ValidateTokenUsages 校验令牌用途并收集所有错误。
func ValidateTokenUsages(usages []string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 校验用途。
	if err := bootstraputil.ValidateUsages(usages); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, usages, err.Error()))
	}

	return allErrs
}

// ValidateEtcd 校验 .Etcd 子结构并收集所有错误。
func ValidateEtcd(e *kubeadm.Etcd, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	localPath := fldPath.Child("local")
	externalPath := fldPath.Child("external")

	if e.Local == nil && e.External == nil {
		allErrs = append(allErrs, field.Invalid(fldPath, "", "either .Etcd.Local or .Etcd.External is required"))
		return allErrs
	}
	if e.Local != nil && e.External != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, "", ".Etcd.Local and .Etcd.External are mutually exclusive"))
		return allErrs
	}
	if e.Local != nil {
		allErrs = append(allErrs, ValidateAbsolutePath(e.Local.DataDir, localPath.Child("dataDir"))...)
		allErrs = append(allErrs, ValidateCertSANs(e.Local.ServerCertSANs, localPath.Child("serverCertSANs"))...)
		allErrs = append(allErrs, ValidateCertSANs(e.Local.PeerCertSANs, localPath.Child("peerCertSANs"))...)
		if len(e.Local.ImageRepository) > 0 {
			allErrs = append(allErrs, ValidateImageRepository(e.Local.ImageRepository, localPath.Child("imageRepository"))...)
		}
		allErrs = append(allErrs, ValidateExtraArgs(e.Local.ExtraArgs, localPath.Child("extraArgs"))...)
	}
	if e.External != nil {
		requireHTTPS := true
		// 未传入证书或密钥时，允许使用 HTTP scheme。
		if e.External.CAFile == "" && e.External.CertFile == "" && e.External.KeyFile == "" {
			requireHTTPS = false
		}
		// 证书和密钥必须同时设置或同时不设置。
		if (e.External.CertFile == "" && e.External.KeyFile != "") || (e.External.CertFile != "" && e.External.KeyFile == "") {
			allErrs = append(allErrs, field.Invalid(externalPath, "", "either both or none of .Etcd.External.CertFile and .Etcd.External.KeyFile must be set"))
		}
		// 同时设置证书和密钥时，还必须设置 CA。
		if e.External.CertFile != "" && e.External.KeyFile != "" && e.External.CAFile == "" {
			allErrs = append(allErrs, field.Invalid(externalPath, "", "setting .Etcd.External.CertFile and .Etcd.External.KeyFile requires .Etcd.External.CAFile"))
		}

		if len(e.External.Endpoints) == 0 {
			allErrs = append(allErrs, field.Invalid(externalPath.Child("endpoints"), "", "at least one endpoint must be specified"))
		} else {
			allErrs = append(allErrs, ValidateURLs(e.External.Endpoints, requireHTTPS, externalPath.Child("endpoints"))...)
		}

		allErrs = append(allErrs, ValidateURLs(e.External.HTTPEndpoints, false /* 要求 HTTPS */, externalPath.Child("httpEndpoints"))...)

		if e.External.CAFile != "" {
			allErrs = append(allErrs, ValidateAbsolutePath(e.External.CAFile, externalPath.Child("caFile"))...)
		}
		if e.External.CertFile != "" {
			allErrs = append(allErrs, ValidateAbsolutePath(e.External.CertFile, externalPath.Child("certFile"))...)
		}
		if e.External.KeyFile != "" {
			allErrs = append(allErrs, ValidateAbsolutePath(e.External.KeyFile, externalPath.Child("keyFile"))...)
		}
	}
	return allErrs
}

// ValidateEncryptionAlgorithm 校验公钥算法并收集所有错误。
func ValidateEncryptionAlgorithm(algo kubeadm.EncryptionAlgorithmType, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	knownAlgorithms := sets.New(
		kubeadm.EncryptionAlgorithmECDSAP384,
		kubeadm.EncryptionAlgorithmECDSAP256,
		kubeadm.EncryptionAlgorithmRSA2048,
		kubeadm.EncryptionAlgorithmRSA3072,
		kubeadm.EncryptionAlgorithmRSA4096,
	)
	if !knownAlgorithms.Has(algo) {
		msg := fmt.Sprintf("Invalid encryption algorithm %q. Must be one of %v", algo, sets.List(knownAlgorithms))
		allErrs = append(allErrs, field.Invalid(fldPath, algo, msg))
	}
	return allErrs
}

// ValidateCertSANs 校验备用名称并收集所有错误。
func ValidateCertSANs(altnames []string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, altname := range altnames {
		if errs := validation.IsDNS1123Subdomain(altname); len(errs) != 0 {
			if errs2 := validation.IsWildcardDNS1123Subdomain(altname); len(errs2) != 0 {
				if netutils.ParseIPSloppy(altname) == nil {
					allErrs = append(allErrs, field.Invalid(fldPath, altname, fmt.Sprintf("altname is not a valid IP address, DNS label or a DNS label with subdomain wildcards: %s; %s", strings.Join(errs, "; "), strings.Join(errs2, "; "))))
				}
			}
		}
	}
	return allErrs
}

// ValidateURLs 校验字符串切片中的 URL 可解析，并可选择强制要求使用 HTTPS。
func ValidateURLs(urls []string, requireHTTPS bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	for _, urlstr := range urls {
		u, err := url.Parse(urlstr)
		if err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath, urlstr, fmt.Sprintf("URL parse error: %v", err)))
			continue
		}
		if requireHTTPS && u.Scheme != "https" {
			allErrs = append(allErrs, field.Invalid(fldPath, urlstr, "the URL must be using the HTTPS scheme"))
		}
		if u.Scheme == "" {
			allErrs = append(allErrs, field.Invalid(fldPath, urlstr, "the URL without scheme is not allowed"))
		}
	}
	return allErrs
}

// ValidateIPFromString 校验 IP 地址并收集所有错误。
func ValidateIPFromString(ipaddr string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if netutils.ParseIPSloppy(ipaddr) == nil {
		allErrs = append(allErrs, field.Invalid(fldPath, ipaddr, "ip address is not valid"))
	}
	return allErrs
}

// ValidatePort 校验端口号并收集所有错误。
func ValidatePort(port int32, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if _, err := kubeadmutil.ParsePort(strconv.Itoa(int(port))); err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, port, "port number is not valid"))
	}
	return allErrs
}

// ValidateHostPort 校验 host[:port] 格式的端点并收集所有错误。
func ValidateHostPort(endpoint string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if _, _, err := kubeadmutil.ParseHostPort(endpoint); endpoint != "" && err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, endpoint, "endpoint is not valid"))
	}
	return allErrs
}

// ValidateIPNetFromString 校验 IP 地址的网络部分并收集所有错误。
func ValidateIPNetFromString(subnetStr string, minAddrs int64, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	subnets, err := netutils.ParseCIDRs(strings.Split(subnetStr, ","))
	if err != nil {
		allErrs = append(allErrs, field.Invalid(fldPath, subnetStr, "couldn't parse subnet"))
		return allErrs
	}
	switch {
	// 双栈网络只允许两个 CIDR。
	case len(subnets) > 2:
		allErrs = append(allErrs, field.Invalid(fldPath, subnetStr, "expected one (IPv4 or IPv6) CIDR or two CIDRs from each family for dual-stack networking"))
	// 双栈网络有两个 CIDR 时，校验是否分别包含两种 IP 协议族。
	case len(subnets) == 2:
		areDualStackCIDRs, err := netutils.IsDualStackCIDRs(subnets)
		if err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath, subnetStr, err.Error()))
		} else if !areDualStackCIDRs {
			allErrs = append(allErrs, field.Invalid(fldPath, subnetStr, "expected one (IPv4 or IPv6) CIDR or two CIDRs from each family for dual-stack networking"))
		}
	}
	// 校验每个子网。
	for _, s := range subnets {
		numAddresses := netutils.RangeSize(s)
		if numAddresses < minAddrs {
			allErrs = append(allErrs, field.Invalid(fldPath, s.String(), fmt.Sprintf("subnet with %d address(es) is too small, the minimum is %d", numAddresses, minAddrs)))
		}

		// 子网位于 site-local 范围（包含 fec0::/10 中的地址）时发出警告。
		_, siteLocalNet, _ := netutils.ParseCIDRSloppy("fec0::/10")
		if siteLocalNet.Contains(s.IP) || s.Contains(siteLocalNet.IP) {
			klog.Warningf("the subnet %v contains IPv6 site-local addresses that belong to fec0::/10 which has been deprecated by rfc3879", s)
		}
	}
	return allErrs
}

// ValidateServiceSubnetSize 校验是否超过最大子网大小。
// 由于子网存储在 etcd 中，其 CIDR 应较小；较大的 CIDR（尤其是 IPv6 提供的范围）
// 不会带来额外价值，还会显著增加快照耗时。此校验与 apiserver 中的校验一致。
func ValidateServiceSubnetSize(subnetStr string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// 子网已完成校验。
	subnets, _ := netutils.ParseCIDRs(strings.Split(subnetStr, ","))
	for _, serviceSubnet := range subnets {
		ones, bits := serviceSubnet.Mask.Size()
		if bits-ones > constants.MaximumBitsForServiceSubnet {
			errMsg := fmt.Sprintf("specified service subnet is too large; for %d-bit addresses, the mask must be >= %d", bits, bits-constants.MaximumBitsForServiceSubnet)
			allErrs = append(allErrs, field.Invalid(fldPath, serviceSubnet.String(), errMsg))
		}
	}
	return allErrs
}

// ValidatePodSubnetNodeMask 校验 podSubnet 与节点掩码之间的关系是否正确。
func ValidatePodSubnetNodeMask(subnetStr string, c *kubeadm.ClusterConfiguration, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	// 子网已完成校验。
	subnets, _ := netutils.ParseCIDRs(strings.Split(subnetStr, ","))
	for _, podSubnet := range subnets {
		// 获取 podSubnet 掩码。
		mask := podSubnet.Mask
		maskSize, _ := mask.Size()
		// 获取 node-cidr-mask。
		nodeMask, err := getClusterNodeMask(c, netutils.IsIPv6(podSubnet.IP))
		if err != nil {
			allErrs = append(allErrs, field.Invalid(fldPath, podSubnet.String(), err.Error()))
			continue
		}
		// Pod 子网掩码必须能容纳一个或多个节点掩码；
		// 例如，若为 /24，IPv4 节点掩码必须介于 24 和 32 之间。
		if maskSize > nodeMask {
			allErrs = append(allErrs, field.Invalid(fldPath, podSubnet.String(), fmt.Sprintf("the size of pod subnet with mask %d is smaller than the size of node subnet with mask %d", maskSize, nodeMask)))
		} else if (nodeMask - maskSize) > constants.PodSubnetNodeMaskMaxDiff {
			allErrs = append(allErrs, field.Invalid(fldPath, podSubnet.String(), fmt.Sprintf("pod subnet mask (%d) and node-mask (%d) difference is greater than %d", maskSize, nodeMask, constants.PodSubnetNodeMaskMaxDiff)))
		}
	}
	return allErrs
}

// getClusterNodeMask 根据集群配置和 IP 协议族返回对应的 node-cidr-mask。
// IPv4 默认为 24，IPv6 默认为 64。
func getClusterNodeMask(c *kubeadm.ClusterConfiguration, isIPv6 bool) (int, error) {
	// defaultNodeMaskCIDRIPv4 是控制器管理器使用的 IPv4 节点 CIDR 默认掩码长度。
	const defaultNodeMaskCIDRIPv4 = 24
	// defaultNodeMaskCIDRIPv6 是控制器管理器使用的 IPv6 节点 CIDR 默认掩码长度。
	const defaultNodeMaskCIDRIPv6 = 64
	var maskSize int
	var maskArg string
	var err error

	if isIPv6 {
		maskArg = "node-cidr-mask-size-ipv6"
	} else {
		maskArg = "node-cidr-mask-size-ipv4"
	}

	maskValue, _ := kubeadm.GetArgValue(c.ControllerManager.ExtraArgs, maskArg, -1)
	if len(maskValue) != 0 {
		// 假定值为整数；若不是，后续转换会失败。
		maskSize, err = strconv.Atoi(maskValue)
		if err != nil {
			return 0, errors.Wrapf(err, "could not parse the value of the kube-controller-manager flag %s as an integer", maskArg)
		}
	} else if isIPv6 {
		maskSize = defaultNodeMaskCIDRIPv6
	} else {
		maskSize = defaultNodeMaskCIDRIPv4
	}
	return maskSize, nil
}

// ValidateDNS 校验 DNS 对象并收集所有错误。
func ValidateDNS(dns *kubeadm.DNS, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(dns.ImageRepository) > 0 {
		allErrs = append(allErrs, ValidateImageRepository(dns.ImageRepository, fldPath.Child("imageRepository"))...)
	}

	return allErrs
}

// ValidateNetworking 校验网络配置并收集所有错误。
func ValidateNetworking(c *kubeadm.ClusterConfiguration, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	dnsDomainFldPath := fldPath.Child("dnsDomain")
	for _, err := range validation.IsDNS1123Subdomain(c.Networking.DNSDomain) {
		allErrs = append(allErrs, field.Invalid(dnsDomainFldPath, c.Networking.DNSDomain, err))
	}

	if len(c.Networking.ServiceSubnet) != 0 {
		allErrs = append(allErrs, ValidateIPNetFromString(c.Networking.ServiceSubnet, constants.MinimumAddressesInServiceSubnet, fldPath.Child("serviceSubnet"))...)
		// 服务子网已完成校验，现在校验其大小。
		allErrs = append(allErrs, ValidateServiceSubnetSize(c.Networking.ServiceSubnet, fldPath.Child("serviceSubnet"))...)
	}
	if len(c.Networking.PodSubnet) != 0 {
		allErrs = append(allErrs, ValidateIPNetFromString(c.Networking.PodSubnet, constants.MinimumAddressesInPodSubnet, fldPath.Child("podSubnet"))...)
		val, _ := kubeadm.GetArgValue(c.ControllerManager.ExtraArgs, "allocate-node-cidrs", -1)
		if val != "false" {
			// Pod 子网已完成校验，现在校验其与节点掩码的关系。
			allErrs = append(allErrs, ValidatePodSubnetNodeMask(c.Networking.PodSubnet, c, fldPath.Child("podSubnet"))...)
		}
	}
	return allErrs
}

// ValidateAbsolutePath 校验提供的路径是否为绝对路径并收集所有错误。
func ValidateAbsolutePath(path string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	if !isAbs(path) {
		allErrs = append(allErrs, field.Invalid(fldPath, path, "path is not absolute"))
	}
	return allErrs
}

// ValidateMixedArguments 校验传入的参数并返回错误。
func ValidateMixedArguments(flag *pflag.FlagSet) error {
	// 未设置 --config 时，无需校验。
	if !flag.Changed("config") {
		return nil
	}

	mixedInvalidFlags := []string{}
	flag.Visit(func(f *pflag.Flag) {
		if isAllowedFlag(f.Name) {
			// "--skip-*" 等其他允许的参数可与 --config 同时设置。
			return
		}
		mixedInvalidFlags = append(mixedInvalidFlags, f.Name)
	})

	if len(mixedInvalidFlags) != 0 {
		return errors.Errorf("can not mix '--config' with arguments %v", mixedInvalidFlags)
	}
	return nil
}

func isAllowedFlag(flagName string) bool {
	allowedFlags := sets.New(kubeadmcmdoptions.CfgPath,
		kubeadmcmdoptions.IgnorePreflightErrors,
		kubeadmcmdoptions.DryRun,
		kubeadmcmdoptions.KubeconfigPath,
		kubeadmcmdoptions.NodeName,
		kubeadmcmdoptions.KubeconfigDir,
		kubeadmcmdoptions.UploadCerts,
		kubeadmcmdoptions.PrintManifest,
		"allow-missing-template-keys", "output", "show-managed-fields",
		"print-join-command", "rootfs", "v", "log-file", "yes")
	if allowedFlags.Has(flagName) {
		return true
	}
	return strings.HasPrefix(flagName, "skip-")
}

// ValidateFeatureGates 校验提供的功能开关并收集所有错误。
func ValidateFeatureGates(featureGates map[string]bool, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	// 检查是否提供了有效的功能名称。
	for k := range featureGates {
		if !features.Supports(features.InitFeatureGates, k) {
			allErrs = append(allErrs, field.Invalid(fldPath, featureGates,
				fmt.Sprintf("%s is not a valid feature name.", k)))
		}
	}
	return allErrs
}

// ValidateAPIEndpoint 校验 API 服务器端点并收集所有错误。
func ValidateAPIEndpoint(c *kubeadm.APIEndpoint, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateIPFromString(c.AdvertiseAddress, fldPath.Child("advertiseAddress"))...)
	allErrs = append(allErrs, ValidatePort(c.BindPort, fldPath.Child("bindPort"))...)
	return allErrs
}

// ValidateCertificateKey 校验证书密钥是否为有效的十六进制编码 AES 密钥。
func ValidateCertificateKey(certificateKey string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(certificateKey) > 0 {
		decodedKey, err := hex.DecodeString(certificateKey)
		if err != nil {
			return append(allErrs, field.Invalid(fldPath, certificateKey, fmt.Sprintf("certificate key decoding error: %v", err)))
		}

		k := len(decodedKey)
		if k != constants.CertificateKeySize {
			allErrs = append(allErrs, field.Invalid(fldPath, certificateKey, fmt.Sprintf("invalid certificate key size %d, the key must be an AES key of size %d", k, constants.CertificateKeySize)))
		}
	}

	return allErrs
}

// ValidateIgnorePreflightErrors 校验以下两处是否存在重复项：
// - ignore-preflight-errors 参数；
// - {Init,Join}Configuration 文件中的 ignorePreflightErrors 字段。
func ValidateIgnorePreflightErrors(ignorePreflightErrorsFromCLI, ignorePreflightErrorsFromConfigFile []string) (sets.Set[string], error) {
	ignoreErrors := sets.New[string]()
	allErrs := field.ErrorList{}

	for _, item := range ignorePreflightErrorsFromConfigFile {
		ignoreErrors.Insert(strings.ToLower(item)) // 参数不区分大小写。
	}

	for _, item := range ignorePreflightErrorsFromCLI {
		ignoreErrors.Insert(strings.ToLower(item)) // 参数不区分大小写。
	}

	if ignoreErrors.Has("all") && ignoreErrors.Len() > 1 {
		allErrs = append(allErrs, field.Invalid(field.NewPath("ignore-preflight-errors"), strings.Join(sets.List(ignoreErrors), ","), "don't specify individual checks if 'all' is used"))
	}

	return ignoreErrors, allErrs.ToAggregate()
}

// ValidateSocketPath 校验 socket 路径或 URL 的格式并收集所有错误。
func ValidateSocketPath(socket string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	if len(socket) == 0 { // 静态和动态默认值逻辑应已为此字段填入值。
		return append(allErrs, field.Invalid(fldPath, socket, "empty CRI socket"))
	}

	u, err := url.Parse(socket)
	if err != nil {
		return append(allErrs, field.Invalid(fldPath, socket, fmt.Sprintf("URL parsing error: %v", err)))
	}

	// 静态和动态默认值逻辑应已确保使用 URL scheme。
	if u.Scheme != kubeadmapiv1.DefaultContainerRuntimeURLScheme {
		return append(allErrs, field.Invalid(fldPath, socket, fmt.Sprintf("only URL scheme %q is supported, got %q", kubeadmapiv1.DefaultContainerRuntimeURLScheme, u.Scheme)))
	}

	return allErrs
}

// ValidateImageRepository 校验镜像仓库格式并收集所有错误。
func ValidateImageRepository(imageRepository string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	image := fmt.Sprintf("%s/%s:%s", imageRepository, "name", "tag")
	if !reference.ReferenceRegexp.MatchString(image) {
		return append(allErrs, field.Invalid(fldPath, imageRepository, "invalid image repository format"))
	}

	return allErrs
}

// ValidateResetConfiguration 校验 ResetConfiguration 对象并收集所有错误。
func ValidateResetConfiguration(c *kubeadm.ResetConfiguration) field.ErrorList {
	allErrs := field.ErrorList{}
	allErrs = append(allErrs, ValidateSocketPath(c.CRISocket, field.NewPath("criSocket"))...)
	allErrs = append(allErrs, ValidateAbsolutePath(c.CertificatesDir, field.NewPath("certificatesDir"))...)
	allErrs = append(allErrs, ValidateUnmountFlags(c.UnmountFlags, field.NewPath("unmountFlags"))...)
	return allErrs
}

// ValidateExtraArgs 校验一组参数并收集所有错误。
func ValidateExtraArgs(args []kubeadm.Arg, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	for idx, arg := range args {
		if len(arg.Name) == 0 {
			allErrs = append(allErrs, field.Invalid(fldPath, fmt.Sprintf("index %d", idx), "argument has no name"))
		}
		if arg.MergeMethod != "" {
			if arg.MergeMethod != kubeadm.ArgMergeMethodAppend && arg.MergeMethod != kubeadm.ArgMergeMethodPrepend {
				allErrs = append(allErrs, field.Invalid(fldPath, fmt.Sprintf("index %d", idx), "argument has an invalid merge method"))
			}
		}
	}

	return allErrs
}

// ValidateUnmountFlags 校验一组卸载标志并收集所有错误。
func ValidateUnmountFlags(flags []string, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}

	for idx, flag := range flags {
		switch flag {
		case kubeadm.UnmountFlagMNTForce, kubeadm.UnmountFlagMNTDetach, kubeadm.UnmountFlagMNTExpire, kubeadm.UnmountFlagUmountNoFollow:
			continue
		default:
			allErrs = append(allErrs, field.Invalid(fldPath, fmt.Sprintf("index %d", idx), fmt.Sprintf("unknown unmount flag %s", flag)))
		}
	}

	return allErrs
}

// ValidateImagePullPolicy 校验用户指定的拉取策略是否正确。
func ValidateImagePullPolicy(policy corev1.PullPolicy, fldPath *field.Path) field.ErrorList {
	allErrs := field.ErrorList{}
	switch policy {
	case "", corev1.PullAlways, corev1.PullIfNotPresent, corev1.PullNever:
		return allErrs
	default:
		allErrs = append(allErrs, field.Invalid(fldPath, policy, "invalid pull policy"))
		return allErrs
	}
}

// ValidateUpgradeConfiguration 校验 UpgradeConfiguration 对象并收集所有错误。
func ValidateUpgradeConfiguration(c *kubeadm.UpgradeConfiguration) field.ErrorList {
	allErrs := field.ErrorList{}
	if c.Apply.Patches != nil {
		allErrs = append(allErrs, ValidateAbsolutePath(c.Apply.Patches.Directory,
			field.NewPath("apply").Child("patches").Child("directory"))...)
	}
	if c.Node.Patches != nil {
		allErrs = append(allErrs, ValidateAbsolutePath(c.Node.Patches.Directory,
			field.NewPath("node").Child("patches").Child("directory"))...)
	}
	allErrs = append(allErrs, ValidateImagePullPolicy(c.Apply.ImagePullPolicy,
		field.NewPath("apply").Child("imagePullPolicy"))...)
	allErrs = append(allErrs, ValidateImagePullPolicy(c.Node.ImagePullPolicy,
		field.NewPath("node").Child("imagePullPolicy"))...)
	return allErrs
}

// ValidateCertValidity 校验证书有效期是否过长或与 CA 证书有效期不匹配。
func ValidateCertValidity(cfg *kubeadm.ClusterConfiguration) []error {
	var allErrs []error
	if cfg.CertificateValidityPeriod != nil && cfg.CertificateValidityPeriod.Duration > constants.CertificateValidityPeriod {
		allErrs = append(allErrs,
			errors.Errorf("certificateValidityPeriod: the value %v is more than the recommended default for certificate expiration: %v",
				cfg.CertificateValidityPeriod.Duration, constants.CertificateValidityPeriod))
	}
	if cfg.CACertificateValidityPeriod != nil && cfg.CACertificateValidityPeriod.Duration > constants.CACertificateValidityPeriod {
		allErrs = append(allErrs,
			errors.Errorf("caCertificateValidityPeriod: the value %v is more than the recommended default for CA certificate expiration: %v",
				cfg.CACertificateValidityPeriod.Duration, constants.CACertificateValidityPeriod))
	}
	if cfg.CertificateValidityPeriod != nil && cfg.CACertificateValidityPeriod != nil {
		if cfg.CertificateValidityPeriod.Duration > cfg.CACertificateValidityPeriod.Duration {
			allErrs = append(allErrs,
				errors.Errorf("certificateValidityPeriod: the value %v is more than the caCertificateValidityPeriod: %v",
					cfg.CertificateValidityPeriod.Duration, cfg.CACertificateValidityPeriod.Duration))
		}
	}
	return allErrs
}
