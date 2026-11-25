/*
Copyright 2016 The Kubernetes Authors.

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

package preflight

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	netutil "k8s.io/apimachinery/pkg/util/net"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/validation"
	versionutil "k8s.io/apimachinery/pkg/util/version"
	kubeadmversion "k8s.io/component-base/version"
	"k8s.io/klog/v2"
	system "k8s.io/system-validators/validators"
	utilsexec "k8s.io/utils/exec"
	netutils "k8s.io/utils/net"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/images"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/initsystem"
	utilruntime "k8s.io/kubernetes/cmd/kubeadm/app/util/runtime"
)

const (
	ipv4Forward                 = "/proc/sys/net/ipv4/ip_forward"
	ipv6DefaultForwarding       = "/proc/sys/net/ipv6/conf/default/forwarding"
	externalEtcdRequestTimeout  = 10 * time.Second
	externalEtcdRequestRetries  = 3
	externalEtcdRequestInterval = 5 * time.Second
)

var (
	minExternalEtcdVersion = versionutil.MustParseSemantic(kubeadmconstants.MinExternalEtcdVersion)
)

// Checker 校验系统状态，尽可能确保 kubeadm 执行成功。
type Checker interface {
	Check() (warnings, errorList []error)
	Name() string
}

// ContainerRuntimeCheck 校验容器运行时。
type ContainerRuntimeCheck struct {
	criSocket string

	// 测试时使用的替身实现。
	impl utilruntime.Impl
}

// Name 返回 RuntimeCheck 的标签。
func (ContainerRuntimeCheck) Name() string {
	return "CRI"
}

// Check 校验容器运行时。
func (crc ContainerRuntimeCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infoln("validating the container runtime")
	containerRuntime := utilruntime.NewContainerRuntime(crc.criSocket)
	if crc.impl != nil {
		containerRuntime.SetImpl(crc.impl)
	}
	if err := containerRuntime.Connect(); err != nil {
		return nil, []error{errors.Wrap(err, "could not connect to the container runtime")}
	}
	defer containerRuntime.Close(context.Background())

	if err := containerRuntime.IsRunning(); err != nil {
		errorList = append(errorList, err)
	}
	return warnings, errorList
}

// ContainerRuntimeVersionCheck 校验已安装容器运行时与 kubelet 的版本兼容性。
type ContainerRuntimeVersionCheck struct {
	criSocket string

	// 测试时使用的替身实现。
	impl utilruntime.Impl
}

// Name 返回 RuntimeCheck 的标签。
func (ContainerRuntimeVersionCheck) Name() string {
	return "ContainerRuntimeVersion"
}

// Check 校验容器运行时与 kubelet 的版本兼容性。
func (crvc ContainerRuntimeVersionCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infoln("validating the container runtime version compatibility")
	containerRuntime := utilruntime.NewContainerRuntime(crvc.criSocket)
	if crvc.impl != nil {
		containerRuntime.SetImpl(crvc.impl)
	}
	if err := containerRuntime.Connect(); err != nil {
		return nil, []error{errors.Wrap(err, "could not connect to the container runtime")}
	}
	defer containerRuntime.Close(context.Background())

	ok, err := containerRuntime.IsRuntimeConfigImplemented()
	if err != nil {
		return nil, []error{errors.Wrap(err, "could not check if the runtime config is available")}
	}
	if !ok {
		// TODO: kubelet 版本达到 1.38 或更高时返回错误。
		// https://github.com/kubernetes/kubeadm/issues/3229
		err := errors.New("You must update your container runtime to a version that supports the CRI method RuntimeConfig. " +
			"Falling back to using cgroupDriver from kubelet config will be removed in 1.38. " +
			"For more information, see https://git.k8s.io/enhancements/keps/sig-node/4033-group-driver-detection-over-cri")
		warnings = append(warnings, err)
	}

	return warnings, errorList
}

// ServiceCheck 校验指定服务是否已启用并处于活动状态。
// 如果未检测到受支持的 init 系统，则跳过所有检查并返回警告。
type ServiceCheck struct {
	Service       string
	CheckIfActive bool
	Label         string
}

// Name 返回 ServiceCheck 的标签；未提供标签时根据 service 参数生成。
func (sc ServiceCheck) Name() string {
	if sc.Label != "" {
		return sc.Label
	}
	return fmt.Sprintf("Service-%s", sc.Service)
}

// Check 校验服务是否已启用并处于活动状态。
func (sc ServiceCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infof("validating if the %q service is enabled and active", sc.Service)
	initSystem, err := initsystem.GetInitSystem()
	if err != nil {
		return []error{err}, nil
	}

	if !initSystem.ServiceExists(sc.Service) {
		return []error{errors.Errorf("%s service does not exist", sc.Service)}, nil
	}

	if !initSystem.ServiceIsEnabled(sc.Service) {
		warnings = append(warnings,
			errors.Errorf("%s service is not enabled, please run '%s'",
				sc.Service, initSystem.EnableCommand(sc.Service)))
	}

	if sc.CheckIfActive && !initSystem.ServiceIsActive(sc.Service) {
		errorList = append(errorList,
			errors.Errorf("%s service is not active, please run 'systemctl start %s.service'",
				sc.Service, sc.Service))
	}

	return warnings, errorList
}

// FirewalldCheck 检查 firewalld 是否已启用或处于活动状态；如果是，则警告用户不采取措施可能产生问题。
type FirewalldCheck struct {
	ports []int
}

// Name 返回 FirewalldCheck 的标签。
func (FirewalldCheck) Name() string {
	return "Firewalld"
}

// Check 校验防火墙是否已启用并处于活动状态。
func (fc FirewalldCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infoln("validating if the firewall is enabled and active")
	initSystem, err := initsystem.GetInitSystem()
	if err != nil {
		return []error{err}, nil
	}

	if !initSystem.ServiceExists("firewalld") {
		return nil, nil
	}

	if initSystem.ServiceIsActive("firewalld") {
		err := errors.Errorf("firewalld is active, please ensure ports %v are open or your cluster may not function correctly",
			fc.ports)
		return []error{err}, nil
	}

	return nil, nil
}

// PortOpenCheck 确保指定端口可用。
type PortOpenCheck struct {
	address    string
	port       int
	label      string
	listenFunc func(string, string) (net.Listener, error)
}

// Name 返回 PortOpenCheck 的名称；未知时根据端口号返回 "Port-XXXX"。
func (poc PortOpenCheck) Name() string {
	if poc.label != "" {
		return poc.label
	}
	return fmt.Sprintf("Port-%d", poc.port)
}

// Check 校验指定端口是否可用。
func (poc PortOpenCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infof("validating availability of port %d on address %q", poc.port, poc.address)

	listenAddress := ":" + strconv.Itoa(poc.port)
	if poc.address != "" {
		listenAddress = net.JoinHostPort(poc.address, strconv.Itoa(poc.port))
	}

	listen := poc.listenFunc
	if listen == nil {
		listen = net.Listen
	}

	ln, err := listen("tcp", listenAddress)
	if err != nil {
		errorList = []error{errors.Errorf("Port %d is in use", poc.port)}
	}
	if ln != nil {
		if err = ln.Close(); err != nil {
			warnings = append(warnings,
				errors.Errorf("when closing port %d, encountered %v", poc.port, err))
		}
	}

	return warnings, errorList
}

// IsPrivilegedUserCheck 校验用户是否具有特权（Linux 为 root，Windows 为 Administrator）。
type IsPrivilegedUserCheck struct{}

// Name 返回 IsPrivilegedUserCheck 的名称。
func (IsPrivilegedUserCheck) Name() string {
	return "IsPrivilegedUser"
}

// DirAvailableCheck 校验指定目录不存在或为空。
type DirAvailableCheck struct {
	Path  string
	Label string
}

// Name 返回各个 DirAvailableCheck 的标签；未知时根据路径生成。
func (dac DirAvailableCheck) Name() string {
	if dac.Label != "" {
		return dac.Label
	}
	return fmt.Sprintf("DirAvailable-%s", strings.Replace(dac.Path, "/", "-", -1))
}

// Check 校验目录不存在或为空。
func (dac DirAvailableCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infof("validating the existence and emptiness of directory %s", dac.Path)

	// 目录不存在时无需继续检查。
	if _, err := os.Stat(dac.Path); os.IsNotExist(err) {
		return nil, nil
	}

	f, err := os.Open(dac.Path)
	if err != nil {
		return nil, []error{errors.Wrapf(err, "unable to check if %s is empty", dac.Path)}
	}
	defer f.Close()

	_, err = f.Readdirnames(1)
	if err != io.EOF {
		return nil, []error{errors.Errorf("%s is not empty", dac.Path)}
	}

	return nil, nil
}

// FileAvailableCheck 校验指定文件尚未存在。
type FileAvailableCheck struct {
	Path  string
	Label string
}

// Name 返回各个 FileAvailableCheck 的标签；未知时根据路径生成。
func (fac FileAvailableCheck) Name() string {
	if fac.Label != "" {
		return fac.Label
	}
	return fmt.Sprintf("FileAvailable-%s", strings.Replace(fac.Path, "/", "-", -1))
}

// Check 校验指定文件尚未存在。
func (fac FileAvailableCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infof("validating the existence of file %s", fac.Path)

	if _, err := os.Stat(fac.Path); err == nil {
		return nil, []error{errors.Errorf("%s already exists", fac.Path)}
	}
	return nil, nil
}

// FileExistingCheck 校验指定文件已经存在。
type FileExistingCheck struct {
	Path  string
	Label string
}

// Name 返回各个 FileExistingCheck 的标签；未知时根据路径生成。
func (fac FileExistingCheck) Name() string {
	if fac.Label != "" {
		return fac.Label
	}
	return fmt.Sprintf("FileExisting-%s", strings.Replace(fac.Path, "/", "-", -1))
}

// Check 校验指定文件已经存在。
func (fac FileExistingCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infof("validating the existence of file %s", fac.Path)

	if _, err := os.Stat(fac.Path); err != nil {
		return nil, []error{errors.Errorf("%s doesn't exist", fac.Path)}
	}
	return nil, nil
}

// FileContentCheck 校验指定文件包含 Content 字符串。
type FileContentCheck struct {
	Path    string
	Content []byte
	Label   string
}

// Name 返回各个 FileContentCheck 的标签；未知时根据路径生成。
func (fcc FileContentCheck) Name() string {
	if fcc.Label != "" {
		return fcc.Label
	}
	return fmt.Sprintf("FileContent-%s", strings.Replace(fcc.Path, "/", "-", -1))
}

// Check 校验指定文件是否包含给定内容。
func (fcc FileContentCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infof("validating the contents of file %s", fcc.Path)
	f, err := os.Open(fcc.Path)
	if err != nil {
		return nil, []error{errors.Errorf("%s does not exist", fcc.Path)}
	}

	lr := io.LimitReader(f, int64(len(fcc.Content)))
	defer f.Close()

	buf := &bytes.Buffer{}
	_, err = io.Copy(buf, lr)
	if err != nil {
		return nil, []error{errors.Errorf("%s could not be read", fcc.Path)}
	}

	if !bytes.Equal(buf.Bytes(), fcc.Content) {
		return nil, []error{errors.Errorf("%s contents are not set to %s", fcc.Path, fcc.Content)}
	}
	return nil, []error{}

}

// InPathCheck 校验指定可执行文件是否存在于 $PATH 中。
type InPathCheck struct {
	executable string
	mandatory  bool
	exec       utilsexec.Interface
	label      string
	suggestion string
}

// Name 返回各个 InPathCheck 的标签；未知时根据路径生成。
func (ipc InPathCheck) Name() string {
	if ipc.label != "" {
		return ipc.label
	}
	return fmt.Sprintf("FileExisting-%s", strings.Replace(ipc.executable, "/", "-", -1))
}

// Check 校验指定可执行文件是否存在于路径中。
func (ipc InPathCheck) Check() (warnings, errs []error) {
	klog.V(1).Infof("validating the presence of executable %s", ipc.executable)
	_, err := ipc.exec.LookPath(ipc.executable)
	if err != nil {
		if ipc.mandatory {
			// 作为错误返回：
			return nil, []error{errors.Errorf("%s not found in system path", ipc.executable)}
		}
		// 作为警告返回：
		warningMessage := fmt.Sprintf("%s not found in system path", ipc.executable)
		if ipc.suggestion != "" {
			warningMessage += fmt.Sprintf("\nSuggestion: %s", ipc.suggestion)
		}
		return []error{errors.New(warningMessage)}, nil
	}
	return nil, nil
}

// HostnameCheck 校验主机名是否匹配 DNS 子域名正则表达式。
// 如果不匹配，kubelet 将无法启动 kube-apiserver、kube-controller-manager 等静态 Pod。
type HostnameCheck struct {
	nodeName string
}

// Name 返回 HostnameCheck 的名称 Hostname。
func (HostnameCheck) Name() string {
	return "Hostname"
}

// Check 校验主机名是否匹配 DNS 子域名正则表达式，并检查其长度和格式。
func (hc HostnameCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infoln("checking whether the given node name is valid and reachable using net.LookupHost")
	for _, msg := range validation.IsQualifiedName(hc.nodeName) {
		warnings = append(warnings, errors.Errorf("invalid node name format %q: %s", hc.nodeName, msg))
	}

	addr, err := net.LookupHost(hc.nodeName)
	if addr == nil {
		warnings = append(warnings, errors.Errorf("hostname \"%s\" could not be reached", hc.nodeName))
	}
	if err != nil {
		warnings = append(warnings, errors.Wrapf(err, "hostname \"%s\"", hc.nodeName))
	}
	return warnings, errorList
}

// HTTPProxyCheck 校验到指定主机的 HTTPS 连接是直连还是通过代理；检测到代理时返回警告。
type HTTPProxyCheck struct {
	Proto string
	Host  string
}

// Name 返回 HTTPProxyCheck 的名称 HTTPProxy。
func (hst HTTPProxyCheck) Name() string {
	return "HTTPProxy"
}

// Check 校验 HTTP 连接是直连还是通过代理。
func (hst HTTPProxyCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infoln("validating if the connectivity type is via proxy or direct")
	u := &url.URL{Scheme: hst.Proto, Host: hst.Host}
	if netutils.IsIPv6String(hst.Host) {
		u.Host = net.JoinHostPort(hst.Host, "1234")
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, []error{err}
	}

	proxy, err := netutil.SetOldTransportDefaults(&http.Transport{}).Proxy(req)
	if err != nil {
		return nil, []error{err}
	}
	if proxy != nil {
		return []error{errors.Errorf("Connection to %q uses proxy %q. If that is not intended, adjust your proxy settings", u, proxy)}, nil
	}
	return nil, nil
}

// HTTPProxyCIDRCheck 校验到指定子网的 HTTPS 连接是直连还是通过代理；检测到代理时返回警告。
// 与 HTTPProxyCheck 类似，但以子网为操作对象，并使用 API machinery 的默认 transport 设置，
// 模拟 kube-apiserver 访问集群服务和 Pod。
type HTTPProxyCIDRCheck struct {
	Proto string
	CIDR  string
}

// Name 返回 HTTPProxyCIDRCheck 的名称 HTTPProxyCIDR。
func (HTTPProxyCIDRCheck) Name() string {
	return "HTTPProxyCIDR"
}

// Check 校验到 CIDR 中首个 IP 地址的 HTTP 连接；如果不是直连而是通过代理，则产生警告。
func (subnet HTTPProxyCIDRCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infoln("validating http connectivity to first IP address in the CIDR")
	if len(subnet.CIDR) == 0 {
		return nil, nil
	}

	_, cidr, err := netutils.ParseCIDRSloppy(subnet.CIDR)
	if err != nil {
		return nil, []error{errors.Wrapf(err, "error parsing CIDR %q", subnet.CIDR)}
	}

	testIP, err := netutils.GetIndexedIP(cidr, 1)
	if err != nil {
		return nil, []error{errors.Wrapf(err, "unable to get first IP address from the given CIDR (%s)", cidr.String())}
	}

	testHostString := net.JoinHostPort(testIP.String(), "1234")
	url := fmt.Sprintf("%s://%s/", subnet.Proto, testHostString)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, []error{err}
	}

	// 使用与 API server 相同的 transport 默认设置。
	proxy, err := netutil.SetOldTransportDefaults(&http.Transport{}).Proxy(req)
	if err != nil {
		return nil, []error{err}
	}
	if proxy != nil {
		return []error{errors.Errorf("connection to %q uses proxy %q. This may lead to malfunctional cluster setup. Make sure that Pod and Services IP ranges specified correctly as exceptions in proxy configuration", subnet.CIDR, proxy)}, nil
	}
	return nil, nil
}

// SystemVerificationCheck 定义用于运行 test/e2e_node/system 中系统验证节点检查的结构体。
type SystemVerificationCheck struct {
	isUpgrade bool
	exec      utilsexec.Interface
}

// Name 返回 SystemVerificationCheck 的名称 SystemVerification。
func (SystemVerificationCheck) Name() string {
	return "SystemVerification"
}

// Check 运行所有独立检查。
func (sysver SystemVerificationCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infoln("running system verification checks")
	// 创建缓冲写入器并设置足够大的容量（1M），假定系统验证测试的输出不会超过该限制。
	// 运行系统验证检查，但写入缓冲写入器而不是 stdout。
	bufw := bufio.NewWriterSize(os.Stdout, 1*1024*1024)
	reporter := &system.StreamReporter{WriteStream: bufw}

	var errs []error
	var warns []error
	// 需要运行的所有通用验证器：
	var validators = []system.Validator{
		&system.KernelValidator{Reporter: reporter}}

	// 在 addOSValidator() 添加的 CgroupsValidator 中考虑 KubeletVersion。
	kubeletVersion, err := GetKubeletVersion(sysver.exec)
	if err != nil {
		return nil, []error{errors.Wrap(err, "couldn't get kubelet version")}
	}
	// 升级期间检查下一个 kubelet MINOR 版本。
	// 以下方法不支持 k8s MAJOR 版本升级。
	if sysver.isUpgrade {
		kubeletVersion = kubeletVersion.WithMinor(kubeletVersion.Minor() + 1)
	}
	validators = addOSValidator(validators, reporter, kubeletVersion.String())

	// 运行所有验证器。
	for _, v := range validators {
		warn, err := v.Validate(system.DefaultSysSpec)
		if err != nil {
			errs = append(errs, err...)
		}
		if warn != nil {
			warns = append(warns, warn...)
		}
	}

	if len(errs) != 0 {
		// 仅在检查失败时输出系统验证检查的结果。
		fmt.Println("[preflight] The system verification failed. Printing the output from the verification:")
		bufw.Flush()
		return warns, errs
	}
	return warns, nil
}

// KubernetesVersionCheck 校验 Kubernetes 和 kubeadm 版本。
type KubernetesVersionCheck struct {
	KubeadmVersion    string
	KubernetesVersion string
}

// Name 返回 KubernetesVersionCheck 的名称 KubernetesVersion。
func (KubernetesVersionCheck) Name() string {
	return "KubernetesVersion"
}

// Check 校验 Kubernetes 和 kubeadm 版本。
func (kubever KubernetesVersionCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infoln("validating Kubernetes and kubeadm version")
	// 对于未设置 apimachinery 或整个代码库版本的 "super-custom builds"，跳过此检查。
	if strings.HasPrefix(kubever.KubeadmVersion, "v0.0.0") {
		return nil, nil
	}

	kadmVersion, err := versionutil.ParseSemantic(kubever.KubeadmVersion)
	if err != nil {
		return nil, []error{errors.Wrapf(err, "couldn't parse kubeadm version %q", kubever.KubeadmVersion)}
	}

	k8sVersion, err := versionutil.ParseSemantic(kubever.KubernetesVersion)
	if err != nil {
		return nil, []error{errors.Wrapf(err, "couldn't parse Kubernetes version %q", kubever.KubernetesVersion)}
	}

	// 检查 k8sVersion 是否大于或等于当前 kubeadm 支持的首个不受支持版本，
	// 即 major.minor+1（包括所有 patch 和预发布版本）。
	// 注意：在 semver 中 patch 号是数字，而预发布版本是字符串，数字标识符的优先级始终低于非数字标识符。
	// 因此将值设为 x.y.0-0 表示 x.y 次版本中的首个 patch 预发布版本。
	firstUnsupportedVersion := versionutil.MustParseSemantic(fmt.Sprintf("%d.%d.%s", kadmVersion.Major(), kadmVersion.Minor()+1, "0-0"))
	if k8sVersion.AtLeast(firstUnsupportedVersion) {
		return []error{errors.Errorf("Kubernetes version is greater than kubeadm version. Please consider to upgrade kubeadm. Kubernetes version: %s. Kubeadm version: %d.%d.x", k8sVersion, kadmVersion.Components()[0], kadmVersion.Components()[1])}, nil
	}

	return nil, nil
}

// KubeletVersionCheck 校验已安装的 kubelet 版本。
type KubeletVersionCheck struct {
	KubernetesVersion string
	minKubeletVersion *versionutil.Version
	exec              utilsexec.Interface
}

// Name 返回 KubeletVersionCheck 的名称 KubeletVersion。
func (KubeletVersionCheck) Name() string {
	return "KubeletVersion"
}

// Check 校验 kubelet 版本不低于最低支持版本。
func (kubever KubeletVersionCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infoln("validating kubelet version")
	kubeletVersion, err := GetKubeletVersion(kubever.exec)
	if err != nil {
		return nil, []error{errors.Wrap(err, "couldn't get kubelet version")}
	}
	if kubever.minKubeletVersion == nil {
		kubever.minKubeletVersion = kubeadmconstants.MinimumKubeletVersion
	}
	if kubeletVersion.LessThan(kubever.minKubeletVersion) {
		return nil, []error{errors.Errorf("Kubelet version %q is lower than kubeadm can support. Please upgrade kubelet", kubeletVersion)}
	}

	if kubever.KubernetesVersion != "" {
		k8sVersion, err := versionutil.ParseSemantic(kubever.KubernetesVersion)
		if err != nil {
			return nil, []error{errors.Wrapf(err, "couldn't parse Kubernetes version %q", kubever.KubernetesVersion)}
		}
		if kubeletVersion.Major() > k8sVersion.Major() || kubeletVersion.Minor() > k8sVersion.Minor() {
			return nil, []error{errors.Errorf("the kubelet version is higher than the control plane version. This is not a supported version skew and may lead to a malfunctional cluster. Kubelet version: %q Control plane version: %q", kubeletVersion, k8sVersion)}
		}
	}
	return nil, nil
}

// SwapCheck 在启用 swap 时发出警告。
type SwapCheck struct{}

// Name 返回 SwapCheck 的名称 Swap。
func (SwapCheck) Name() string {
	return "Swap"
}

// Check 校验是否启用了 swap。
func (swc SwapCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infoln("validating whether swap is enabled or not")
	f, err := os.Open("/proc/swaps")
	if err != nil {
		// /proc/swaps 不可用，因此无需警告。
		return nil, nil
	}
	defer f.Close()
	var buf []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		buf = append(buf, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return []error{errors.Wrap(err, "error parsing /proc/swaps")}, nil
	}

	if len(buf) > 1 {
		return []error{errors.New("swap is supported for cgroup v2 only. " +
			"The kubelet must be properly configured to use swap. Please refer to https://kubernetes.io/docs/concepts/architecture/nodes/#swap-memory, " +
			"or disable swap on the node")}, nil
	}

	return nil, nil
}

type etcdVersionResponse struct {
	Etcdserver  string `json:"etcdserver"`
	Etcdcluster string `json:"etcdcluster"`
}

// ExternalEtcdVersionCheck 校验外部 etcd 版本是否满足 kubeadm 的要求。
type ExternalEtcdVersionCheck struct {
	Etcd kubeadmapi.Etcd
}

// Name 返回 ExternalEtcdVersionCheck 的名称 ExternalEtcdVersion。
func (ExternalEtcdVersionCheck) Name() string {
	return "ExternalEtcdVersion"
}

// Check 校验外部 etcd 版本。
// TODO: 是否改用官方 etcd Go 客户端？
func (evc ExternalEtcdVersionCheck) Check() (warnings, errorList []error) {
	klog.V(1).Infoln("validating the external etcd version")

	// 用户未使用外部 etcd 时直接返回。
	if len(evc.Etcd.External.HTTPEndpoints) == 0 {
		return nil, nil
	}

	var config *tls.Config
	var err error
	if config, err = evc.configRootCAs(config); err != nil {
		errorList = append(errorList, err)
		return nil, errorList
	}
	if config, err = evc.configCertAndKey(config); err != nil {
		errorList = append(errorList, err)
		return nil, errorList
	}

	client := evc.getHTTPClient(config)
	for _, endpoint := range evc.Etcd.External.HTTPEndpoints {
		if _, err := url.Parse(endpoint); err != nil {
			errorList = append(errorList, errors.Wrapf(err, "failed to parse external etcd endpoint %s", endpoint))
			continue
		}
		resp := etcdVersionResponse{}
		var err error
		versionURL := fmt.Sprintf("%s/%s", endpoint, "version")
		if tmpVersionURL, err := normalizeURLString(versionURL); err != nil {
			errorList = append(errorList, errors.Wrapf(err, "failed to normalize external etcd version url %s", versionURL))
			continue
		} else {
			versionURL = tmpVersionURL
		}
		if err = getEtcdVersionResponse(client, versionURL, &resp); err != nil {
			errorList = append(errorList, err)
			continue
		}

		etcdVersion, err := versionutil.ParseSemantic(resp.Etcdserver)
		if err != nil {
			errorList = append(errorList, errors.Wrapf(err, "couldn't parse external etcd version %q", resp.Etcdserver))
			continue
		}
		if etcdVersion.LessThan(minExternalEtcdVersion) {
			errorList = append(errorList, errors.Errorf("this version of kubeadm only supports external etcd version >= %s. Current version: %s", kubeadmconstants.MinExternalEtcdVersion, resp.Etcdserver))
			continue
		}
	}

	return nil, errorList
}

// configRootCAs 在提供 CAFile 时配置并返回 tls.Config 实例的引用。
func (evc ExternalEtcdVersionCheck) configRootCAs(config *tls.Config) (*tls.Config, error) {
	var CACertPool *x509.CertPool
	if evc.Etcd.External.CAFile != "" {
		CACert, err := os.ReadFile(evc.Etcd.External.CAFile)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't load external etcd's server certificate %s", evc.Etcd.External.CAFile)
		}
		CACertPool = x509.NewCertPool()
		CACertPool.AppendCertsFromPEM(CACert)
	}
	if CACertPool != nil {
		if config == nil {
			config = &tls.Config{}
		}
		config.RootCAs = CACertPool
	}
	return config, nil
}

// configCertAndKey 在提供 CertFile 和 KeyFile 对时配置并返回 tls.Config 实例的引用。
func (evc ExternalEtcdVersionCheck) configCertAndKey(config *tls.Config) (*tls.Config, error) {
	var cert tls.Certificate
	if evc.Etcd.External.CertFile != "" && evc.Etcd.External.KeyFile != "" {
		var err error
		cert, err = tls.LoadX509KeyPair(evc.Etcd.External.CertFile, evc.Etcd.External.KeyFile)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't load external etcd's certificate and key pair %s, %s", evc.Etcd.External.CertFile, evc.Etcd.External.KeyFile)
		}
		if config == nil {
			config = &tls.Config{}
		}
		config.Certificates = []tls.Certificate{cert}
	}
	return config, nil
}

func (evc ExternalEtcdVersionCheck) getHTTPClient(config *tls.Config) *http.Client {
	if config != nil {
		transport := netutil.SetOldTransportDefaults(&http.Transport{
			TLSClientConfig: config,
		})
		return &http.Client{
			Transport: transport,
			Timeout:   externalEtcdRequestTimeout,
		}
	}
	return &http.Client{Timeout: externalEtcdRequestTimeout, Transport: netutil.SetOldTransportDefaults(&http.Transport{})}
}

func getEtcdVersionResponse(client *http.Client, url string, target interface{}) error {
	loopCount := externalEtcdRequestRetries + 1
	var err error
	var stopRetry bool
	for loopCount > 0 {
		if loopCount <= externalEtcdRequestRetries {
			time.Sleep(externalEtcdRequestInterval)
		}
		stopRetry, err = func() (stopRetry bool, err error) {
			r, err := client.Get(url)
			if err != nil {
				loopCount--
				return false, err
			}
			defer r.Body.Close()

			if r.StatusCode >= 500 && r.StatusCode <= 599 {
				loopCount--
				return false, errors.Errorf("server responded with non-successful status: %s", r.Status)
			}
			return true, json.NewDecoder(r.Body).Decode(target)

		}()
		if stopRetry {
			break
		}
	}
	return err
}

// ImagePullCheck 拉取 kubeadm 使用的容器镜像。
type ImagePullCheck struct {
	criSocket       string
	imageList       []string
	sandboxImage    string
	imagePullPolicy v1.PullPolicy
	imagePullSerial bool

	// 测试时使用的替身实现。
	impl utilruntime.Impl
}

// Name 返回 ImagePullCheck 的标签。
func (ImagePullCheck) Name() string {
	return "ImagePull"
}

// Check 拉取 kubeadm 所需的镜像；此检查会修改系统状态。
func (ipc ImagePullCheck) Check() (warnings, errorList []error) {
	containerRuntime := utilruntime.NewContainerRuntime(ipc.criSocket)
	if ipc.impl != nil {
		containerRuntime.SetImpl(ipc.impl)
	}
	if err := containerRuntime.Connect(); err != nil {
		return nil, []error{errors.Wrap(err, "could not connect to the container runtime")}
	}
	defer containerRuntime.Close(context.Background())

	// 处理不支持的镜像拉取策略以及 Never 策略。
	policy := ipc.imagePullPolicy
	switch policy {
	case v1.PullAlways, v1.PullIfNotPresent:
		klog.V(1).Infof("using image pull policy: %s", policy)
	case v1.PullNever:
		klog.V(1).Infof("skipping the pull of all images due to policy: %s", policy)
		return warnings, errorList
	default:
		errorList = append(errorList, errors.Errorf("unsupported pull policy %q", policy))
		return warnings, errorList
	}

	// 处理 CRI sandbox 镜像警告。
	criSandboxImage, err := containerRuntime.SandboxImage()
	if err != nil {
		klog.V(4).Infof("failed to detect the sandbox image for local container runtime, %v", err)
	} else if criSandboxImage != ipc.sandboxImage {
		klog.Warningf("detected that the sandbox image %q of the container runtime is inconsistent with that used by kubeadm. "+
			"It is recommended to use %q as the CRI sandbox image.", criSandboxImage, ipc.sandboxImage)
	}

	// 并行拉取镜像。
	if !ipc.imagePullSerial {
		if err := containerRuntime.PullImagesInParallel(ipc.imageList, policy == v1.PullIfNotPresent); err != nil {
			errorList = append(errorList, err)
		}
		return warnings, errorList
	}

	// 串行拉取镜像。
	for _, image := range ipc.imageList {
		switch policy {
		case v1.PullIfNotPresent:
			if containerRuntime.ImageExists(image) {
				klog.V(1).Infof("image exists: %s", image)
				continue
			}
			fallthrough // 镜像不存在时继续拉取。
		case v1.PullAlways:
			klog.V(1).Infof("pulling: %s", image)
			if err := containerRuntime.PullImage(image); err != nil {
				errorList = append(errorList, errors.WithMessagef(err, "failed to pull image %s", image))
			}
		}
	}

	return warnings, errorList
}

// NumCPUCheck 校验当前 CPU 数量不低于要求。
type NumCPUCheck struct {
	NumCPU int
}

// Name 返回 NumCPUCheck 的标签。
func (NumCPUCheck) Name() string {
	return "NumCPU"
}

// Check 校验 CPU 数量是否满足 kubeadm 的要求。
func (ncc NumCPUCheck) Check() (warnings, errorList []error) {
	numCPU := runtime.NumCPU()
	if numCPU < ncc.NumCPU {
		errorList = append(errorList, errors.Errorf("the number of available CPUs %d is less than the required %d", numCPU, ncc.NumCPU))
	}
	return warnings, errorList
}

// MemCheck 校验内存兆字节数不低于要求。
type MemCheck struct {
	Mem uint64
}

// Name 返回内存检查的标签。
func (MemCheck) Name() string {
	return "Mem"
}

// InitNodeChecks 返回 "kubeadm init" 特有的检查。
func InitNodeChecks(execer utilsexec.Interface, cfg *kubeadmapi.InitConfiguration, ignorePreflightErrors sets.Set[string], isSecondaryControlPlane bool, downloadCerts bool) ([]Checker, error) {
	manifestsDir := filepath.Join(kubeadmconstants.KubernetesDir, kubeadmconstants.ManifestsSubDirName)

	// 检查用户是否在 extraArgs 中为控制平面组件配置了 bind-address；
	// 如果是，则将其用于 PortOpenCheck，避免误报。
	apiServerBindAddress, _ := kubeadmapi.GetArgValue(cfg.APIServer.ExtraArgs, "bind-address", -1)
	schedulerBindAddress, _ := kubeadmapi.GetArgValue(cfg.Scheduler.ExtraArgs, "bind-address", -1)
	controllerManagerBindAddress, _ := kubeadmapi.GetArgValue(cfg.ControllerManager.ExtraArgs, "bind-address", -1)

	checks := []Checker{
		NumCPUCheck{NumCPU: kubeadmconstants.ControlPlaneNumCPU},
		// 仅适用于 Linux。
		// TODO: 如果其他操作系统支持控制平面，则增加相应支持。
		MemCheck{Mem: kubeadmconstants.ControlPlaneMem},
		KubernetesVersionCheck{KubernetesVersion: cfg.KubernetesVersion, KubeadmVersion: kubeadmversion.Get().GitVersion},
		FirewalldCheck{ports: []int{int(cfg.LocalAPIEndpoint.BindPort), kubeadmconstants.KubeletPort}},
		PortOpenCheck{port: int(cfg.LocalAPIEndpoint.BindPort), address: apiServerBindAddress},
		PortOpenCheck{port: kubeadmconstants.KubeSchedulerPort, address: schedulerBindAddress},
		PortOpenCheck{port: kubeadmconstants.KubeControllerManagerPort, address: controllerManagerBindAddress},
		FileAvailableCheck{Path: kubeadmconstants.GetStaticPodFilepath(kubeadmconstants.KubeAPIServer, manifestsDir)},
		FileAvailableCheck{Path: kubeadmconstants.GetStaticPodFilepath(kubeadmconstants.KubeControllerManager, manifestsDir)},
		FileAvailableCheck{Path: kubeadmconstants.GetStaticPodFilepath(kubeadmconstants.KubeScheduler, manifestsDir)},
		FileAvailableCheck{Path: kubeadmconstants.GetStaticPodFilepath(kubeadmconstants.Etcd, manifestsDir)},
		HTTPProxyCheck{Proto: "https", Host: cfg.LocalAPIEndpoint.AdvertiseAddress},
	}

	// 以下情况需要检查 IPv4 和 IPv6 的文件内容：
	// （双协议栈）`--service-cidr` 或 `--pod-network-cidr` 同时设置为 IPv4 和 IPv6 CIDR；`--apiserver-advertise-address` 可选，因为可以自动检测。
	// （单协议栈）由 `--apiserver-advertise-address` 决定。
	// 注意：双协议栈下，用户可能只为 `--service-cidr` 提供 IPv6 CIDR，而将 `--apiserver-advertise-address` 保留为默认值。
	// 该地址会被自动检测并正确绑定到 IPv4 地址，最终导致集群无法正常工作；应通过校验避免这种情况。
	// 也就是说，这里不关心这些参数的输入值是否完全正确，只要 CIDR 或地址属于 IPv4 或 IPv6，就添加相应的文件内容检查。
	IPV4Check := false
	IPV6Check := false
	cidrs := strings.Split(cfg.Networking.ServiceSubnet, ",")
	for _, cidr := range cidrs {
		checks = append(checks, HTTPProxyCIDRCheck{Proto: "https", CIDR: cidr})
		if !IPV4Check && netutils.IsIPv4CIDRString(cidr) {
			IPV4Check = true
		}
		if !IPV6Check && netutils.IsIPv6CIDRString(cidr) {
			IPV6Check = true
		}

	}
	cidrs = strings.Split(cfg.Networking.PodSubnet, ",")
	for _, cidr := range cidrs {
		checks = append(checks, HTTPProxyCIDRCheck{Proto: "https", CIDR: cidr})
		if !IPV4Check && netutils.IsIPv4CIDRString(cidr) {
			IPV4Check = true
		}
		if !IPV6Check && netutils.IsIPv6CIDRString(cidr) {
			IPV6Check = true
		}
	}

	if !isSecondaryControlPlane {
		checks = addCommonChecks(execer, cfg.KubernetesVersion, &cfg.NodeRegistration, checks)

		// 检查 Bridge-netfilter 和 IPv6 相关标志是否已设置。
		if ip := netutils.ParseIPSloppy(cfg.LocalAPIEndpoint.AdvertiseAddress); ip != nil {
			if !IPV4Check && netutils.IsIPv4(ip) {
				IPV4Check = true
			}
			if !IPV6Check && netutils.IsIPv6(ip) {
				IPV6Check = true
			}
		}

		if IPV4Check {
			checks = addIPv4Checks(checks)
		}
		if IPV6Check {
			checks = addIPv6Checks(checks)
		}

		// 使用外部 etcd 时。
		if cfg.Etcd.External != nil {
			// 创建集群前检查外部 etcd 版本。
			checks = append(checks, ExternalEtcdVersionCheck{Etcd: cfg.Etcd})
		}
	}

	if cfg.Etcd.Local != nil {
		// 仅在需要安装本地 etcd 时执行 etcd 相关检查。
		// etcd 使用与 API server 公布地址相同的 IP，而不是绑定地址。
		// 如果 extraArgs 中定义了 --advertise-address，则使用该值，否则回退到 LocalAPIEndpoint.AdvertiseAddress。
		etcdAddress := cfg.LocalAPIEndpoint.AdvertiseAddress
		if advertiseAddress, _ := kubeadmapi.GetArgValue(cfg.APIServer.ExtraArgs, "advertise-address", -1); advertiseAddress != "" {
			etcdAddress = advertiseAddress
		}
		checks = append(checks,
			PortOpenCheck{port: kubeadmconstants.EtcdListenClientPort, address: etcdAddress},
			PortOpenCheck{port: kubeadmconstants.EtcdListenPeerPort, address: etcdAddress},
			DirAvailableCheck{Path: cfg.Etcd.Local.DataDir},
		)
	}

	if cfg.Etcd.External != nil && !(isSecondaryControlPlane && downloadCerts) {
		// 仅在使用外部 etcd 且未通过自动下载证书加入时检查 etcd 证书。
		if cfg.Etcd.External.CAFile != "" {
			checks = append(checks, FileExistingCheck{Path: cfg.Etcd.External.CAFile, Label: "ExternalEtcdClientCertificates"})
		}
		if cfg.Etcd.External.CertFile != "" {
			checks = append(checks, FileExistingCheck{Path: cfg.Etcd.External.CertFile, Label: "ExternalEtcdClientCertificates"})
		}
		if cfg.Etcd.External.KeyFile != "" {
			checks = append(checks, FileExistingCheck{Path: cfg.Etcd.External.KeyFile, Label: "ExternalEtcdClientCertificates"})
		}
	}
	return checks, nil
}

// RunInitNodeChecks 执行所有适用于控制平面节点的检查。
// 布尔标志 isSecondaryControlPlane 控制是否在 --join-control-plane 场景下运行检查。
// 布尔标志 downloadCerts 控制是否因正在下载证书而跳过证书检查。
// 如果该标志为 true，则应跳过 RunJoinNodeChecks 已执行的检查。
func RunInitNodeChecks(execer utilsexec.Interface, cfg *kubeadmapi.InitConfiguration, ignorePreflightErrors sets.Set[string], isSecondaryControlPlane bool, downloadCerts bool) error {
	checks, err := InitNodeChecks(execer, cfg, ignorePreflightErrors, isSecondaryControlPlane, downloadCerts)
	if err != nil {
		return err
	}
	return RunChecks(checks, os.Stderr, ignorePreflightErrors)
}

// JoinNodeChecks 返回 "kubeadm join" 特有的检查。
func JoinNodeChecks(execer utilsexec.Interface, cfg *kubeadmapi.JoinConfiguration, ignorePreflightErrors sets.Set[string]) ([]Checker, error) {
	checks := []Checker{
		FileAvailableCheck{Path: filepath.Join(kubeadmconstants.KubernetesDir, kubeadmconstants.KubeletKubeConfigFileName)},
		FileAvailableCheck{Path: filepath.Join(kubeadmconstants.KubernetesDir, kubeadmconstants.KubeletBootstrapKubeConfigFileName)},
	}
	checks = addCommonChecks(execer, "", &cfg.NodeRegistration, checks)
	if cfg.ControlPlane == nil {
		checks = append(checks, FileAvailableCheck{Path: cfg.CACertPath})
	}

	if cfg.Discovery.BootstrapToken != nil {
		ipstr, _, err := net.SplitHostPort(cfg.Discovery.BootstrapToken.APIServerEndpoint)
		if err == nil {
			checks = append(checks,
				HTTPProxyCheck{Proto: "https", Host: ipstr},
			)
			if ip := netutils.ParseIPSloppy(ipstr); ip != nil {
				if netutils.IsIPv4(ip) {
					checks = addIPv4Checks(checks)
				}
				if netutils.IsIPv6(ip) {
					checks = addIPv6Checks(checks)
				}
			}
		}
	}
	return checks, nil
}

// RunJoinNodeChecks 执行所有适用于节点的检查。
func RunJoinNodeChecks(execer utilsexec.Interface, cfg *kubeadmapi.JoinConfiguration, ignorePreflightErrors sets.Set[string]) error {
	checks, err := JoinNodeChecks(execer, cfg, ignorePreflightErrors)
	if err != nil {
		return err
	}
	return RunChecks(checks, os.Stderr, ignorePreflightErrors)
}

// addCommonChecks 是一个辅助函数，用于添加 kubeadm init 和 kubeadm join 命令共有的检查。
func addCommonChecks(execer utilsexec.Interface, k8sVersion string, nodeReg *kubeadmapi.NodeRegistrationOptions, checks []Checker) []Checker {
	checks = append(checks, ContainerRuntimeCheck{criSocket: nodeReg.CRISocket})
	checks = append(checks, ContainerRuntimeVersionCheck{criSocket: nodeReg.CRISocket})

	// 非 Windows 检查。
	checks = addSwapCheck(checks)
	checks = addExecChecks(checks, execer, k8sVersion)
	checks = append(checks,
		SystemVerificationCheck{isUpgrade: false, exec: execer},
		HostnameCheck{nodeName: nodeReg.Name},
		KubeletVersionCheck{KubernetesVersion: k8sVersion, exec: execer},
		ServiceCheck{Service: "kubelet", CheckIfActive: false},
		PortOpenCheck{port: kubeadmconstants.KubeletPort})
	return checks
}

// RunRootCheckOnly 初始化检查结构体切片并调用 RunChecks。
func RunRootCheckOnly(ignorePreflightErrors sets.Set[string]) error {
	checks := []Checker{
		IsPrivilegedUserCheck{},
	}

	return RunChecks(checks, os.Stderr, ignorePreflightErrors)
}

// RunUpgradeChecks 初始化检查结构体切片并调用 RunChecks。
func RunUpgradeChecks(execer utilsexec.Interface, cfg *kubeadmapi.InitConfiguration, ignorePreflightErrors sets.Set[string]) error {
	checks := []Checker{
		SystemVerificationCheck{isUpgrade: true, exec: execer},
		ContainerRuntimeVersionCheck{criSocket: cfg.NodeRegistration.CRISocket},
	}

	return RunChecks(checks, os.Stderr, ignorePreflightErrors)
}

// RunPullImagesCheck 在系统中找不到所需镜像时拉取 kubeadm 需要的镜像。
func RunPullImagesCheck(execer utilsexec.Interface, cfg *kubeadmapi.InitConfiguration, ignorePreflightErrors sets.Set[string]) error {
	serialPull := true
	if cfg.NodeRegistration.ImagePullSerial != nil {
		serialPull = *cfg.NodeRegistration.ImagePullSerial
	}

	checks := []Checker{
		ImagePullCheck{
			criSocket:       cfg.NodeRegistration.CRISocket,
			imageList:       images.GetControlPlaneImages(&cfg.ClusterConfiguration),
			sandboxImage:    images.GetPauseImage(&cfg.ClusterConfiguration),
			imagePullPolicy: cfg.NodeRegistration.ImagePullPolicy,
			imagePullSerial: serialPull,
		},
	}
	return RunChecks(checks, os.Stderr, ignorePreflightErrors)
}

// RunChecks 运行每个检查并显示其警告或错误；处理完所有检查后，如果出现任何错误则退出。
func RunChecks(checks []Checker, ww io.Writer, ignorePreflightErrors sets.Set[string]) error {
	var errsBuffer bytes.Buffer

	for _, c := range checks {
		name := c.Name()
		warnings, errs := c.Check()

		if setHasItemOrAll(ignorePreflightErrors, name) {
			// 将此检查的错误降级为警告。
			warnings = append(warnings, errs...)
			errs = []error{}
		}

		for _, w := range warnings {
			io.WriteString(ww, fmt.Sprintf("\t[WARNING %s]: %v\n", name, w))
		}
		for _, i := range errs {
			errsBuffer.WriteString(fmt.Sprintf("\t[ERROR %s]: %v\n", name, i.Error()))
		}
	}
	if errsBuffer.Len() > 0 {
		return handleError(ww, errsBuffer.String())
	}
	return nil
}

// setHasItemOrAll 是一个辅助函数；当 item 存在于集合中（忽略大小写）或集合包含特殊键 'all' 时返回 true。
func setHasItemOrAll(s sets.Set[string], item string) bool {
	if s.Has("all") || s.Has(strings.ToLower(item)) {
		return true
	}
	return false
}

// normalizeURLString 返回规范化后的字符串；如果无法解析为 URL 对象，则返回错误。
// 输入为 URL 字符串。
func normalizeURLString(s string) (string, error) {
	u, err := url.Parse(s)
	if err != nil {
		return "", err
	}
	if len(u.Path) > 0 {
		u.Path = strings.ReplaceAll(u.Path, "//", "/")
	}
	return u.String(), nil
}

func handleError(ww io.Writer, msg string) error {
	_, _ = fmt.Fprintf(ww, "[preflight] Some fatal errors occurred:\n%s%s", msg,
		"[preflight] If you know what you are doing, you can make a check non-fatal with `--ignore-preflight-errors=...`\n")
	return errors.New("preflight checks failed")
}
