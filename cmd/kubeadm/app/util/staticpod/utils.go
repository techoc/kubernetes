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

package staticpod

import (
	"bytes"
	"fmt"
	"hash"
	"hash/fnv"
	"io"
	"math"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"

	"github.com/pmezard/go-difflib/difflib"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	utilerrors "k8s.io/apimachinery/pkg/util/errors"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/utils/dump"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/patches"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/users"
)

const (
	// kubeControllerManagerBindAddressArg 表示 kube-controller-manager 配置的 bind-address 参数。
	kubeControllerManagerBindAddressArg = "bind-address"

	// kubeSchedulerBindAddressArg 表示 kube-scheduler 配置的 bind-address 参数。
	kubeSchedulerBindAddressArg = "bind-address"
)

var (
	usersAndGroups     *users.UsersAndGroups
	usersAndGroupsOnce sync.Once
)

// ComponentPod 根据容器、卷和注解规格创建 Pod 对象。
func ComponentPod(container v1.Container, volumes map[string]v1.Volume, annotations map[string]string) v1.Pod {
	// system-node-critical 优先级的 priority 值。
	priority := int32(2000001000)
	return v1.Pod{
		TypeMeta: metav1.TypeMeta{
			APIVersion: "v1",
			Kind:       "Pod",
		},
		ObjectMeta: metav1.ObjectMeta{
			Name:      container.Name,
			Namespace: metav1.NamespaceSystem,
			// component 和 tier 标签便于在 kube-system 中列出控制平面 Pod 时快速识别，
			// 例如可与 WaitForPodsWithLabel 配合使用。
			Labels:      map[string]string{"component": container.Name, "tier": kubeadmconstants.ControlPlaneTier},
			Annotations: annotations,
		},
		Spec: v1.PodSpec{
			Containers:        []v1.Container{container},
			Priority:          &priority,
			PriorityClassName: "system-node-critical",
			HostNetwork:       true,
			Volumes:           VolumeMapToSlice(volumes),
			SecurityContext: &v1.PodSecurityContext{
				SeccompProfile: &v1.SeccompProfile{
					Type: v1.SeccompProfileTypeRuntimeDefault,
				},
			},
		},
	}
}

// ComponentResources 返回分配指定 CPU 数量所需的 v1.ResourceRequirements。
func ComponentResources(cpu string) v1.ResourceRequirements {
	return v1.ResourceRequirements{
		Requests: v1.ResourceList{
			v1.ResourceCPU: resource.MustParse(cpu),
		},
	}
}

// NewVolume 创建挂载到指定位置的 hostPath v1.Volume。
func NewVolume(name, path string, pathType *v1.HostPathType) v1.Volume {
	return v1.Volume{
		Name: name,
		VolumeSource: v1.VolumeSource{
			HostPath: &v1.HostPathVolumeSource{
				Path: path,
				Type: pathType,
			},
		},
	}
}

// NewVolumeMount 创建指向指定位置的 v1.VolumeMount。
func NewVolumeMount(name, path string, readOnly bool) v1.VolumeMount {
	return v1.VolumeMount{
		Name:      name,
		MountPath: path,
		ReadOnly:  readOnly,
	}
}

// VolumeMapToSlice 将映射中的值转换为卷切片。
func VolumeMapToSlice(volumes map[string]v1.Volume) []v1.Volume {
	v := make([]v1.Volume, 0, len(volumes))

	for _, vol := range volumes {
		v = append(v, vol)
	}

	sort.Slice(v, func(i, j int) bool {
		return strings.Compare(v[i].Name, v[j].Name) == -1
	})

	return v
}

// VolumeMountMapToSlice 将映射中的值转换为卷挂载切片。
func VolumeMountMapToSlice(volumeMounts map[string]v1.VolumeMount) []v1.VolumeMount {
	v := make([]v1.VolumeMount, 0, len(volumeMounts))

	for _, volMount := range volumeMounts {
		v = append(v, volMount)
	}

	sort.Slice(v, func(i, j int) bool {
		return strings.Compare(v[i].Name, v[j].Name) == -1
	})

	return v
}

// PatchStaticPod 将 patchesDir 中的补丁应用到静态 Pod。
func PatchStaticPod(pod *v1.Pod, patchesDir string, output io.Writer) (*v1.Pod, error) {
	// 将 Pod 清单序列化为 YAML。
	podYAML, err := kubeadmutil.MarshalToYaml(pod, v1.SchemeGroupVersion)
	if err != nil {
		return pod, errors.Wrapf(err, "failed to marshal Pod manifest to YAML")
	}

	patchManager, err := patches.GetPatchManagerForPath(patchesDir, patches.KnownTargets(), output)
	if err != nil {
		return pod, err
	}

	patchTarget := &patches.PatchTarget{
		Name:                      pod.Name,
		StrategicMergePatchObject: v1.Pod{},
		Data:                      podYAML,
	}
	if err := patchManager.ApplyPatchesToTarget(patchTarget); err != nil {
		return pod, err
	}

	obj, err := kubeadmutil.UniversalUnmarshal(patchTarget.Data)
	if err != nil {
		return pod, errors.Wrap(err, "failed to unmarshal patched manifest")
	}

	pod2, ok := obj.(*v1.Pod)
	if !ok {
		return pod, errors.Wrap(err, "patched manifest is not a valid Pod object")
	}

	return pod2, nil
}

// WriteStaticPodToDisk 将静态 Pod 文件写入磁盘。
func WriteStaticPodToDisk(componentName, manifestDir string, pod v1.Pod) error {

	// 必要时创建目标目录。
	if err := os.MkdirAll(manifestDir, 0700); err != nil {
		return errors.Wrapf(err, "failed to create directory %q", manifestDir)
	}

	// 将 Pod 写入磁盘。
	serialized, err := kubeadmutil.MarshalToYaml(&pod, v1.SchemeGroupVersion)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal manifest for %q to YAML", componentName)
	}

	filename := kubeadmconstants.GetStaticPodFilepath(componentName, manifestDir)

	if err := os.WriteFile(filename, serialized, 0600); err != nil {
		return errors.Wrapf(err, "failed to write static pod manifest file for %q (%q)", componentName, filename)
	}

	return nil
}

// ReadStaticPodFromDisk 从磁盘读取静态 Pod 文件。
func ReadStaticPodFromDisk(manifestPath string) (*v1.Pod, error) {
	buf, err := os.ReadFile(manifestPath)
	if err != nil {
		return &v1.Pod{}, errors.Wrapf(err, "failed to read manifest for %q", manifestPath)
	}

	obj, err := kubeadmutil.UniversalUnmarshal(buf)
	if err != nil {
		return &v1.Pod{}, errors.Errorf("failed to unmarshal manifest for %q: %v", manifestPath, err)
	}

	pod, ok := obj.(*v1.Pod)
	if !ok {
		return &v1.Pod{}, errors.Errorf("failed to parse Pod object defined in %q", manifestPath)
	}

	return pod, nil
}

// ReadMultipleStaticPodsFromDisk 从 manifestDir 读取多个已知组件的静态 Pod，并返回 Pod 列表。
func ReadMultipleStaticPodsFromDisk(manifestDir string, components ...string) (map[string]*v1.Pod, error) {
	var (
		podMap = map[string]*v1.Pod{}
		errs   []error
	)
	for _, c := range components {
		path := kubeadmconstants.GetStaticPodFilepath(c, manifestDir)
		pod, err := ReadStaticPodFromDisk(path)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		podMap[c] = pod
	}
	if len(errs) > 0 {
		return nil, utilerrors.NewAggregate(errs)
	}
	return podMap, nil
}

// LivenessProbe 创建带 HTTPGet 处理器的 Probe。
func LivenessProbe(host, path, port string, scheme v1.URIScheme) *v1.Probe {
	// 将 initialDelaySeconds 设为 periodSeconds，跳过一个周期后再执行检查。
	return createHTTPProbe(host, path, port, scheme, 10, 15, 8, 10)
}

// ReadinessProbe 创建带 HTTPGet 处理器的 Probe。
func ReadinessProbe(host, path, port string, scheme v1.URIScheme) *v1.Probe {
	// initialDelaySeconds 设为 0，避免延迟基础设施检查 kubeadm 静态 Pod 的 "ready" 状态。
	return createHTTPProbe(host, path, port, scheme, 0, 15, 3, 1)
}

// StartupProbe 创建带 HTTPGet 处理器的 Probe。
func StartupProbe(host, path, port string, scheme v1.URIScheme, timeoutForControlPlane *metav1.Duration) *v1.Probe {
	periodSeconds, timeoutForControlPlaneSeconds := int32(10), kubeadmconstants.ControlPlaneComponentHealthCheckTimeout.Seconds()
	if timeoutForControlPlane != nil {
		timeoutForControlPlaneSeconds = timeoutForControlPlane.Seconds()
	}
	// failureThreshold 足够大，确保完整超时覆盖控制平面启动的最坏情况；
	// 为简化计算，忽略 initialDelaySeconds。
	failureThreshold := int32(math.Ceil(timeoutForControlPlaneSeconds / float64(periodSeconds)))
	// 将 initialDelaySeconds 设为 periodSeconds，跳过一个周期后再执行检查。
	return createHTTPProbe(host, path, port, scheme, periodSeconds, 15, failureThreshold, periodSeconds)
}

func createHTTPProbe(host, path, port string, scheme v1.URIScheme, initialDelaySeconds, timeoutSeconds, failureThreshold, periodSeconds int32) *v1.Probe {
	return &v1.Probe{
		ProbeHandler: v1.ProbeHandler{
			HTTPGet: &v1.HTTPGetAction{
				Host:   host,
				Path:   path,
				Port:   intstr.FromString(port),
				Scheme: scheme,
			},
		},
		InitialDelaySeconds: initialDelaySeconds,
		TimeoutSeconds:      timeoutSeconds,
		FailureThreshold:    failureThreshold,
		PeriodSeconds:       periodSeconds,
	}
}

// GetAPIServerProbeAddress 返回 API Server 的探测地址。
func GetAPIServerProbeAddress(endpoint *kubeadmapi.APIEndpoint) string {
	if endpoint != nil && endpoint.AdvertiseAddress != "" {
		return getProbeAddress(endpoint.AdvertiseAddress)
	}

	return "127.0.0.1"
}

// GetControllerManagerProbeAddress 返回 Kubernetes 控制器管理器的探测地址。
func GetControllerManagerProbeAddress(cfg *kubeadmapi.ClusterConfiguration) string {
	if addr, idx := kubeadmapi.GetArgValue(cfg.ControllerManager.ExtraArgs, kubeControllerManagerBindAddressArg, -1); idx > -1 {
		return getProbeAddress(addr)
	}
	return "127.0.0.1"
}

// GetSchedulerProbeAddress 返回 Kubernetes 调度器的探测地址。
func GetSchedulerProbeAddress(cfg *kubeadmapi.ClusterConfiguration) string {
	if addr, idx := kubeadmapi.GetArgValue(cfg.Scheduler.ExtraArgs, kubeSchedulerBindAddressArg, -1); idx > -1 {
		return getProbeAddress(addr)
	}
	return "127.0.0.1"
}

// GetEtcdProbeEndpoint 解析 kubeadm Etcd 配置中 listen-metrics-urls 参数的第一个 URL，
// 返回 etcd 探测主机名、端口和 scheme。
func GetEtcdProbeEndpoint(cfg *kubeadmapi.Etcd, isIPv6 bool) (string, int32, v1.URIScheme) {
	localhost := "127.0.0.1"
	if isIPv6 {
		localhost = "::1"
	}
	if cfg.Local == nil || cfg.Local.ExtraArgs == nil {
		return localhost, kubeadmconstants.EtcdMetricsPort, v1.URISchemeHTTP
	}
	if arg, idx := kubeadmapi.GetArgValue(cfg.Local.ExtraArgs, "listen-metrics-urls", -1); idx > -1 {
		// 指定多个 URL 时使用 listen-metrics-urls 中的第一个。
		arg = strings.Split(arg, ",")[0]
		parsedURL, err := url.Parse(arg)
		if err != nil {
			return localhost, kubeadmconstants.EtcdMetricsPort, v1.URISchemeHTTP
		}
		// 解析 scheme。
		scheme := v1.URISchemeHTTP
		if parsedURL.Scheme == "https" {
			scheme = v1.URISchemeHTTPS
		}
		// 解析主机名。
		hostname := parsedURL.Hostname()
		if len(hostname) == 0 {
			hostname = localhost
		}
		// 解析端口。
		port := kubeadmconstants.EtcdMetricsPort
		portStr := parsedURL.Port()
		if len(portStr) != 0 {
			p, err := kubeadmutil.ParsePort(portStr)
			if err == nil {
				port = p
			}
		}
		return hostname, int32(port), scheme
	}
	return localhost, kubeadmconstants.EtcdMetricsPort, v1.URISchemeHTTP
}

// ManifestFilesAreEqual 比较两个文件，内容相同返回 true，否则返回 false。
func ManifestFilesAreEqual(path1, path2 string) (bool, string, error) {
	pod1, err := ReadStaticPodFromDisk(path1)
	if err != nil {
		return false, "", err
	}
	pod2, err := ReadStaticPodFromDisk(path2)
	if err != nil {
		return false, "", err
	}

	hasher := fnv.New128a()
	DeepHashObject(hasher, pod1)
	hash1 := hasher.Sum(nil)[0:]
	DeepHashObject(hasher, pod2)
	hash2 := hasher.Sum(nil)[0:]
	if bytes.Equal(hash1, hash2) {
		return true, "", nil
	}

	manifest1, err := kubeadmutil.MarshalToYaml(pod1, v1.SchemeGroupVersion)
	if err != nil {
		return false, "", errors.Wrapf(err, "failed to marshal Pod manifest for %q to YAML", path1)
	}

	manifest2, err := kubeadmutil.MarshalToYaml(pod2, v1.SchemeGroupVersion)
	if err != nil {
		return false, "", errors.Wrapf(err, "failed to marshal Pod manifest for %q to YAML", path2)
	}

	diff := difflib.UnifiedDiff{
		A: difflib.SplitLines(string(manifest1)),
		B: difflib.SplitLines(string(manifest2)),
	}

	diffStr, err := difflib.GetUnifiedDiffString(diff)
	if err != nil {
		return false, "", errors.Wrapf(err, "failed to generate the differences between manifest %q and manifest %q", path1, path2)
	}

	return false, diffStr, nil
}

// getProbeAddress 返回有效探测地址。kubeadm 使用 bind-address 配置探测地址；
// 监听所有接口时常设为未指定地址 "0.0.0.0" 或 "::"，但该地址不能用于探测（见 #86504）。
// 若为未指定地址，返回空字符串，使 kubelet 使用 PodIP 作为探测地址。
func getProbeAddress(addr string) string {
	if addr == "0.0.0.0" || addr == "::" {
		return ""
	}
	return addr
}

// GetUsersAndGroups 返回本地 usersAndGroups；首次调用时以线程安全方式创建一次。
func GetUsersAndGroups() (*users.UsersAndGroups, error) {
	var err error
	usersAndGroupsOnce.Do(func() {
		usersAndGroups, err = users.AddUsersAndGroups()
	})
	return usersAndGroups, err
}

// DeepHashObject 使用 spew 库将对象写入哈希，跟随指针并输出嵌套对象的实际值，
// 确保指针变化时哈希不变。复制自 k8s.io/kubernetes/pkg/util/hash/hash.go#DeepHashObject。
func DeepHashObject(hasher hash.Hash, objectToWrite interface{}) {
	hasher.Reset()
	fmt.Fprintf(hasher, "%v", dump.ForHash(objectToWrite))
}

// IsControlPlaneNode 在宿主机存在 kube-apiserver 静态 Pod 清单时返回 true。
func IsControlPlaneNode() bool {
	isControlPlaneNode := true

	filepath := kubeadmconstants.GetStaticPodFilepath(kubeadmconstants.KubeAPIServer,
		kubeadmconstants.GetStaticPodDirectory())

	if _, err := os.Stat(filepath); os.IsNotExist(err) {
		isControlPlaneNode = false
	}

	return isControlPlaneNode
}
