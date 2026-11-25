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

package controlplane

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	staticpodutil "k8s.io/kubernetes/cmd/kubeadm/app/util/staticpod"
)

const (
	caCertsVolumeName = "ca-certs"
	caCertsVolumePath = "/etc/ssl/certs"
)

// caCertsExtraVolumePaths 指定可条件性挂载到 API Server 和 Controller Manager 容器的路径，
// 因为 /etc/ssl/certs 可能是指向这些路径的符号链接。此变量可在单元测试中修改，
// 运行时正常代码路径不得修改。
var caCertsExtraVolumePaths = []string{"/etc/pki/ca-trust", "/etc/pki/tls/certs", "/etc/ca-certificates", "/usr/share/ca-certificates", "/usr/local/share/ca-certificates"}

// getHostPathVolumesForTheControlPlane 获取控制平面所需的 hostPath 卷和挂载。
func getHostPathVolumesForTheControlPlane(cfg *kubeadmapi.ClusterConfiguration) controlPlaneHostPathMounts {
	hostPathDirectoryOrCreate := v1.HostPathDirectoryOrCreate
	hostPathFileOrCreate := v1.HostPathFileOrCreate
	mounts := newControlPlaneHostPathMounts()

	// API Server 的 hostPath 卷：
	// 证书目录的只读挂载。
	// TODO: 始终将 K8s 证书目录挂载到容器内的固定路径。
	mounts.NewHostPathMount(kubeadmconstants.KubeAPIServer, kubeadmconstants.KubeCertificatesVolumeName, cfg.CertificatesDir, cfg.CertificatesDir, true, &hostPathDirectoryOrCreate)
	// CA 证书目录（/etc/ssl/certs）的只读挂载
	mounts.NewHostPathMount(kubeadmconstants.KubeAPIServer, caCertsVolumeName, caCertsVolumePath, caCertsVolumePath, true, &hostPathDirectoryOrCreate)

	// 如果指定外部 etcd，挂载访问 CA 证书、服务证书和私钥所需的目录
	if cfg.Etcd.External != nil {
		etcdVols, etcdVolMounts := getEtcdCertVolumes(cfg.Etcd.External, cfg.CertificatesDir)
		mounts.AddHostPathMounts(kubeadmconstants.KubeAPIServer, etcdVols, etcdVolMounts)
	}

	// Controller Manager 的 hostPath 卷：
	// 证书目录的只读挂载。
	// TODO: 始终将 K8s 证书目录挂载到容器内的固定路径。
	mounts.NewHostPathMount(kubeadmconstants.KubeControllerManager, kubeadmconstants.KubeCertificatesVolumeName, cfg.CertificatesDir, cfg.CertificatesDir, true, &hostPathDirectoryOrCreate)
	// CA 证书目录（/etc/ssl/certs）的只读挂载
	mounts.NewHostPathMount(kubeadmconstants.KubeControllerManager, caCertsVolumeName, caCertsVolumePath, caCertsVolumePath, true, &hostPathDirectoryOrCreate)
	// 控制器管理器 kubeconfig 文件的只读挂载
	controllerManagerKubeConfigFile := filepath.Join(kubeadmconstants.KubernetesDir, kubeadmconstants.ControllerManagerKubeConfigFileName)
	mounts.NewHostPathMount(kubeadmconstants.KubeControllerManager, kubeadmconstants.KubeConfigVolumeName, controllerManagerKubeConfigFile, controllerManagerKubeConfigFile, true, &hostPathFileOrCreate)

	// 调度器的 hostPath 卷：
	// 调度器 kubeconfig 文件的只读挂载。
	schedulerKubeConfigFile := filepath.Join(kubeadmconstants.KubernetesDir, kubeadmconstants.SchedulerKubeConfigFileName)
	mounts.NewHostPathMount(kubeadmconstants.KubeScheduler, kubeadmconstants.KubeConfigVolumeName, schedulerKubeConfigFile, schedulerKubeConfigFile, true, &hostPathFileOrCreate)

	// 某些系统挂载 /etc/ssl/certs 后，还需挂载其他目录。
	// 因为该目录中的文件可能通过符号链接指向这些目录。
	for _, caCertsExtraVolumePath := range caCertsExtraVolumePaths {
		if isExtraVolumeMountNeeded(caCertsExtraVolumePath) {
			caCertsExtraVolumeName := strings.Replace(caCertsExtraVolumePath, "/", "-", -1)[1:]
			mounts.NewHostPathMount(kubeadmconstants.KubeAPIServer, caCertsExtraVolumeName, caCertsExtraVolumePath, caCertsExtraVolumePath, true, &hostPathDirectoryOrCreate)
			mounts.NewHostPathMount(kubeadmconstants.KubeControllerManager, caCertsExtraVolumeName, caCertsExtraVolumePath, caCertsExtraVolumePath, true, &hostPathDirectoryOrCreate)
		}
	}

	// 合并用户定义的挂载，并确保卷名和卷挂载名唯一
	mounts.AddExtraHostPathMounts(kubeadmconstants.KubeAPIServer, cfg.APIServer.ExtraVolumes)
	mounts.AddExtraHostPathMounts(kubeadmconstants.KubeControllerManager, cfg.ControllerManager.ExtraVolumes)
	mounts.AddExtraHostPathMounts(kubeadmconstants.KubeScheduler, cfg.Scheduler.ExtraVolumes)

	return mounts
}

// controlPlaneHostPathMounts 是用于统一管理控制平面全部 hostPath 挂载的辅助结构体。
type controlPlaneHostPathMounts struct {
	// volumes 是确保卷唯一的嵌套映射。外层映射的键指定要添加卷的目标组件；
	// 外层映射的值是内层映射，内层以字符串为键、v1.Volume 为值，键指定卷名。
	volumes map[string]map[string]v1.Volume
	// volumeMounts 是确保卷挂载唯一的嵌套映射。外层映射的键指定要添加卷挂载的目标组件；
	// 外层映射的值是内层映射，内层以字符串为键、v1.VolumeMount 为值，键指定卷挂载名。
	volumeMounts map[string]map[string]v1.VolumeMount
}

func newControlPlaneHostPathMounts() controlPlaneHostPathMounts {
	return controlPlaneHostPathMounts{
		volumes:      map[string]map[string]v1.Volume{},
		volumeMounts: map[string]map[string]v1.VolumeMount{},
	}
}

// NewHostPathMount 为组件添加一对 hostPath 卷与卷挂载；同名挂载会覆盖已有条目。
func (c *controlPlaneHostPathMounts) NewHostPathMount(component, mountName, hostPath, containerPath string, readOnly bool, hostPathType *v1.HostPathType) {
	vol := staticpodutil.NewVolume(mountName, hostPath, hostPathType)
	c.addComponentVolume(component, vol)
	volMount := staticpodutil.NewVolumeMount(mountName, containerPath, readOnly)
	c.addComponentVolumeMount(component, volMount)
}

// AddHostPathMounts 合并已构造的卷与挂载；卷名或挂载名相同时以后加入者为准。
func (c *controlPlaneHostPathMounts) AddHostPathMounts(component string, vols []v1.Volume, volMounts []v1.VolumeMount) {
	for _, v := range vols {
		c.addComponentVolume(component, v)
	}
	for _, v := range volMounts {
		c.addComponentVolumeMount(component, v)
	}
}

// AddExtraHostPathMounts 添加 hostPath 挂载；如果用户指定同名卷或卷挂载，则覆盖默认路径。
func (c *controlPlaneHostPathMounts) AddExtraHostPathMounts(component string, extraVols []kubeadmapi.HostPathMount) {
	for _, extraVol := range extraVols {
		hostPathType := extraVol.PathType
		c.NewHostPathMount(component, extraVol.Name, extraVol.HostPath, extraVol.MountPath, extraVol.ReadOnly, &hostPathType)
	}
}

// GetVolumes 返回组件的卷集合；尚未注册组件时返回 nil，调用方不应假定可直接写入。
func (c *controlPlaneHostPathMounts) GetVolumes(component string) map[string]v1.Volume {
	return c.volumes[component]
}

// GetVolumeMounts 返回组件的卷挂载集合；尚未注册组件时返回 nil，调用方不应假定可直接写入。
func (c *controlPlaneHostPathMounts) GetVolumeMounts(component string) map[string]v1.VolumeMount {
	return c.volumeMounts[component]
}

func (c *controlPlaneHostPathMounts) addComponentVolume(component string, vol v1.Volume) {
	if _, ok := c.volumes[component]; !ok {
		c.volumes[component] = map[string]v1.Volume{}
	}
	c.volumes[component][vol.Name] = vol
}

func (c *controlPlaneHostPathMounts) addComponentVolumeMount(component string, volMount v1.VolumeMount) {
	if _, ok := c.volumeMounts[component]; !ok {
		c.volumeMounts[component] = map[string]v1.VolumeMount{}
	}
	c.volumeMounts[component][volMount.Name] = volMount
}

// getEtcdCertVolumes 返回访问外部 etcd 集群所需的卷和卷挂载。
func getEtcdCertVolumes(etcdCfg *kubeadmapi.ExternalEtcd, k8sCertificatesDir string) ([]v1.Volume, []v1.VolumeMount) {
	certPaths := []string{etcdCfg.CAFile, etcdCfg.CertFile, etcdCfg.KeyFile}
	certDirs := sets.New[string]()
	for _, certPath := range certPaths {
		certDir := filepath.ToSlash(filepath.Dir(certPath))
		// 忽略空路径转换得到的 "."。
		// 同时忽略可能已挂载的证书目录：/etc/ssl/certs、/etc/pki/ca-trust/ 或 Kubernetes CertificatesDir。
		// 如果 etcd 证书位于其中，则无需额外处理。
		extraVolumePath := false
		for _, caCertsExtraVolumePath := range caCertsExtraVolumePaths {
			if strings.HasPrefix(certDir, caCertsExtraVolumePath) {
				extraVolumePath = true
				break
			}
		}
		if certDir == "." || extraVolumePath || strings.HasPrefix(certDir, caCertsVolumePath) || strings.HasPrefix(certDir, k8sCertificatesDir) {
			continue
		}
		// 从列表中过滤掉包含当前路径子集的现有 hostPath 挂载
		alreadyExists := false
		for _, existingCertDir := range sets.List(certDirs) {
			// 如果当前目录是现有目录的父目录，移除现有挂载
			if strings.HasPrefix(existingCertDir, certDir) {
				certDirs.Delete(existingCertDir)
			} else if strings.HasPrefix(certDir, existingCertDir) {
				// 如果现有目录是当前目录的父目录，不添加当前挂载
				alreadyExists = true
			}
		}
		if alreadyExists {
			continue
		}
		certDirs.Insert(certDir)
	}

	volumes := []v1.Volume{}
	volumeMounts := []v1.VolumeMount{}
	pathType := v1.HostPathDirectoryOrCreate
	for i, certDir := range sets.List(certDirs) {
		name := fmt.Sprintf("etcd-certs-%d", i)
		volumes = append(volumes, staticpodutil.NewVolume(name, certDir, &pathType))
		volumeMounts = append(volumeMounts, staticpodutil.NewVolumeMount(name, certDir, true))
	}
	return volumes, volumeMounts
}

// isExtraVolumeMountNeeded 判断是否应将 /etc/pki/ca-trust/ 挂载到容器。
// 某些系统挂载 /etc/ssl/certs 后还需挂载 /etc/pki/ca-trust/，
// 因为 /etc/ssl/certs 中的文件可能通过符号链接指向该目录。
func isExtraVolumeMountNeeded(caCertsExtraVolumePath string) bool {
	if _, err := os.Stat(caCertsExtraVolumePath); err == nil {
		return true
	}
	return false
}
