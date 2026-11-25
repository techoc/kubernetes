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

package kubelet

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	v1 "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	kubeletconfig "k8s.io/kubelet/config/v1beta1"
	"sigs.k8s.io/yaml"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/componentconfigs"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/patches"
)

var applyKubeletConfigPatchesFunc = applyKubeletConfigPatches

// WriteConfigToDisk 将 kubelet 配置对象写入文件。
// 在 "kubeadm init" 和 "kubeadm upgrade" 时使用。
func WriteConfigToDisk(cfg *kubeadmapi.ClusterConfiguration, kubeletDir, patchesDir string, output io.Writer) error {
	kubeletCfg, ok := cfg.ComponentConfigs[componentconfigs.KubeletGroup]
	if !ok {
		return errors.New("no kubelet component config found")
	}

	// 节点特定修改不得泄漏到写入文件后可能上传的集群级配置中。
	localKubeletCfg := kubeletCfg.DeepCopy()
	if err := localKubeletCfg.Mutate(); err != nil {
		return err
	}

	kubeletBytes, err := localKubeletCfg.Marshal()
	if err != nil {
		return err
	}

	// 将补丁应用到 KubeletConfiguration。
	if len(patchesDir) != 0 {
		kubeletBytes, err = applyKubeletConfigPatchesFunc(kubeletBytes, patchesDir, output)
		if err != nil {
			return errors.Wrap(err, "could not apply patches to the KubeletConfiguration")
		}
	}

	file := filepath.Join(kubeletDir, kubeadmconstants.KubeletInstanceConfigurationFileName)
	kubeletBytes, err = applyKubeletConfigPatchFromFile(kubeletBytes, file, output)
	if err != nil {
		return errors.Wrapf(err, "could not apply kubelet instance configuration as a patch from %q", file)
	}

	return writeConfigBytesToDisk(kubeletBytes, kubeletDir, kubeadmconstants.KubeletConfigurationFileName)
}

// WriteInstanceConfigToDisk 将容器运行时端点配置写入指定 kubelet 目录中的实例配置文件。
func WriteInstanceConfigToDisk(cfg *kubeletconfig.KubeletConfiguration, kubeletDir string) error {
	instanceFileContent := fmt.Sprintf("containerRuntimeEndpoint: %q\n", cfg.ContainerRuntimeEndpoint)
	return writeConfigBytesToDisk([]byte(instanceFileContent), kubeletDir, kubeadmconstants.KubeletInstanceConfigurationFileName)
}

// ApplyPatchesToConfig 将 patchesDir 中的补丁应用到 ClusterConfiguration.ComponentConfigs 中存储的 KubeletConfiguration。
func ApplyPatchesToConfig(cfg *kubeadmapi.ClusterConfiguration, patchesDir string) error {
	kubeletCfg, ok := cfg.ComponentConfigs[componentconfigs.KubeletGroup]
	if !ok {
		return errors.New("no kubelet component config found")
	}

	if err := kubeletCfg.Mutate(); err != nil {
		return err
	}

	kubeletBytes, err := kubeletCfg.Marshal()
	if err != nil {
		return err
	}

	// 将补丁应用到 KubeletConfiguration，并丢弃输出。
	if len(patchesDir) != 0 {
		kubeletBytes, err = applyKubeletConfigPatchesFunc(kubeletBytes, patchesDir, io.Discard)
		if err != nil {
			return errors.Wrap(err, "could not apply patches to the KubeletConfiguration")
		}
	}

	gvkmap, err := kubeadmutil.SplitConfigDocuments(kubeletBytes)
	if err != nil {
		return err
	}
	if err := kubeletCfg.Unmarshal(gvkmap); err != nil {
		return err
	}

	return nil
}

// CreateConfigMap 创建包含通用 kubelet 配置的 ConfigMap。
// 在 "kubeadm init" 和 "kubeadm upgrade" 时使用。
func CreateConfigMap(cfg *kubeadmapi.ClusterConfiguration, client clientset.Interface) error {
	configMapName := kubeadmconstants.KubeletBaseConfigurationConfigMap
	fmt.Printf("[kubelet] Creating a ConfigMap %q in namespace %s with the configuration for the kubelets in the cluster\n", configMapName, metav1.NamespaceSystem)

	kubeletCfg, ok := cfg.ComponentConfigs[componentconfigs.KubeletGroup]
	if !ok {
		return errors.New("no kubelet component config found in the active component config set")
	}

	kubeletBytes, err := kubeletCfg.Marshal()
	if err != nil {
		return err
	}

	configMap := &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      configMapName,
			Namespace: metav1.NamespaceSystem,
		},
		Data: map[string]string{
			kubeadmconstants.KubeletBaseConfigurationConfigMapKey: string(kubeletBytes),
		},
	}

	if !kubeletCfg.IsUserSupplied() {
		componentconfigs.SignConfigMap(configMap)
	}

	if err := apiclient.CreateOrUpdate(client.CoreV1().ConfigMaps(configMap.GetNamespace()), configMap); err != nil {
		return err
	}

	if err := createConfigMapRBACRules(client); err != nil {
		return errors.Wrap(err, "error creating kubelet configuration configmap RBAC rules")
	}
	return nil
}

// createConfigMapRBACRules 创建 RBAC 规则，允许未认证用户访问 kube-system 命名空间中的基础 kubelet ConfigMap。
func createConfigMapRBACRules(client clientset.Interface) error {
	if err := apiclient.CreateOrUpdate(client.RbacV1().Roles(metav1.NamespaceSystem), &rbac.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kubeadmconstants.KubeletBaseConfigMapRole,
			Namespace: metav1.NamespaceSystem,
		},
		Rules: []rbac.PolicyRule{
			{
				Verbs:         []string{"get"},
				APIGroups:     []string{""},
				Resources:     []string{"configmaps"},
				ResourceNames: []string{kubeadmconstants.KubeletBaseConfigurationConfigMap},
			},
		},
	}); err != nil {
		return err
	}

	return apiclient.CreateOrUpdate(client.RbacV1().RoleBindings(metav1.NamespaceSystem), &rbac.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kubeadmconstants.KubeletBaseConfigMapRole,
			Namespace: metav1.NamespaceSystem,
		},
		RoleRef: rbac.RoleRef{
			APIGroup: rbac.GroupName,
			Kind:     "Role",
			Name:     kubeadmconstants.KubeletBaseConfigMapRole,
		},
		Subjects: []rbac.Subject{
			{
				Kind: rbac.GroupKind,
				Name: kubeadmconstants.NodesGroup,
			},
			{
				Kind: rbac.GroupKind,
				Name: kubeadmconstants.NodeBootstrapTokenAuthGroup,
			},
		},
	})
}

// writeConfigBytesToDisk 将字节切片写入 kubelet 配置文件指定位置。
func writeConfigBytesToDisk(b []byte, kubeletDir, fileName string) error {
	configFile := filepath.Join(kubeletDir, fileName)
	fmt.Printf("[kubelet-start] Writing kubelet configuration to file %q\n", configFile)

	// 若目标目录不存在，则创建。
	if err := os.MkdirAll(kubeletDir, 0700); err != nil {
		return errors.Wrapf(err, "failed to create directory %q", kubeletDir)
	}

	if err := os.WriteFile(configFile, b, 0644); err != nil {
		return errors.Wrapf(err, "failed to write kubelet configuration file %q", configFile)
	}
	return nil
}

// applyKubeletConfigPatches 从目录读取补丁，并将其应用到传入的 kubeletBytes。
func applyKubeletConfigPatches(kubeletBytes []byte, patchesDir string, output io.Writer) ([]byte, error) {
	patchManager, err := patches.GetPatchManagerForPath(patchesDir, patches.KnownTargets(), output)
	if err != nil {
		return nil, err
	}

	patchTarget := &patches.PatchTarget{
		Name:                      patches.KubeletConfiguration,
		StrategicMergePatchObject: kubeletconfig.KubeletConfiguration{},
		Data:                      kubeletBytes,
	}
	if err := patchManager.ApplyPatchesToTarget(patchTarget); err != nil {
		return nil, err
	}

	kubeletBytes, err = yaml.JSONToYAML(patchTarget.Data)
	if err != nil {
		return nil, err
	}
	return kubeletBytes, nil
}

// applyKubeletConfigPatchFromFile 将单个补丁文件应用到 kubelet 配置内容。
func applyKubeletConfigPatchFromFile(kubeletConfigBytes []byte, patchFilePath string, output io.Writer) ([]byte, error) {
	// 从文件读取补丁数据。
	data, err := os.ReadFile(patchFilePath)
	if err != nil {
		return nil, errors.Wrapf(err, "could not read patch file %q", patchFilePath)
	}

	patchSet, err := patches.CreatePatchSet(patches.KubeletConfiguration, types.StrategicMergePatchType, string(data))
	if err != nil {
		return nil, err
	}

	patchManager := patches.NewPatchManager([]*patches.PatchSet{patchSet}, []string{patches.KubeletConfiguration}, output)

	// 始终将目标数据转换为 JSON。
	patchData, err := yaml.YAMLToJSON(kubeletConfigBytes)
	if err != nil {
		return nil, err
	}

	// 定义补丁目标。
	patchTarget := &patches.PatchTarget{
		Name:                      patches.KubeletConfiguration,
		StrategicMergePatchObject: kubeletconfig.KubeletConfiguration{},
		Data:                      patchData,
	}

	err = patchManager.ApplyPatchesToTarget(patchTarget)
	if err != nil {
		return nil, err
	}

	// 将打过补丁的数据转换回 YAML 并返回。
	kubeletConfigBytes, err = yaml.JSONToYAML(patchTarget.Data)
	if err != nil {
		return nil, errors.Wrap(err, "failed to convert patched data to YAML")
	}

	return kubeletConfigBytes, nil
}
