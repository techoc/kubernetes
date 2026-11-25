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

package proxy

import (
	"bufio"
	"bytes"
	"fmt"
	"io"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clientset "k8s.io/client-go/kubernetes"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/yaml"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/componentconfigs"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/images"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/patches"
)

const (
	// KubeProxyServiceAccountName 描述 kube-proxy 插件的 ServiceAccount 名称。
	KubeProxyServiceAccountName = "kube-proxy"

	// KubeProxyConfigMapRoleName 设置 ConfigMap 所用 ClusterRole 的名称。
	KubeProxyConfigMapRoleName = "kube-proxy"
)

// EnsureProxyAddon 创建 kube-proxy 插件。
func EnsureProxyAddon(cfg *kubeadmapi.ClusterConfiguration, localEndpoint *kubeadmapi.APIEndpoint, client clientset.Interface, patchesDir string, out io.Writer, printManifest bool) error {
	cmByte, err := createKubeProxyConfigMap(cfg, localEndpoint, client, printManifest)
	if err != nil {
		return err
	}

	dsByte, err := createKubeProxyAddon(cfg, client, patchesDir, out, printManifest)
	if err != nil {
		return err
	}

	if err := printOrCreateKubeProxyObjects(cmByte, dsByte, client, out, printManifest); err != nil {
		return err
	}

	return nil
}

// 创建 ServiceAccount 和 RBAC 规则；若 printManifest 为 true，则将其清单输出到 out。
func printOrCreateKubeProxyObjects(cmByte []byte, dsByte []byte, client clientset.Interface, out io.Writer, printManifest bool) error {
	var saBytes, crbBytes, roleBytes, roleBindingBytes []byte
	var err error

	sa := &v1.ServiceAccount{
		ObjectMeta: metav1.ObjectMeta{
			Name:      KubeProxyServiceAccountName,
			Namespace: metav1.NamespaceSystem,
		},
	}

	crb := &rbac.ClusterRoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name: constants.KubeProxyClusterRoleBindingName,
		},
		RoleRef: rbac.RoleRef{
			APIGroup: rbac.GroupName,
			Kind:     "ClusterRole",
			Name:     constants.KubeProxyClusterRoleName,
		},
		Subjects: []rbac.Subject{
			{
				Kind:      rbac.ServiceAccountKind,
				Name:      KubeProxyServiceAccountName,
				Namespace: metav1.NamespaceSystem,
			},
		},
	}

	role := &rbac.Role{
		ObjectMeta: metav1.ObjectMeta{
			Name:      KubeProxyConfigMapRoleName,
			Namespace: metav1.NamespaceSystem,
		},
		Rules: []rbac.PolicyRule{
			{
				Verbs:         []string{"get"},
				APIGroups:     []string{""},
				Resources:     []string{"configmaps"},
				ResourceNames: []string{constants.KubeProxyConfigMap},
			},
		},
	}

	rb := &rbac.RoleBinding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      KubeProxyConfigMapRoleName,
			Namespace: metav1.NamespaceSystem,
		},
		RoleRef: rbac.RoleRef{
			APIGroup: rbac.GroupName,
			Kind:     "Role",
			Name:     KubeProxyConfigMapRoleName,
		},
		Subjects: []rbac.Subject{
			{
				Kind: rbac.GroupKind,
				Name: constants.NodeBootstrapTokenAuthGroup,
			},
		},
	}

	// 若 printManifest 为 false，则创建对象。
	if !printManifest {
		if err := apiclient.CreateOrUpdate(client.CoreV1().ServiceAccounts(sa.GetNamespace()), sa); err != nil {
			return errors.Wrap(err, "error when creating kube-proxy service account")
		}

		if err := apiclient.CreateOrUpdate(client.RbacV1().ClusterRoleBindings(), crb); err != nil {
			return err
		}

		if err := apiclient.CreateOrUpdate(client.RbacV1().Roles(role.GetNamespace()), role); err != nil {
			return err
		}

		if err := apiclient.CreateOrUpdate(client.RbacV1().RoleBindings(rb.GetNamespace()), rb); err != nil {
			return err
		}

		fmt.Fprintln(out, "[addons] Applied essential addon: kube-proxy")

		return nil

	}

	gv := schema.GroupVersion{Group: "", Version: "v1"}
	if saBytes, err = kubeadmutil.MarshalToYaml(sa, gv); err != nil {
		return err
	}

	gv = schema.GroupVersion{Group: "rbac.authorization.k8s.io", Version: "v1"}
	if crbBytes, err = kubeadmutil.MarshalToYaml(crb, gv); err != nil {
		return err
	}

	if roleBytes, err = kubeadmutil.MarshalToYaml(role, gv); err != nil {
		return err
	}

	if roleBindingBytes, err = kubeadmutil.MarshalToYaml(rb, gv); err != nil {
		return err
	}

	fmt.Fprintln(out, "---")
	fmt.Fprintf(out, "%s", saBytes)
	fmt.Fprintln(out, "---")
	fmt.Fprintf(out, "%s", crbBytes)
	fmt.Fprintln(out, "---")
	fmt.Fprintf(out, "%s", roleBytes)
	fmt.Fprintln(out, "---")
	fmt.Fprintf(out, "%s", roleBindingBytes)
	fmt.Fprint(out, "---")
	fmt.Fprintf(out, "%s", cmByte)
	fmt.Fprint(out, "---")
	fmt.Fprintf(out, "%s", dsByte)

	return nil
}

func createKubeProxyConfigMap(cfg *kubeadmapi.ClusterConfiguration, localEndpoint *kubeadmapi.APIEndpoint, client clientset.Interface, printManifest bool) ([]byte, error) {
	// 生成控制平面端点 kubeconfig 文件。
	controlPlaneEndpoint, err := kubeadmutil.GetControlPlaneEndpoint(cfg.ControlPlaneEndpoint, localEndpoint)
	if err != nil {
		return []byte(""), err
	}

	kubeProxyCfg, ok := cfg.ComponentConfigs[componentconfigs.KubeProxyGroup]
	if !ok {
		return []byte(""), errors.New("no kube-proxy component config found in the active component config set")
	}

	proxyBytes, err := kubeProxyCfg.Marshal()
	if err != nil {
		return []byte(""), errors.Wrap(err, "error when marshaling")
	}

	// 在代理 ConfigMap 内容前添加四个空格，以符合模板中的缩进位置。
	var prefixBytes bytes.Buffer
	scanner := bufio.NewScanner(bytes.NewReader(proxyBytes))
	for scanner.Scan() {
		fmt.Fprintf(&prefixBytes, "    %s\n", scanner.Text())
	}

	configMapBytes, err := kubeadmutil.ParseTemplate(KubeProxyConfigMap19,
		struct {
			ControlPlaneEndpoint string
			ProxyConfig          string
			ProxyConfigMap       string
			ProxyConfigMapKey    string
		}{
			ControlPlaneEndpoint: controlPlaneEndpoint,
			ProxyConfig:          prefixBytes.String(),
			ProxyConfigMap:       constants.KubeProxyConfigMap,
			ProxyConfigMapKey:    constants.KubeProxyConfigMapKey,
		})
	if err != nil {
		return []byte(""), errors.Wrap(err, "error when parsing kube-proxy configmap template")
	}

	if printManifest {
		return configMapBytes, nil
	}

	kubeproxyConfigMap := &v1.ConfigMap{}
	if err := kuberuntime.DecodeInto(clientsetscheme.Codecs.UniversalDecoder(), configMapBytes, kubeproxyConfigMap); err != nil {
		return []byte(""), errors.Wrap(err, "unable to decode kube-proxy configmap")
	}

	if !kubeProxyCfg.IsUserSupplied() {
		componentconfigs.SignConfigMap(kubeproxyConfigMap)
	}

	// 创建 kube-proxy ConfigMap；若已存在，则更新。
	return []byte(""), apiclient.CreateOrUpdate(client.CoreV1().ConfigMaps(kubeproxyConfigMap.GetNamespace()), kubeproxyConfigMap)
}

func createKubeProxyAddon(cfg *kubeadmapi.ClusterConfiguration, client clientset.Interface, patchesDir string, output io.Writer, printManifest bool) ([]byte, error) {
	daemonSetbytes, err := kubeadmutil.ParseTemplate(KubeProxyDaemonSet19, struct{ Image, ProxyConfigMap, ProxyConfigMapKey string }{
		Image:             images.GetKubernetesImage(constants.KubeProxy, cfg),
		ProxyConfigMap:    constants.KubeProxyConfigMap,
		ProxyConfigMapKey: constants.KubeProxyConfigMapKey,
	})
	if err != nil {
		return []byte(""), errors.Wrap(err, "error when parsing kube-proxy daemonset template")
	}

	if len(patchesDir) != 0 {
		daemonSetbytes, err = applyKubeProxyDaemonSetPatches(daemonSetbytes, patchesDir, output)
		if err != nil {
			return []byte(""), errors.Wrap(err, "could not apply patches to the kube-proxy DaemonSet")
		}
	}

	if printManifest {
		return daemonSetbytes, nil
	}

	kubeproxyDaemonSet := &apps.DaemonSet{}
	if err := kuberuntime.DecodeInto(clientsetscheme.Codecs.UniversalDecoder(), daemonSetbytes, kubeproxyDaemonSet); err != nil {
		return []byte(""), errors.Wrap(err, "unable to decode kube-proxy daemonset")
	}
	// 将 HTTP/HTTPS 代理主机环境变量传递给容器。
	env := &kubeproxyDaemonSet.Spec.Template.Spec.Containers[0].Env
	*env = append(*env, kubeadmutil.MergeKubeadmEnvVars(kubeadmutil.GetProxyEnvVars(nil))...)

	// 创建 kube-proxy DaemonSet；若已存在，则更新。
	return []byte(""), apiclient.CreateOrUpdate(client.AppsV1().DaemonSets(kubeproxyDaemonSet.GetNamespace()), kubeproxyDaemonSet)
}

// applyKubeProxyDaemonSetPatches 从目录读取补丁，并将其应用到传入的 kubeProxyDaemonSetBytes。
func applyKubeProxyDaemonSetPatches(kubeProxyDaemonSetBytes []byte, patchesDir string, output io.Writer) ([]byte, error) {
	patchManager, err := patches.GetPatchManagerForPath(patchesDir, patches.KnownTargets(), output)
	if err != nil {
		return nil, err
	}

	patchTarget := &patches.PatchTarget{
		Name:                      patches.KubeProxyDaemonSet,
		StrategicMergePatchObject: apps.DaemonSet{},
		Data:                      kubeProxyDaemonSetBytes,
	}
	if err := patchManager.ApplyPatchesToTarget(patchTarget); err != nil {
		return nil, err
	}

	kubeProxyDaemonSetBytes, err = yaml.JSONToYAML(patchTarget.Data)
	if err != nil {
		return nil, err
	}

	return kubeProxyDaemonSetBytes, nil
}
