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
	"strings"

	"github.com/coredns/corefile-migration/migration"

	apps "k8s.io/api/apps/v1"
	v1 "k8s.io/api/core/v1"
	rbac "k8s.io/api/rbac/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	kuberuntime "k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientset "k8s.io/client-go/kubernetes"
	clientsetscheme "k8s.io/client-go/kubernetes/scheme"
	"k8s.io/klog/v2"
	"sigs.k8s.io/yaml"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/images"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/apiclient"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/image"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/patches"
)

const (
	unableToDecodeCoreDNS = "unable to decode CoreDNS"
	coreDNSReplicas       = 2
)

// DeployedDNSAddon 返回当前已部署 DNS 插件的镜像标签。
func DeployedDNSAddon(client clientset.Interface) (string, error) {
	deploymentsClient := client.AppsV1().Deployments(metav1.NamespaceSystem)
	deployments, err := deploymentsClient.List(context.TODO(), metav1.ListOptions{LabelSelector: "k8s-app=kube-dns"})
	if err != nil {
		return "", errors.Wrap(err, "couldn't retrieve DNS addon deployments")
	}

	switch len(deployments.Items) {
	case 0:
		return "", nil
	case 1:
		return image.TagFromImage(deployments.Items[0].Spec.Template.Spec.Containers[0].Image), nil
	default:
		return "", errors.Errorf("multiple DNS addon deployments found: %v", deployments.Items)
	}
}

// deployedDNSReplicas 返回当前 DNS Deployment 的副本数。
func deployedDNSReplicas(client clientset.Interface, replicas int32) (*int32, error) {
	deploymentsClient := client.AppsV1().Deployments(metav1.NamespaceSystem)
	deployments, err := deploymentsClient.List(context.TODO(), metav1.ListOptions{LabelSelector: "k8s-app=kube-dns"})
	if err != nil {
		return &replicas, errors.Wrap(err, "couldn't retrieve DNS addon deployments")
	}
	switch len(deployments.Items) {
	case 0:
		return &replicas, nil
	case 1:
		return deployments.Items[0].Spec.Replicas, nil
	default:
		return &replicas, errors.Errorf("multiple DNS addon deployments found: %v", deployments.Items)
	}
}

// EnsureDNSAddon 创建 CoreDNS 插件。
func EnsureDNSAddon(cfg *kubeadmapi.ClusterConfiguration, client clientset.Interface, patchesDir string, out io.Writer, printManifest bool) error {
	var replicas *int32
	var err error
	if !printManifest {
		replicas, err = deployedDNSReplicas(client, coreDNSReplicas)
		if err != nil {
			return err
		}
	} else {
		var defaultReplicas int32 = coreDNSReplicas
		replicas = &defaultReplicas
	}
	return coreDNSAddon(cfg, client, replicas, patchesDir, out, printManifest)
}

func coreDNSAddon(cfg *kubeadmapi.ClusterConfiguration, client clientset.Interface, replicas *int32, patchesDir string, out io.Writer, printManifest bool) error {
	// 获取 YAML 清单。
	coreDNSDeploymentBytes, err := kubeadmutil.ParseTemplate(CoreDNSDeployment, struct {
		DeploymentName, Image, ControlPlaneTaintKey string
		Replicas                                    *int32
	}{
		DeploymentName:       kubeadmconstants.CoreDNSDeploymentName,
		Image:                images.GetDNSImage(cfg),
		ControlPlaneTaintKey: kubeadmconstants.LabelNodeRoleControlPlane,
		Replicas:             replicas,
	})
	if err != nil {
		return errors.Wrap(err, "error when parsing CoreDNS deployment template")
	}

	// 将补丁应用到 CoreDNS Deployment。
	if len(patchesDir) != 0 {
		coreDNSDeploymentBytes, err = applyCoreDNSDeploymentPatches(coreDNSDeploymentBytes, patchesDir, out)
		if err != nil {
			return errors.Wrap(err, "could not apply patches to the CoreDNS Deployment")
		}
	}

	// 获取 CoreDNS 配置文件。
	coreDNSConfigMapBytes, err := kubeadmutil.ParseTemplate(CoreDNSConfigMap, struct{ DNSDomain, UpstreamNameserver, StubDomain string }{
		DNSDomain: cfg.Networking.DNSDomain,
	})
	if err != nil {
		return errors.Wrap(err, "error when parsing CoreDNS configMap template")
	}

	dnsip, err := kubeadmconstants.GetDNSIP(cfg.Networking.ServiceSubnet)
	if err != nil {
		return err
	}

	coreDNSServiceBytes, err := kubeadmutil.ParseTemplate(CoreDNSService, struct{ DNSIP string }{
		DNSIP: dnsip.String(),
	})

	if err != nil {
		return errors.Wrap(err, "error when parsing CoreDNS service template")
	}

	if printManifest {
		fmt.Fprint(out, "---")
		fmt.Fprintf(out, "%s", coreDNSDeploymentBytes)
		fmt.Fprint(out, "---")
		fmt.Fprintf(out, "%s", coreDNSConfigMapBytes)
		fmt.Fprint(out, "---")
		fmt.Fprintf(out, "%s", coreDNSServiceBytes)
		fmt.Fprint(out, "---")
		fmt.Fprintf(out, "%s", []byte(CoreDNSClusterRole))
		fmt.Fprint(out, "---")
		fmt.Fprintf(out, "%s", []byte(CoreDNSClusterRoleBinding))
		fmt.Fprint(out, "---")
		fmt.Fprintf(out, "%s", []byte(CoreDNSServiceAccount))
		return nil
	}

	if err := createCoreDNSAddon(coreDNSDeploymentBytes, coreDNSServiceBytes, coreDNSConfigMapBytes, client); err != nil {
		return err
	}
	fmt.Fprintln(out, "[addons] Applied essential addon: CoreDNS")
	return nil
}

func createCoreDNSAddon(deploymentBytes, serviceBytes, configBytes []byte, client clientset.Interface) error {
	coreDNSConfigMap := &v1.ConfigMap{}
	if err := kuberuntime.DecodeInto(clientsetscheme.Codecs.UniversalDecoder(), configBytes, coreDNSConfigMap); err != nil {
		return errors.Wrapf(err, "%s ConfigMap", unableToDecodeCoreDNS)
	}

	// 创建 CoreDNS ConfigMap；若已存在，则更新或迁移。
	_, corefile, currentInstalledCoreDNSVersion, err := GetCoreDNSInfo(client)
	if err != nil {
		return errors.Wrap(err, "unable to fetch CoreDNS current installed version and ConfigMap.")
	}

	corefileMigrationRequired, err := isCoreDNSConfigMapMigrationRequired(corefile, currentInstalledCoreDNSVersion)
	if err != nil {
		return err
	}

	// 假定迁移始终可行，若不可行则交由 migrateCoreDNSCorefile() 返回错误。
	canMigrateCorefile := true

	configMapClient := client.CoreV1().ConfigMaps(coreDNSConfigMap.GetNamespace())

	if corefile == "" || migration.Default("", corefile) {
		// 若 Corefile 为空或为默认内容，则应用最新的默认 Corefile。
		if err := apiclient.CreateOrUpdate(configMapClient, coreDNSConfigMap); err != nil {
			return err
		}
	} else if corefileMigrationRequired {
		// 若需要迁移，则尝试迁移 Corefile。
		if err := migrateCoreDNSCorefile(client, coreDNSConfigMap, corefile, currentInstalledCoreDNSVersion); err != nil {
			// Corefile 迁移错误已在预检中验证。仅当用户选择忽略预检错误时，才会执行此处逻辑。
			canMigrateCorefile = false
			klog.Warningf("the CoreDNS Configuration was not migrated: %v. The existing CoreDNS Corefile configuration has been retained.", err)
			if err := apiclient.CreateOrRetain(configMapClient, coreDNSConfigMap, kubeadmconstants.CoreDNSConfigMap); err != nil {
				return err
			}
		}
	} else {
		// 若 Corefile 已修改且无需迁移，则保留其内容，以免影响用户配置。
		if err := apiclient.CreateOrRetain(configMapClient, coreDNSConfigMap, kubeadmconstants.CoreDNSConfigMap); err != nil {
			return err
		}
	}

	coreDNSClusterRoles := &rbac.ClusterRole{}
	if err := kuberuntime.DecodeInto(clientsetscheme.Codecs.UniversalDecoder(), []byte(CoreDNSClusterRole), coreDNSClusterRoles); err != nil {
		return errors.Wrapf(err, "%s ClusterRole", unableToDecodeCoreDNS)
	}

	// 创建 CoreDNS ClusterRole；若已存在，则更新。
	if err := apiclient.CreateOrUpdate(client.RbacV1().ClusterRoles(), coreDNSClusterRoles); err != nil {
		return err
	}

	coreDNSClusterRolesBinding := &rbac.ClusterRoleBinding{}
	if err := kuberuntime.DecodeInto(clientsetscheme.Codecs.UniversalDecoder(), []byte(CoreDNSClusterRoleBinding), coreDNSClusterRolesBinding); err != nil {
		return errors.Wrapf(err, "%s ClusterRoleBinding", unableToDecodeCoreDNS)
	}

	// 创建 CoreDNS ClusterRoleBinding；若已存在，则更新。
	if err := apiclient.CreateOrUpdate(client.RbacV1().ClusterRoleBindings(), coreDNSClusterRolesBinding); err != nil {
		return err
	}

	coreDNSServiceAccount := &v1.ServiceAccount{}
	if err := kuberuntime.DecodeInto(clientsetscheme.Codecs.UniversalDecoder(), []byte(CoreDNSServiceAccount), coreDNSServiceAccount); err != nil {
		return errors.Wrapf(err, "%s ServiceAccount", unableToDecodeCoreDNS)
	}

	// 创建 CoreDNS ConfigMap；若已存在，则更新。
	if err := apiclient.CreateOrUpdate(client.CoreV1().ServiceAccounts(coreDNSServiceAccount.GetNamespace()), coreDNSServiceAccount); err != nil {
		return err
	}

	coreDNSDeployment := &apps.Deployment{}
	if err := kuberuntime.DecodeInto(clientsetscheme.Codecs.UniversalDecoder(), deploymentBytes, coreDNSDeployment); err != nil {
		return errors.Wrapf(err, "%s Deployment", unableToDecodeCoreDNS)
	}

	// 创建 CoreDNS Deployment；若升级期间 CoreDNS 迁移失败，则保留现有 Deployment。
	deploymentsClient := client.AppsV1().Deployments(coreDNSDeployment.GetNamespace())
	if !canMigrateCorefile {
		if err := apiclient.CreateOrRetain(deploymentsClient, coreDNSDeployment, kubeadmconstants.CoreDNSDeploymentName); err != nil {
			return err
		}
	} else {
		// 创建 CoreDNS Deployment；若已存在，则更新。
		if err := apiclient.CreateOrUpdate(deploymentsClient, coreDNSDeployment); err != nil {
			return err
		}
	}

	coreDNSService := &v1.Service{}
	return createDNSService(coreDNSService, serviceBytes, client)
}

func createDNSService(dnsService *v1.Service, serviceBytes []byte, client clientset.Interface) error {
	if err := kuberuntime.DecodeInto(clientsetscheme.Codecs.UniversalDecoder(), serviceBytes, dnsService); err != nil {
		return errors.Wrap(err, "unable to decode the DNS service")
	}

	// 此处不能使用通用 apiclient 辅助函数，因为还需容忍 AlreadyExists 之外的情况。
	if _, err := client.CoreV1().Services(metav1.NamespaceSystem).Create(context.TODO(), dnsService, metav1.CreateOptions{}); err != nil {
		// 若 Service 因以下错误而无效，则忽略：
		// 	Service "kube-dns" is invalid: spec.clusterIP: Invalid value: "10.96.0.10": provided IP is already allocated

		if !apierrors.IsAlreadyExists(err) && !apierrors.IsInvalid(err) {
			return errors.Wrap(err, "unable to create a new DNS service")
		}

		if _, err := client.CoreV1().Services(metav1.NamespaceSystem).Update(context.TODO(), dnsService, metav1.UpdateOptions{}); err != nil {
			return errors.Wrap(err, "unable to create/update the DNS service")
		}
	}
	return nil
}

// isCoreDNSConfigMapMigrationRequired 检查是否需要迁移 CoreDNS ConfigMap。
func isCoreDNSConfigMapMigrationRequired(corefile, currentInstalledCoreDNSVersion string) (bool, error) {
	var isMigrationRequired bool

	// 初始化时，预期当前已安装版本为空。
	if currentInstalledCoreDNSVersion == "" {
		return isMigrationRequired, nil
	}
	currentInstalledCoreDNSVersion = strings.TrimLeft(currentInstalledCoreDNSVersion, "v")
	targetCoreDNSVersion := strings.TrimLeft(kubeadmconstants.CoreDNSVersion, "v")
	if currentInstalledCoreDNSVersion == targetCoreDNSVersion {
		return isMigrationRequired, nil
	}
	deprecated, err := migration.Deprecated(currentInstalledCoreDNSVersion, targetCoreDNSVersion, corefile)
	if err != nil {
		return isMigrationRequired, errors.Wrap(err, "unable to get list of changes to the configuration.")
	}

	// 检查是否存在需要移除的插件或选项，或已成为新默认值的插件或选项。
	for _, dep := range deprecated {
		if dep.Severity == "removed" || dep.Severity == "newdefault" {
			isMigrationRequired = true
		}
	}

	return isMigrationRequired, nil
}

func migrateCoreDNSCorefile(client clientset.Interface, cm *v1.ConfigMap, corefile, currentInstalledCoreDNSVersion string) error {
	// 由于当前配置不是默认版本，尝试迁移。
	updatedCorefile, err := migration.Migrate(currentInstalledCoreDNSVersion, strings.TrimLeft(kubeadmconstants.CoreDNSVersion, "v"), corefile, false)
	if err != nil {
		return errors.Wrap(err, "unable to migrate CoreDNS ConfigMap")
	}

	// 将现有 Corefile 数据复制为 `Corefile-backup`，并更新 ConfigMap。
	if _, err := client.CoreV1().ConfigMaps(cm.ObjectMeta.Namespace).Update(context.TODO(), &v1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{
			Name:      kubeadmconstants.CoreDNSConfigMap,
			Namespace: metav1.NamespaceSystem,
		},
		Data: map[string]string{
			"Corefile":        updatedCorefile,
			"Corefile-backup": corefile,
		},
	}, metav1.UpdateOptions{}); err != nil {
		return errors.Wrap(err, "unable to update the CoreDNS ConfigMap")
	}

	// 将 CoreDNS Deployment 指向 `Corefile-backup` 数据。
	if err := setCorefile(client, "Corefile-backup"); err != nil {
		return err
	}

	fmt.Println("[addons] Migrating CoreDNS Corefile")
	changes, err := migration.Deprecated(currentInstalledCoreDNSVersion, strings.TrimLeft(kubeadmconstants.CoreDNSVersion, "v"), corefile)
	if err != nil {
		return errors.Wrap(err, "unable to get list of changes to the configuration.")
	}
	// 显示迁移变更。
	klog.V(2).Infof("the CoreDNS configuration has been migrated and applied: %v.", updatedCorefile)
	klog.V(2).Infoln("the old migration has been saved in the CoreDNS ConfigMap under the name [Corefile-backup]")
	klog.V(2).Infoln("The changes in the new CoreDNS Configuration are as follows:")
	for _, change := range changes {
		klog.V(2).Infof("%v", change.ToString())
	}
	return nil
}

// GetCoreDNSInfo 获取当前安装的 CoreDNS 及其 Corefile 配置。
func GetCoreDNSInfo(client clientset.Interface) (*v1.ConfigMap, string, string, error) {
	coreDNSConfigMap, err := client.CoreV1().ConfigMaps(metav1.NamespaceSystem).Get(context.TODO(), kubeadmconstants.CoreDNSConfigMap, metav1.GetOptions{})
	if err != nil {
		if apierrors.IsNotFound(err) {
			return nil, "", "", nil
		}
		return nil, "", "", err
	}
	corefile, ok := coreDNSConfigMap.Data["Corefile"]
	if !ok {
		return nil, "", "", errors.New("unable to find the CoreDNS Corefile data")
	}

	currentCoreDNSversion, err := DeployedDNSAddon(client)
	if err != nil {
		return nil, "", "", err
	}

	return coreDNSConfigMap, corefile, currentCoreDNSversion, nil
}

func setCorefile(client clientset.Interface, coreDNSCorefileName string) error {
	dnsDeployment, err := client.AppsV1().Deployments(metav1.NamespaceSystem).Get(context.TODO(), kubeadmconstants.CoreDNSDeploymentName, metav1.GetOptions{})
	if err != nil {
		return err
	}
	patch := fmt.Sprintf(`{"spec":{"template":{"spec":{"volumes":[{"name": "config-volume", "configMap":{"name": "coredns", "items":[{"key": "%s", "path": "Corefile"}]}}]}}}}`, coreDNSCorefileName)

	if _, err := client.AppsV1().Deployments(dnsDeployment.ObjectMeta.Namespace).Patch(context.TODO(), dnsDeployment.Name, types.StrategicMergePatchType, []byte(patch), metav1.PatchOptions{}); err != nil {
		return errors.Wrap(err, "unable to patch the CoreDNS deployment")
	}
	return nil
}

// applyCoreDNSDeploymentPatches 从目录读取补丁，并将其应用到传入的 coreDNSDeploymentBytes。
func applyCoreDNSDeploymentPatches(coreDNSDeploymentBytes []byte, patchesDir string, output io.Writer) ([]byte, error) {
	patchManager, err := patches.GetPatchManagerForPath(patchesDir, patches.KnownTargets(), output)
	if err != nil {
		return nil, err
	}

	patchTarget := &patches.PatchTarget{
		Name:                      patches.CoreDNSDeployment,
		StrategicMergePatchObject: apps.Deployment{},
		Data:                      coreDNSDeploymentBytes,
	}
	if err := patchManager.ApplyPatchesToTarget(patchTarget); err != nil {
		return nil, err
	}

	coreDNSDeploymentBytes, err = yaml.JSONToYAML(patchTarget.Data)
	if err != nil {
		return nil, err
	}

	return coreDNSDeploymentBytes, nil
}
