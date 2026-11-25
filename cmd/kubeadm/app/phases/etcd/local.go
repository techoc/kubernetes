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

package etcd

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/klog/v2"
	utilsnet "k8s.io/utils/net"

	kubeadmapi "k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	kubeadmconstants "k8s.io/kubernetes/cmd/kubeadm/app/constants"
	"k8s.io/kubernetes/cmd/kubeadm/app/features"
	"k8s.io/kubernetes/cmd/kubeadm/app/images"
	kubeadmutil "k8s.io/kubernetes/cmd/kubeadm/app/util"
	dryrunutil "k8s.io/kubernetes/cmd/kubeadm/app/util/dryrun"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/errors"
	etcdutil "k8s.io/kubernetes/cmd/kubeadm/app/util/etcd"
	staticpodutil "k8s.io/kubernetes/cmd/kubeadm/app/util/staticpod"
	"k8s.io/kubernetes/cmd/kubeadm/app/util/users"
)

const (
	etcdVolumeName           = "etcd-data"
	certsVolumeName          = "etcd-certs"
	etcdHealthyCheckInterval = 5 * time.Second
	etcdHealthyCheckRetries  = 8
)

// CreateLocalEtcdStaticPodManifestFile 写入本地 etcd 静态 Pod 清单。
// init 在 etcd 集群为空时调用此函数；kubeadm upgrade 在集群已运行时调用，此时 --initial-cluster 参数无效。
func CreateLocalEtcdStaticPodManifestFile(manifestDir, patchesDir string, nodeName string, cfg *kubeadmapi.ClusterConfiguration, endpoint *kubeadmapi.APIEndpoint, isDryRun bool) error {
	if cfg.Etcd.External != nil {
		return errors.New("etcd static pod manifest cannot be generated for cluster using external etcd")
	}

	if err := prepareAndWriteEtcdStaticPod(manifestDir, patchesDir, cfg, endpoint, nodeName, []etcdutil.Member{}, isDryRun); err != nil {
		return err
	}

	klog.V(1).Infof("[etcd] wrote Static Pod manifest for a local etcd member to %q\n", kubeadmconstants.GetStaticPodFilepath(kubeadmconstants.Etcd, manifestDir))
	return nil
}

// CheckLocalEtcdClusterStatus 在安装新 etcd 成员前检查本地或堆叠 etcd 集群的健康状态。
func CheckLocalEtcdClusterStatus(client clientset.Interface, certificatesDir string) error {
	klog.V(1).Info("[etcd] Checking etcd cluster health")

	// 创建连接所有本地或堆叠 etcd 成员的客户端
	klog.V(1).Info("creating etcd client that connects to etcd pods")
	etcdClient, err := etcdutil.NewFromCluster(client, certificatesDir)
	if err != nil {
		return err
	}

	// 检查健康状态
	err = etcdClient.CheckClusterHealth()
	if err != nil {
		return errors.Wrap(err, "etcd cluster is not healthy")
	}

	return nil
}

// RemoveStackedEtcdMemberFromCluster 在重置控制平面节点时，从 etcd 集群移除本地 etcd 成员。
func RemoveStackedEtcdMemberFromCluster(client clientset.Interface, cfg *kubeadmapi.InitConfiguration) error {
	// 创建连接所有本地或堆叠 etcd 成员的客户端
	klog.V(1).Info("[etcd] creating etcd client that connects to etcd pods")
	etcdClient, err := etcdutil.NewFromCluster(client, cfg.CertificatesDir)
	if err != nil {
		return err
	}

	members, err := etcdClient.ListMembers()
	if err != nil {
		return err
	}
	// 如果这是集群中唯一的堆叠 etcd 成员，无需调用 RemoveMember()。
	if len(members) == 1 {
		etcdClientAddress := etcdutil.GetClientURL(&cfg.LocalAPIEndpoint)
		if slices.Contains(etcdClient.Endpoints, etcdClientAddress) {
			klog.V(1).Info("[etcd] This is the only remaining etcd member in the etcd cluster, skip removing it")
			return nil
		}
	}

	// 通知 etcd 集群中的其他成员移除该成员
	etcdPeerAddress := etcdutil.GetPeerURL(&cfg.LocalAPIEndpoint)

	klog.V(2).Infof("[etcd] get the member id from peer: %s", etcdPeerAddress)
	id, err := etcdClient.GetMemberID(etcdPeerAddress)
	if err != nil {
		if errors.Is(err, etcdutil.ErrNoMemberIDForPeerURL) {
			klog.V(5).Infof("[etcd] member was already removed, because no member id exists for peer %s", etcdPeerAddress)
			return nil
		}
		return err
	}

	klog.V(1).Infof("[etcd] removing etcd member: %s, id: %d", etcdPeerAddress, id)
	members, err = etcdClient.RemoveMember(id)
	if err != nil {
		return err
	}
	klog.V(1).Infof("[etcd] Updated etcd member list: %v", members)

	return nil
}

// CreateStackedEtcdStaticPodManifestFile 为加入现有本地或堆叠 etcd 集群的新成员写入本地 etcd 静态 Pod 清单。
// 同时提前通知 etcd 集群中的其他成员有新节点加入。
func CreateStackedEtcdStaticPodManifestFile(client clientset.Interface, manifestDir, patchesDir string, nodeName string, cfg *kubeadmapi.ClusterConfiguration, endpoint *kubeadmapi.APIEndpoint, isDryRun bool, certificatesDir string) error {
	etcdPeerAddress := etcdutil.GetPeerURL(endpoint)

	var cluster []etcdutil.Member
	var etcdClient *etcdutil.Client
	var err error
	if isDryRun {
		fmt.Printf("[etcd] Would add etcd member: %s\n", etcdPeerAddress)
	} else {
		// 创建连接所有本地或堆叠 etcd 成员的客户端。
		klog.V(1).Info("creating etcd client that connects to etcd pods")
		etcdClient, err = etcdutil.NewFromCluster(client, certificatesDir)
		if err != nil {
			return err
		}
		klog.V(1).Infof("[etcd] Adding etcd member: %s", etcdPeerAddress)
		cluster, err = etcdClient.AddMemberAsLearner(nodeName, etcdPeerAddress)
		if err != nil {
			return err
		}
		fmt.Println("[etcd] Announced new etcd member joining to the existing etcd cluster")
		klog.V(1).Infof("Updated etcd member list: %v", cluster)
	}

	fmt.Printf("[etcd] Creating static Pod manifest for %q\n", kubeadmconstants.Etcd)

	if err := prepareAndWriteEtcdStaticPod(manifestDir, patchesDir, cfg, endpoint, nodeName, cluster, isDryRun); err != nil {
		return err
	}

	if isDryRun {
		fmt.Println("[etcd] Would wait for the new etcd member to join the cluster")
		return nil
	}

	learnerID, err := etcdClient.GetMemberID(etcdPeerAddress)
	if err != nil {
		return err
	}
	err = etcdClient.MemberPromote(learnerID)
	if err != nil {
		return err
	}

	fmt.Printf("[etcd] Waiting for the new etcd member to join the cluster. This can take up to %v\n", etcdHealthyCheckInterval*etcdHealthyCheckRetries)
	if _, err := etcdClient.WaitForClusterAvailable(etcdHealthyCheckRetries, etcdHealthyCheckInterval); err != nil {
		return err
	}

	return nil
}

// GetEtcdPodSpec 返回按当前配置填充的 etcd 静态 Pod 配置。
// 注意：此方法包含 kubeadm 创建 etcd 静态 Pod 清单所需的信息。
func GetEtcdPodSpec(cfg *kubeadmapi.ClusterConfiguration, endpoint *kubeadmapi.APIEndpoint, nodeName string, initialCluster []etcdutil.Member) v1.Pod {
	pathType := v1.HostPathDirectoryOrCreate
	etcdMounts := map[string]v1.Volume{
		etcdVolumeName:  staticpodutil.NewVolume(etcdVolumeName, cfg.Etcd.Local.DataDir, &pathType),
		certsVolumeName: staticpodutil.NewVolume(certsVolumeName, cfg.CertificatesDir+"/etcd", &pathType),
	}
	componentHealthCheckTimeout := kubeadmapi.GetActiveTimeouts().ControlPlaneComponentHealthCheck

	// probeHostname 根据端点的 AdvertiseAddress 返回正确的 localhost IP 协议族。
	probeHostname, probePort, probeScheme := staticpodutil.GetEtcdProbeEndpoint(&cfg.Etcd, utilsnet.IsIPv6String(endpoint.AdvertiseAddress))
	return staticpodutil.ComponentPod(
		v1.Container{
			Name:            kubeadmconstants.Etcd,
			Command:         getEtcdCommand(cfg, endpoint, nodeName, initialCluster, kubeadmconstants.SupportedEtcdVersion),
			Image:           images.GetEtcdImage(cfg, kubeadmconstants.SupportedEtcdVersion),
			ImagePullPolicy: v1.PullIfNotPresent,
			// 以读写方式挂载 etcd 数据目录，以便持久化存储数据
			VolumeMounts: []v1.VolumeMount{
				staticpodutil.NewVolumeMount(etcdVolumeName, cfg.Etcd.Local.DataDir, false),
				staticpodutil.NewVolumeMount(certsVolumeName, cfg.CertificatesDir+"/etcd", false),
			},
			Resources: v1.ResourceRequirements{
				Requests: v1.ResourceList{
					v1.ResourceCPU:    resource.MustParse("100m"),
					v1.ResourceMemory: resource.MustParse("100Mi"),
				},
			},
			// etcd 探测端点的说明参见：
			// https://github.com/kubernetes/kubeadm/issues/3039
			LivenessProbe:  staticpodutil.LivenessProbe(probeHostname, "/livez", kubeadmconstants.ProbePort, probeScheme),
			ReadinessProbe: staticpodutil.ReadinessProbe(probeHostname, "/readyz", kubeadmconstants.ProbePort, probeScheme),
			StartupProbe:   staticpodutil.StartupProbe(probeHostname, "/readyz", kubeadmconstants.ProbePort, probeScheme, componentHealthCheckTimeout),
			Env:            kubeadmutil.MergeKubeadmEnvVars(cfg.Etcd.Local.ExtraEnvs),
			Ports: []v1.ContainerPort{
				{
					Name:          kubeadmconstants.ProbePort,
					ContainerPort: probePort,
					Protocol:      v1.ProtocolTCP,
				},
			},
		},
		etcdMounts,
		// etcd 将在 API Server 的公告地址上监听另一个端口（2379）
		map[string]string{kubeadmconstants.EtcdAdvertiseClientUrlsAnnotationKey: etcdutil.GetClientURL(endpoint)},
	)
}

// getEtcdCommand 根据给定配置对象构建 etcd 命令。
func getEtcdCommand(cfg *kubeadmapi.ClusterConfiguration, endpoint *kubeadmapi.APIEndpoint, nodeName string, initialCluster []etcdutil.Member, supportedEtcdVersion map[uint8]string) []string {
	// localhost 的 IP 协议族应与 AdvertiseAddress 一致
	etcdLocalhostAddress := "127.0.0.1"
	if utilsnet.IsIPv6String(endpoint.AdvertiseAddress) {
		etcdLocalhostAddress = "::1"
	}
	defaultArguments := []kubeadmapi.Arg{
		{Name: "name", Value: nodeName},
		{Name: "listen-client-urls", Value: fmt.Sprintf("%s,%s", etcdutil.GetClientURLByIP(etcdLocalhostAddress), etcdutil.GetClientURL(endpoint))},
		{Name: "advertise-client-urls", Value: etcdutil.GetClientURL(endpoint)},
		{Name: "listen-peer-urls", Value: etcdutil.GetPeerURL(endpoint)},
		{Name: "initial-advertise-peer-urls", Value: etcdutil.GetPeerURL(endpoint)},
		{Name: "data-dir", Value: cfg.Etcd.Local.DataDir},
		{Name: "cert-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.EtcdServerCertName)},
		{Name: "key-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.EtcdServerKeyName)},
		{Name: "trusted-ca-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.EtcdCACertName)},
		{Name: "client-cert-auth", Value: "true"},
		{Name: "peer-cert-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.EtcdPeerCertName)},
		{Name: "peer-key-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.EtcdPeerKeyName)},
		{Name: "peer-trusted-ca-file", Value: filepath.Join(cfg.CertificatesDir, kubeadmconstants.EtcdCACertName)},
		{Name: "peer-client-cert-auth", Value: "true"},
		{Name: "snapshot-count", Value: "10000"},
		{Name: "listen-metrics-urls", Value: fmt.Sprintf("http://%s", net.JoinHostPort(etcdLocalhostAddress, strconv.Itoa(kubeadmconstants.EtcdMetricsPort)))},
		{Name: "feature-gates", Value: "InitialCorruptCheck=true"},
		{Name: "watch-progress-notify-interval", Value: "5s"},
	}

	if len(initialCluster) == 0 {
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "initial-cluster", fmt.Sprintf("%s=%s", nodeName, etcdutil.GetPeerURL(endpoint)), 1)
	} else {
		// 注意：加入的 etcd 成员应包含在 initialCluster 列表中
		endpoints := []string{}
		for _, member := range initialCluster {
			endpoints = append(endpoints, fmt.Sprintf("%s=%s", member.Name, member.PeerURL))
		}

		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "initial-cluster", strings.Join(endpoints, ","), 1)
		defaultArguments = kubeadmapi.SetArgValues(defaultArguments, "initial-cluster-state", "existing", 1)
	}

	command := []string{"etcd"}
	command = append(command, kubeadmutil.ArgumentsToCommand(defaultArguments, cfg.Etcd.Local.ExtraArgs)...)
	return command
}

func prepareAndWriteEtcdStaticPod(manifestDir string, patchesDir string, cfg *kubeadmapi.ClusterConfiguration, endpoint *kubeadmapi.APIEndpoint, nodeName string, initialCluster []etcdutil.Member, isDryRun bool) error {
	// 获取按当前 ClusterConfiguration 和新 etcd 成员列表填充的 etcd StaticPodSpec
	spec := GetEtcdPodSpec(cfg, endpoint, nodeName, initialCluster)

	var usersAndGroups *users.UsersAndGroups
	var err error
	if features.Enabled(cfg.FeatureGates, features.RootlessControlPlane) {
		if isDryRun {
			fmt.Printf("[etcd] Would create users and groups for %q to run as non-root\n", kubeadmconstants.Etcd)
			fmt.Printf("[etcd] Would update static pod manifest for %q to run run as non-root\n", kubeadmconstants.Etcd)
		} else {
			usersAndGroups, err = staticpodutil.GetUsersAndGroups()
			if err != nil {
				return errors.Wrap(err, "failed to create users and groups")
			}
			// 在非 Linux 系统上 usersAndGroups 为 nil。
			if usersAndGroups != nil {
				if err := staticpodutil.RunComponentAsNonRoot(kubeadmconstants.Etcd, &spec, usersAndGroups, cfg); err != nil {
					return errors.Wrapf(err, "failed to run component %q as non-root", kubeadmconstants.Etcd)
				}
			}
		}
	}

	// 如果定义了 patchesDir，则修补静态 Pod 清单
	if patchesDir != "" {
		patchedSpec, err := staticpodutil.PatchStaticPod(&spec, patchesDir, os.Stdout)
		if err != nil {
			return errors.Wrapf(err, "failed to patch static Pod manifest file for %q", kubeadmconstants.Etcd)
		}
		spec = *patchedSpec
	}

	// 将 etcd StaticPod 写入磁盘
	if err := staticpodutil.WriteStaticPodToDisk(kubeadmconstants.Etcd, manifestDir, spec); err != nil {
		return err
	}

	// 试运行时打印静态 etcd Pod 清单。
	if isDryRun {
		realPath := kubeadmconstants.GetStaticPodFilepath(kubeadmconstants.Etcd, manifestDir)
		outputPath := kubeadmconstants.GetStaticPodFilepath(kubeadmconstants.Etcd, kubeadmconstants.GetStaticPodDirectory())
		return dryrunutil.PrintDryRunFiles([]dryrunutil.FileToPrint{dryrunutil.NewFileToPrint(realPath, outputPath)}, os.Stdout)
	}
	return nil
}
