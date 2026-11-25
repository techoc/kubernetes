/*
Copyright 2023 The Kubernetes Authors.

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

// +k8s:defaulter-gen=TypeMeta
// +groupName=kubeadm.k8s.io
// +k8s:deepcopy-gen=package
// +k8s:conversion-gen=k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm

// Package v1beta4 定义 kubeadm 配置文件的 v1beta4 版本格式。
// 此版本在 v1beta3 基础上修复了一些小问题并新增了若干字段。
//
// 自 v1beta3 以来的变更：
// v1.35:
//   - 为 `ClusterConfiguration.Etcd.ExternalEtcd` 新增 `HTTPEndpoints` 字段，用于配置 v1beta4 中 etcd 通信的 HTTP 端点。
//     此字段将 /metrics 和 /health 等 HTTP 流量与 Endpoints 处理的 gRPC 流量分离。
//     分离后可只暴露 HTTP 端点而不暴露主要 gRPC 接口，便于访问控制。
//     对应 etcd 的 --listen-client-http-urls 配置；未提供时，两类流量均使用 Endpoints。
//
// v1.34:
//   - 为 `ClusterConfiguration.EncryptionAlgorithm` 增加 "ECDSA-P384" 选项。
//
// v1.33:
//   - 为 `UpgradeConfiguration.Plan` 新增 `EtcdUpgrade` 字段，用于控制是否显示 etcd 升级计划。
//
// v1.31:
//   - 支持在 `ClusterConfiguration` 下为控制平面组件设置自定义环境变量。
//     可使用 `APIServer.ExtraEnvs`、`ControllerManager.ExtraEnvs`、`Scheduler.ExtraEnvs`、`Etcd.Local.ExtraEnvs`。
//   - v1beta4 支持 `ResetConfiguration` API 类型，用户可向 `kubeadm reset` 传入 `--config` 文件来重置节点。
//   - `InitConfiguration` 和 `JoinConfiguration` 现在支持配置演练模式。
//   - 将原有 string/string 额外参数映射替换为支持重复参数的结构化额外参数。
//     适用于 `ClusterConfiguration` 的 `APIServer.ExtraArgs`、`ControllerManager.ExtraArgs`、
//     `Scheduler.ExtraArgs`、`Etcd.Local.ExtraArgs`，以及 `NodeRegistrationOptions.KubeletExtraArgs`。
//   - 新增 `ClusterConfiguration.EncryptionAlgorithm`，用于设置集群密钥和证书的非对称加密算法。
//     可选值为 "RSA-2048"（默认）、"RSA-3072"、"RSA-4096" 或 "ECDSA-P256"。
//   - 新增 `ClusterConfiguration.DNS.Disabled` 和 `ClusterConfiguration.Proxy.Disabled`，用于在集群初始化期间禁用
//     CoreDNS 和 kube-proxy 插件。创建集群时跳过相关插件阶段会将这些字段设置为 `true`。
//   - 在 `InitConfiguration` 和 `JoinConfiguration` 中新增 `NodeRegistration.ImagePullSerial` 字段，用于控制 kubeadm 串行还是并行拉取镜像。
//   - 向 `kubeadm upgrade` 子命令传入 `--config` 时，v1beta4 支持 `UpgradeConfiguration` API。
//     Deprecated: 向升级子命令传入 `--config` 时，kubelet 和 kube-proxy 的组件配置、`InitConfiguration`
//     和 `ClusterConfiguration` 已弃用，将被忽略。
//   - 为 `InitConfiguration`、`JoinConfiguration`、`ResetConfiguration` 和 `UpgradeConfiguration` 新增 `Timeouts` 结构，用于配置各种超时。
//     `ClusterConfiguration.TimeoutForControlPlane` 由 `Timeouts.ControlPlaneComponentHealthCheck` 替代，
//     `JoinConfiguration.Discovery.Timeout` 由 `Timeouts.Discovery` 替代。
//   - 为 `ClusterConfiguration` 新增 `CertificateValidityPeriod` 和 `CACertificateValidityPeriod` 字段，
//     用于控制 kubeadm 在 init、join、upgrade 和 certs 等子命令中生成的证书有效期。
//     非 CA 证书默认有效期仍为 1 年，CA 证书为 10 年；仍只有非 CA 证书可通过 `kubeadm certs renew` 续期。
//
// 从旧版 kubeadm 配置迁移
//
//   - kubeadm v1.15.x 及更新版本可用于从 v1beta1 迁移到 v1beta2。
//   - kubeadm v1.22.x 及更新版本不再支持 v1beta1 及更早 API，但可用于从 v1beta2 迁移到 v1beta3。
//   - kubeadm v1.27.x 及更新版本不再支持 v1beta2 及更早 API。
//   - kubeadm v1.31.x 及更新版本可用于从 v1beta3 迁移到 v1beta4。
//
// # 基础
//
// 配置 kubeadm 的首选方式是通过 --config 选项传入 YAML 配置文件。
// 配置文件中的部分选项也可通过命令行参数设置，但此方式仅支持最常见、最简单的用例。
//
// 一个 kubeadm 配置文件可以使用三个短横线（"---"）分隔多个配置类型。
//
// kubeadm 支持以下配置类型：
//
//	apiVersion: kubeadm.k8s.io/v1beta4
//	kind: InitConfiguration
//
//	apiVersion: kubeadm.k8s.io/v1beta4
//	kind: ClusterConfiguration
//
//	apiVersion: kubelet.config.k8s.io/v1beta1
//	kind: KubeletConfiguration
//
//	apiVersion: kubeproxy.config.k8s.io/v1alpha1
//	kind: KubeProxyConfiguration
//
//	apiVersion: kubeadm.k8s.io/v1beta4
//	kind: JoinConfiguration
//
//	apiVersion: kubeadm.k8s.io/v1beta4
//	kind: ResetConfiguration
//
//	apiVersion: kubeadm.k8s.io/v1beta4
//	kind: UpgradeConfiguration
//
// 使用以下命令打印 init 和 join 等操作的默认配置：
//
//	kubeadm config print init-defaults
//	kubeadm config print join-defaults
//	kubeadm config print reset-defaults
//	kubeadm config print upgrade-defaults
//
// 配置文件中必须包含的配置类型取决于执行的操作（init 或 join）以及要使用的配置选项（默认值或高级自定义）。
//
// 若未提供某些配置类型或仅提供部分内容，kubeadm 将使用默认值；必要时还会强制保持各组件间取值一致，
// 例如控制器管理器的 cluster-cidr 参数与 kube-proxy 的 clusterCIDR。
//
// 用户始终可以覆盖默认值，但少数与安全相关的设置除外（例如强制 API Server 使用 authorization-mode Node 和 RBAC）。
//
// 若用户提供了当前操作不需要的配置类型，kubeadm 将忽略这些类型并输出警告。
//
// # kubeadm init 配置类型
//
// 执行带 --config 选项的 kubeadm init 时，可使用 InitConfiguration、ClusterConfiguration、
// KubeProxyConfiguration 和 KubeletConfiguration，但 InitConfiguration 与 ClusterConfiguration 至少必须提供一个。
//
//	apiVersion: kubeadm.k8s.io/v1beta4
//	kind: InitConfiguration
//	bootstrapTokens:
//	    ...
//	nodeRegistration:
//	    ...
//
// InitConfiguration 用于配置 kubeadm init 的运行时设置，包括 Bootstrap Token 配置以及仅对执行 kubeadm 的节点生效的设置：
//
// - NodeRegistration 保存将新节点注册到集群所需的字段；可用于自定义节点名称、CRI 套接字或其他节点专属设置（如节点 IP）。
//
// - LocalAPIEndpoint 表示将部署在此节点上的 API Server 实例端点；可用于自定义 API Server 公布地址。
//
//	apiVersion: kubeadm.k8s.io/v1beta4
//	kind: ClusterConfiguration
//	networking:
//	    ...
//	etcd:
//	    ...
//	apiServer:
//	  extraArgs:
//	    ...
//	  extraVolumes:
//	    ...
//	...
//
// ClusterConfiguration 用于配置集群范围的设置，包括：
//
// - Networking 保存集群网络拓扑配置；可用于自定义 Pod 子网或服务子网。
//
// - Etcd 配置；可用于自定义本地 etcd，或配置 API Server 使用外部 etcd 集群。
//
// - kube-apiserver、kube-scheduler、kube-controller-manager 配置；可通过添加自定义设置或覆盖 kubeadm 默认设置来定制控制平面组件。
//
//	apiVersion: kubeproxy.config.k8s.io/v1alpha1
//	kind: KubeProxyConfiguration
//	   ...
//
// KubeProxyConfiguration 用于修改传递给集群中 kube-proxy 实例的配置。未提供此对象或仅提供部分内容时，kubeadm 会应用默认值。
//
// kube-proxy 官方文档：https://kubernetes.io/docs/reference/command-line-tools-reference/kube-proxy/ 或
// https://pkg.go.dev/k8s.io/kube-proxy/config/v1alpha1#KubeProxyConfiguration
//
//	apiVersion: kubelet.config.k8s.io/v1beta1
//	kind: KubeletConfiguration
//	   ...
//
// KubeletConfiguration 用于修改传递给集群中所有 kubelet 实例的配置。未提供此对象或仅提供部分内容时，kubeadm 会应用默认值。
//
// kubelet 官方文档：https://kubernetes.io/docs/reference/command-line-tools-reference/kubelet/ 或
// https://pkg.go.dev/k8s.io/kubelet/config/v1beta1#KubeletConfiguration
//
// 以下是一个内容完整的 YAML 文件示例，包含 `kubeadm init` 运行所需的多种配置类型。
//
//	apiVersion: kubeadm.k8s.io/v1beta4
//	kind: InitConfiguration
//	bootstrapTokens:
//	- token: "9a08jv.c0izixklcxtmnze7"
//	  description: "kubeadm bootstrap token"
//	  ttl: "24h"
//	- token: "783bde.3f89s0fje9f38fhf"
//	  description: "another bootstrap token"
//	  usages:
//	  - authentication
//	  - signing
//	  groups:
//	  - system:bootstrappers:kubeadm:default-node-token
//	nodeRegistration:
//	  name: "ec2-10-100-0-1"
//	  criSocket: "unix:///var/run/containerd/containerd.sock"
//	  taints:
//	  - key: "kubeadmNode"
//	    value: "someValue"
//	    effect: "NoSchedule"
//	  kubeletExtraArgs:
//	  - name: "v"
//	    value: "5"
//	  ignorePreflightErrors:
//	  - IsPrivilegedUser
//	  imagePullPolicy: "IfNotPresent"
//	  imagePullSerial: true
//	localAPIEndpoint:
//	  advertiseAddress: "10.100.0.1"
//	  bindPort: 6443
//	certificateKey: "e6a2eb8581237ab72a4f494f30285ec12a9694d750b9785706a83bfcbbbd2204"
//	skipPhases:
//	- preflight
//	timeouts:
//	  controlPlaneComponentHealthCheck: "60s"
//	  kubernetesAPICall: "40s"
//	---
//	apiVersion: kubeadm.k8s.io/v1beta4
//	kind: ClusterConfiguration
//	etcd:
//	  # one of local or external
//	  local:
//	    imageRepository: "registry.k8s.io"
//	    imageTag: "3.2.24"
//	    dataDir: "/var/lib/etcd"
//	    extraArgs:
//	    - name: "listen-client-urls"
//	      value: "http://10.100.0.1:2379"
//	    extraEnvs:
//	    - name: "SOME_VAR"
//	      value: "SOME_VALUE"
//	    serverCertSANs:
//	    - "ec2-10-100-0-1.compute-1.amazonaws.com"
//	    peerCertSANs:
//	    - "10.100.0.1"
//	  # external:
//	    # endpoints:
//	    # - "10.100.0.1:2379"
//	    # - "10.100.0.2:2379"
//	    # caFile: "/etcd/kubernetes/pki/etcd/etcd-ca.crt"
//	    # certFile: "/etcd/kubernetes/pki/etcd/etcd.crt"
//	    # keyFile: "/etcd/kubernetes/pki/etcd/etcd.key"
//	networking:
//	  serviceSubnet: "10.96.0.0/16"
//	  podSubnet: "10.244.0.0/24"
//	  dnsDomain: "cluster.local"
//	kubernetesVersion: "v1.21.0"
//	controlPlaneEndpoint: "10.100.0.1:6443"
//	apiServer:
//	  extraArgs:
//	  - name: "authorization-mode"
//	    value: "Node,RBAC"
//	  extraEnvs:
//	  - name: "SOME_VAR"
//	    value: "SOME_VALUE"
//	  extraVolumes:
//	  - name: "some-volume"
//	    hostPath: "/etc/some-path"
//	    mountPath: "/etc/some-pod-path"
//	    readOnly: false
//	    pathType: File
//	  certSANs:
//	  - "10.100.1.1"
//	  - "ec2-10-100-0-1.compute-1.amazonaws.com"
//	controllerManager:
//	  extraArgs:
//	  - name: "node-cidr-mask-size"
//	    value: "20"
//	  extraVolumes:
//	  - name: "some-volume"
//	    hostPath: "/etc/some-path"
//	    mountPath: "/etc/some-pod-path"
//	    readOnly: false
//	    pathType: File
//	scheduler:
//	  extraArgs:
//	  - name: "address"
//	    value: "10.100.0.1"
//	  extraVolumes:
//	  - name: "some-volume"
//	    hostPath: "/etc/some-path"
//	    mountPath: "/etc/some-pod-path"
//	    readOnly: false
//	    pathType: File
//	certificatesDir: "/etc/kubernetes/pki"
//	imageRepository: "registry.k8s.io"
//	clusterName: "example-cluster"
//	encryptionAlgorithm: "ECDSA-P256"
//	dns:
//	  disabled: true # disable CoreDNS
//	proxy:
//	  disabled: true # disable kube-proxy
//	---
//	apiVersion: kubelet.config.k8s.io/v1beta1
//	kind: KubeletConfiguration
//	# kubelet specific options here
//	---
//	apiVersion: kubeproxy.config.k8s.io/v1alpha1
//	kind: KubeProxyConfiguration
//	# kube-proxy specific options here
//
// # kubeadm join 配置类型
//
// 执行带 --config 选项的 kubeadm join 时，应提供 JoinConfiguration 类型。
//
//	apiVersion: kubeadm.k8s.io/v1beta4
//	kind: JoinConfiguration
//	discovery:
//	  bootstrapToken:
//	    apiServerEndpoint: some-address:6443
//	    token: abcdef.0123456789abcdef
//	    unsafeSkipCAVerification: true
//	  tlsBootstrapToken: abcdef.0123456789abcdef
//
// JoinConfiguration 用于配置 kubeadm join 的运行时设置，包括访问集群信息的发现方式以及仅对执行 kubeadm 的节点生效的设置：
//
// - NodeRegistration 保存将新节点注册到集群所需的字段；可用于自定义节点名称、CRI 套接字或其他节点专属设置（如节点 IP）。
//
// - APIEndpoint 表示最终将部署在此节点上的 API Server 实例端点。
//
// # kubeadm reset 配置类型
//
// 执行带 --config 选项的 kubeadm reset 时，应提供 ResetConfiguration 类型。
//
//	apiVersion: kubeadm.k8s.io/v1beta4
//	kind: ResetConfiguration
//	   ...
//
// # kubeadm upgrade 配置类型
//
// 执行带 --config 选项的 kubeadm upgrade 时，应提供 UpgradeConfiguration 类型。
//
//	apiVersion: kubeadm.k8s.io/v1beta4
//	kind: UpgradeConfiguration
//	apply:
//	  ...
//	diff:
//	  ...
//	node:
//	  ...
//	plan:
//	  ...
//
// UpgradeConfiguration 包含多个分别适用于 `kubeadm upgrade` 不同子命令的子结构。
// 例如，`kubeadm upgrade apply` 只使用 apply 子结构，其他子结构会被忽略。
package v1beta4
