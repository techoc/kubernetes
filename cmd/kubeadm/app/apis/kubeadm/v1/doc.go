/*
Copyright The Kubernetes Authors.

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

// Package v1 定义 kubeadm 配置文件的 v1 版本格式。
// 此版本在 v1beta4 基础上修复了一些小问题并新增了若干字段。
//
// TODO: v1 的跟踪问题：
//   - https://github.com/kubernetes/kubeadm/issues/3160
//
// 自 v1beta4 以来的变更：
// vx.yy:
//   - 为 `Arg` 新增 `MergeMethod` 字段，可选值为 `append`、`prepend` 或空值。
//     此字段用于控制覆盖参数与同名基础参数的合并方式；空值为默认行为，表示替换。
//
// 从旧版 kubeadm 配置迁移
//
//   - kubeadm v1.15.x 及更新版本可用于从 v1beta1 迁移到 v1beta2。
//   - kubeadm v1.22.x 及更新版本不再支持 v1beta1 及更早 API，但可用于从 v1beta2 迁移到 v1beta3。
//   - kubeadm v1.27.x 及更新版本不再支持 v1beta2 及更早 API。
//   - kubeadm v1.31.x 及更新版本可用于从 v1beta3 迁移到 v1beta4。
//   - 未来的 kubeadm 版本可用于从 v1beta4 迁移到 v1。
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
//	apiVersion: kubeadm.k8s.io/v1
//	kind: InitConfiguration
//
//	apiVersion: kubeadm.k8s.io/v1
//	kind: ClusterConfiguration
//
//	apiVersion: kubelet.config.k8s.io/v1beta1
//	kind: KubeletConfiguration
//
//	apiVersion: kubeproxy.config.k8s.io/v1alpha1
//	kind: KubeProxyConfiguration
//
//	apiVersion: kubeadm.k8s.io/v1
//	kind: JoinConfiguration
//
//	apiVersion: kubeadm.k8s.io/v1
//	kind: ResetConfiguration
//
//	apiVersion: kubeadm.k8s.io/v1
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
//	apiVersion: kubeadm.k8s.io/v1
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
//	apiVersion: kubeadm.k8s.io/v1
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
//	apiVersion: kubeadm.k8s.io/v1
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
//	apiVersion: kubeadm.k8s.io/v1
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
//	apiVersion: kubeadm.k8s.io/v1
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
//	apiVersion: kubeadm.k8s.io/v1
//	kind: ResetConfiguration
//	   ...
//
// # kubeadm upgrade 配置类型
//
// 执行带 --config 选项的 kubeadm upgrade 时，应提供 UpgradeConfiguration 类型。
//
//	apiVersion: kubeadm.k8s.io/v1
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
package v1
