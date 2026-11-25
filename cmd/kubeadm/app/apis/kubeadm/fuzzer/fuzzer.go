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

package fuzzer

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/randfill"

	bootstraptokenv1 "k8s.io/kubernetes/cmd/kubeadm/app/apis/bootstraptoken/v1"
	"k8s.io/kubernetes/cmd/kubeadm/app/apis/kubeadm"
	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
)

// Funcs 返回 kubeadm API 的模糊测试函数。
func Funcs(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		fuzzInitConfiguration,
		fuzzClusterConfiguration,
		fuzzComponentConfigMap,
		fuzzDNS,
		fuzzNodeRegistration,
		fuzzLocalEtcd,
		fuzzExternalEtcd,
		fuzzNetworking,
		fuzzJoinConfiguration,
		fuzzJoinControlPlane,
		fuzzResetConfiguration,
		fuzzUpgradeConfiguration,
	}
}

func fuzzInitConfiguration(obj *kubeadm.InitConfiguration, c randfill.Continue) {
	c.FillNoCustom(obj)

	// 固定那些在模糊值为空字符串或 nil 时会应用默认值的字段（否则往返测试会失败）。

	// 避免往返 InitConfiguration 中嵌入的 ClusterConfiguration，因为它仅存在于内部版本，而不存在于公开版本中。
	obj.ClusterConfiguration = kubeadm.ClusterConfiguration{}

	// 使用默认的引导令牌，否则往返测试无法通过。
	obj.BootstrapTokens = []bootstraptokenv1.BootstrapToken{
		{
			Groups: []string{"foo"},
			Usages: []string{"foo"},
			TTL:    &metav1.Duration{Duration: 1234},
		},
	}
	obj.SkipPhases = nil
	obj.NodeRegistration.ImagePullPolicy = corev1.PullIfNotPresent
	obj.NodeRegistration.ImagePullSerial = ptr.To(true)
	obj.Patches = nil
	obj.DryRun = false
	kubeadm.SetDefaultTimeouts(&obj.Timeouts)

	// v1beta4 中没有 MergeMethod 字段，必须置空才能通过往返测试。
	// TODO: v1beta4 移除后即可删除此处。
	obj.NodeRegistration.KubeletExtraArgs = nil
}

func fuzzNodeRegistration(obj *kubeadm.NodeRegistrationOptions, c randfill.Continue) {
	c.FillNoCustom(obj)

	// 固定那些在模糊值为空字符串或 nil 时会应用默认值的字段（否则往返测试会失败）。
	obj.IgnorePreflightErrors = nil
	obj.ImagePullSerial = ptr.To(true)

	// v1beta4 中没有 MergeMethod 字段，必须置空才能通过往返测试。
	// TODO: v1beta4 移除后即可删除此处。
	obj.KubeletExtraArgs = nil
}

func fuzzClusterConfiguration(obj *kubeadm.ClusterConfiguration, c randfill.Continue) {
	c.FillNoCustom(obj)

	// 固定那些在模糊值为空字符串或 nil 时会应用默认值的字段（否则往返测试会失败）。
	obj.CertificatesDir = "foo"
	obj.ClusterName = "bar"
	obj.ImageRepository = "baz"
	obj.CIImageRepository = "" // 公开 API 中不存在该字段，因此使用默认值以保证往返测试通过
	obj.KubernetesVersion = "qux"
	obj.CIKubernetesVersion = "" // 公开 API 中不存在该字段，因此使用默认值以保证往返测试通过
	obj.ControllerManager.ExtraEnvs = nil
	obj.APIServer.ExtraEnvs = nil
	obj.Scheduler.ExtraEnvs = nil
	obj.Etcd.Local.ExtraEnvs = nil
	obj.EncryptionAlgorithm = kubeadm.EncryptionAlgorithmRSA2048
	obj.Proxy.Disabled = false
	obj.CertificateValidityPeriod = &metav1.Duration{Duration: constants.CertificateValidityPeriod}
	obj.CACertificateValidityPeriod = &metav1.Duration{Duration: constants.CACertificateValidityPeriod}

	// v1beta4 中没有 MergeMethod 字段，必须置空才能通过往返测试。
	// TODO: v1beta4 移除后即可删除此处。
	obj.APIServer.ExtraArgs = nil
	obj.ControllerManager.ExtraArgs = nil
	obj.Scheduler.ExtraArgs = nil
	obj.Etcd.Local.ExtraArgs = nil
}

func fuzzDNS(obj *kubeadm.DNS, c randfill.Continue) {
	c.FillNoCustom(obj)
	obj.Disabled = false
}

func fuzzComponentConfigMap(obj *kubeadm.ComponentConfigMap, c randfill.Continue) {
	// 此处故意留空：公开 API 中没有 component config。
	// 留空意味着 ComponentConfigs 的所有字段均为 nil，这是往返测试通过的前提。
}

func fuzzLocalEtcd(obj *kubeadm.LocalEtcd, c randfill.Continue) {
	c.FillNoCustom(obj)

	// 固定那些在模糊值为空字符串或 nil 时会应用默认值的字段（否则往返测试会失败）。
	obj.DataDir = "foo"
}

func fuzzExternalEtcd(obj *kubeadm.ExternalEtcd, c randfill.Continue) {
	c.FillNoCustom(obj)

	// HTTPEndpoints 会被默认成 Endpoints，因此需显式赋值。
	obj.HTTPEndpoints = obj.Endpoints
}

func fuzzNetworking(obj *kubeadm.Networking, c randfill.Continue) {
	c.FillNoCustom(obj)

	// 固定那些在模糊值为空字符串或 nil 时会应用默认值的字段（否则往返测试会失败）。
	obj.DNSDomain = "foo"
	obj.ServiceSubnet = "bar"
}

func fuzzJoinConfiguration(obj *kubeadm.JoinConfiguration, c randfill.Continue) {
	c.FillNoCustom(obj)

	// 固定那些在模糊值为空字符串或 nil 时会应用默认值的字段（否则往返测试会失败）。
	obj.CACertPath = "foo"
	obj.Discovery = kubeadm.Discovery{
		BootstrapToken:    &kubeadm.BootstrapTokenDiscovery{Token: "baz"},
		TLSBootstrapToken: "qux",
	}
	obj.SkipPhases = nil
	obj.NodeRegistration.ImagePullPolicy = corev1.PullIfNotPresent
	obj.NodeRegistration.ImagePullSerial = ptr.To(true)
	obj.Patches = nil
	obj.DryRun = false
	kubeadm.SetDefaultTimeouts(&obj.Timeouts)

	// v1beta4 中没有 MergeMethod 字段，必须置空才能通过往返测试。
	// TODO: v1beta4 移除后即可删除此处。
	obj.NodeRegistration.KubeletExtraArgs = nil
}

func fuzzJoinControlPlane(obj *kubeadm.JoinControlPlane, c randfill.Continue) {
	c.FillNoCustom(obj)
}

func fuzzResetConfiguration(obj *kubeadm.ResetConfiguration, c randfill.Continue) {
	c.FillNoCustom(obj)

	// 固定那些在模糊值为空字符串或 nil 时会应用默认值的字段（否则往返测试会失败）。
	obj.CertificatesDir = "/tmp"
	kubeadm.SetDefaultTimeouts(&obj.Timeouts)
}

func fuzzUpgradeConfiguration(obj *kubeadm.UpgradeConfiguration, c randfill.Continue) {
	c.FillNoCustom(obj)

	// 固定那些在模糊值为空字符串或 nil 时会应用默认值的字段（否则往返测试会失败）。
	obj.Node.EtcdUpgrade = ptr.To(true)
	obj.Node.CertificateRenewal = ptr.To(false)
	obj.Node.ImagePullPolicy = corev1.PullIfNotPresent
	obj.Node.ImagePullSerial = ptr.To(true)

	obj.Apply.EtcdUpgrade = ptr.To(true)
	obj.Apply.CertificateRenewal = ptr.To(false)
	obj.Apply.ImagePullPolicy = corev1.PullIfNotPresent
	obj.Apply.ImagePullSerial = ptr.To(true)

	obj.Plan.EtcdUpgrade = ptr.To(true)

	kubeadm.SetDefaultTimeouts(&obj.Timeouts)
}
