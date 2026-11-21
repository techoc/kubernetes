/*
Copyright 2019 The Kubernetes Authors.

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

package config

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// CSRSigningControllerConfiguration 包含描述 CSRSigningController 的配置项。
type CSRSigningControllerConfiguration struct {
	// ClusterSigningCertFile 是 PEM 编码的 X509 CA 证书文件路径，
	// 用于签发集群范围的证书。
	ClusterSigningCertFile string
	// ClusterSigningKeyFile 是 PEM 编码的 RSA 或 ECDSA 私钥文件路径，
	// 用于签发集群范围的证书。
	ClusterSigningKeyFile string

	// KubeletServingSignerConfiguration 保存用于签发 kubernetes.io/kubelet-serving
	// signer 证书的证书和私钥。
	KubeletServingSignerConfiguration CSRSigningConfiguration
	// KubeletClientSignerConfiguration 保存用于签发
	// kubernetes.io/kube-apiserver-client-kubelet signer 证书的证书和私钥。
	KubeletClientSignerConfiguration CSRSigningConfiguration
	// KubeAPIServerClientSignerConfiguration 保存用于签发
	// kubernetes.io/kube-apiserver-client signer 证书的证书和私钥。
	KubeAPIServerClientSignerConfiguration CSRSigningConfiguration
	// LegacyUnknownSignerConfiguration 保存用于签发
	// kubernetes.io/legacy-unknown signer 证书的证书和私钥。
	LegacyUnknownSignerConfiguration CSRSigningConfiguration

	// ClusterSigningDuration 是签发证书的最长有效期。
	// 单个 CSR 可通过 spec.expirationSeconds 请求更短的有效期。
	ClusterSigningDuration metav1.Duration
}

// CSRSigningConfiguration 保存某个 CSR signer 所需的证书和私钥信息。
type CSRSigningConfiguration struct {
	// CertFile 是 PEM 编码的 X509 CA 证书文件路径，用于签发证书。
	CertFile string
	// KeyFile 是 PEM 编码的 RSA 或 ECDSA 私钥文件路径，用于签发证书。
	KeyFile string
}
