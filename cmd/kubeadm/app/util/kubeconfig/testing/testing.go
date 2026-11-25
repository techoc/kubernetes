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

// Package testing 包含在测试中管理 kubeadm kubeconfig 的工具。
package testing

import (
	"crypto/x509"
	"encoding/pem"
	"testing"
	"time"

	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"

	certstestutil "k8s.io/kubernetes/cmd/kubeadm/app/util/certs/testing"
)

// AssertKubeConfigCurrentCluster 断言 KubeConfig.CurrentCluster 引用指定集群，供 kubeadm 测试使用。
func AssertKubeConfigCurrentCluster(t *testing.T, config *clientcmdapi.Config, expectedAPIServerAddress string, expectedAPIServerCaCert *x509.Certificate) {
	currentContext := config.Contexts[config.CurrentContext]
	currentCluster := config.Clusters[currentContext.Cluster]

	if currentCluster.Server != expectedAPIServerAddress {
		t.Errorf("kubeconfig.currentCluster.Server is [%s], expected [%s]", currentCluster.Server, expectedAPIServerAddress)
	}

	if len(currentCluster.CertificateAuthorityData) == 0 {
		t.Error("kubeconfig.currentCluster.CertificateAuthorityData is empty, expected not empty")
		return
	}

	block, _ := pem.Decode(currentCluster.CertificateAuthorityData)
	currentAPIServerCaCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Errorf("kubeconfig.currentCluster.CertificateAuthorityData is not a valid CA: %v", err)
		return
	}

	if !currentAPIServerCaCert.Equal(expectedAPIServerCaCert) {
		t.Errorf("kubeconfig.currentCluster.CertificateAuthorityData not correspond to the expected CA cert")
	}
}

// AssertKubeConfigCurrentAuthInfoWithClientCert 断言 CurrentAuthInfo 中的 clientCert 引用指定客户端，
// 由预期 CA 签署且包含预期组织，供 kubeadm 测试使用。
func AssertKubeConfigCurrentAuthInfoWithClientCert(t *testing.T, config *clientcmdapi.Config, signinCa *x509.Certificate, expectedNotAfter time.Time, expectedClientName string, expectedOrganizations ...string) {
	currentContext := config.Contexts[config.CurrentContext]
	currentAuthInfo := config.AuthInfos[currentContext.AuthInfo]

	if len(currentAuthInfo.ClientCertificateData) == 0 {
		t.Error("kubeconfig.currentAuthInfo.ClientCertificateData is empty, expected not empty")
		return
	}

	block, _ := pem.Decode(config.AuthInfos[currentContext.AuthInfo].ClientCertificateData)
	currentClientCert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Errorf("kubeconfig.currentAuthInfo.ClientCertificateData is not a valid CA: %v", err)
		return
	}

	certstestutil.AssertCertificateIsSignedByCa(t, currentClientCert, signinCa)
	certstestutil.AssertCertificateHasNotAfter(t, currentClientCert, expectedNotAfter)
	certstestutil.AssertCertificateHasClientAuthUsage(t, currentClientCert)
	certstestutil.AssertCertificateHasCommonName(t, currentClientCert, expectedClientName)
	certstestutil.AssertCertificateHasOrganizations(t, currentClientCert, expectedOrganizations...)
}

// AssertKubeConfigCurrentAuthInfoWithToken 断言 CurrentAuthInfo 引用预期令牌，供 kubeadm 测试使用。
func AssertKubeConfigCurrentAuthInfoWithToken(t *testing.T, config *clientcmdapi.Config, expectedClientName, expectedToken string) {
	currentContext := config.Contexts[config.CurrentContext]
	currentAuthInfo := config.AuthInfos[currentContext.AuthInfo]

	if currentAuthInfo.Token != expectedToken {
		t.Errorf("kubeconfig.currentAuthInfo.Token [%s], expected [%s]", currentAuthInfo.Token, expectedToken)
	}
}

// AssertKubeConfigCurrentContextWithClusterName 断言当前集群配置引用预期集群名称，供 kubeadm 测试使用。
func AssertKubeConfigCurrentContextWithClusterName(t *testing.T, config *clientcmdapi.Config, expectedClusterName string) {
	currentContext := config.Contexts[config.CurrentContext]

	if currentContext.Cluster != expectedClusterName {
		t.Errorf("kubeconfig.currentContext.clusterName [%s], expected [%s]", currentContext.Cluster, expectedClusterName)
	}
}
