/*
Copyright 2020 The Kubernetes Authors.

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

package certificates

import (
	"context"
	"testing"

	certv1 "k8s.io/api/certificates/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"

	kubeapiservertesting "k8s.io/kubernetes/cmd/kube-apiserver/app/testing"
	"k8s.io/kubernetes/test/integration/framework"
)

// 验证 CertificateSubjectRestriction 准入控制器按预期工作。
func TestCertificateSubjectRestrictionPlugin(t *testing.T) {
	tests := map[string]struct {
		signerName string
		group      string
		error      string
	}{
		"should reject a request if signerName is kube-apiserver-client and group is system:masters": {
			signerName: certv1.KubeAPIServerClientSignerName,
			group:      "system:masters",
			error:      `certificatesigningrequests.certificates.k8s.io "csr" is forbidden: use of kubernetes.io/kube-apiserver-client signer with system:masters group is not allowed`,
		},
		"should admit a request if signerName is kube-apiserver-client and group is NOT system:masters": {
			signerName: certv1.KubeAPIServerClientSignerName,
			group:      "system:notmasters",
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			// 使用默认配置选项启动 apiserver。
			s := kubeapiservertesting.StartTestServerOrDie(t, kubeapiservertesting.NewDefaultTestServerOptions(), []string{""}, framework.SharedEtcd())
			defer s.TearDownFn()
			client := clientset.NewForConfigOrDie(s.ClientConfig)

			// 尝试创建 CSR 资源。
			csr := buildTestingCSR("csr", test.signerName, test.group)
			_, err := client.CertificatesV1().CertificateSigningRequests().Create(context.TODO(), csr, metav1.CreateOptions{})
			if err != nil && test.error != err.Error() {
				t.Errorf("expected error %q but got: %v", test.error, err)
			}
			if err == nil && test.error != "" {
				t.Errorf("expected to get an error %q but got none", test.error)
			}
		})
	}
}
