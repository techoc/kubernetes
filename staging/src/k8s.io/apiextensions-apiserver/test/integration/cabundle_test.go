/*
Copyright 2024 The Kubernetes Authors.

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

package integration

import (
	"context"
	"testing"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/test/integration/fixtures"
)

// exampleCert 由 crypto/tls/generate_cert.go 以如下命令生成：
//
//	go run generate_cert.go  --rsa-bits 2048 --host example.com --ca --start-date "Jan 1 00:00:00 1970" --duration=1000000h
var exampleCert = []byte(`-----BEGIN CERTIFICATE-----
MIIDADCCAeigAwIBAgIQVHG3Fn9SdWayyLOZKCW1vzANBgkqhkiG9w0BAQsFADAS
MRAwDgYDVQQKEwdBY21lIENvMCAXDTcwMDEwMTAwMDAwMFoYDzIwODQwMTI5MTYw
MDAwWjASMRAwDgYDVQQKEwdBY21lIENvMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8A
MIIBCgKCAQEArTCu9fiIclNgDdWHphewM+JW55dCb5yYGlJgCBvwbOx547M9p+tn
zm9QOhsdZDHDZsG9tqnWxE2Nc1HpIJyOlfYsOoonpEoG/Ep6nnK91ngj0bn/JlNy
+i/bwU4r97MOukvnOIQez9/D9jAJaOX2+b8/d4lRz9BsqiwJyg+ynZ5tVVYj7aMi
vXnd6HOnJmtqutOtr3beucJnkd6XbwRkLUcAYATT+ZihOWRbTuKqhCg6zGkJOoUG
f8sX61JjoilxiURA//ftGVbdTCU3DrmGmardp5NNOHbumMYU8Vhmqgx1Bqxb+9he
7G42uW5YWYK/GqJzgVPjjlB2dOGj9KrEWQIDAQABo1AwTjAOBgNVHQ8BAf8EBAMC
AqQwEwYDVR0lBAwwCgYIKwYBBQUHAwEwDwYDVR0TAQH/BAUwAwEB/zAWBgNVHREE
DzANggtleGFtcGxlLmNvbTANBgkqhkiG9w0BAQsFAAOCAQEAig4AIi9xWs1+pLES
eeGGdSDoclplFpcbXANnsYYFyLf+8pcWgVi2bOmb2gXMbHFkB07MA82wRJAUTaA+
2iNXVQMhPCoA7J6ADUbww9doJX2S9HGyArhiV/MhHtE8txzMn2EKNLdhhk3N9rmV
x/qRbWAY1U2z4BpdrAR87Fe81Nlj7h45csW9K+eS+NgXipiNTIfEShKgCFM8EdxL
1WXg7r9AvYV3TNDPWTjLsm1rQzzZQ7Uvcf6deWiNodZd8MOT/BFLclDPTK6cF2Hr
UU4dq6G4kCwMSxWE4cM3HlZ4u1dyIt47VbkP0rtvkBCXx36y+NXYA5lzntchNFZP
uvEQdw==
-----END CERTIFICATE-----`)

// invalidCert 是内容为 "Cg==" 的非法 PEM，用于验证无效 CA 证书的处理。
var invalidCert = []byte("Cg==")

// TestInvalidCABundle 验证非法 CA 证书会阻止新 CRD 进入 Established 状态。
func TestInvalidCABundle(t *testing.T) {
	ctx := context.Background()
	tearDown, apiExtensionClient, _, err := fixtures.StartDefaultServerWithClients(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	crd := fixtures.NewRandomNameV1CustomResourceDefinition(apiextensionsv1.NamespaceScoped)
	crd.Spec.Conversion = &apiextensionsv1.CustomResourceConversion{
		Strategy: apiextensionsv1.WebhookConverter,
		Webhook: &apiextensionsv1.WebhookConversion{
			ClientConfig: &apiextensionsv1.WebhookClientConfig{
				CABundle: invalidCert,
				Service: &apiextensionsv1.ServiceReference{
					Namespace: "default",
					Name:      "example",
				},
			},
			ConversionReviewVersions: []string{"v1beta1"},
		},
	}
	crd, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// 轮询直到 Established 条件变为 False 且原因为 InvalidCABundle。
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		localCrd, err := apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crd.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		condition := findCRDCondition(localCrd, apiextensionsv1.Established)
		if condition == nil {
			return false, nil
		}
		if condition.Status == apiextensionsv1.ConditionFalse && condition.Reason == "InvalidCABundle" {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestValidCABundle 验证合法 CA 证书会让 CRD 进入 Established 状态。
func TestValidCABundle(t *testing.T) {
	ctx := context.Background()
	tearDown, apiExtensionClient, _, err := fixtures.StartDefaultServerWithClients(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	crd := fixtures.NewRandomNameV1CustomResourceDefinition(apiextensionsv1.NamespaceScoped)
	crd.Spec.Conversion = &apiextensionsv1.CustomResourceConversion{
		Strategy: apiextensionsv1.WebhookConverter,
		Webhook: &apiextensionsv1.WebhookConversion{
			ClientConfig: &apiextensionsv1.WebhookClientConfig{
				CABundle: exampleCert,
				Service: &apiextensionsv1.ServiceReference{
					Namespace: "default",
					Name:      "example",
				},
			},
			ConversionReviewVersions: []string{"v1beta1"},
		},
	}

	crd, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	// 轮询直到 CRD 进入 Established 状态。
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		localCrd, err := apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crd.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		condition := findCRDCondition(localCrd, apiextensionsv1.Established)
		if condition == nil {
			return false, nil
		}
		if condition.Status == apiextensionsv1.ConditionTrue {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}

}

// TestMissingCABundle 验证未配置 conversion 时 CRD 也能进入 Established 状态。
func TestMissingCABundle(t *testing.T) {
	ctx := context.Background()
	tearDown, apiExtensionClient, _, err := fixtures.StartDefaultServerWithClients(t)
	if err != nil {
		t.Fatal(err)
	}
	defer tearDown()

	crd := fixtures.NewRandomNameV1CustomResourceDefinition(apiextensionsv1.NamespaceScoped)
	crd, err = apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	err = wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 10*time.Second, true, func(ctx context.Context) (bool, error) {
		localCrd, err := apiExtensionClient.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, crd.Name, metav1.GetOptions{})
		if err != nil {
			return false, err
		}
		condition := findCRDCondition(localCrd, apiextensionsv1.Established)
		if condition == nil {
			return false, nil
		}
		if condition.Status == apiextensionsv1.ConditionTrue {
			return true, nil
		}
		return false, nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
