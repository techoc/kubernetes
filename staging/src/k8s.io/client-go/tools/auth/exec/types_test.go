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

package exec

import (
	"fmt"
	"reflect"
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/sets"
	clientauthenticationv1 "k8s.io/client-go/pkg/apis/clientauthentication/v1"
	clientauthenticationv1beta1 "k8s.io/client-go/pkg/apis/clientauthentication/v1beta1"
	clientcmdv1 "k8s.io/client-go/tools/clientcmd/api/v1"
)

// TestClientAuthenticationClusterTypesAreSynced 对每个 ExecCredential 外部版本的 Cluster 类型
// 分别运行字段同步校验。
func TestClientAuthenticationClusterTypesAreSynced(t *testing.T) {
	t.Parallel()

	for _, cluster := range []interface{}{
		clientauthenticationv1beta1.Cluster{},
		clientauthenticationv1.Cluster{},
	} {
		t.Run(fmt.Sprintf("%T", cluster), func(t *testing.T) {
			t.Parallel()
			testClientAuthenticationClusterTypesAreSynced(t, cluster)
		})
	}
}

// testClientAuthenticationClusterTypesAreSynced 确保给定 Cluster 类型与
// clientcmdv1.Cluster 保持同步。
//
// clientauthentication*.Cluster 需要提供与 clientcmdv1.Cluster 相同的配置项，以便连接
// Kubernetes API。该测试双向比对字段名、类型与 tag；任一结构体新增字段而未同步另一个时，
// 测试应失败，防止两者出现语义漂移。
func testClientAuthenticationClusterTypesAreSynced(t *testing.T, cluster interface{}) {
	execType := reflect.TypeOf(cluster)
	clientcmdType := reflect.TypeOf(clientcmdv1.Cluster{})

	t.Run("exec cluster fields match clientcmd cluster fields", func(t *testing.T) {
		t.Parallel()

		// 以下是 Cluster 特有、不应出现在 clientcmdv1.Cluster 中的字段。
		execSkippedFieldNames := sets.NewString(
			// Cluster 使用 Config 提供集群专属的配置对象。
			"Config",
		)

		for i := 0; i < execType.NumField(); i++ {
			execField := execType.Field(i)
			if execSkippedFieldNames.Has(execField.Name) {
				continue
			}

			t.Run(execField.Name, func(t *testing.T) {
				t.Parallel()
				clientcmdField, ok := clientcmdType.FieldByName(execField.Name)
				if !ok {
					t.Errorf("unknown field (please add field to clientcmdv1.Cluster): '%s'", execField.Name)
				} else if execField.Type != clientcmdField.Type {
					t.Errorf(
						"type mismatch (please update Cluster.%s field type to match clientcmdv1.Cluster.%s field type): %q != %q",
						execField.Name,
						clientcmdField.Name,
						execField.Type,
						clientcmdField.Type,
					)
				} else if execField.Tag != clientcmdField.Tag {
					t.Errorf(
						"tag mismatch (please update Cluster.%s tag to match clientcmdv1.Cluster.%s tag): %q != %q",
						execField.Name,
						clientcmdField.Name,
						execField.Tag,
						clientcmdField.Tag,
					)
				}
			})
		}
	})

	t.Run("clientcmd cluster fields match exec cluster fields", func(t *testing.T) {
		t.Parallel()

		// 以下字段不应从 clientcmdv1.Cluster 复制到 exec 的 Cluster。
		// 这两个字段有替代来源（CA 数据走 CertificateAuthorityData，扩展不经 exec 传递）。
		clientcmdSkippedFieldNames := sets.NewString(
			// CA 数据通过 CertificateAuthorityData 传递，因此无需此字段。
			"CertificateAuthority",
			// 扩展配置不经 exec 插件传递，因此无需复制 Extensions。
			"Extensions",
		)

		for i := 0; i < clientcmdType.NumField(); i++ {
			clientcmdField := clientcmdType.Field(i)
			if clientcmdSkippedFieldNames.Has(clientcmdField.Name) {
				continue
			}

			t.Run(clientcmdField.Name, func(t *testing.T) {
				t.Parallel()
				execField, ok := execType.FieldByName(clientcmdField.Name)
				if !ok {
					t.Errorf("unknown field (please add field to Cluster): '%s'", clientcmdField.Name)
				} else if clientcmdField.Type != execField.Type {
					t.Errorf(
						"type mismatch (please update clientcmdv1.Cluster.%s field type to match Cluster.%s field type): %q != %q",
						clientcmdField.Name,
						execField.Name,
						clientcmdField.Type,
						execField.Type,
					)
				} else if clientcmdField.Tag != execField.Tag {
					t.Errorf(
						"tag mismatch (please update clientcmdv1.Cluster.%s tag to match Cluster.%s tag): %q != %q",
						clientcmdField.Name,
						execField.Name,
						clientcmdField.Tag,
						execField.Tag,
					)
				}
			})
		}
	})
}

// TestAllClusterTypesAreSynced 是防漏测试：枚举所有已注册的 ExecCredential 版本。
// 若本包新增其他 ExecCredential 类型而遗漏测试，此测试应失败，提醒补充类似
// TestClientAuthenticationClusterTypesAreSynced 的字段同步测试。
func TestAllClusterTypesAreSynced(t *testing.T) {
	versionsThatDontNeedTests := sets.NewString(
		// 内部 Cluster 类型仅供包内使用，无需与 clientcmdv1 同步。
		runtime.APIVersionInternal,
		// 上文已有 v1beta1 的测试。
		clientauthenticationv1beta1.SchemeGroupVersion.Version,
		// 上文已有 v1 的测试。
		clientauthenticationv1.SchemeGroupVersion.Version,
	)
	for gvk := range scheme.AllKnownTypes() {
		if gvk.Group == clientauthenticationv1beta1.SchemeGroupVersion.Group &&
			gvk.Kind == "ExecCredential" {
			if !versionsThatDontNeedTests.Has(gvk.Version) {
				t.Errorf(
					"TODO: add test similar to TestV1beta1ClusterTypesAreSynced for client.authentication.k8s.io/%s",
					gvk.Version,
				)
			}
		}
	}
}
