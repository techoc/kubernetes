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

// exec 包为 exec 凭据插件提供辅助函数。
// 插件是 kubeconfig 中 user.exec 指定的外部命令，通过环境变量 KUBERNETES_EXEC_INFO
// 获取集群信息，再据此访问 API server。
package exec

import (
	"errors"
	"fmt"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/client-go/pkg/apis/clientauthentication"
	"k8s.io/client-go/pkg/apis/clientauthentication/install"
	"k8s.io/client-go/rest"
)

// execInfoEnv 是 transport 向 exec 插件传递集群信息的约定环境变量名。
const execInfoEnv = "KUBERNETES_EXEC_INFO"

var scheme = runtime.NewScheme()
var codecs = serializer.NewCodecFactory(scheme)

func init() {
	// 注册所有已知的 client.authentication.k8s.io 版本，使插件可按任意受支持版本反序列化。
	install.Install(scheme)
}

// LoadExecCredentialFromEnv 是 LoadExecCredential 的封装，从约定的
// KUBERNETES_EXEC_INFO 环境变量读取数据。
// 该环境变量未设置或为空时，函数立即返回错误。
func LoadExecCredentialFromEnv() (runtime.Object, *rest.Config, error) {
	env := os.Getenv(execInfoEnv)
	if env == "" {
		return nil, nil, errors.New("KUBERNETES_EXEC_INFO env var is unset or empty")
	}
	return LoadExecCredential([]byte(env))
}

// LoadExecCredential 加载 exec 插件与集群通信所需的配置。
//
// 输入数据应为已序列化的 client.authentication.k8s.io ExecCredential 对象，版本不限。
// 若数据无法反序列化为任何已知版本，则返回错误；否则第一个返回值是反序列化后的
// ExecCredential。若对象未包含集群信息（即 ExecCredential.Spec.Cluster == nil），也会返回错误。
//
// 返回的 rest.Config 使用匿名认证，因为 exec 插件尚未返回该集群的凭据。
func LoadExecCredential(data []byte) (runtime.Object, *rest.Config, error) {
	// 用 UniversalDeserializer 避免预先确定版本；之后再显式转换到内部类型。
	obj, gvk, err := codecs.UniversalDeserializer().Decode(data, nil, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("decode: %w", err)
	}

	// 仅接受 ExecCredential，防止插件误传其他资源类型。
	expectedGK := schema.GroupKind{
		Group: clientauthentication.SchemeGroupVersion.Group,
		Kind:  "ExecCredential",
	}
	if gvk.GroupKind() != expectedGK {
		return nil, nil, fmt.Errorf(
			"invalid group/kind: wanted %s, got %s",
			expectedGK.String(),
			gvk.GroupKind().String(),
		)
	}

	// 在此显式转换对象，以便前面已对类型不匹配给出更明确的错误信息。
	var execCredential clientauthentication.ExecCredential
	if err := scheme.Convert(obj, &execCredential, nil); err != nil {
		return nil, nil, fmt.Errorf("cannot convert to ExecCredential: %w", err)
	}

	// 缺少集群信息时插件无法自行访问 API server，直接返回错误而不是半成品配置。
	if execCredential.Spec.Cluster == nil {
		return nil, nil, errors.New("ExecCredential does not contain cluster information")
	}

	restConfig, err := rest.ExecClusterToConfig(execCredential.Spec.Cluster)
	if err != nil {
		return nil, nil, fmt.Errorf("cannot create rest.Config: %w", err)
	}

	return obj, restConfig, nil
}
