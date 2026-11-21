/*
Copyright 2018 The Kubernetes Authors.

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
	"fmt"

	"sigs.k8s.io/randfill"

	runtimeserializer "k8s.io/apimachinery/pkg/runtime/serializer"
	kubectrlmgrconfig "k8s.io/kubernetes/pkg/controller/apis/config"
)

// Funcs 返回 kube-controller-manager 配置 API 的模糊测试（fuzzer）函数集合。
// 测试框架用这些函数为配置对象填充随机值，以验证序列化/反序列化的往返一致性。
func Funcs(codecs runtimeserializer.CodecFactory) []interface{} {
	return []interface{}{
		// 自定义填充规则：随机值必须满足字段自身的校验约束，否则往返测试会
		// 因为非法值被拒绝而产生误报。
		func(obj *kubectrlmgrconfig.KubeControllerManagerConfiguration, c randfill.Continue) {
			// 先按默认规则填充所有字段，再逐个覆盖需要特殊约束的字段。
			c.FillNoCustom(obj)
			// Address 必须是合法的 IP 字面量，因此逐段限制在 0-255。
			obj.Generic.Address = fmt.Sprintf("%d.%d.%d.%d", c.Intn(256), c.Intn(256), c.Intn(256), c.Intn(256))
			// ContentType 必须是 "type/subtype" 形式，随机字符串直接拼接才能通过解析。
			obj.Generic.ClientConnection.ContentType = fmt.Sprintf("%s/%s.%s.%s", c.String(0), c.String(0), c.String(0), c.String(0))
			// ResourceLock 为空会触发默认值逻辑，导致往返结果不一致，这里固定为有效值。
			if obj.Generic.LeaderElection.ResourceLock == "" {
				obj.Generic.LeaderElection.ResourceLock = "leases"
			}
			// 控制器名列表至少保留一项，避免空配置被默认值逻辑改写。
			obj.Generic.Controllers = []string{c.String(0)}
			// 集群名同样有默认值，非空才能保证序列化前后值不变。
			if obj.KubeCloudShared.ClusterName == "" {
				obj.KubeCloudShared.ClusterName = "kubernetes"
			}
			obj.CSRSigningController.ClusterSigningCertFile = fmt.Sprintf("/%s", c.String(0))
			obj.CSRSigningController.ClusterSigningKeyFile = fmt.Sprintf("/%s", c.String(0))
			obj.PersistentVolumeBinderController.VolumeConfiguration.FlexVolumePluginDir = fmt.Sprintf("/%s", c.String(0))
			obj.TTLAfterFinishedController.ConcurrentTTLSyncs = c.Int31()
		},
	}
}
