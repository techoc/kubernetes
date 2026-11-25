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

package componentconfigs

import (
	"crypto/sha256"
	"fmt"
	"sort"

	v1 "k8s.io/api/core/v1"

	"k8s.io/kubernetes/cmd/kubeadm/app/constants"
)

// ChecksumForConfigMap 计算给定 ConfigMap 的校验和。
func ChecksumForConfigMap(cm *v1.ConfigMap) string {
	hash := sha256.New()

	// map 本身无序，必须确定键的顺序，才能让相同 ConfigMap 始终得到相同校验和。
	// 这里将键提取到 slice 中排序，再按该顺序取值并计算哈希。
	keys := []string{}
	for key := range cm.Data {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		hash.Write([]byte(cm.Data[key]))
	}

	// 对 binaryData 重复上述过程。
	keys = []string{}
	for key := range cm.BinaryData {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	for _, key := range keys {
		hash.Write(cm.BinaryData[key])
	}

	return fmt.Sprintf("sha256:%x", hash.Sum(nil))
}

// SignConfigMap 计算给定 ConfigMap 的校验和，并将其写入注解。
func SignConfigMap(cm *v1.ConfigMap) {
	if cm.Annotations == nil {
		cm.Annotations = map[string]string{}
	}
	cm.Annotations[constants.ComponentConfigHashAnnotationKey] = ChecksumForConfigMap(cm)
}

// VerifyConfigMapSignature 在 ConfigMap 含校验和注解且校验和匹配时返回 true，否则返回 false。
func VerifyConfigMapSignature(cm *v1.ConfigMap) bool {
	signature, ok := cm.Annotations[constants.ComponentConfigHashAnnotationKey]
	if !ok {
		return false
	}
	return signature == ChecksumForConfigMap(cm)
}
