/*
Copyright 2016 The Kubernetes Authors.

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

package util

import (
	"encoding/json"
	"hash/adler32"
	"strconv"
	"strings"
	"testing"

	v1 "k8s.io/api/core/v1"
	"k8s.io/kubernetes/pkg/controller"
	hashutil "k8s.io/kubernetes/pkg/util/hash"
)

// podSpec 是用于哈希测试的 PodTemplateSpec JSON 模板，@@VERSION@@ 为可替换的版本占位符。
var podSpec = `
{
    "metadata": {
        "labels": {
            "app": "cats"
        }
    },
    "spec": {
        "containers": [
            {
                "name": "cats",
                "image": "registry/test/cats:v0.@@VERSION@@.0",
                "ports": [
                    {
                        "name": "http",
                        "containerPort": 9077,
                        "protocol": "TCP"
                    }
                ],
                "env": [
                    {
                        "name": "DEPLOYMENT_ENVIRONMENT",
                        "value": "cats-stubbed-functional"
                    },
                    {
                        "name": "APP_NAME",
                        "value": "cats"
                    }
                ],
                "resources": {
                    "limits": {
                        "cpu": "1",
                        "memory": "1Gi"
                    },
                    "requests": {
                        "cpu": "1",
                        "memory": "1Gi"
                    }
                },
                "livenessProbe": {
                    "httpGet": {
                        "path": "/private/status",
                        "port": 9077,
                        "scheme": "HTTP"
                    },
                    "initialDelaySeconds": 30,
                    "timeoutSeconds": 1,
                    "periodSeconds": 10,
                    "successThreshold": 1,
                    "failureThreshold": 3
                },
                "readinessProbe": {
                    "httpGet": {
                        "path": "/private/status",
                        "port": 9077,
                        "scheme": "HTTP"
                    },
                    "initialDelaySeconds": 1,
                    "timeoutSeconds": 1,
                    "periodSeconds": 10,
                    "successThreshold": 1,
                    "failureThreshold": 3
                },
                "terminationMessagePath": "/dev/termination-log",
                "imagePullPolicy": "IfNotPresent"
            }
        ],
        "restartPolicy": "Always",
        "terminationGracePeriodSeconds": 30,
        "dnsPolicy": "ClusterFirst",
        "securityContext": {}
    }
}
`

// TestPodTemplateSpecHash 测试：仅改变镜像版本时，ComputeHash 能为每个模板生成唯一哈希，
// 确保 Deployment 控制器可以用哈希区分不同版本的 Pod 模板。
func TestPodTemplateSpecHash(t *testing.T) {
	seenHashes := make(map[string]int)

	for i := 0; i < 1000; i++ {
		// 每次替换版本占位符生成不同的模板，验证哈希不会冲突。
		specJSON := strings.Replace(podSpec, "@@VERSION@@", strconv.Itoa(i), 1)
		spec := v1.PodTemplateSpec{}
		json.Unmarshal([]byte(specJSON), &spec)
		hash := controller.ComputeHash(&spec, nil)
		if v, ok := seenHashes[hash]; ok {
			t.Errorf("Hash collision, old: %d new: %d", v, i)
			break
		}
		seenHashes[hash] = i
	}
}

// BenchmarkAdler 测量旧的 adler32 深度哈希实现的性能，作为与 FNV 实现对比的基准。
func BenchmarkAdler(b *testing.B) {
	spec := v1.PodTemplateSpec{}
	json.Unmarshal([]byte(podSpec), &spec)

	for i := 0; i < b.N; i++ {
		getPodTemplateSpecOldHash(spec)
	}
}

// getPodTemplateSpecOldHash 复现旧的哈希计算方式：adler32 配合 DeepHashObject。
func getPodTemplateSpecOldHash(template v1.PodTemplateSpec) uint32 {
	podTemplateSpecHasher := adler32.New()
	hashutil.DeepHashObject(podTemplateSpecHasher, template)
	return podTemplateSpecHasher.Sum32()
}

// BenchmarkFnv 测量当前 ComputeHash（FNV）实现的性能，用于与旧实现对比。
func BenchmarkFnv(b *testing.B) {
	spec := v1.PodTemplateSpec{}
	json.Unmarshal([]byte(podSpec), &spec)

	for i := 0; i < b.N; i++ {
		controller.ComputeHash(&spec, nil)
	}
}
