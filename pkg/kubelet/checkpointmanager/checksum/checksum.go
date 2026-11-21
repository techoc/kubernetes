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

package checksum

import (
	"hash/fnv"

	"k8s.io/kubernetes/pkg/kubelet/checkpointmanager/errors"
	hashutil "k8s.io/kubernetes/pkg/util/hash"
)

// Checksum 是随 checkpoint 一起存储的校验和。
type Checksum uint64

// Verify 校验传入数据的计算校验和是否与本校验和一致，不一致时返回损坏错误。
func (cs Checksum) Verify(data interface{}) error {
	actualCS := New(data)
	if cs != actualCS {
		return &errors.CorruptCheckpointError{ActualCS: uint64(actualCS), ExpectedCS: uint64(cs)}
	}
	return nil
}

// New 计算并返回 checkpoint 数据的校验和。
func New(data interface{}) Checksum {
	return Checksum(getChecksum(data))
}

// getChecksum 计算数据的 FNV-1a 32 位哈希并转为 uint64。
func getChecksum(data interface{}) uint64 {
	hash := fnv.New32a()
	hashutil.DeepHashObject(hash, data)
	return uint64(hash.Sum32())
}
