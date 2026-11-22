//go:build linux

/*
Copyright 2014 The Kubernetes Authors.

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

package iptables

import (
	"bytes"
	"fmt"

	"k8s.io/apimachinery/pkg/util/sets"
)

// MakeChainLine 返回 iptables-save/restore 格式的链声明行。
func MakeChainLine(chain Chain) string {
	return fmt.Sprintf(":%s - [0:0]", chain)
}

// GetChainsFromTable 解析 iptables-save 输出以找出定义了哪些链。
// 它假定 save 只包含一个表的数据，返回的集合中包含该表中每条链的名称。
func GetChainsFromTable(save []byte) sets.Set[Chain] {
	chainsSet := sets.New[Chain]()

	for {
		i := bytes.Index(save, []byte("\n:"))
		if i == -1 {
			break
		}
		start := i + 2
		save = save[start:]
		end := bytes.Index(save, []byte(" "))
		if end == -1 {
			// 按格式不应出现，但避免异常输入导致死循环。
			break
		}
		chain := Chain(save[:end])
		chainsSet.Insert(chain)
		save = save[end:]
	}
	return chainsSet
}
