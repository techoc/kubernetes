/*
Copyright 2017 The Kubernetes Authors.

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

//TODO: consider making these methods functions, because we don't want helper
//functions in the k8s.io/api repo.

package core

import "fmt"

// MatchTaint 判断当前污点是否与 taintToMatch 匹配。
// 污点以 key:effect 作为唯一标识，因此两者 key 与 effect 相同即视为匹配，value 不参与判定。
func (t *Taint) MatchTaint(taintToMatch Taint) bool {
	return t.Key == taintToMatch.Key && t.Effect == taintToMatch.Effect
}

// ToString 把污点按 '<key>=<value>:<effect>'、'<key>=<value>:'、'<key>:<effect>' 或 '<key>' 格式转为字符串。
// 省略空值/空 effect 对应的字段，保证输出可读且便于在日志/命令行中展示。
func (t *Taint) ToString() string {
	if len(t.Effect) == 0 {
		if len(t.Value) == 0 {
			return fmt.Sprintf("%v", t.Key)
		}
		return fmt.Sprintf("%v=%v:", t.Key, t.Value)
	}
	if len(t.Value) == 0 {
		return fmt.Sprintf("%v:%v", t.Key, t.Effect)
	}
	return fmt.Sprintf("%v=%v:%v", t.Key, t.Value, t.Effect)
}
