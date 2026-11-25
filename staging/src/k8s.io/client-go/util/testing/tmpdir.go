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

package testing

import (
	"os"
)

// MkTmpdir 在系统临时目录下创建带指定前缀的目录。
// 成功时返回临时目录路径，出错时返回空字符串和错误。
// 目录需用 "os.RemoveAll(...)" 删除，清理责任在调用方。
func MkTmpdir(prefix string) (string, error) {
	tmpDir, err := os.MkdirTemp(os.TempDir(), prefix)
	if err != nil {
		return "", err
	}
	return tmpDir, nil
}

// MkTmpdirOrDie 与 "MkTmpdir" 作用相同，只是出错时会触发 panic。
// 适用于测试初始化等"失败即无法继续"的场景，省去逐处 if err 检查。
func MkTmpdirOrDie(prefix string) string {
	tmpDir, err := MkTmpdir(prefix)
	if err != nil {
		panic(err)
	}
	return tmpDir
}
