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

// Package exec 提供与 os/exec 解耦的退出错误抽象。
//
// 控制器只关心“命令是否以非零码退出、退出码是多少”，这一层抽象让测试可以
// 直接构造退出错误，而不必真的启动子进程。
package exec

// ExitError 提供类似 os/exec.ExitError（底层为 os.ProcessState）的 API，便于测试构造，
// 但会损失底层库的一些跨平台特性。
type ExitError interface {
	String() string
	Error() string
	Exited() bool
	ExitStatus() int
}

// CodeExitError 是 ExitError 的实现，由错误对象与退出码（os.exec.ExitStatus 的高位）组成。
type CodeExitError struct {
	// Err 是底层错误，Error()/String() 直接返回其文本。
	Err error
	// Code 是进程退出码。
	Code int
}

var _ ExitError = CodeExitError{}

// Error 返回底层错误文本。
func (e CodeExitError) Error() string {
	return e.Err.Error()
}

// String 返回底层错误文本，与 os/exec.ExitError 的输出习惯保持一致。
func (e CodeExitError) String() string {
	return e.Err.Error()
}

// Exited 表示进程确实运行并退出；CodeExitError 只表达这一种状态，故恒为 true。
func (e CodeExitError) Exited() bool {
	return true
}

// ExitStatus 返回记录的退出码。
func (e CodeExitError) ExitStatus() int {
	return e.Code
}
