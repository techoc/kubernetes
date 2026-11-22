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

// ExitError is an interface that presents an API similar to os.ProcessState, which is
// what ExitError from os/exec is.  This is designed to make testing a bit easier and
// probably loses some of the cross-platform properties of the underlying library.
type ExitError interface {
	String() string
	Error() string
	Exited() bool
	ExitStatus() int
}

// CodeExitError is an implementation of ExitError consisting of an error object
// and an exit code (the upper bits of os.exec.ExitStatus).
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
