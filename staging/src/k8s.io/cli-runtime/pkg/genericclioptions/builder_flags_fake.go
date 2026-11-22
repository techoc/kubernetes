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

package genericclioptions

import (
	"k8s.io/cli-runtime/pkg/resource"
)

// NewSimpleFakeResourceFinder builds a super simple ResourceFinder that just iterates over the objects you provided
//
// 返回的 FakeResourceFinder 不做任何 API 交互，仅按传入顺序遍历 Info，
// 供命令层的单元测试替换真实 builder。
func NewSimpleFakeResourceFinder(infos ...*resource.Info) *FakeResourceFinder {
	return &FakeResourceFinder{
		Infos: infos,
	}
}

// WithError 汇总一个错误：Do() 返回的 visitor 在遍历前会直接返回该错误。
func (f *FakeResourceFinder) WithError(err error) *FakeResourceFinder {
	f.err = err
	return f
}

// FakeResourceFinder 是 ResourceFinder 的测试实现，持有预置的 Info 列表和可选错误。
type FakeResourceFinder struct {
	Infos []*resource.Info
	err   error
}

// Do implements the interface
func (f *FakeResourceFinder) Do() resource.Visitor {
	return &fakeResourceResult{
		Infos: f.Infos,
		err:   f.err,
	}
}

// fakeResourceResult 是 FakeResourceFinder 的 Visitor 实现。
type fakeResourceResult struct {
	Infos []*resource.Info
	err   error
}

// Visit just iterates over info
//
// 与真实 visitor 的差异：fn 的第二个参数始终为 nil；遇到错误立即中止，
// 因为测试中通常希望首个失败就能定位问题。
func (r *fakeResourceResult) Visit(fn resource.VisitorFunc) error {
	if r.err != nil {
		return r.err
	}
	for _, info := range r.Infos {
		err := fn(info, nil)
		if err != nil {
			return err
		}
	}
	return nil
}
