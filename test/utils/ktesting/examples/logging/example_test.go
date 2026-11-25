//go:build example

/*
Copyright 2023 The Kubernetes Authors.

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

package logging

// 下面的测试会失败，因此通过 "example" 构建标签将其排除在
// 常规的 "make test" 之外。要运行这些测试并查看输出，请使用
// "go test -tags example ."

import (
	"testing"

	"k8s.io/kubernetes/test/utils/ktesting"
)

func TestError(t *testing.T) {
	tCtx := ktesting.Init(t)
	tCtx.Error("some", "thing")
}

func TestErrorf(t *testing.T) {
	tCtx := ktesting.Init(t)
	tCtx.Errorf("some %s", "thing")
}

func TestFatal(t *testing.T) {
	tCtx := ktesting.Init(t)
	tCtx.Fatal("some", "thing")
	tCtx.Log("not reached")
}

func TestFatalf(t *testing.T) {
	tCtx := ktesting.Init(t)
	tCtx.Fatalf("some %s", "thing")
	tCtx.Log("not reached")
}

func TestInfo(t *testing.T) {
	tCtx := ktesting.Init(t)
	tCtx.Log("hello via Log")
	tCtx.Logger().Info("hello via Info")
	tCtx.Error("some", "thing")
}

func TestWithStep(t *testing.T) {
	tCtx := ktesting.Init(t)
	bake(tCtx.WithStep("bake cake"))
}

func bake(tCtx ktesting.TContext) {
	heatOven(tCtx.WithStep("set heat for baking"))
}

func heatOven(tCtx ktesting.TContext) {
	tCtx.Log("Log()")
	tCtx.Logger().Info("Logger().Info()")
	tCtx.Fatal("oven not found")
}
