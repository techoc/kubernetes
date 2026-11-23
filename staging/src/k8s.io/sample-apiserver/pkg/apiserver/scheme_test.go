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

package apiserver

import (
	"testing"

	"k8s.io/apimachinery/pkg/api/apitesting/roundtrip"
	wardlefuzzer "k8s.io/sample-apiserver/pkg/apis/wardle/fuzzer"
)

// 用 fuzzer 覆盖各版本对象，验证编码再解码不丢字段；
// 这正是多版本 API 最容易出错的地方，所以单独跑一次完整 Scheme 的往返。
func TestRoundTripTypes(t *testing.T) {
	roundtrip.RoundTripTestForScheme(t, Scheme, wardlefuzzer.Funcs)
}
