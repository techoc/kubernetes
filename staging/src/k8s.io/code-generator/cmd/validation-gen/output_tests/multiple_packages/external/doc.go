/*
Copyright The Kubernetes Authors.

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

// A non-registering copy of ../types' validators, selecting all types via *.
// +k8s:validation-gen=*
// +k8s:validation-gen-input=k8s.io/code-generator/cmd/validation-gen/output_tests/multiple_packages/types
// +k8s:validation-gen-scheme-registry=nil

// This is a test package.
// external 是 multiple_packages 测试中的非注册副本：通过
// validation-gen-input 指向 types 包，并设置 scheme-registry=nil，
// 用于验证同一份校验逻辑可以被生成到另一个包而不注册 scheme。
// +k8s:validation-gen-nolint
package external
