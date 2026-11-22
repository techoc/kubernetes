/*
Copyright 2024 The Kubernetes Authors.

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

package jwt

// CredentialIDForJTI converts a given JTI string into a credential identifier for use in a
// users 'extra' info.
// JTI 为空时不生成标识，避免把空值当作可用的凭据 ID；
// 前缀 "JTI=" 让消费方能在 extra 字段中区分不同来源的凭据标识。
func CredentialIDForJTI(jti string) string {
	if len(jti) == 0 {
		return ""
	}
	return "JTI=" + jti
}
