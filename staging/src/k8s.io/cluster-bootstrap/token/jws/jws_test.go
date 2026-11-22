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

package jws

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// 固定输入与预期签名，用于锁定 HS256 输出的字节级兼容性；一旦签名格式
// 变化，已有节点将无法验证 cluster-info，因此这里采用精确值断言。
const (
	content = "Hello from the other side. I must have called a thousand times."
	secret  = "my voice is my passcode"
	id      = "joshua"
)

// TestComputeDetachedSignature 覆盖普通内容、空内容以及空 Secret 三种
// 输入，确保签名器在这些边界情况下仍产生稳定的 detached JWS。
func TestComputeDetachedSignature(t *testing.T) {
	sig, err := ComputeDetachedSignature(content, id, secret)
	assert.NoError(t, err, "Error when computing signature: %v", err)
	assert.Equal(
		t,
		"eyJhbGciOiJIUzI1NiIsImtpZCI6Impvc2h1YSJ9..VShe2taLd-YTrmWuRkcL_8QTNDHYxQIEBsAYYiIj1_8",
		sig,
		"Wrong signature. Got: %v", sig)

	// Try with null content
	sig, err = ComputeDetachedSignature("", id, secret)
	assert.NoError(t, err, "Error when computing signature: %v", err)
	assert.Equal(
		t,
		"eyJhbGciOiJIUzI1NiIsImtpZCI6Impvc2h1YSJ9..7Ui1ALizW4jXphVUB7xUqC9vLYLL9RZeOFfVLoB7Tgk",
		sig,
		"Wrong signature. Got: %v", sig)

	// Try with no secret
	sig, err = ComputeDetachedSignature(content, id, "")
	assert.NoError(t, err, "Error when computing signature: %v", err)
	assert.Equal(
		t,
		"eyJhbGciOiJIUzI1NiIsImtpZCI6Impvc2h1YSJ9..UfkqvDGiIFxrMnFseDj9LYJOLNrvjW8aHhF71mvvAs8",
		sig,
		"Wrong signature. Got: %v", sig)
}

// TestDetachedTokenIsValid 确认正确签名通过、被追加字符的签名失败，
// 后者防止宽松比较把伪造签名误判为有效。
func TestDetachedTokenIsValid(t *testing.T) {
	// Valid detached JWS token and valid inputs should succeed
	sig := "eyJhbGciOiJIUzI1NiIsImtpZCI6Impvc2h1YSJ9..VShe2taLd-YTrmWuRkcL_8QTNDHYxQIEBsAYYiIj1_8"
	assert.True(t, DetachedTokenIsValid(sig, content, id, secret),
		"Content %q and token \"%s:%s\" should equal signature: %q", content, id, secret, sig)

	// Invalid detached JWS token and valid inputs should fail
	sig2 := sig + "foo"
	assert.False(t, DetachedTokenIsValid(sig2, content, id, secret),
		"Content %q and token \"%s:%s\" should not equal signature: %q", content, id, secret, sig)
}
