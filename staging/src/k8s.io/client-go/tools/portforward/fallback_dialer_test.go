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

package portforward

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/util/httpstream"
)

// TestFallbackDialer 依次验证 fallback 判定的四种情形：
// 主拨号成功、升级失败回退、HTTPS 代理错误回退、普通错误不回退。
func TestFallbackDialer(t *testing.T) {
	primaryProtocol := "primary.fake.protocol"
	secondaryProtocol := "secondary.fake.protocol"
	protocols := []string{primaryProtocol, secondaryProtocol}
	// If primary dialer error is nil, then no fallback and primary negotiated protocol returned.
	primary := &fakeDialer{dialed: false, negotiatedProtocol: primaryProtocol}
	secondary := &fakeDialer{dialed: false, negotiatedProtocol: secondaryProtocol}
	fallbackDialer := NewFallbackDialer(primary, secondary, notCalled)
	_, negotiated, err := fallbackDialer.Dial(protocols...)
	assert.True(t, primary.dialed, "no fallback; primary should have dialed")
	assert.False(t, secondary.dialed, "no fallback; secondary should *not* have dialed")
	assert.Equal(t, primaryProtocol, negotiated, "primary negotiated protocol returned")
	require.NoError(t, err, "error from primary dialer should be nil")
	// If primary dialer error is upgrade error, then fallback returning secondary dial response.
	// 升级失败（例如服务端不支持 websocket 子协议）说明该传输方式不可用，应尝试备用拨号器。
	primary = &fakeDialer{dialed: false, negotiatedProtocol: primaryProtocol, err: &httpstream.UpgradeFailureError{Cause: fmt.Errorf("fake error")}}
	secondary = &fakeDialer{dialed: false, negotiatedProtocol: secondaryProtocol}
	fallbackDialer = NewFallbackDialer(primary, secondary, httpstream.IsUpgradeFailure)
	_, negotiated, err = fallbackDialer.Dial(protocols...)
	assert.True(t, primary.dialed, "fallback; primary should have dialed")
	assert.True(t, secondary.dialed, "fallback; secondary should have dialed")
	assert.Equal(t, secondaryProtocol, negotiated, "negotiated protocol is from secondary dialer")
	require.NoError(t, err, "error from secondary dialer should be nil")
	// If primary dialer error is https proxy dialing error, then fallback returning secondary dial response.
	// 某些代理不支持 CONNECT 到 https，这类错误同样值得回退到另一种拨号方式。
	primary = &fakeDialer{negotiatedProtocol: primaryProtocol, err: errors.New("proxy: unknown scheme: https")}
	secondary = &fakeDialer{negotiatedProtocol: secondaryProtocol}
	fallbackDialer = NewFallbackDialer(primary, secondary, func(err error) bool {
		return httpstream.IsUpgradeFailure(err) || httpstream.IsHTTPSProxyError(err)
	})
	_, negotiated, err = fallbackDialer.Dial(protocols...)
	assert.True(t, primary.dialed, "fallback; primary should have dialed")
	assert.True(t, secondary.dialed, "fallback; secondary should have dialed")
	assert.Equal(t, secondaryProtocol, negotiated, "negotiated protocol is from secondary dialer")
	require.NoError(t, err, "error from secondary dialer should be nil")
	// If primary dialer returns non-upgrade error, then primary error is returned.
	// 普通错误（如认证失败、连接被拒）回退也不会成功，因此直接返回主拨号器的错误。
	nonUpgradeErr := fmt.Errorf("This is a non-upgrade error")
	primary = &fakeDialer{dialed: false, err: nonUpgradeErr}
	secondary = &fakeDialer{dialed: false}
	fallbackDialer = NewFallbackDialer(primary, secondary, httpstream.IsUpgradeFailure)
	_, _, err = fallbackDialer.Dial(protocols...)
	assert.True(t, primary.dialed, "no fallback; primary should have dialed")
	assert.False(t, secondary.dialed, "no fallback; secondary should *not* have dialed")
	assert.Equal(t, nonUpgradeErr, err, "error is from primary dialer")
}

// notCalled 作为“永不回退”的判定函数。
func notCalled(err error) bool { return false }
