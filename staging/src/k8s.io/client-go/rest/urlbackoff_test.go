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

package rest

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"k8s.io/client-go/util/flowcontrol"
)

// parse 解析 URL；测试中只使用合法输入，忽略错误。
func parse(raw string) *url.URL {
	theUrl, _ := url.Parse(raw)
	return theUrl
}

// TestURLBackoffFunctionalityCollisions 验证退避状态按 host:port 而非仅 host 区分，
// 避免不同端口的 URL 互相污染退避计数。
func TestURLBackoffFunctionalityCollisions(t *testing.T) {
	myBackoff := &URLBackoff{
		Backoff: flowcontrol.NewBackOff(1*time.Second, 60*time.Second),
	}

	// 加入一些干扰后，确认未被退避的 URL 退避值仍为 0。
	myBackoff.UpdateBackoff(parse("http://100.200.300.400:8080"), nil, 500)

	myBackoff.UpdateBackoff(parse("http://1.2.3.4:8080"), nil, 500)

	if myBackoff.CalculateBackoff(parse("http://1.2.3.4:100")) > 0 {
		t.Errorf("URLs are colliding in the backoff map!")
	}
}

// TestURLBackoffFunctionality 总体测试 URLBackoff 包装器，避免与 backoff/request 的测试重复。
//
// 用一条包含递增、重置、再递增的返回码序列，验证 2xx/3xx 会清零退避，而
// 429/5xx 会按指数增长（断言允许 ±5s 的抖动余量）。
func TestURLBackoffFunctionality(t *testing.T) {
	myBackoff := &URLBackoff{
		Backoff: flowcontrol.NewBackOff(1*time.Second, 60*time.Second),
	}

	// 测试退避先增长后恢复：200/300 应清零退避，其他如 429 应增长退避。
	seconds := []int{0,
		1, 2, 4, 8, 0,
		1, 2}
	returnCodes := []int{
		429, 500, 501, 502, 300,
		500, 501, 502,
	}

	if len(seconds) != len(returnCodes) {
		t.Fatalf("responseCode to backoff arrays should be the same length... sanity check failed.")
	}

	for i, sec := range seconds {
		backoffSec := myBackoff.CalculateBackoff(parse("http://1.2.3.4:100"))
		if backoffSec < time.Duration(sec)*time.Second || backoffSec > time.Duration(sec+5)*time.Second {
			t.Errorf("Backoff out of range %v: %v %v", i, sec, backoffSec)
		}
		myBackoff.UpdateBackoff(parse("http://1.2.3.4:100/responseCodeForFuncTest"), nil, returnCodes[i])
	}

	if myBackoff.CalculateBackoff(parse("http://1.2.3.4:100")) == 0 {
		t.Errorf("The final return code %v should have resulted in a backoff ! ", returnCodes[7])
	}
}

// TestBackoffManagerNopContext 验证带 context 的包装函数只是把调用原样转发给
// 内层 BackoffManager（mock 断言了参数与调用次数）。
func TestBackoffManagerNopContext(t *testing.T) {
	mock := NewMockBackoffManager(t)

	sleepDuration := 42 * time.Second
	mock.On("Sleep", sleepDuration).Return()
	url := &url.URL{}
	mock.On("CalculateBackoff", url).Return(time.Second)
	err := errors.New("fake error")
	responseCode := 404
	mock.On("UpdateBackoff", url, err, responseCode).Return()

	ctx := context.Background()
	wrapper := backoffManagerNopContext{BackoffManager: mock}
	wrapper.SleepWithContext(ctx, sleepDuration)
	wrapper.CalculateBackoffWithContext(ctx, url)
	wrapper.UpdateBackoffWithContext(ctx, url, err, responseCode)
}

// TestNoBackoff 验证 NoBackoff 永远不等待：计算恒为 0，Sleep 立即返回，
// 且已取消的 context 不会导致睡眠。
func TestNoBackoff(t *testing.T) {
	var backoff NoBackoff
	assert.Equal(t, 0*time.Second, backoff.CalculateBackoff(nil))
	assert.Equal(t, 0*time.Second, backoff.CalculateBackoffWithContext(context.Background(), nil))

	start := time.Now()
	backoff.Sleep(0 * time.Second)
	assert.WithinDuration(t, start, time.Now(), time.Minute /* pretty generous, but we don't want to flake */, time.Since(start), "backoff.Sleep")

	// 立即取消 context，避免真正睡眠。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start = time.Now()
	backoff.SleepWithContext(ctx, 10*time.Minute)
	assert.WithinDuration(t, start, time.Now(), time.Minute /* pretty generous, but we don't want to flake */, time.Since(start), "backoff.SleepWithContext")
}
