//go:build linux

/*
Copyright 2020 The Kubernetes Authors.

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

package systemd

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/godbus/dbus/v5"
	"github.com/stretchr/testify/assert"
	"k8s.io/klog/v2/ktesting"
)

// fakeDBusObject 是返回预设属性与调用结果的 dbus 对象桩。
type fakeDBusObject struct {
	properties map[string]interface{}
	bodyValue  interface{}
}

// Call 返回包含预设 body 的调用结果。
func (obj *fakeDBusObject) Call(method string, flags dbus.Flags, args ...interface{}) *dbus.Call {
	return &dbus.Call{Err: nil, Body: []interface{}{obj.bodyValue}}
}

// CallWithContext 返回 nil，测试不使用。
func (obj *fakeDBusObject) CallWithContext(ctx context.Context, method string, flags dbus.Flags, args ...interface{}) *dbus.Call {
	return nil
}

// Go 返回 nil，测试不使用异步调用。
func (obj *fakeDBusObject) Go(method string, flags dbus.Flags, ch chan *dbus.Call, args ...interface{}) *dbus.Call {
	return nil
}

// GoWithContext 返回 nil，测试不使用。
func (obj *fakeDBusObject) GoWithContext(ctx context.Context, method string, flags dbus.Flags, ch chan *dbus.Call, args ...interface{}) *dbus.Call {
	return nil
}

// AddMatchSignal 返回 nil，测试不注册信号匹配。
func (obj *fakeDBusObject) AddMatchSignal(iface, member string, options ...dbus.MatchOption) *dbus.Call {
	return nil
}

// RemoveMatchSignal 返回 nil。
func (obj *fakeDBusObject) RemoveMatchSignal(iface, member string, options ...dbus.MatchOption) *dbus.Call {
	return nil
}

// GetProperty 返回预设属性；属性不存在时返回错误。
func (obj *fakeDBusObject) GetProperty(p string) (dbus.Variant, error) {
	value, ok := obj.properties[p]

	if !ok {
		return dbus.Variant{}, fmt.Errorf("property %q does not exist in properties: %+v", p, obj.properties)
	}

	return dbus.MakeVariant(value), nil
}

// SetProperty 空实现。
func (obj *fakeDBusObject) SetProperty(p string, v interface{}) error {
	return nil
}

// StoreProperty 空实现。
func (obj *fakeDBusObject) StoreProperty(p string, v interface{}) error {
	return nil
}

// Destination 返回空字符串。
func (obj *fakeDBusObject) Destination() string {
	return ""
}

// Path 返回空路径。
func (obj *fakeDBusObject) Path() dbus.ObjectPath {
	return ""
}

// fakeSystemDBus 是返回 fake 对象并捕获信号通道的系统总线桩。
type fakeSystemDBus struct {
	fakeDBusObject *fakeDBusObject
	signalChannel  chan<- *dbus.Signal
}

// Object 返回预设的 fake 对象。
func (f *fakeSystemDBus) Object(dest string, path dbus.ObjectPath) dbus.BusObject {
	return f.fakeDBusObject
}

// Signal 记录调用方提供的信号通道。
func (f *fakeSystemDBus) Signal(ch chan<- *dbus.Signal) {
	f.signalChannel = ch
}

// AddMatchSignal 返回 nil。
func (f *fakeSystemDBus) AddMatchSignal(options ...dbus.MatchOption) error {
	return nil
}

// Close 返回 nil。
func (f *fakeSystemDBus) Close() error {
	return nil
}

// TestCurrentInhibitDelay 验证从 logind 读取 InhibitDelayMaxUSec 并转换为 Duration。
func TestCurrentInhibitDelay(t *testing.T) {
	thirtySeconds := time.Duration(30) * time.Second

	// 构造返回 30 秒延迟属性的 fake 总线。
	bus := DBusCon{
		SystemBus: &fakeSystemDBus{
			fakeDBusObject: &fakeDBusObject{
				properties: map[string]interface{}{
					"org.freedesktop.login1.Manager.InhibitDelayMaxUSec": uint64(thirtySeconds / time.Microsecond),
				},
			},
		},
	}

	delay, err := bus.CurrentInhibitDelay()
	assert.NoError(t, err)
	assert.Equal(t, thirtySeconds, delay)
}

// TestInhibitShutdown 验证 InhibitShutdown 返回文件描述符对应的锁。
func TestInhibitShutdown(t *testing.T) {
	var fakeFd uint32 = 42

	bus := DBusCon{
		SystemBus: &fakeSystemDBus{
			fakeDBusObject: &fakeDBusObject{
				bodyValue: fakeFd,
			},
		},
	}

	fdLock, err := bus.InhibitShutdown()
	assert.Equal(t, InhibitLock(fakeFd), fdLock)
	assert.NoError(t, err)
}

// TestReloadLogindConf 验证 ReloadLogindConf 调用成功。
func TestReloadLogindConf(t *testing.T) {
	bus := DBusCon{
		SystemBus: &fakeSystemDBus{
			fakeDBusObject: &fakeDBusObject{},
		},
	}
	assert.NoError(t, bus.ReloadLogindConf())
}

// TestMonitorShutdown 验证 MonitorShutdown 把 dbus 信号转换为关机状态。
func TestMonitorShutdown(t *testing.T) {
	logger, _ := ktesting.NewTestContext(t)
	var tests = []struct {
		desc           string
		shutdownActive bool
	}{
		{
			desc:           "shutdown is active",
			shutdownActive: true,
		},
		{
			desc:           "shutdown is not active",
			shutdownActive: false,
		},
	}

	// 分别验证关机激活与未激活两种情况。
	for _, tc := range tests {
		t.Run(tc.desc, func(t *testing.T) {
			fakeSystemBus := &fakeSystemDBus{}
			bus := DBusCon{
				SystemBus: fakeSystemBus,
			}

			// 启动监控并通过 fake 信号通道注入事件。
			outChan, err := bus.MonitorShutdown(logger)
			assert.NoError(t, err)

			// 等待结果或超时，避免测试挂起。
			done := make(chan bool)

			go func() {
				select {
				case res := <-outChan:
					assert.Equal(t, tc.shutdownActive, res)
					done <- true
				case <-time.After(5 * time.Second):
					t.Errorf("Timed out waiting for shutdown message")
					done <- true
				}
			}()

			signal := &dbus.Signal{Body: []interface{}{tc.shutdownActive}}
			fakeSystemBus.signalChannel <- signal
			<-done
		})
	}
}
