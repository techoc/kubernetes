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

package server

import (
	"encoding/binary"
	"fmt"
	"io"
	"strconv"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/websocket"

	"k8s.io/apimachinery/pkg/types"
	"k8s.io/cri-streaming/pkg/streaming/portforward"
	"k8s.io/kubernetes/test/utils/ktesting"
)

const (
	// WebSocket 帧的通道编号：0 为数据通道，1 为错误通道。
	dataChannel = iota
	errorChannel
)

// TestServeWSPortForward 验证端口转发 WebSocket 接口对端口参数、UID 与数据转发的处理。
func TestServeWSPortForward(t *testing.T) {
	tCtx := ktesting.Init(t)
	tests := map[string]struct {
		port          string
		uid           bool
		clientData    string
		containerData string
		shouldError   bool
	}{
		"no port":                       {port: "", shouldError: true},
		"none number port":              {port: "abc", shouldError: true},
		"negative port":                 {port: "-1", shouldError: true},
		"too large port":                {port: "65536", shouldError: true},
		"0 port":                        {port: "0", shouldError: true},
		"min port":                      {port: "1", shouldError: false},
		"normal port":                   {port: "8000", shouldError: false},
		"normal port with data forward": {port: "8000", clientData: "client data", containerData: "container data", shouldError: false},
		"max port":                      {port: "65535", shouldError: false},
		"normal port with uid":          {port: "8000", uid: true, shouldError: false},
	}

	podNamespace := "other"
	podName := "foo"

	for desc := range tests {
		test := tests[desc]
		t.Run(desc, func(t *testing.T) {
			// 每个用例独立启动流式服务器与带调试端口的 kubelet 测试服务器。
			ss, err := newTestStreamingServer(0)
			require.NoError(t, err)
			defer ss.testHTTPServer.Close()
			fw := newServerTestWithDebug(tCtx, true, ss)
			defer fw.testHTTPServer.Close()

			portForwardFuncDone := make(chan struct{})

			// 校验端口转发请求携带的 Pod 名称、命名空间与可选 UID。
			fw.fakeKubelet.getPortForwardCheck = func(name, namespace string, uid types.UID, opts portforward.V4Options) {
				assert.Equal(t, podName, name, "pod name")
				assert.Equal(t, podNamespace, namespace, "pod namespace")
				if test.uid {
					assert.Equal(t, testUID, string(uid), "uid")
				}
			}

			ss.fakeRuntime.portForwardFunc = func(podSandboxID string, port int32, stream io.ReadWriteCloser) error {
				defer close(portForwardFuncDone)
				assert.Equal(t, testPodSandboxID, podSandboxID, "pod sandbox id")
				// 能走到这里说明端口已通过校验。
				testPort, err := strconv.ParseInt(test.port, 10, 32)
				require.NoError(t, err, "parse port")
				assert.Equal(t, int32(testPort), port, "port")

				if test.clientData != "" {
					fromClient := make([]byte, 32)
					n, err := stream.Read(fromClient)
					assert.NoError(t, err, "reading client data")
					assert.Equal(t, test.clientData, string(fromClient[0:n]), "client data")
				}

				if test.containerData != "" {
					_, err := stream.Write([]byte(test.containerData))
					assert.NoError(t, err, "writing container data")
				}

				return nil
			}

			var url string
			if test.uid {
				url = fmt.Sprintf("ws://%s/portForward/%s/%s/%s?port=%s", fw.testHTTPServer.Listener.Addr().String(), podNamespace, podName, testUID, test.port)
			} else {
				url = fmt.Sprintf("ws://%s/portForward/%s/%s?port=%s", fw.testHTTPServer.Listener.Addr().String(), podNamespace, podName, test.port)
			}

			ws, err := websocket.Dial(url, "", "http://127.0.0.1/")
			assert.Equal(t, test.shouldError, err != nil, "websocket dial")
			if test.shouldError {
				return
			}
			defer ws.Close()

			p, err := strconv.ParseUint(test.port, 10, 16)
			require.NoError(t, err, "parse port")
			p16 := uint16(p)

			// 先读取数据通道与错误通道各一帧，内容都是小端序的端口号。
			channel, data, err := wsRead(ws)
			require.NoError(t, err, "read")
			assert.Equal(t, dataChannel, int(channel), "channel")
			assert.Len(t, data, binary.Size(p16), "data size")
			assert.Equal(t, p16, binary.LittleEndian.Uint16(data), "data")

			channel, data, err = wsRead(ws)
			assert.NoError(t, err, "read")
			assert.Equal(t, errorChannel, int(channel), "channel")
			assert.Len(t, data, binary.Size(p16), "data size")
			assert.Equal(t, p16, binary.LittleEndian.Uint16(data), "data")

			if test.clientData != "" {
				// 通过数据通道把客户端数据发给容器，并等待容器回写数据。
				println("writing the client data")
				err := wsWrite(ws, dataChannel, []byte(test.clientData))
				assert.NoError(t, err, "writing client data")
			}

			if test.containerData != "" {
				_, data, err = wsRead(ws)
				assert.NoError(t, err, "reading container data")
				assert.Equal(t, test.containerData, string(data), "container data")
			}

			// 等待容器侧转发函数退出，确保数据流已处理完毕。
			<-portForwardFuncDone
		})
	}
}

// TestServeWSMultiplePortForward 验证单个 WebSocket 连接上同时转发多个端口。
func TestServeWSMultiplePortForward(t *testing.T) {
	tCtx := ktesting.Init(t)
	portsText := []string{"7000,8000", "9000"}
	ports := []uint16{7000, 8000, 9000}
	podNamespace := "other"
	podName := "foo"

	// 启动流式服务器与 kubelet 测试服务器。
	ss, err := newTestStreamingServer(0)
	require.NoError(t, err)
	defer ss.testHTTPServer.Close()
	fw := newServerTestWithDebug(tCtx, true, ss)
	defer fw.testHTTPServer.Close()

	portForwardWG := sync.WaitGroup{}
	// 通过 WaitGroup 等待所有端口的转发函数各执行一次。
	portForwardWG.Add(len(ports))

	portsMutex := sync.Mutex{}
	// 记录实际转发过的端口，用于最终校验。
	portsForwarded := map[int32]struct{}{}

	fw.fakeKubelet.getPortForwardCheck = func(name, namespace string, uid types.UID, opts portforward.V4Options) {
		assert.Equal(t, podName, name, "pod name")
		assert.Equal(t, podNamespace, namespace, "pod namespace")
	}

	ss.fakeRuntime.portForwardFunc = func(podSandboxID string, port int32, stream io.ReadWriteCloser) error {
		defer portForwardWG.Done()
		assert.Equal(t, testPodSandboxID, podSandboxID, "pod sandbox id")

		// 多个端口并发写入，需加锁保护。
		portsMutex.Lock()
		portsForwarded[port] = struct{}{}
		portsMutex.Unlock()

		fromClient := make([]byte, 32)
		n, err := stream.Read(fromClient)
		assert.NoError(t, err, "reading client data")
		assert.Equal(t, fmt.Sprintf("client data on port %d", port), string(fromClient[0:n]), "client data")

		_, err = stream.Write([]byte(fmt.Sprintf("container data on port %d", port)))
		assert.NoError(t, err, "writing container data")

		return nil
	}

	url := fmt.Sprintf("ws://%s/portForward/%s/%s?", fw.testHTTPServer.Listener.Addr().String(), podNamespace, podName)
	for _, port := range portsText {
		url = url + fmt.Sprintf("port=%s&", port)
	}

	ws, err := websocket.Dial(url, "", "http://127.0.0.1/")
	require.NoError(t, err, "websocket dial")

	defer ws.Close()

	// 依次读取每个端口的数据通道与错误通道帧。
	for i, port := range ports {
		channel, data, err := wsRead(ws)
		assert.NoError(t, err, "port %d read", port)
		assert.Equal(t, i*2+dataChannel, int(channel), "port %d channel", port)
		assert.Len(t, data, binary.Size(port), "port %d data size", port)
		assert.Equal(t, binary.LittleEndian.Uint16(data), port, "port %d data", port)

		channel, data, err = wsRead(ws)
		assert.NoError(t, err, "port %d read", port)
		assert.Equal(t, i*2+errorChannel, int(channel), "port %d channel", port)
		assert.Len(t, data, binary.Size(port), "port %d data size", port)
		assert.Equal(t, binary.LittleEndian.Uint16(data), port, "port %d data", port)
	}

	for i, port := range ports {
		// 再逐个端口写入客户端数据，并读取容器返回的数据。
		t.Logf("port %d writing the client data", port)
		err := wsWrite(ws, byte(i*2+dataChannel), []byte(fmt.Sprintf("client data on port %d", port)))
		assert.NoError(t, err, "port %d write client data", port)

		channel, data, err := wsRead(ws)
		assert.NoError(t, err, "port %d read container data", port)
		assert.Equal(t, i*2+dataChannel, int(channel), "port %d channel", port)
		assert.Equal(t, fmt.Sprintf("container data on port %d", port), string(data), "port %d container data", port)
	}

	// 所有转发完成后校验端口集合完整。
	portForwardWG.Wait()

	portsMutex.Lock()
	defer portsMutex.Unlock()
	assert.Len(t, portsForwarded, len(ports), "all ports forwarded")
}

// wsWrite 在数据前加一字节通道号，作为单个 WebSocket 消息发送。
func wsWrite(conn *websocket.Conn, channel byte, data []byte) error {
	frame := make([]byte, len(data)+1)
	frame[0] = channel
	copy(frame[1:], data)
	err := websocket.Message.Send(conn, frame)
	return err
}

// wsRead 读取一个 WebSocket 消息，返回通道号与去掉通道号后的数据。
func wsRead(conn *websocket.Conn) (byte, []byte, error) {
	for {
		var data []byte
		err := websocket.Message.Receive(conn, &data)
		if err != nil {
			return 0, nil, err
		}

		// 忽略空消息，继续等待有效帧。
		if len(data) == 0 {
			continue
		}

		channel := data[0]
		data = data[1:]

		return channel, data, err
	}
}
