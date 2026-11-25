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

package portforward

import (
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	gwebsocket "github.com/gorilla/websocket"

	"k8s.io/klog/v2"
)

var _ net.Conn = &TunnelingConnection{}

// TunnelingConnection 实现 httpstream.Connection 接口，封装通过 WebSocket 隧道承载 SPDY 的连接。
//
// inProgressMessage 保存当前尚未读完的 WebSocket 消息。Read 不保证一次读满
// 调用方缓冲区，因此必须跨调用保留该 reader，否则会丢消息边界。
// closeOnce 保证底层连接只关闭一次。
type TunnelingConnection struct {
	logger            klog.Logger
	conn              *gwebsocket.Conn
	inProgressMessage io.Reader
	closeOnce         sync.Once
}

// NewTunnelingConnection 将传入的 gorilla/websocket 连接封装为实现 net.Conn 的
// TunnelingConnection，并通过 klog.LoggerWithName 为所有日志添加指定名称。
//
//logcheck:context // NewTunnelingConnectionWithLogger should be used instead of NewTunnelingConnection in code which supports contextual logging.
func NewTunnelingConnection(name string, conn *gwebsocket.Conn) *TunnelingConnection {
	logger := klog.LoggerWithName(klog.Background(), name)
	return NewTunnelingConnectionWithLogger(logger, conn)
}

// NewTunnelingConnectionWithLogger 是 NewTunnelingConnection 的变体，由调用方控制日志。
// 例如可使用 klog.LoggerWithName 为所有日志添加统一名称，以标识该连接。
func NewTunnelingConnectionWithLogger(logger klog.Logger, conn *gwebsocket.Conn) *TunnelingConnection {
	return &TunnelingConnection{
		logger: logger,
		conn:   conn,
	}
}

// Read 实现 io.Reader 接口，从已保存的连接读取数据到缓冲区 p，返回读取字节数和错误。
// 它会跟踪隧道连接中尚未读完的消息：当前消息读尽（n==0 且 EOF）后清理状态并进入下一条消息，
// 其他情况直接返回，确保不丢失消息边界。
func (c *TunnelingConnection) Read(p []byte) (len int, err error) {
	c.logger.V(7).Info("Tunneling connection read...")
	defer func() {
		// 仅在开启高详细度日志时才输出数据内容，避免敏感信息泄漏与大日志量。
		if loggerV := c.logger.V(8); loggerV.Enabled() {
			loggerV.Info("Tunneling connection read...complete", "length", len, "data", p[:len], "err", err)
		} else {
			c.logger.V(7).Info("Tunneling connection read...complete")
		}
	}()
	for {
		if c.inProgressMessage == nil {
			c.logger.V(8).Info("Tunneling connection read before NextReader()...")
			messageType, nextReader, err := c.conn.NextReader()
			if err != nil {
				closeError := &gwebsocket.CloseError{}
				// 对端正常关闭映射为 io.EOF，符合 net.Conn 的读取约定。
				if errors.As(err, &closeError) && closeError.Code == gwebsocket.CloseNormalClosure {
					return 0, io.EOF
				}
				c.logger.V(4).Info("Tunneling connection NextReader() failed", "err", err)
				return 0, err
			}
			// 该隧道只承载二进制数据，文本帧属于协议错误。
			if messageType != gwebsocket.BinaryMessage {
				return 0, fmt.Errorf("invalid message type received")
			}
			c.inProgressMessage = nextReader
		}
		c.logger.V(8).Info("Tunneling connection read in progress...")
		i, err := c.inProgressMessage.Read(p)
		if i == 0 && err == io.EOF {
			c.inProgressMessage = nil
		} else {
			return i, err
		}
	}
}

// Write 实现 io.Writer 接口，将字节数组 p 写入已保存的隧道连接，返回写入字节数和错误。
// 每次 Write 对应一条独立的 WebSocket 二进制消息；必须 Close writer 才能真正发送（flush），
// 因此错误处理要兼顾写入与关闭两个阶段。
func (c *TunnelingConnection) Write(p []byte) (n int, err error) {
	c.logger.V(7).Info("Tunneling connection write", "length", len(p), "data", p)
	defer c.logger.V(7).Info("Tunneling connection write...complete")
	w, err := c.conn.NextWriter(gwebsocket.BinaryMessage)
	if err != nil {
		return 0, err
	}
	defer func() {
		// 关闭 writer 以刷新并发送消息
		closeErr := w.Close()
		if closeErr != nil && err == nil {
			// 关闭或刷新出错且当前尚无其他错误时，返回关闭错误
			err = closeErr
		}
	}()

	n, err = w.Write(p)
	return
}

// Close 实现 io.Closer 接口，通知隧道另一端并确保连接只关闭一次。
// 先用带 1 秒超时的关闭控制帧通知对端，再关闭底层连接；所有操作包在 sync.Once 中，
// 重复调用不会重复发送关闭帧。
func (c *TunnelingConnection) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.logger.V(7).Info("Tunneling connection Close()...")
		// 通知另一端 WebSocket 连接即将关闭；忽略通知错误。
		normalCloseMsg := gwebsocket.FormatCloseMessage(gwebsocket.CloseNormalClosure, "")
		writeControlErr := c.conn.WriteControl(gwebsocket.CloseMessage, normalCloseMsg, time.Now().Add(time.Second))
		closeErr := c.conn.Close()
		if closeErr != nil {
			err = closeErr
		} else if writeControlErr != nil {
			err = writeControlErr
		}
	})
	return err
}

// LocalAddr 实现 net.Conn 接口的一部分，返回隧道连接的本地端点网络地址。
func (c *TunnelingConnection) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// RemoteAddr 实现 net.Conn 接口的一部分，返回隧道连接的远端端点网络地址。
func (c *TunnelingConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline 将读、写截止时间都设置为未来的绝对时间，并合并两个操作产生的错误。
func (c *TunnelingConnection) SetDeadline(t time.Time) error {
	rerr := c.SetReadDeadline(t)
	werr := c.SetWriteDeadline(t)
	return errors.Join(rerr, werr)
}

// SetReadDeadline 将读截止时间设置为未来的绝对时间，出错时返回错误。
func (c *TunnelingConnection) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetWriteDeadline 将写截止时间设置为未来的绝对时间，出错时返回错误。
func (c *TunnelingConnection) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
