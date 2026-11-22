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

// TunnelingConnection implements the "httpstream.Connection" interface, wrapping
// a websocket connection that tunnels SPDY.
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

// NewTunnelingConnection wraps the passed gorilla/websockets connection
// with the TunnelingConnection struct (implementing net.Conn).
// The name is added to all log entries with [klog.LoggerWithName].
//
//logcheck:context // NewTunnelingConnectionWithLogger should be used instead of NewTunnelingConnection in code which supports contextual logging.
func NewTunnelingConnection(name string, conn *gwebsocket.Conn) *TunnelingConnection {
	logger := klog.LoggerWithName(klog.Background(), name)
	return NewTunnelingConnectionWithLogger(logger, conn)
}

// NewTunnelingConnectionWithLogger is a variant of NewTunnelingConnection where
// the caller is in control of logging. For example, [klog.LoggerWithName] can be used
// to add a common name for all log entries to identify the connection.
func NewTunnelingConnectionWithLogger(logger klog.Logger, conn *gwebsocket.Conn) *TunnelingConnection {
	return &TunnelingConnection{
		logger: logger,
		conn:   conn,
	}
}

// Read implements "io.Reader" interface, reading from the stored connection
// into the passed buffer "p". Returns the number of bytes read and an error.
// Can keep track of the "inProgress" messsage from the tunneled connection.
//
// 读取可能跨多条 WebSocket 消息：当前消息读尽（n==0 且 EOF）后清理状态，
// 循环进入下一条消息；其他情况直接返回，确保不丢失消息边界。
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

// Write implements "io.Writer" interface, copying the data in the passed
// byte array "p" into the stored tunneled connection. Returns the number
// of bytes written and an error.
//
// 每次 Write 对应一条独立的 WebSocket 二进制消息；必须 Close writer 才能
// 真正发送（flush），因此错误处理要兼顾写入与关闭两个阶段。
func (c *TunnelingConnection) Write(p []byte) (n int, err error) {
	c.logger.V(7).Info("Tunneling connection write", "length", len(p), "data", p)
	defer c.logger.V(7).Info("Tunneling connection write...complete")
	w, err := c.conn.NextWriter(gwebsocket.BinaryMessage)
	if err != nil {
		return 0, err
	}
	defer func() {
		// close, which flushes the message
		closeErr := w.Close()
		if closeErr != nil && err == nil {
			// if closing/flushing errored and we weren't already returning an error, return the close error
			err = closeErr
		}
	}()

	n, err = w.Write(p)
	return
}

// Close implements "io.Closer" interface, signaling the other tunneled connection
// endpoint, and closing the tunneled connection only once.
//
// 先用带 1 秒超时的 close control frame 通知对端，再关闭底层连接；所有
// 操作包在 sync.Once 中，重复调用不会重复发送关闭帧。
func (c *TunnelingConnection) Close() error {
	var err error
	c.closeOnce.Do(func() {
		c.logger.V(7).Info("Tunneling connection Close()...")
		// Signal other endpoint that websocket connection is closing; ignore error.
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

// LocalAddr implements part of the "net.Conn" interface, returning the local
// endpoint network address of the tunneled connection.
func (c *TunnelingConnection) LocalAddr() net.Addr {
	return c.conn.LocalAddr()
}

// LocalAddr implements part of the "net.Conn" interface, returning the remote
// endpoint network address of the tunneled connection.
//
// （方法名为 RemoteAddr；上面的英文注释沿用了 LocalAddr 的措辞。）
func (c *TunnelingConnection) RemoteAddr() net.Addr {
	return c.conn.RemoteAddr()
}

// SetDeadline sets the *absolute* time in the future for both
// read and write deadlines. Returns an error if one occurs.
//
// 同时设置读、写截止时间，并合并两个错误。
func (c *TunnelingConnection) SetDeadline(t time.Time) error {
	rerr := c.SetReadDeadline(t)
	werr := c.SetWriteDeadline(t)
	return errors.Join(rerr, werr)
}

// SetDeadline sets the *absolute* time in the future for the
// read deadlines. Returns an error if one occurs.
//
// （方法名为 SetReadDeadline，只影响读截止时间。）
func (c *TunnelingConnection) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

// SetDeadline sets the *absolute* time in the future for the
// write deadlines. Returns an error if one occurs.
//
// （方法名为 SetWriteDeadline，只影响写截止时间。）
func (c *TunnelingConnection) SetWriteDeadline(t time.Time) error {
	return c.conn.SetWriteDeadline(t)
}
