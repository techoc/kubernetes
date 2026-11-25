/*
Copyright 2025 The Kubernetes Authors.

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

package kubeletplugin

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path"
)

// endpoint 定义监听传入连接的地址与方式。
// 无论何种方式，关停时监听器总会被关闭。
// 如果未设置 listenFunc，则在 path 指定的路径上创建一个新的 Unix 域套接字监听器。
type endpoint struct {
	// dir 是套接字所在目录，file 是套接字文件名，二者共同决定完整路径；
	// listenFunc 是可选的自定义监听函数，用于替代默认的 Unix 域套接字监听。
	dir, file  string
	listenFunc func(ctx context.Context, socketpath string) (net.Listener, error)
}

// path 返回 dir 与 file 拼接后的套接字完整路径。
func (e endpoint) path() string {
	return path.Join(e.dir, e.file)
}

// listen 创建并返回监听器：优先使用自定义 listenFunc，否则在 Unix 域套接字路径上监听。
func (e endpoint) listen(ctx context.Context) (net.Listener, error) {
	// 先按 dir 与 file 解析出套接字路径。
	socketpath := e.path()

	// 自定义监听函数完全接管监听行为，返回什么就用什么。
	if e.listenFunc != nil {
		return e.listenFunc(ctx, socketpath)
	}

	// 清理残留的旧套接字文件，否则监听会因路径已存在而失败。
	if err := e.removeSocket(); err != nil {
		return nil, err
	}
	// 用带上下文的 ListenConfig 在 Unix 域套接字路径上监听。
	cfg := net.ListenConfig{}
	listener, err := cfg.Listen(ctx, "unix", socketpath)
	// 监听失败时再次尝试删除套接字，并把删除错误与监听错误合并返回。
	if err != nil {
		if removeErr := e.removeSocket(); removeErr != nil {
			err = errors.Join(err, removeErr)
		}
		return nil, err
	}
	// 包装底层监听器，使 Close 时自动删除套接字文件。
	return &unixListener{Listener: listener, endpoint: e}, nil
}

// removeSocket 删除套接字文件；文件不存在视为成功，其他错误被包装返回。
func (e endpoint) removeSocket() error {
	// 调用 os.Remove 清理；只把非“不存在”的错误当作失败。
	if err := os.Remove(e.path()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove Unix domain socket: %w", err)
	}
	return nil
}

// unixListener 在标准 net.Listener 之上增加了 Close 时删除 Unix 域套接字的行为。
type unixListener struct {
	// net.Listener 是被包装的底层监听器。
	// endpoint 用于在关闭时定位并删除对应的套接字文件。
	net.Listener
	endpoint endpoint
}

// Close 先关闭底层监听器，再删除套接字文件；两类错误通过 errors.Join 合并返回。
func (l *unixListener) Close() error {
	// 先关闭底层监听器。
	err := l.Listener.Close()
	// 无论关闭是否成功，都尝试清理套接字文件。
	if removeErr := l.endpoint.removeSocket(); removeErr != nil {
		err = errors.Join(err, removeErr)
	}
	return err
}
