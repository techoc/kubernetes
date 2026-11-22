//go:build linux

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

package nfacct

import (
	"github.com/vishvananda/netlink/nl"
	"github.com/vishvananda/netns"
	"golang.org/x/sys/unix"
)

// handler 封装 netlink 请求的创建，便于在测试中替换。
type handler interface {
	// newRequest 创建携带指定命令与标志位的 netlink 请求。
	newRequest(cmd int, flags uint16) request
}

// request 表示可注入的 netlink 请求接口。
type request interface {
	// Serialize 将请求序列化为可发送的字节流。
	Serialize() []byte
	// AddData 向请求追加结构化的 netlink 数据。
	AddData(data nl.NetlinkRequestData)
	// AddRawData 向请求追加原始字节数据。
	AddRawData(data []byte)
	// Execute 发送请求并返回各条响应的原始字节。
	Execute(sockType int, resType uint16) ([][]byte, error)
}

// netlinkHandler 是 handler 接口的默认实现，持有与 NFAcct 子系统通信的 netlink socket。
type netlinkHandler struct {
	socket *nl.NetlinkSocket
}

// newNetlinkHandler 在当前网络命名空间内创建 netlink socket，
// 并返回持有该 socket 的 netlinkHandler。
func newNetlinkHandler() (handler, error) {
	socket, err := nl.GetNetlinkSocketAt(netns.None(), netns.None(), unix.NETLINK_NETFILTER)
	if err != nil {
		return nil, err
	}
	return &netlinkHandler{socket: socket}, nil
}

// newRequest 创建面向 NFAcct 子系统的 netlink 请求，封装指定的 cmd 与 flags，
// 并在请求中填入 netlink 消息头和 netfilter 通用消息头。
func (n *netlinkHandler) newRequest(cmd int, flags uint16) request {
	req := &nl.NetlinkRequest{
		// netlink 消息类型的高 8 位必须是子系统编号，内核据此把请求分发给 NFAcct 模块。
		// netlink 消息头（定义见 https://github.com/torvalds/linux/blob/v6.7/include/uapi/linux/netlink.h#L44-L58）
		NlMsghdr: unix.NlMsghdr{
			Len:   uint32(unix.SizeofNlMsghdr),
			Type:  uint16(cmd | (unix.NFNL_SUBSYS_ACCT << 8)),
			Flags: flags,
		},
		Sockets: map[int]*nl.SocketHandle{
			unix.NETLINK_NETFILTER: {Socket: n.socket},
		},
		Data: []nl.NetlinkRequestData{
			// netfilter 通用消息头
			// （定义见 https://github.com/torvalds/linux/blob/v6.7/include/uapi/linux/netfilter/nfnetlink.h#L32-L38）
			&nl.Nfgenmsg{
				NfgenFamily: uint8(unix.AF_NETLINK),
				Version:     nl.NFNETLINK_V0,
				ResId:       0,
			},
		},
	}
	return req
}
