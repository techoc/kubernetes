/*
Copyright 2019 The Kubernetes Authors.

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

package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"time"

	netutils "k8s.io/utils/net"
)

// 要监听的 TCP 端口
const port = 9000

func main() {
	ips := getIPs()
	if len(ips) == 0 {
		panic("No valid IP found")
	}

	// 监听 TCP 数据包，以注入乱序的 TCP 数据包
	for _, ip := range ips {
		log.Printf("external ip: %v", ip.String())
		go probe(ip.String())
	}

	log.Printf("listen on %v:%d", "0.0.0.0", port)

	// 开启一个监听中的服务端以建立 TCP 连接
	listener, err := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", port))
	if err != nil {
		panic(err)
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			panic(err)
		}

		go func(conn net.Conn) {
			// 10 秒后关闭连接
			time.Sleep(10 * time.Second)
			conn.Close()
		}(conn)
	}
}

func probe(ip string) {
	log.Printf("probing %v", ip)

	ipAddr, err := net.ResolveIPAddr("ip:tcp", ip)
	if err != nil {
		panic(err)
	}

	conn, err := net.ListenIP("ip:tcp", ipAddr)
	if err != nil {
		panic(err)
	}

	pending := make(map[string]uint32)

	var buffer [4096]byte
	for {
		n, addr, err := conn.ReadFrom(buffer[:])
		if err != nil {
			log.Printf("conn.ReadFrom() error: %v", err)
			continue
		}

		pkt := &tcpPacket{}
		data, err := pkt.decode(buffer[:n])
		if err != nil {
			log.Printf("tcp packet parse error: %v", err)
			continue
		}

		if pkt.DestPort != 9000 {
			continue
		}

		log.Printf("tcp packet: %+v, flag: %v, data: %v, addr: %v", pkt, pkt.FlagString(), data, addr)

		ipPort := fmt.Sprintf("%s:%d-%s:%d", addr.String(), pkt.SrcPort, ip, pkt.DestPort)
		if pkt.Flags&SYN != 0 {
			pending[ipPort] = pkt.Seq + 1
			continue
		}
		if pkt.Flags&RST != 0 {
			log.Println("ERROR: RST received")
		}
		if pkt.Flags&ACK != 0 {
			if seq, ok := pending[ipPort]; ok {
				log.Println("connection established")
				delete(pending, ipPort)

				badPkt := &tcpPacket{
					SrcPort:    pkt.DestPort,
					DestPort:   pkt.SrcPort,
					Ack:        seq,
					Seq:        pkt.Ack - 100000,      // 错误：seq 超出窗口
					Flags:      (5 << 12) | PSH | ACK, // Offset 和 Flags  oooo000F FFFFFFFF (o:offset, F:flags)
					WindowSize: pkt.WindowSize,
				}

				data := []byte("boom!!!")
				remoteIP := netutils.ParseIPSloppy(addr.String())
				localIP := netutils.ParseIPSloppy(conn.LocalAddr().String())
				_, err := conn.WriteTo(badPkt.encode(localIP, remoteIP, data[:]), addr)
				if err != nil {
					log.Printf("conn.WriteTo() error: %v", err)
				} else {
					log.Println("boom packet injected")
				}
			}
		}
	}
}

// getIPs 从 downward API 获取 IP
// 我们无需校验这些 IP，因为它们
// 此前已经由 kubernetes/CNI 校验过
func getIPs() []net.IP {
	var ips []net.IP
	podIP, podIPs := os.Getenv("POD_IP"), os.Getenv("POD_IPS")
	if podIPs != "" {
		for _, ip := range strings.Split(podIPs, ",") {
			ips = append(ips, netutils.ParseIPSloppy(ip))
		}
	} else if podIP != "" {
		ips = append(ips, netutils.ParseIPSloppy(podIP))
	}
	return ips
}
