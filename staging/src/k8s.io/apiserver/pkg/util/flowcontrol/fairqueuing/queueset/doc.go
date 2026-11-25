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

// Package queueset 实现了一种称为「服务器请求公平排队」
// （fair queuing for server requests）的技术。一个 QueueSet 就是
// 按照该技术运行的一组队列。
//
// 服务器请求的公平排队借鉴自网络领域的公平排队
// 技术。可以在 https://dl.acm.org/citation.cfm?doid=75247.75248 或
// http://people.csail.mit.edu/imcgraw/links/research/pubs/networks/WFQ.pdf
// 找到一篇关于它的优秀论文，并且在
// https://en.wikipedia.org/wiki/Fair_queuing 这篇维基百科文章中
// 有一份实现纲要。
//
// 服务器请求的公平排队与传统公平排队有三点
// 不同：(1) 我们是把应用层请求分派给服务器，而不是在网络
// 链路上传输数据包，(2) 可以有多个请求同时执行，(3) 服务
// 时间（执行时长）在执行完成之前是未知的。
//
// 前两个差异可以通过对原论文中称为 "R(t)" 的概念以及
// 实现纲要中称为 "virtual time"（虚拟时间）的概念做直接
// 改编来轻松处理。在那个实现纲要中，记号 now() 用来表示读取
// 虚拟时钟。用原论文的术语来说，"R(t)" 是在真实时间 t 已经完成的
// "轮"（round）数——每一轮的内容是对路由器中每个
// 非空队列虚拟地发送一个比特（不管此刻真正在被发送的数据包
// 属于哪个队列）；在这种概念下，一个数据包直到其发送完成之前都被认为
// "在"它的队列里。对于我们的问题，可以把一轮定义为
// 给 apiserver 中每个非空队列分配一纳秒的 CPU（队列是否为空依据该队列
// 排队中和执行中的请求共同判断），并定义 R(t) =（服务器
// 启动时间）+（1 ns）*（自服务器启动以来的轮数）。
// 我们用 NEQ(t) 表示时间 t 时 apiserver 中非空队列的数量，
// 并用 C 表示并发上限。在原论文中，R(t) 对 t 的偏导数是
//
//	1 / NEQ(t) .
//
// 从一次只发送一个数据包推广到一次执行 C 个请求后，该导数变为
//
//	C / NEQ(t) .
//
// 然而，有时可执行的请求少于 C 个。对于给定的队列 "q"，
// 我们再用 "reqs(q, t)" 表示该队列在那个时刻正在执行的请求
// 数。正在执行的请求总数是 sum[over q] reqs(q, t)，如果它小于 C，
// 那么虚拟时间的推进速度就不如所有 C 个位置都被占用时那么快；这种
// 情况下，该导数中商的分子应当按比例调整。把以上全部内容
// 综合到服务器请求的公平排队中：在特定时刻 t，R(t) 对 t 的偏导
// 数是
//
//	min( C, sum[over q] reqs(q, t) ) / NEQ(t) .
//
// 用实现纲要的术语来说，这就是虚拟时间在时刻 t 的推进速率
// （以每真实纳秒对应的虚拟纳秒数计）。网络场景的实现纲要中是把
// 数据包大小加到虚拟时间上，在我们的版本中这对应于
// 把服务时间（即执行时长）加到虚拟时间上。
//
// 第三个差异通过修改算法来处理：先基于对请求服务时间（时长）
// 的初始猜测进行分派，等请求的实际服务时间已知后再做相应调整。
// 这与原论文为了及时性而做的 `$\delta$` 调整类似，
// 但并不完全同构。
//
// 为了实现的简单（见下文），我们对所有请求使用相同的
// 初始服务时间猜测值；称该时长为 G。一个不错的选择可能是服务时间
// 上限（1 分钟）。不同的猜测值会带来略有不同的动态特性，
// 但任何正数都可以用作 G 而不损害长期行为。
//
// 与普通公平排队一样，与理想状态的偏差存在
// 上界。在朴素公平排队中该上界是一个数据包；在我们的版本中是 C 个请求。
//
// 为了在请求的实际服务时间已知后能高效地做出必要调整，请求的虚拟完成时间与
// 队列的最后虚拟完成时间都不直接表示，而是由队列长度、请求在队列中的位置，
// 以及一个保存队列虚拟起始时间的替代状态变量计算得出。队列空闲且没有请求
// 正在执行时：其虚拟起始时间变量的值被忽略，其最后虚拟完成时间被视为处于
// 虚拟过去。当请求到达一个没有请求正在执行的空队列时，队列的虚拟起始时间
// 被设为当前虚拟时间。队列中第 J 个请求（从队首 J=1 开始计数）的虚拟完成
// 时间是 J * G +（队列的虚拟起始时间）。队列非空时：队列的最后虚拟完成时间
// 是队列中最后一个请求的虚拟完成时间。队列为空但有请求正在执行时：最后虚拟
// 完成时间是队列的虚拟起始时间。请求出队开始服务时，队列的虚拟起始时间前进
// G。请求服务结束、实际服务时间为 S 时，队列的虚拟起始时间回退 G - S。
// 换言之，处理请求时并非真的收发包，而是把“服务时间”折算进虚拟时钟：
// 每个请求按初始估计 G 参与排队，完成后再用实际耗时修正队列的虚拟起始时间。
// 关键不变量是“虚拟时间只增不减且所有队列以同一速率推进”，这样才能保证
// 公平性上界为 C 个请求；任何直接改写 request 的 finish time 的实现都会破坏它。
package queueset
