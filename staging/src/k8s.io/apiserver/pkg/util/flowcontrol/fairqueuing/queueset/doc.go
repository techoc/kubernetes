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

// Package queueset implements a technique called "fair queuing for
// server requests".  One QueueSet is a set of queues operating
// according to this technique.
//
// Fair queuing for server requests is inspired by the fair queuing
// technique from the world of networking.  You can find a good paper
// on that at https://dl.acm.org/citation.cfm?doid=75247.75248 or
// http://people.csail.mit.edu/imcgraw/links/research/pubs/networks/WFQ.pdf
// and there is an implementation outline in the Wikipedia article at
// https://en.wikipedia.org/wiki/Fair_queuing .
//
// Fair queuing for server requests differs from traditional fair
// queuing in three ways: (1) we are dispatching application layer
// requests to a server rather than transmitting packets on a network
// link, (2) multiple requests can be executing at once, and (3) the
// service time (execution duration) is not known until the execution
// completes.
//
// The first two differences can easily be handled by straightforward
// adaptation of the concept called "R(t)" in the original paper and
// "virtual time" in the implementation outline. In that
// implementation outline, the notation now() is used to mean reading
// the virtual clock. In the original paper’s terms, "R(t)" is the
// number of "rounds" that have been completed at real time t ---
// where a round consists of virtually transmitting one bit from every
// non-empty queue in the router (regardless of which queue holds the
// packet that is really being transmitted at the moment); in this
// conception, a packet is considered to be "in" its queue until the
// packet’s transmission is finished. For our problem, we can define a
// round to be giving one nanosecond of CPU to every non-empty queue
// in the apiserver (where emptiness is judged based on both queued
// and executing requests from that queue), and define R(t) = (server
// start time) + (1 ns) * (number of rounds since server start). Let
// us write NEQ(t) for that number of non-empty queues in the
// apiserver at time t.  Let us also write C for the concurrency
// limit.  In the original paper, the partial derivative of R(t) with
// respect to t is
//
//	1 / NEQ(t) .
//
// To generalize from transmitting one packet at a time to executing C
// requests at a time, that derivative becomes
//
//	C / NEQ(t) .
//
// However, sometimes there are fewer than C requests available to
// execute.  For a given queue "q", let us also write "reqs(q, t)" for
// the number of requests of that queue that are executing at that
// time.  The total number of requests executing is sum[over q]
// reqs(q, t) and if that is less than C then virtual time is not
// advancing as fast as it would if all C seats were occupied; in this
// case the numerator of the quotient in that derivative should be
// adjusted proportionally.  Putting it all together for fair queing
// for server requests: at a particular time t, the partial derivative
// of R(t) with respect to t is
//
//	min( C, sum[over q] reqs(q, t) ) / NEQ(t) .
//
// In terms of the implementation outline, this is the rate at which
// virtual time is advancing at time t (in virtual nanoseconds per
// real nanosecond). Where the networking implementation outline adds
// packet size to a virtual time, in our version this corresponds to
// adding a service time (i.e., duration) to virtual time.
//
// The third difference is handled by modifying the algorithm to
// dispatch based on an initial guess at the request’s service time
// (duration) and then make the corresponding adjustments once the
// request’s actual service time is known. This is similar, although
// not exactly isomorphic, to the original paper’s adjustment by
// `$\delta$` for the sake of promptness.
//
// For implementation simplicity (see below), let us use the same
// initial service time guess for every request; call that duration
// G. A good choice might be the service time limit (1
// minute). Different guesses will give slightly different dynamics,
// but any positive number can be used for G without ruining the
// long-term behavior.
//
// As in ordinary fair queuing, there is a bound on divergence from
// the ideal. In plain fair queuing the bound is one packet; in our
// version it is C requests.
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
