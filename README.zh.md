# Kubernetes（K8s）

[![CII Best Practices](https://bestpractices.coreinfrastructure.org/projects/569/badge)](https://bestpractices.coreinfrastructure.org/projects/569) [![Go Report Card](https://goreportcard.com/badge/github.com/kubernetes/kubernetes)](https://goreportcard.com/report/github.com/kubernetes/kubernetes) ![GitHub release (latest SemVer)](https://img.shields.io/github/v/release/kubernetes/kubernetes?sort=semver)

<img src="https://github.com/kubernetes/kubernetes/raw/master/logo/logo.png" width="100">

----

Kubernetes，也称 K8s，是一个开源系统，用于跨多台主机管理[容器化应用]。它为应用的部署、维护和扩缩容提供了基础机制。

Kubernetes 建立在 Google 十五年来使用名为 [Borg] 的系统大规模运行生产负载的经验之上，并结合了社区中最佳实践的思想与实践。

Kubernetes 由云原生计算基金会（[CNCF]）托管。如果你的公司希望参与塑造容器化封装、动态调度、面向微服务的技术演进，可以考虑加入
CNCF。关于有哪些参与者以及 Kubernetes 扮演的角色，请阅读 CNCF 的[公告]。

----

## 开始使用 K8s

请参阅我们在 [kubernetes.io] 上的文档。

可以在 [Scalable Microservices with Kubernetes] 上学习免费课程。

若要将 Kubernetes 代码作为库用于其他应用，请参阅[已发布组件列表](https://git.k8s.io/kubernetes/staging/README.md)。不支持将
`k8s.io/kubernetes` 模块或 `k8s.io/kubernetes/...` 包作为库使用。

## 开始开发 K8s

[社区仓库]包含所有相关信息：如何从源码构建 Kubernetes、如何贡献代码和文档、该找谁解决什么问题等。

如果你想立即构建 Kubernetes，有两种选择：

##### 你已有可用的 [Go 环境]。

```
git clone https://github.com/kubernetes/kubernetes
cd kubernetes
make
```

##### 你已有可用的 [Docker 环境]。

```
git clone https://github.com/kubernetes/kubernetes
cd kubernetes
make quick-release
```

完整说明请前往[开发者文档]。

## 支持

如果需要支持，请先查阅[故障排查指南]，并按照我们列出的流程逐步处理。

尽管如此，如果你有问题，可以通过[某种方式][communication]联系我们。

[announcement]: https://cncf.io/news/announcement/2015/07/new-cloud-native-computing-foundation-drive-alignment-among-container
[Borg]: https://research.google.com/pubs/pub43438.html?authuser=1
[CNCF]: https://www.cncf.io/about
[communication]: https://git.k8s.io/community/communication
[community repository]: https://git.k8s.io/community
[containerized applications]: https://kubernetes.io/docs/concepts/overview/what-is-kubernetes/
[developer's documentation]: https://git.k8s.io/community/contributors/devel#readme
[Docker environment]: https://docs.docker.com/engine
[Go environment]: https://go.dev/doc/install
[kubernetes.io]: https://kubernetes.io
[Scalable Microservices with Kubernetes]: https://www.udacity.com/course/scalable-microservices-with-kubernetes--ud615
[troubleshooting guide]: https://kubernetes.io/docs/tasks/debug/

## 社区会议

[日历](https://www.kubernetes.dev/resources/calendar/)在一个地方列出了 Kubernetes 社区的所有会议。

## 采用者

[用户案例](https://kubernetes.io/case-studies/)网站收录了各行各业组织正在部署或迁移到 Kubernetes 的真实用例。

## 治理

Kubernetes 项目由一套原则、价值观、政策和流程框架治理，帮助我们的社区和相关方朝着共同目标前进。

[Kubernetes 社区](https://github.com/kubernetes/community/blob/master/governance.md)是了解我们如何组织自身的起点。

[Kubernetes Steering 社区仓库](https://github.com/kubernetes/steering)由 Kubernetes 指导委员会使用，该委员会负责监督
Kubernetes 项目的治理。

## 路线图

[Kubernetes Enhancements 仓库](https://github.com/kubernetes/enhancements)提供有关 Kubernetes 版本发布、功能跟踪和待办事项的信息。
