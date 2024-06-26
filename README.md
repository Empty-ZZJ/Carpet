## Carpet
Carpet是花濑基于Garnet推出的新型缓存存储，具有以下几个独特优势：
- Carpet采用流行的RESP网络协议作为起点，这使得可以使用大多数现代编程语言中未修改的Redis客户端（如C#中的StackExchange.Redis）来访问Carpet。
- 相对于可比较的开源缓存存储，Carpet在许多客户端连接和小批量处理方面提供了更好的吞吐量和可扩展性，从而为大型应用程序和服务节省了成本。
- 基于最新的.NET技术，Carpet是跨平台的、可扩展的和现代的。它旨在易于开发和演变，同时在不牺牲常见用例性能的情况下，利用.NET丰富的库生态系统实现API广度，并为优化提供了开放机会。得益于我们对.NET的谨慎使用，Carpet在Linux和Windows上都实现了最先进的性能。

## 功能摘要
Carpet实现了包括原始字符串（例如，获取、设置和键过期）、分析（例如，HyperLogLog和Bitmap）和对象（例如，排序集和列表）操作在内的广泛API。它可以处理客户端端RESP事务和我们在C#中使用的服务器端存储过程形式的多键事务，并允许用户定义对原始字符串和新对象类型的自定义操作，所有这些都在C#的便利性和安全性中完成，降低了开发自定义扩展的门槛。


## 集群模式
除了单节点执行外，Carpet还支持集群模式，允许用户创建和管理分片和复制的部署。Carpet还支持高效的动态键迁移方案以重新平衡分片。用户可以使用标准的Redis集群命令来创建和管理Carpet集群，节点执行八卦以共享和演变集群状态。集群仍在进行中。