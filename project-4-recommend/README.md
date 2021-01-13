# Recommendation System On Flink
## 1. 项目简介
### 1.1 系统架构
系统主要工作流程：

- 用户登录/注册系统。
- 用户对商品进行评分。
- 评分数据通过 Kafka 发送到推荐模块的实时推荐任务中。
- 系统执行实时推荐任务，并且将数据存储到 hbase 的 rating 和 userProduct 表中。实时任务包括：实时 topN 以及 基于用户行为推荐。
- 实时 topN 将计算结果存储到 hbase 的 onlineHot 表中，基于用户行为推荐将计算结果存储到 hbase 的表 onlineRecommend 中。

- web 端通过查询 hbase 获取相关模块所需数据并展示结果。

### 2.1 首页

共有四个模块：

- 猜你喜欢：基于用户行为推荐，当用户对商品进行评分时，Flink  根据用户历史评分商品，结合 itemCF 计算推荐结果。
- 热门商品：历史热门商品
- 好评商品：评分较高的商品
- 实时热门商品： 使用 Flink 时间滑动窗口，对过去一个小时热门商品进行统计，每 5 分钟滑动一次。

### 2.2 商品详情

- 展示商品详细信息

- 看过该商品的人还看了：基于 itemCF 进行推荐

### 2.3 登录

## 3. 模块说明

### 3.1 推荐模块 (recommendation)

**开发环境：** IDEA + Maven + git + windows && wsl

**软件架构：**flink + hbase + kafka + mysql + redis

**开发指导：** flink 的计算任务都存放在 task 包下，DataLoader 为加载数据任务，OfflineRecommender 为离线推荐任务， OnlineRecommender 为实时推荐任务。以模块为单位阅读代码。

#### 3.1.1 猜你喜欢

实时推荐：

- 从 redis 中查询用户最近评分商品列表 ， redis key 为 `“ONLINE_PREFIX_” + userId`
- 从 hbase 表 `userProduct` 中查询用户历史评分商品列表。
- 根据用户刚评分的 `productId` 从 hbase 表 `itemCFRecommend` 表中查询相关的商品列表
- 对相关商品列表根据之前查出的最近评分商品列表和历史评分商品列表过滤。
- 根据最近评分商品与本次商品的相似度以及用户历史评分对推荐商品重新排序。

#### 3.1.2 热门商品

对所有时间用户评分的商品根据评分次数进行逆序排序，选出热门商品。

- flink 将  hbase  `rating` 表加载到内存中，根据 productId group，并且统计出现次数
- 根据出现次数逆序排序。

#### 3.1.3 好评商品

根据商品评分均分逆序排序，

#### 3.1.4 实时热门商品

采用 flink  `timeWindow`  对过去一个小时的数据进行排序，选出热门的商品。时间窗口每五分钟滑动一次。

#### 3.1.5 看过该商品的人还看了

基于物品推荐 （itemCF）

#### 3.1.6 数据装载模块

消费 kafka topic 为 `rating` 的数据，并且将数据存储到 hbase `rating` 表中，为了保证数据的唯一性`rowKey` 格式为：`userId_productId_timestamp`

### 3.2 推荐后端 (recommend_backend)

**开发环境：** IDEA + Maven + git + windows && wsl（ubuntu 20.4）+ postwomen

**技术架构：** Springboot + hibernate + mysql + hbase

**开发指导：** Controller 模块是后端的核心，从 restFul api 入手。

**项目架构：**

![backend.png](https://i.loli.net/2020/08/29/5z2i3LNWjaoEpxm.png)

### 3.3 推荐前端 (recommend_front)

**开发环境：** VScode + nodejs + windows && wsl

**技术架构：** Vue + typescript + element-ui

## 4. 开发运行步骤
### 4.1 环境搭建

- mysql
- hbase
- flink
- redis
- kafka
- zookeeper

### 4.2 创建数据表

- mysql

共有两张表，一个是 `product` 用于存储商品的详细信息，另一个是 `userEntity` 用于存储用户信息。

建表 sql 脚本在 `recommendation/src/main/resources/mysql.sql` 中

- hbase
  - rating
  - userProduct
  - itemCFRecommend
  - goodProducts
  - historyHotProducts
  - onlineRecommend
  - onlineHot

建表语句在 `recommendation/src/main/resources/hbase.txt` 中

### 4.3 数据入库

商品信息存储在`recommendation/src/main/resources/product.csv` 文件里，我们运行一个 flink 任务将数据装载到 mysql 中。对应的表是我们之前创建的 `product` 表

- 启动 flink ,运行 `recommendation/.../task/DataLoader/DataLoaderTask.java`
- 商品信息存储到 mysql 中

- 启动 springboot 后端项目
- 启动 vue 前端
- 启动实时推荐任务
- 离线任务定时启动