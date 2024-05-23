# kafka-consumer-demo

## 1 简介

本项目是一个 Kafka 消费者的 Demo，主要用于演示如何使用 Apache Pool2 来管理 Kafka 消费者的连接，避免频繁创建和销毁 Kafka 消费者对象。

### 1.1 PooledKafkaConsumerFactory

`PooledKafkaConsumerFactory` 是一个 Kafka 消费者工厂类，继承 BasePooledObjectFactory，用于创建和销毁池中的 Kafka 消费者对象，以及验证 Kafka 消费者对象是否有效。根据`application.properties`配置文件中的`consumer.pool.size`属性，创建一个 Kafka 消费者池，同时根据`application.properties`配置文件中的 Spring Kafka 属性，装配 Kafka 消费者对象。
示例配置如下：

```properties
spring.kafka.bootstrap-servers=localhost:9092
spring.kafka.consumer.group-id=1
spring.kafka.consumer.auto-offset-reset=earliest
spring.kafka.consumer.enable-auto-commit=false
spring.kafka.consumer.max-poll-records=3
```

### 1.2 KafkaConsumerPool

`KafkaConsumerPool` 是一个 Kafka 消费者连接池类，用于管理 Kafka 消费者对象。通过`PooledKafkaConsumerFactory`创建 Kafka 消费者对象，并提供获取和释放 Kafka 消费者对象的方法：

`borrowObject`: 从池中获取一个 Kafka 消费者对象，从 paused 状态恢复，并对齐消费者的 offset 到最后一个提交的 offset。

`returnObject`: 将一个 Kafka 消费者对象返回到池中，暂停消费者对象。

### 1.3 KafkaConsumerService

`KafkaConsumerService` 是一个 Kafka 消费者服务类，用于消费 Kafka 消息。通过`KafkaConsumerPool`获取 Kafka 消费者对象，消费 Kafka 消息，手动同步 offset 到 Kafka。并返回消息内容。

### 1.4 TaskController

`TaskController` 用于接收 HTTP 请求，调用`KafkaConsumerService`消费 Kafka 消息，并返回消息内容。

## 2 项目结构

```shell
.
├── README.md
├── pom.xml
├── src
    └── main
        ├── java
        │   └── org
        │       └── demo
        │           └── kafkaconsumer
        │               ├── config
        │               │   └── PooledKafkaConsumerFactory.java
        │               ├── controller
        │               │   └── TaskController.java
        │               ├── service
        │               │   └── KafkaConsumerService.java
        │               ├── pool
        │               │   └── KafkaConsumerPool.java
        │               └── KafkaConsumerApplication.java
        └── resources
            └── application.properties
├── docker-compose.yml # Kafka 集群的 Docker Compose 配置文件
├── pom.xml
└── README.md
```

## 3 使用说明

### 3.1 启动 Kafka 集群

```shell
docker-compose up -d
```

### 3.2 启动项目

```shell
mvn spring-boot:run
```

### 3.3 测试

```shell
curl http://localhost/getContent?type=common
```

## 4 参考

-   [Apache Pool2](https://commons.apache.org/proper/commons-pool/)
-   [Spring Kafka](https://docs.spring.io/spring-kafka/reference/index.html)
-   [Apache Kafka](https://kafka.apachecn.org/3/#offsetflushintervalms)
