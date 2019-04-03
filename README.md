#### 使用说明
将编译好的jar包丢到kafka connect的classpath下面
#### 配置
```
"transforms": "topic",
"transforms.topic.type": "com.gaopeng.kafka.connect.transform.EnhancedTimestampRouter",
"transforms.topic.timestamp.default.field": "created_at",
"transforms.topic.timestamp.format": "yyyyMMdd",
"transforms.topic.timestamp.field.map": "mysql.test.test1:creation_time,mysql.test.test2:creation_time"
"transforms.topic.topics.ignore": "mysql.test.test"
"transforms.topic.topic.format": "${topic}.${timestamp}",
```