### 使用说明
将编译好的jar包丢到kafka connect的classpath下面
### 配置

#### com.gaopeng.kafka.connect.transform.EnhancedTimestampRouter

用于根据value中的某个时间字段来格式化时间，并配置到topic上面进行路由
```
"transforms": "topic",
"transforms.topic.type": "com.gaopeng.kafka.connect.transform.EnhancedTimestampRouter",
"transforms.topic.timestamp.default.field": "created_at",
"transforms.topic.timestamp.format": "yyyyMMdd",
"transforms.topic.timestamp.field.map": "mysql.test.test1:creation_time,mysql.test.test2:creation_time"
"transforms.topic.topics.ignore": "mysql.test.test"
"transforms.topic.topic.format": "${topic}.${timestamp}",
```

#### com.gaopeng.kafka.connect.transform.FieldRouter

用于根据value中的某个字段来进行计算后，配置到topic上面进行路由
```
"transforms": "topic",
"transforms.topic.type": "com.gaopeng.kafka.connect.transform.FieldRouter",
"transforms.topic.default.field": "user_id",
"transforms.topic.value.expression": "string.substring(value,string.length(value)-1)",
"transforms.topic.topics.field.map": "mysql.test.test1:job_id,mysql.test.test2:task_id"
"transforms.topic.topics.ignore": "mysql.test.test"
"transforms.topic.topic.format": "${topic}.${timestamp}",
```