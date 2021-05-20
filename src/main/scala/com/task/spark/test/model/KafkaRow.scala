package com.task.spark.test.model

import java.sql.Timestamp

case class KafkaRow(topic: String,
                    partition: Int,
                    offset: Long,
                    key: Array[Byte],
                    value: Array[Byte],
                    timestamp: Timestamp,
                    timestampType: Int)