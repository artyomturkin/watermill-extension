# Watermill Extension

[watermill.io](https://watermill.io) extensions and middleware

## OpenTelemetryMiddleware

OpenTelemetry middlewre to track metrics and inject tracinf spans into message context.

Metrics tracked:

|Metric                           |Type     |
|---------------------------------|---------|
|watermill_kafka_subscriber_offset|counter  |
|watermill_kafka_subscriber_lag_ms|histogram|
|watermill_kafka_publish_ms       |histogram|
|watermill_processing_ms          |histogram|

## KafkaDeadlineMiddleware

Middleware to ignore message from kafka older then duration specified.
