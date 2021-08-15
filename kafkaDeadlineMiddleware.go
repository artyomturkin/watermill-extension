package watermillext

import (
	"time"

	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
)

// KafkaDeadlineMiddleware short-circuits message processing without an error, if message was in kafka for longer, then duration provided
func KafkaDeadlineMiddleware(fromPublish time.Duration) func(message.HandlerFunc) message.HandlerFunc {
	return func(h message.HandlerFunc) message.HandlerFunc {
		meter := global.Meter("watermill")
		skipTracker := metric.Must(meter).NewInt64Counter("watermill_kafka_deadline_skipped")

		return func(msg *message.Message) ([]*message.Message, error) {
			ctx := msg.Context()
			_, sub, _ := buildAttrs(ctx)

			t, ok := kafka.MessageTimestampFromCtx(ctx)
			if ok {
				if time.Since(t) > fromPublish {
					meter.RecordBatch(
						ctx,
						sub,
						skipTracker.Measurement(1),
					)

					return []*message.Message{}, nil
				}
			}

			return h(msg)
		}
	}
}
