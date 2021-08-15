package watermillext

import (
	"context"
	"fmt"
	"time"

	"github.com/ThreeDotsLabs/watermill-kafka/v2/pkg/kafka"
	"github.com/ThreeDotsLabs/watermill/message"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/trace"
)

// OpenTelemetryMiddleware collects metrics and injects tracing data into context
// If trace propagation is configured will extract or inject tracing data into metadata for watermill to pass it on the wire,
// If metrics are enabled collects them
func OpenTelemetryMiddleware(h message.HandlerFunc) message.HandlerFunc {
	// Setup tracing
	tr := otel.Tracer("watermill")

	// Setup metrics
	meter := global.Meter("watermill")
	offsetTracker := metric.Must(meter).NewInt64UpDownCounter("watermill_kafka_subscriber_offset")
	subLagTracker := metric.Must(meter).NewInt64ValueRecorder("watermill_kafka_subscriber_lag_ms")
	publishTracker := metric.Must(meter).NewInt64ValueRecorder("watermill_kafka_publish_ms")
	processingTracker := metric.Must(meter).NewInt64ValueRecorder("watermill_processing_ms")

	return func(msg *message.Message) ([]*message.Message, error) {
		ctx := msg.Context()
		proAttr, subAttr, pubAttr := buildAttrs(ctx)

		// Measure offsets and lag from kafka write time
		o, ok := kafka.MessagePartitionOffsetFromCtx(ctx)
		if ok {
			meter.RecordBatch(
				ctx,
				subAttr,
				offsetTracker.Measurement(o),
			)
		}

		t, ok := kafka.MessageTimestampFromCtx(ctx)
		if ok {
			meter.RecordBatch(
				ctx,
				subAttr,
				subLagTracker.Measurement(time.Since(t).Milliseconds()),
			)
		}

		// Extract open telemetry data form metadata, create new span and propagate it
		ctx = otel.GetTextMapPropagator().Extract(ctx, metadataCarrier(msg.Metadata))
		ctx, span := tr.Start(ctx, "process message", trace.WithSpanKind(trace.SpanKindConsumer))
		span.SetAttributes(subAttr...)
		defer span.End()
		msg.SetContext(ctx)

		// Call next handler and measure execution time
		processingStart := time.Now()
		producedMessages, err := h(msg)
		meter.RecordBatch(
			ctx,
			proAttr,
			processingTracker.Measurement(time.Since(processingStart).Milliseconds()),
		)

		// Inject open telemetry data to metadata
		for _, m := range producedMessages {
			ctx, span := tr.Start(m.Context(), "publish messages", trace.WithSpanKind(trace.SpanKindProducer))
			span.SetAttributes(pubAttr...)

			// Track processing complete
			publishStart := time.Now()
			go func(msg *message.Message, publishStart time.Time) {
				select {
				case <-msg.Acked():
				case <-msg.Nacked():
					span.SetStatus(codes.Error, "nacked")
					span.RecordError(fmt.Errorf("message nacked"))
				}

				span.End()
				meter.RecordBatch(
					ctx,
					pubAttr,
					publishTracker.Measurement(time.Since(publishStart).Milliseconds()),
				)
			}(m, publishStart)

			otel.GetTextMapPropagator().Inject(ctx, metadataCarrier(m.Metadata))
		}

		if err != nil {
			// Mark span as error
			span.SetStatus(codes.Error, "handler failed to process message")
			span.RecordError(err)
		}

		return producedMessages, err
	}
}

// buildAttrs build otel attributes from watermill context data
func buildAttrs(ctx context.Context) (processor, subscriber, publisher []attribute.KeyValue) {
	handler := attribute.String("watermill_handler", message.HandlerNameFromCtx(ctx))

	proAttrs := []attribute.KeyValue{handler}
	subAttrs := []attribute.KeyValue{handler, attribute.String("kafka_topic", message.SubscribeTopicFromCtx(ctx))}
	pubAttrs := []attribute.KeyValue{handler, attribute.String("kafka_topic", message.PublishTopicFromCtx(ctx))}

	p, pok := kafka.MessagePartitionFromCtx(ctx)
	if pok {
		subAttrs = append(subAttrs, attribute.Int("kafka_partition", int(p)))
	}

	return proAttrs, subAttrs, pubAttrs
}

// metadataCarrier for open telemetry propagation
type metadataCarrier message.Metadata

func (m metadataCarrier) Keys() []string {
	keys := make([]string, len(m))

	i := 0
	for key := range m {
		keys[i] = key
		i++
	}

	return keys
}

func (m metadataCarrier) Get(key string) string {
	if v, ok := m[key]; ok {
		return v
	}

	return ""
}

func (m metadataCarrier) Set(key, value string) {
	m[key] = value
}
