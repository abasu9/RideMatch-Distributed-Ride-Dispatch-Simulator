package kafkax

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/segmentio/kafka-go"
)

type DriverLocationEvent struct {
	DriverID   string  `json:"driver_id"`
	Lat        float64 `json:"lat"`
	Lng        float64 `json:"lng"`
	H3Cell     string  `json:"h3_cell"`
	TsUnixNano int64   `json:"ts_unix_nano"`
}

type LocationPublisher struct {
	w *kafka.Writer
}

func NewLocationPublisher(brokers []string, topic string) *LocationPublisher {
	return &LocationPublisher{
		w: &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafka.Hash{},
			BatchTimeout: 5 * time.Millisecond,
			Async:        false,
			RequiredAcks: kafka.RequireOne,
		},
	}
}

func (p *LocationPublisher) Close() error {
	if p == nil || p.w == nil {
		return nil
	}
	return p.w.Close()
}

func (p *LocationPublisher) PublishDriverLocation(ctx context.Context, evt DriverLocationEvent) error {
	if p == nil || p.w == nil {
		return fmt.Errorf("kafkax: publisher is nil")
	}
	b, err := json.Marshal(evt)
	if err != nil {
		return err
	}
	return p.w.WriteMessages(ctx, kafka.Message{
		Key:   []byte(evt.DriverID),
		Value: b,
		Time:  time.Now().UTC(),
	})
}
