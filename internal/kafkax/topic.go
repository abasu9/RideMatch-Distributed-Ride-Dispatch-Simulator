package kafkax

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

func EnsureTopic(ctx context.Context, brokers []string, topic string) error {
	if len(brokers) == 0 {
		return fmt.Errorf("kafkax: brokers is empty")
	}

	var lastErr error
	for attempt := 0; attempt < 30; attempt++ {
		conn, err := kafka.DialContext(ctx, "tcp", brokers[0])
		if err != nil {
			lastErr = err
			time.Sleep(600 * time.Millisecond)
			continue
		}

		controller, err := conn.Controller()
		if err != nil {
			_ = conn.Close()
			lastErr = err
			time.Sleep(600 * time.Millisecond)
			continue
		}
		_ = conn.Close()

		caddr := fmt.Sprintf("%s:%d", controller.Host, controller.Port)
		ctlConn, err := kafka.DialContext(ctx, "tcp", caddr)
		if err != nil {
			lastErr = err
			time.Sleep(600 * time.Millisecond)
			continue
		}

		err = ctlConn.CreateTopics(kafka.TopicConfig{
			Topic:             topic,
			NumPartitions:     6,
			ReplicationFactor: 1,
		})
		_ = ctlConn.Close()

		switch {
		case err == nil:
			return nil
		case strings.Contains(strings.ToLower(err.Error()), "already"):
			return nil
		default:
			lastErr = err
		}

		time.Sleep(600 * time.Millisecond)
	}

	return fmt.Errorf("kafkax: ensure topic %q: %w", topic, lastErr)
}
