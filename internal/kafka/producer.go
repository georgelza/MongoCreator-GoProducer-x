package kafka

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde"
	"github.com/confluentinc/confluent-kafka-go/schemaregistry/serde/protobuf"
	"google.golang.org/protobuf/proto"
)

const (
	nullOffset = -1
)

// SRProducer interface
type SRProducer interface {
	ProduceMessage(msg proto.Message, topic string, key string) (int64, error)
	Close()
	Flush(t int) int
}

type srProducer struct {
	producer   *kafka.Producer
	serializer serde.Serializer
}

// NewProducer returns kafka producer with schema registry
func NewProducer(cm kafka.ConfigMap, srURL string) (SRProducer, error) {

	p, err := kafka.NewProducer(&cm)
	if err != nil {
		return nil, err
	}

	c, err := schemaregistry.NewClient(schemaregistry.NewConfig(srURL))
	if err != nil {
		return nil, err
	}

	s, err := protobuf.NewSerializer(c, serde.ValueSerde, protobuf.NewSerializerConfig())
	if err != nil {
		return nil, err
	}

	return &srProducer{
		producer:   p,
		serializer: s,
	}, nil
}

// ProduceMessage sends serialized message to kafka using schema registry
func (p *srProducer) ProduceMessage(msg proto.Message, topic string, key string) (int64, error) {

	kafkaChan := make(chan kafka.Event)
	defer close(kafkaChan)

	payload, err := p.serializer.Serialize(topic, msg)

	if err != nil {
		fmt.Println("p.serializer.Serialize(topic, msg)")
		return nullOffset, err
	}

	if err = p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic},
		Value:          payload,
		Key:            []byte(key),
	}, kafkaChan); err != nil {
		fmt.Println("p.producer.Produce(&kafka.Message")

		return nullOffset, err
	}

	e := <-kafkaChan
	switch ev := e.(type) {
	case *kafka.Message:
		return int64(ev.TopicPartition.Offset), nil

	case kafka.Error:
		return nullOffset, err

	}

	return nullOffset, nil
}

// Close schema registry and Kafka
func (p *srProducer) Close() {
	p.serializer.Close()
	p.producer.Close()
}

// Close schema registry and Kafka
func (p *srProducer) Flush(t int) int {
	return p.producer.Flush(t)

}
