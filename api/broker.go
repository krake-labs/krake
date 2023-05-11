package api

import (
	"errors"
	"hash/fnv"
	"io"
	"log"
	"time"
)

type Broker interface {
	Produce(s string, msg *Message) error
}

type WriteStrategy interface {
	io.Writer
	Write(p []byte) (n int, err error)
}

type topicConfiguration struct {
	Name            string
	PartitionCount  int
	RetentionPeriod time.Duration
}

type KrakeBroker struct {
	writeStrategy WriteStrategy
	topics        map[string]topicConfiguration

	// used for round-robin partitioning
	lastPartitionIndex int32
}

func NewKrakeBroker(writeStrategy WriteStrategy) *KrakeBroker {
	return &KrakeBroker{
		writeStrategy: writeStrategy,
		topics:        map[string]topicConfiguration{},
	}
}

func (k KrakeBroker) partitionIndex(key []byte) int32 {
	if len(key) == 0 {
		// FIXME(FELIX): this needs to work
		// for now it writes to index 0
		return k.lastPartitionIndex
	}

	hash := fnv.New32a()
	_, err := hash.Write(key)
	if err != nil {
		log.Println("failed to compute partition index")
		return -1
	}
	return int32(hash.Sum32())
}

var (
	ErrWriteFailed        = errors.New("failed to write bytes")
	ErrTopicAlreadyExists = errors.New("topic already exists")
)

func (k KrakeBroker) Produce(topic string, msg *Message) error {
	_, err := k.writeStrategy.Write(msg.Message)
	if err != nil {
		return ErrWriteFailed
	}
	return nil
}

func (k KrakeBroker) CreateTopic(cfg topicConfiguration) error {
	if _, ok := k.topics[cfg.Name]; ok {
		return ErrTopicAlreadyExists
	}
	k.topics[cfg.Name] = cfg
	return nil
}
