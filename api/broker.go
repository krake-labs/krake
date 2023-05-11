package api

import (
	"errors"
	"github.com/bat-labs/krake/internal"
	"hash/fnv"
	"log"
	"time"
)

type Broker interface {
	Produce(s string, msg *Message) error
	CreateTopic(configuration TopicConfiguration) error
}

type FilePool struct {
	// partition index -> file
	data map[int32]internal.File
}

func (f FilePool) Open(path string) internal.File {
	return internal.NewMemoryFile()
}

type PartitionWriter struct {
	filePool *FilePool
}

func NewPartitionWriter() *PartitionWriter {
	return &PartitionWriter{
		filePool: &FilePool{
			data: map[int32]internal.File{},
		},
	}
}

func (pw *PartitionWriter) Append(partition int32, p []byte) (n int, err error) {
	// for now this only handles writing to the latest segment in the partition
	seg := pw.ActiveSegment(partition)
	count, err := seg.Write(p)
	if err != nil {
		return count, err
	}
	if count != len(p) {
		panic("unhandled edgecase")
	}
	return count, nil
}

func (pw *PartitionWriter) ActiveSegment(partitionIndex int32) internal.File {
	seg, ok := pw.filePool.data[partitionIndex]
	if !ok {
		log.Printf("no such segment %d\n", partitionIndex)
		f := pw.filePool.Open("")
		pw.filePool.data[partitionIndex] = f
		return f
	}
	return seg
}

type TopicConfiguration struct {
	Name            string
	PartitionCount  int
	RetentionPeriod time.Duration
}

type KrakeBroker struct {
	writeStrategy *PartitionWriter
	topics        map[string]TopicConfiguration

	// used for round-robin partitioning
	currPartitionIndex int32
}

func NewKrakeBroker(writeStrategy *PartitionWriter) *KrakeBroker {
	return &KrakeBroker{
		writeStrategy: writeStrategy,
		topics:        map[string]TopicConfiguration{},
	}
}

func (k *KrakeBroker) partitionIndex(key []byte, partitionCount int) int32 {
	if len(key) == 0 {
		curr := k.currPartitionIndex
		k.currPartitionIndex++
		if k.currPartitionIndex >= int32(partitionCount) {
			k.currPartitionIndex = 0
		}
		return curr
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
	ErrNoSuchTopic        = errors.New("no such topic")
)

func (k *KrakeBroker) Produce(topic string, msg *Message) error {
	topicCfg, ok := k.topics[topic]
	if !ok {
		return ErrNoSuchTopic
	}

	partitionIdx := k.partitionIndex(msg.Key, topicCfg.PartitionCount)
	log.Println("partition index", partitionIdx)

	if partitionIdx == -1 {
		panic("unhandled error")
	}

	_, err := k.writeStrategy.Append(partitionIdx, msg.Message)
	if err != nil {
		return ErrWriteFailed
	}
	return nil
}

func (k *KrakeBroker) CreateTopic(cfg TopicConfiguration) error {
	if _, ok := k.topics[cfg.Name]; ok {
		return ErrTopicAlreadyExists
	}
	k.topics[cfg.Name] = cfg
	return nil
}
