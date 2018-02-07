package kafka

import (
	"fmt"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
)

// returns elements that are in a but not in b
func DiffPartitions(a []int32, b []int32) []int32 {
	var diff []int32
Iter:
	for _, eA := range a {
		for _, eB := range b {
			if eA == eB {
				continue Iter
			}
		}
		diff = append(diff, eA)
	}
	return diff
}

func GetPartitions(metadata confluent.Metadata, topics []string) ([]int32, error) {
	partitionCount := 0
	partitions := make([]int32, 0)
	var ok bool
	var topicMetadata confluent.TopicMetadata
	for i, topic := range topics {
		if topicMetadata, ok = metadata.Topics[topic]; !ok || len(topicMetadata.Partitions) == 0 {
			return nil, fmt.Errorf("No partitions returned for topic %s", topic)
		}
		if i == 0 {
			for _, partitionMetadata := range topicMetadata.Partitions {
				partitions = append(partitions, partitionMetadata.ID)
			}
			partitionCount = len(partitions)
		} else {
			if len(partitions) != partitionCount {
				return nil, fmt.Errorf("Configured topics have different partition counts, this is not supported")
			}
		}
	}

	return partitions, nil
}
