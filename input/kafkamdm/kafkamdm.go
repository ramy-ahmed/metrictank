package kafkamdm

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"

	confluent "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/input"
	"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/stats"
	"gopkg.in/raintank/schema.v1"
)

// metric input.kafka-mdm.metrics_per_message is how many metrics per message were seen.
var metricsPerMessage = stats.NewMeter32("input.kafka-mdm.metrics_per_message", false)

// metric input.kafka-mdm.metrics_decode_err is a count of times an input message failed to parse
var metricsDecodeErr = stats.NewCounter32("input.kafka-mdm.metrics_decode_err")

const OffsetUndefined = -100

type KafkaMdm struct {
	input.Handler
	consumer   confluent.Consumer
	lagMonitor *LagMonitor
	wg         sync.WaitGroup

	// signal to PartitionConsumers to shutdown
	stopConsuming chan struct{}
	// signal to caller that it should shutdown
	fatal chan struct{}
}

func (k *KafkaMdm) Name() string {
	return "kafka-mdm"
}

var LogLevel int
var Enabled bool
var brokerStr string
var brokers []string
var topicStr string
var topics []string
var partitionStr string
var partitions []int32
var offsetStr string
var DataDir string
var config confluent.ConfigMap
var channelBufferSize int
var consumerFetchMin int
var consumerFetchDefault int
var consumerMaxWaitTime time.Duration
var consumerMetadataTimeout time.Duration
var netMaxOpenRequests int
var offsetMgr *kafka.OffsetMgr
var offsetDuration time.Duration
var offsetCommitInterval time.Duration
var partitionOffset map[int32]*stats.Gauge64
var partitionLogSize map[int32]*stats.Gauge64
var partitionLag map[int32]*stats.Gauge64

func ConfigSetup() {
	inKafkaMdm := flag.NewFlagSet("kafka-mdm-in", flag.ExitOnError)
	inKafkaMdm.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdm.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&topicStr, "topics", "mdm", "kafka topic (may be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&offsetStr, "offset", "last", "Set the offset to start consuming from. Can be one of newest, oldest,last or a time duration")
	inKafkaMdm.StringVar(&partitionStr, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's")
	inKafkaMdm.DurationVar(&offsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	inKafkaMdm.StringVar(&DataDir, "data-dir", "", "Directory to store partition offsets index")
	inKafkaMdm.IntVar(&channelBufferSize, "channel-buffer-size", 1000000, "Maximum number of messages allowed on the producer queue")
	inKafkaMdm.IntVar(&consumerFetchMin, "consumer-fetch-min", 1, "Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting")
	inKafkaMdm.DurationVar(&consumerMaxWaitTime, "consumer-max-wait-time", time.Second, "Maximum time the broker may wait to fill the response with fetch.min.bytes")
	inKafkaMdm.DurationVar(&consumerMetadataTimeout, "consumer-metadata-timeout", time.Second*10, "Maximum time to wait for the broker to send its metadata")
	inKafkaMdm.IntVar(&netMaxOpenRequests, "net-max-open-requests", 100, "Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests.")
	globalconf.Register("kafka-mdm-in", inKafkaMdm)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}

	if offsetCommitInterval == 0 {
		log.Fatal(4, "kafkamdm: offset-commit-interval must be greater then 0")
	}
	if consumerMaxWaitTime == 0 {
		log.Fatal(4, "kafkamdm: consumer-max-wait-time must be greater then 0")
	}
	var err error
	switch offsetStr {
	case "last":
	case "oldest":
	case "newest":
	default:
		offsetDuration, err = time.ParseDuration(offsetStr)
		if err != nil {
			log.Fatal(4, "kafkamdm: invalid offest format. %s", err)
		}
	}

	offsetMgr, err = kafka.NewOffsetMgr(DataDir)
	if err != nil {
		log.Fatal(4, "kafka-mdm couldnt create offsetMgr. %s", err)
	}
	brokers = strings.Split(brokerStr, ",")
	topics = strings.Split(topicStr, ",")

	config = confluent.ConfigMap{}

	config.SetKey("client.id", instance+"-mdm")
	config.SetKey("queue.buffering.max.messages", channelBufferSize)
	config.SetKey("fetch.min.bytes", consumerFetchMin)
	config.SetKey("fetch.wait.max.ms", consumerMaxWaitTime)
	config.SetKey("max.in.flight.requests.per.connection", netMaxOpenRequests)
	client, err = kafka.NewConsumer(&config)
	if err != nil {
		log.Fatal(4, "failed to initialize kafka client. %s", err)
	}

	metadata := client.GetMetadata(nil, true, consumerMetadataTimeout)
	availParts, err := kafka.GetPartitions(metadata, topics)
	if err != nil {
		log.Fatal(4, "kafka-mdm: %s", err.Error())
	}
	log.Info("kafka-mdm: available partitions %v", availParts)
	if partitionStr == "*" {
		partitions = availParts
	} else {
		parts := strings.Split(partitionStr, ",")
		for _, part := range parts {
			i, err := strconv.Atoi(part)
			if err != nil {
				log.Fatal(4, "could not parse partition %q. partitions must be '*' or a comma separated list of id's", part)
			}
			partitions = append(partitions, int32(i))
		}
		missing := kafka.DiffPartitions(partitions, availParts)
		if len(missing) > 0 {
			log.Fatal(4, "kafka-mdm: configured partitions not in list of available partitions. missing %v", missing)
		}
	}
	// record our partitions so others (MetricIdx) can use the partitioning information.
	// but only if the manager has been created (e.g. in metrictank), not when this input plugin is used in other contexts
	if cluster.Manager != nil {
		cluster.Manager.SetPartitions(partitions)
	}

	// initialize our offset metrics
	partitionOffset = make(map[int32]*stats.Gauge64)
	partitionLogSize = make(map[int32]*stats.Gauge64)
	partitionLag = make(map[int32]*stats.Gauge64)
	for _, part := range partitions {
		partitionOffset[part] = stats.NewGauge64(fmt.Sprintf("input.kafka-mdm.partition.%d.offset", part))
		partitionLogSize[part] = stats.NewGauge64(fmt.Sprintf("input.kafka-mdm.partition.%d.log_size", part))
		partitionLag[part] = stats.NewGauge64(fmt.Sprintf("input.kafka-mdm.partition.%d.lag", part))
	}
}

func New() *KafkaMdm {
	consumer, err := kafka.NewConsumer(&config)
	if err != nil {
		log.Fatal(4, "failed to initialize kafka consumer. %s", err)
	}
	log.Info("kafka-mdm consumer created without error")
	k := KafkaMdm{
		consumer:      consumer,
		lagMonitor:    NewLagMonitor(10, partitions),
		stopConsuming: make(chan struct{}),
	}

	return &k
}

func (k *KafkaMdm) startConsumer() error {
	var offset int64
	var err error
	partitions := make([]TopicPartition, 0, len(topics)*len(partitions))

	for _, topic := range topics {
		for _, partition := range partitions {
			switch offsetStr {
			case "oldest":
				offset = confluent.OffsetBeginning
			case "newest":
				offset = confluent.OffsetEnd
			case "last":
				offset, err = offsetMgr.Last(topic, partition)
			default:
				offset, err = k.tryGetOffset(topic, partition, confluent.OffsetStored, time.Now().Add(-1*offsetDuration).UnixNano()/int64(time.Millisecond))
			}

			if err != nil {
				return err
			}

			partitions = append(partitions, confluent.TopicPartitions{
				Topic:     &topic,
				Partition: partition,
				Offset:    offset,
			})
		}
	}

	err = k.consumer.SubscribeTopics(topics, nil)
	if err != nil {
		return err
	}
	err = k.consumer.Assign(partitions)
	if err != nil {
		return err
	}
}

func (k *KafkaMdm) Start(handler input.Handler, fatal chan struct{}) error {
	k.Handler = handler
	k.fatal = fatal

	err := k.startConsumer()
	if err != nil {
		log.Error(4, "kafka-mdm: Failed to start consumer: %q", err)
		return err
	}

	for i := 0; i < len(topics)*len(partitions); i++ {
		k.wg.Add(1)
		go k.consume()
	}

	/*for _, topic := range topics {
		for _, partition := range partitions {
			var offset int64
			switch offsetStr {
			case "oldest":
				offset = confluent.OffsetBeginning
			case "newest":
				offset = confluent.OffsetEnd
			case "last":
				offset, err = offsetMgr.Last(topic, partition)
			default:
				offset, err = k.tryGetOffset(topic, partition, confluent.OffsetStored, time.Now().Add(-1*offsetDuration).UnixNano()/int64(time.Millisecond))
			}
			if err != nil {
				log.Error(4, "kafka-mdm: Failed to get %q duration offset for %s:%d. %q", offsetStr, topic, partition, err)
				return err
			}
		}
	}
	return nil*/
}

// tryGetOffset will to query kafka repeatedly for the requested offset and give up after attempts unsuccesfull attempts.
// - if the given offset is <0 (f.e. confluent.OffsetStored) then the first & second returned values will be the
//   oldest & newest offset for the given topic and partition.
// - if it is >=0 then the first returned value is the earliest offset whose timestamp is greater than or equal to the
//   given timestamp and the second returned value can be ignored.
func (k *KafkaMdm) tryGetOffset(topic string, partition int32, offset int64, attempts int, sleep time.Duration) (int64, int64, error) {

	var val1, val2 int64
	var err error
	var offsetStr string

	switch offset {
	case confluent.OffsetEnd:
		offsetStr = "newest"
	case confluent.OffsetBeginning:
		offsetStr = "oldest"
	default:
		offsetStr = "custom"
	}

	attempt := 1
	for {
		if offset == confluent.OffsetBeginning || offset == confluent.OffsetEnd {
			val1, val2, err = k.consumer.QueryWatermarkOffsets(topic, partition, consumerMetadataTimeout)
		} else {
			times := []confluent.TopicPartition{{Topic: &topic, Partition: partition, Offset: offset}}
			times, err = k.consumer.OffsetForTime(times, consumerMetadataTimeout)
			if err == nil {
				if len(times) == 0 {
					err = fmt.Errorf("Got 0 topics returned from broker")
				} else {
					val1 = times[0].Offset
				}
			}
		}

		if err == nil {
			break
		}

		err = fmt.Errorf("failed to get offset %s of partition %s:%d. %s (attempt %d/%d)", offsetStr, topic, partition, err, attempt, attempts)
		if attempt == attempts {
			break
		}

		log.Warn("kafka-mdm %s", err)
		attempt += 1
		time.Sleep(sleep)
	}
	return val1, val2, err
}

func (k *KafkaMdm) consume() {
	defer k.wg.Done()

	events := k.consumer.Events()
	ticker := time.NewTicker(offsetCommitInterval)
	for {
		select {
		case events := <-events:
			switch e := ev.(type) {
			case *kafka.Message:
				tp := e.TopicPartition
				if LogLevel < 2 {
					log.Debug("kafka-mdm received message: Topic %s, Partition: %d, Offset: %d, Key: %x", tp.Topic, tp.Partition, tp.Offset, e.Key)
				}
				k.handleMsg(e.Value, tp.Partition)
				currentOffset = tp.Offset
			case *kafka.Error:
				log.Error(3, "kafka-mdm: kafka consumer for %s:%d has shutdown. stop consuming", topic, partition)
				if err := offsetMgr.Commit(topic, partition, currentOffset); err != nil {
					log.Error(3, "kafka-mdm failed to commit offset for %s:%d, %s", topic, partition, err)
				}
				close(k.fatal)
				return
			}
		case ts := <-ticker.C:
			if err := offsetMgr.Commit(topic, partition, currentOffset); err != nil {
				log.Error(3, "kafka-mdm failed to commit offset for %s:%d, %s", topic, partition, err)
			}
			k.lagMonitor.StoreOffset(partition, currentOffset, ts)
			newest, err := k.tryGetOffset(topic, partition, sarama.OffsetNewest, 1, 0)
			if err != nil {
				log.Error(3, "kafka-mdm %s", err)
			} else {
				partitionLogSizeMetric.Set(int(newest))
			}

			partitionOffsetMetric.Set(int(currentOffset))
			if err == nil {
				lag := int(newest - currentOffset)
				partitionLagMetric.Set(lag)
				k.lagMonitor.StoreLag(partition, lag)
			}
		case <-k.stopConsuming:
			pc.Close()
			if err := offsetMgr.Commit(topic, partition, currentOffset); err != nil {
				log.Error(3, "kafka-mdm failed to commit offset for %s:%d, %s", topic, partition, err)
			}
			log.Info("kafka-mdm consumer for %s:%d ended.", topic, partition)
			return
		}
	}
}

// this will continually consume from the topic until k.stopConsuming is triggered.
/*func (k *KafkaMdm) consumePartition(topic string, partition int32, currentOffset int64) {
	defer k.wg.Done()

	partitionOffsetMetric := partitionOffset[partition]
	partitionLogSizeMetric := partitionLogSize[partition]
	partitionLagMetric := partitionLag[partition]

	// determine the pos of the topic and the initial offset of our consumer
	newest, oldest, err := k.tryGetOffset(topic, partition, confluent.OffsetStored, 7, time.Second*10)
	if err != nil {
		log.Error(3, "kafka-mdm %s", err)
		close(k.fatal)
		return
	}
	if currentOffset == confluent.OffsetEnd {
		currentOffset = newest
	} else if currentOffset == confluent.OffsetBeginning {
		currentOffset = oldest
	}

	partitionOffsetMetric.Set(int(currentOffset))
	partitionLogSizeMetric.Set(int(newest))
	partitionLagMetric.Set(int(newest - currentOffset))

	log.Info("kafka-mdm: consuming from %s:%d from offset %d", topic, partition, currentOffset)
	err := k.consumer.Subscribe(topic, nil)
	if err != nil {
		log.Error(4, "kafka-mdm: failed to subscribe to topic %s:%d. %s", topic, partition, err)
		close(k.fatal)
		return
	}
	err := k.consumer.Assign(partitions)
	if err != nil {
		log.Error(4, "kafka-mdm: failed to assgn partitions %+v on topic %s. %s", partitions, topic, err)
		close(k.fatal)
		return
	}

	messages := pc.Messages()
	ticker := time.NewTicker(offsetCommitInterval)
	for {
		select {
		case msg, ok := <-messages:
			// https://github.com/Shopify/sarama/wiki/Frequently-Asked-Questions#why-am-i-getting-a-nil-message-from-the-sarama-consumer
			if !ok {
				log.Error(3, "kafka-mdm: kafka consumer for %s:%d has shutdown. stop consuming", topic, partition)
				if err := offsetMgr.Commit(topic, partition, currentOffset); err != nil {
					log.Error(3, "kafka-mdm failed to commit offset for %s:%d, %s", topic, partition, err)
				}
				close(k.fatal)
				return
			}
			if LogLevel < 2 {
				log.Debug("kafka-mdm received message: Topic %s, Partition: %d, Offset: %d, Key: %x", msg.Topic, msg.Partition, msg.Offset, msg.Key)
			}
			k.handleMsg(msg.Value, partition)
			currentOffset = msg.Offset
		case ts := <-ticker.C:
			if err := offsetMgr.Commit(topic, partition, currentOffset); err != nil {
				log.Error(3, "kafka-mdm failed to commit offset for %s:%d, %s", topic, partition, err)
			}
			k.lagMonitor.StoreOffset(partition, currentOffset, ts)
			newest, err := k.tryGetOffset(topic, partition, sarama.OffsetNewest, 1, 0)
			if err != nil {
				log.Error(3, "kafka-mdm %s", err)
			} else {
				partitionLogSizeMetric.Set(int(newest))
			}

			partitionOffsetMetric.Set(int(currentOffset))
			if err == nil {
				lag := int(newest - currentOffset)
				partitionLagMetric.Set(lag)
				k.lagMonitor.StoreLag(partition, lag)
			}
		case <-k.stopConsuming:
			pc.Close()
			if err := offsetMgr.Commit(topic, partition, currentOffset); err != nil {
				log.Error(3, "kafka-mdm failed to commit offset for %s:%d, %s", topic, partition, err)
			}
			log.Info("kafka-mdm consumer for %s:%d ended.", topic, partition)
			return
		}
	}
}*/

func (k *KafkaMdm) handleMsg(data []byte, partition int32) {
	md := schema.MetricData{}
	_, err := md.UnmarshalMsg(data)
	if err != nil {
		metricsDecodeErr.Inc()
		log.Error(3, "kafka-mdm decode error, skipping message. %s", err)
		return
	}
	metricsPerMessage.ValueUint32(1)
	k.Handler.Process(&md, partition)
}

// Stop will initiate a graceful stop of the Consumer (permanent)
// and block until it stopped.
func (k *KafkaMdm) Stop() {
	// closes notifications and messages channels, amongst others
	close(k.stopConsuming)
	k.wg.Wait()
	k.client.Close()
	offsetMgr.Close()
}

func (k *KafkaMdm) MaintainPriority() {
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		for {
			select {
			case <-k.stopConsuming:
				return
			case <-ticker.C:
				cluster.Manager.SetPriority(k.lagMonitor.Metric())
			}
		}
	}()
}
