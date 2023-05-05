package main

import (
	"context"
	"fmt"
	"io/ioutil"

	"github.com/linkedin/goavro/v2"
	"github.com/segmentio/kafka-go"
)

func NewKafkaClient() *Kafka {
	kafkaWriter := &kafka.Writer{
		Addr:                   kafka.TCP("localhost:9092"),
		AllowAutoTopicCreation: false,
		RequiredAcks:           kafka.RequireOne,
		MaxAttempts:            5,
		BatchSize:              1,
		Balancer:               kafka.Murmur2Balancer{},
		//BatchTimeout: time.Millisecond * 10,
	}
	return &Kafka{
		kafkaWriter: kafkaWriter,
	}
}

type Kafka struct {
	kafkaWriter *kafka.Writer
}

func (kf *Kafka) BatchPublish(messages []kafka.Message) error {
	fmt.Println(messages)

	return kf.kafkaWriter.WriteMessages(context.Background(), messages...)
}

func main() {
	kafkaClient := NewKafkaClient()
	PublishLikes(kafkaClient)
	PublishGifts(kafkaClient)
	PublishCohosts(kafkaClient)
	PublishComments(kafkaClient)
	PublishShares(kafkaClient)
	PublishFollows(kafkaClient)
}

func PublishLikes(kafkaClient *Kafka) {
	PublishToKafka(map[string]interface{}{
		"livestreamId":    goavro.Union("string", "livestreamId1"),
		"hostId":          goavro.Union("string", "hostId1"),
		"memberId":        goavro.Union("string", "authorId1"),
		"shortPressCount": goavro.Union("int", 1),
		"longPressCount":  goavro.Union("int", 2),
		"time":            goavro.Union("long", 1683116230000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-likeEvent", "schema/moj-livestream-likeEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId":    goavro.Union("string", "livestreamId1"),
		"hostId":          goavro.Union("string", "hostId1"),
		"memberId":        goavro.Union("string", "authorId1"),
		"shortPressCount": goavro.Union("int", 2),
		"longPressCount":  goavro.Union("int", 3),
		"time":            goavro.Union("long", 1683116240000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-likeEvent", "schema/moj-livestream-likeEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId":    goavro.Union("string", "livestreamId2"),
		"hostId":          goavro.Union("string", "hostId1"),
		"memberId":        goavro.Union("string", "authorId1"),
		"shortPressCount": goavro.Union("int", 3),
		"longPressCount":  goavro.Union("int", 4),
		"time":            goavro.Union("long", 1683116250000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-likeEvent", "schema/moj-livestream-likeEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId":    goavro.Union("string", "livestreamId3"),
		"memberId":        goavro.Union("string", "authorId1"),
		"hostId":          goavro.Union("string", "hostId2"),
		"shortPressCount": goavro.Union("int", 4),
		"longPressCount":  goavro.Union("int", 5),
		"time":            goavro.Union("long", 1683116550000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-likeEvent", "schema/moj-livestream-likeEvent.avsc", kafkaClient)
}

func PublishGifts(kafkaClient *Kafka) {
	PublishToKafka(map[string]interface{}{
		"entity":      goavro.Union("string", "livestream"),
		"entityId":    goavro.Union("string", "livestreamId1"),
		"recipientId": goavro.Union("string", "hostId1"),
		"senderId":    goavro.Union("string", "authorId1"),
		"giftCount":   goavro.Union("int", 1),
		"fromAmount":  goavro.Union("float", 2.0),
		"time":        goavro.Union("long", 1683116230000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-giftTransactionEvent", "schema/moj-livestream-giftTransactionEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"entity":      goavro.Union("string", "livestream"),
		"entityId":    goavro.Union("string", "livestreamId1"),
		"recipientId": goavro.Union("string", "hostId1"),
		"senderId":    goavro.Union("string", "authorId1"),
		"giftCount":   goavro.Union("int", 1),
		"fromAmount":  goavro.Union("float", 3.0),
		"time":        goavro.Union("long", 1683116240000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-giftTransactionEvent", "schema/moj-livestream-giftTransactionEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"entity":      goavro.Union("string", "livestream"),
		"entityId":    goavro.Union("string", "livestreamId2"),
		"recipientId": goavro.Union("string", "hostId1"),
		"senderId":    goavro.Union("string", "authorId1"),
		"giftCount":   goavro.Union("int", 1),
		"fromAmount":  goavro.Union("float", 4.0),
		"time":        goavro.Union("long", 1683116250000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-giftTransactionEvent", "schema/moj-livestream-giftTransactionEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"entity":      goavro.Union("string", "livestream"),
		"entityId":    goavro.Union("string", "livestreamId3"),
		"senderId":    goavro.Union("string", "authorId1"),
		"recipientId": goavro.Union("string", "hostId2"),
		"giftCount":   goavro.Union("int", 1),
		"fromAmount":  goavro.Union("float", 5.0),
		"time":        goavro.Union("long", 1683116550000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-giftTransactionEvent", "schema/moj-livestream-giftTransactionEvent.avsc", kafkaClient)
}

func PublishCohosts(kafkaClient *Kafka) {
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId1"),
		"hostId":       goavro.Union("string", "hostId1"),
		"memberId":     goavro.Union("string", "authorId1"),
		"time":         goavro.Union("long", 1683116230000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-cohostRequestEvent", "schema/moj-livestream-cohostRequestEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId1"),
		"hostId":       goavro.Union("string", "hostId1"),
		"memberId":     goavro.Union("string", "authorId1"),
		"time":         goavro.Union("long", 1683116240000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-cohostRequestEvent", "schema/moj-livestream-cohostRequestEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId2"),
		"hostId":       goavro.Union("string", "hostId1"),
		"memberId":     goavro.Union("string", "authorId1"),
		"time":         goavro.Union("long", 1683116250000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-cohostRequestEvent", "schema/moj-livestream-cohostRequestEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId3"),
		"memberId":     goavro.Union("string", "authorId1"),
		"hostId":       goavro.Union("string", "hostId2"),
		"time":         goavro.Union("long", 1683116550000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-cohostRequestEvent", "schema/moj-livestream-cohostRequestEvent.avsc", kafkaClient)
}

func PublishComments(kafkaClient *Kafka) {
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId1"),
		"hostId":       goavro.Union("string", "hostId1"),
		"authorId":     goavro.Union("string", "authorId1"),
		"time":         goavro.Union("long", 1683116230000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-commentEvent", "schema/moj-livestream-commentEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId1"),
		"hostId":       goavro.Union("string", "hostId1"),
		"authorId":     goavro.Union("string", "authorId1"),
		"time":         goavro.Union("long", 1683116240000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-commentEvent", "schema/moj-livestream-commentEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId2"),
		"hostId":       goavro.Union("string", "hostId1"),
		"authorId":     goavro.Union("string", "authorId1"),
		"time":         goavro.Union("long", 1683116250000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-commentEvent", "schema/moj-livestream-commentEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId3"),
		"authorId":     goavro.Union("string", "authorId1"),
		"hostId":       goavro.Union("string", "hostId2"),
		"time":         goavro.Union("long", 1683116550000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-commentEvent", "schema/moj-livestream-commentEvent.avsc", kafkaClient)
}

func PublishShares(kafkaClient *Kafka) {
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId1"),
		"hostId":       goavro.Union("string", "hostId1"),
		"memberId":     goavro.Union("string", "authorId1"),
		"time":         goavro.Union("long", 1683116230000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-shareEvent", "schema/moj-livestream-shareEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId1"),
		"hostId":       goavro.Union("string", "hostId1"),
		"memberId":     goavro.Union("string", "authorId1"),
		"time":         goavro.Union("long", 1683116240000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-shareEvent", "schema/moj-livestream-shareEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId2"),
		"hostId":       goavro.Union("string", "hostId1"),
		"memberId":     goavro.Union("string", "authorId1"),
		"time":         goavro.Union("long", 1683116250000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-shareEvent", "schema/moj-livestream-shareEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId3"),
		"memberId":     goavro.Union("string", "authorId1"),
		"hostId":       goavro.Union("string", "hostId2"),
		"time":         goavro.Union("long", 1683116550000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-shareEvent", "schema/moj-livestream-shareEvent.avsc", kafkaClient)
}

func PublishFollows(kafkaClient *Kafka) {
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId1"),
		"hostId":       goavro.Union("string", "hostId1"),
		"followeeId":   goavro.Union("string", "hostId1"),
		"followerId":   goavro.Union("string", "authorId1"),
		"time":         goavro.Union("long", 1683116230000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-followEvent", "schema/moj-livestream-followEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId1"),
		"hostId":       goavro.Union("string", "hostId1"),
		"followerId":   goavro.Union("string", "authorId1"),
		"followeeId":   goavro.Union("string", "hostId1"),
		"time":         goavro.Union("long", 1683116240000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-followEvent", "schema/moj-livestream-followEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId2"),
		"hostId":       goavro.Union("string", "hostId1"),
		"followerId":   goavro.Union("string", "authorId1"),
		"followeeId":   goavro.Union("string", "hostId1"),
		"time":         goavro.Union("long", 1683116250000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-followEvent", "schema/moj-livestream-followEvent.avsc", kafkaClient)
	PublishToKafka(map[string]interface{}{
		"livestreamId": goavro.Union("string", "livestreamId3"),
		"hostId":       goavro.Union("string", "hostId2"),
		"followerId":   goavro.Union("string", "authorId1"),
		"followeeId":   goavro.Union("string", "hostId2"),
		"time":         goavro.Union("long", 1683116550000), // Wed  3 May 2023 13:17:10 BST
	}, "moj-livestream-followEvent", "schema/moj-livestream-followEvent.avsc", kafkaClient)
}

func PublishToKafka(msg map[string]interface{}, topic, schemaPath string, kafkaClient *Kafka) {
	codec := getAvroCodecBySchemaPath(schemaPath)
	binary, err := codec.BinaryFromNative(nil, msg)
	if err != nil {
		fmt.Println("error while converting native data to avro binary", map[string]interface{}{
			"err":  err.Error(),
			"data": msg,
		})
	}
	err = kafkaClient.BatchPublish([]kafka.Message{
		{
			Topic: topic,
			Value: binary,
		},
	})
	if err != nil {
		fmt.Println("error while publishing message to kafka", map[string]interface{}{
			"err":  err.Error(),
			"data": msg,
		})
	}
}

func getAvroCodecBySchemaPath(filename string) *goavro.Codec {
	data, err := ioutil.ReadFile(filename)
	if err != nil {
		fmt.Println("error in reading avro schema file", map[string]interface{}{
			"err": err.Error(),
		})
		return nil
	}
	avroSchema := string(data)
	avroCodec, err := goavro.NewCodecForStandardJSON(avroSchema)
	if err != nil {
		fmt.Println("error in loading the Avro schmea", map[string]interface{}{
			"err":      err.Error(),
			"fileName": filename,
		})
		return nil
	}
	return avroCodec
}
