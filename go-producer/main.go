package main

import (
	"fmt"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
	"io/ioutil"
	"log"
	"os"
)

func readFile(fname string) ([]byte, error) {
	file, err := os.Open(fname)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	fileinfo, err := file.Stat()
	if err != nil {
		fmt.Println(err)
	}

	filesize := fileinfo.Size()

	data := make([]byte, filesize)
	if _, err = file.Read(data); err != nil {
		return nil, err
	}
	//fmt.Println(filesize)
	return data, nil
}

func checkError(e error) {
	if e != nil {
		panic(e)
	}
}

func main () {
	fmt.Println("GO - Produce Messages for Apache Kafka")

	filePath := "./msghl7"

	files, err := ioutil.ReadDir(filePath)
	checkError(err)

	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": os.Getenv("kafkaBootstrapServers")})
	if err != nil {
		panic(err)
	}

	defer p.Close()

	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Delivery failed: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Delivered message to %v\n", ev.TopicPartition)
				}
			}
		}
	}()


	topic := os.Getenv("kafkaHL7Topic")
	for _, file :=  range files {

		content, err := readFile("" + filePath + "/" + file.Name())
		if err != nil {
			log.Fatal(err)
		}

		p.Produce(&kafka.Message{
			TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
			Value:          []byte(string(content)),
		}, nil)
	}

	p.Flush(15 * 1000)


}