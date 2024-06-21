package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueObject struct {
	Con *amqp.Connection
	Ch  *amqp.Channel
	Que amqp.Queue
}

var queObj QueueObject

func init() {
	conn, err := amqp.Dial("amqp://admin:1qaz@WSX@192.168.41.26:32779/")
	if err != nil {
		fmt.Println(err)
		return
	}

	ch, err := conn.Channel()
	if err != nil {
		fmt.Println(err)
		return
	}

	q, err := ch.QueueDeclare("my-frist-queue", true, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	queObj = QueueObject{
		Con: conn,
		Ch:  ch,
		Que: q,
	}
}

func main() {
	defer queObj.Con.Close()
	var msg string
	//启动多个消费者时，生产着的消息会均匀的发送到每个消费者端
	go consume("tk1", queObj)
	go consume("tk2", queObj)
	for true {
		fmt.Println("input msg:")
		fmt.Scan(&msg)
		if msg == "exit" {
			break
		}

		err := queObj.Ch.Publish("", queObj.Que.Name, false, false, amqp.Publishing{
			ContentType: "text/plan",
			Body:        []byte(msg),
		})
		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func consume(name string, que QueueObject) {
	fmt.Printf("%s task started...\n", name)
	msgs, err := que.Ch.Consume(que.Que.Name, "", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	for msg := range msgs {
		fmt.Printf("%s task Received a message: %s\n", name, msg.Body)
		msg.Ack(false)
	}
}
