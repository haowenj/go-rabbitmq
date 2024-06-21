package main

import (
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"strings"
	"time"
)

type QueueObject struct {
	Con           *amqp.Connection
	Ch            *amqp.Channel
	ExchangeName  string
	DelayExchange string
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

	err = ch.ExchangeDeclare("my-first-exchange", amqp.ExchangeDirect, true, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
		return
	}

	//创建延迟队列交换机, kind指定为"x-delayed-message"，设置x-delayed-type参数
	err = ch.ExchangeDeclare("delayed-exchange", "x-delayed-message", true, false, false, false, amqp.Table{"x-delayed-type": "direct"})
	if err != nil {
		fmt.Println(err)
		return
	}

	//指定当消费消息时调用了NACK方法，切requeue为false时放入死信队列
	ch.QueueDeclare("queue-1", true, false, false, false, amqp.Table{"x-dead-letter-exchange": "my-first-exchange", "x-dead-letter-routing-key": "die"})
	ch.QueueDeclare("queue-2", true, false, false, false, amqp.Table{"x-dead-letter-exchange": "my-first-exchange", "x-dead-letter-routing-key": "die"})
	//死信队列
	ch.QueueDeclare("queue-die", true, false, false, false, nil)
	//延迟队列
	ch.QueueDeclare("queue-delayed", true, false, false, false, nil)

	//qu1 qu2都绑定了error这个key，带带着error这个key的消息到了交换机以后，会同时转发给qu1和qu2
	ch.QueueBind("queue-1", "error", "my-first-exchange", false, nil)
	ch.QueueBind("queue-2", "error", "my-first-exchange", false, nil)
	ch.QueueBind("queue-2", "info", "my-first-exchange", false, nil)
	ch.QueueBind("queue-2", "warn", "my-first-exchange", false, nil)
	ch.QueueBind("queue-die", "die", "my-first-exchange", false, nil)
	//延迟队列绑定延迟交换机，路由key填空
	ch.QueueBind("queue-delayed", "", "delayed-exchange", false, nil)

	queObj = QueueObject{
		Con:           conn,
		Ch:            ch,
		ExchangeName:  "my-first-exchange",
		DelayExchange: "delayed-exchange",
	}
}

func main() {
	defer queObj.Con.Close()

	go consume("queue-1", queObj)
	go consume("queue-2", queObj)
	go consume("queue-die", queObj)
	go consume("queue-delayed", queObj)

	var msg, key string
	for true {
		fmt.Println("input msg and route key:")
		fmt.Scan(&msg, &key)
		if msg == "exit" {
			break
		}
		var err error
		if !strings.Contains(msg, "delay") {
			err = queObj.Ch.Publish(queObj.ExchangeName, key, false, false, amqp.Publishing{
				ContentType: "text/plan",
				Body:        []byte(msg),
				Expiration:  "10000", //消息过期时间为毫秒，超时不消费或者不不确认消费就会丢到死信队列或者丢弃，注意，这里如果是消费了没有ack的话，不会进入死信队列，只有没被消费的消息进入死信队列。
			})
		} else {
			//发送延迟消息，路由key填空
			err = queObj.Ch.Publish(queObj.DelayExchange, "", false, false, amqp.Publishing{
				ContentType: "text/plan",
				Body:        []byte(msg),
				// 设置消息的延迟时间（毫秒）
				Headers: amqp.Table{
					"x-delay": int64(10000), // 延迟10秒
				},
			})
		}

		if err != nil {
			fmt.Println(err)
			return
		}
	}
}

func consume(name string, que QueueObject) {
	fmt.Printf("%s queue start listening...\n", name)
	msgs, err := que.Ch.Consume(name, "", false, false, false, false, nil)
	if err != nil {
		fmt.Println(err)
		return
	}
	num := 1
	for msg := range msgs {
		fmt.Printf("%s queue Received a message: %s\n", name, msg.Body)
		//如果是死信队列里消费的消息，直接确认
		if name == "queue-die" {
			fmt.Printf("%s in die queue ack\n", msg.Body)
			//如果设置为true，将确认所有未确认的消息
			msg.Ack(false)
		} else {
			//如果消息是requeue，模拟重放回队列，放回三次以上直接确认
			if string(msg.Body) == "requeue" {
				if num > 3 {
					msg.Ack(false)
				} else {
					fmt.Printf("%d requeue num\n", num)
					msg.Nack(false, true)
				}
				num++
			} else if string(msg.Body) == "ttl" {
				//模拟消息超时，丢入死信队列
				fmt.Println("in die queue")
			} else if string(msg.Body) == "nack" {
				//模拟消息处理失败，放入死信队列
				msg.Nack(false, false)
			} else {
				//其他情况直接确认
				msg.Ack(false)
			}
		}
		time.Sleep(3 * time.Second)
	}
}
