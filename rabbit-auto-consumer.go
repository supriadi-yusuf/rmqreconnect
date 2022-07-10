package rmqreconnect

import (
	"context"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// IRqAutoConsumer is interface defining method of rabbit mq auto connect for consumer
type IRqAutoConsumer interface {
	IRqAutoConnect
	GetMessageChanel(queue string) <-chan amqp.Delivery
	SetReadQueue(queue ...string)
	//PrepareConsumeChannel()
	ConsumeMessage() (err error)
}

type rMqAutoConsumer struct {
	rMqAutoConnect
	deliveryCh     map[string]<-chan amqp.Delivery
	msgCh          map[string]chan amqp.Delivery
	ctxConsumeMsg  map[string]context.Context
	stopConsumeMsg map[string]context.CancelFunc
	isBroken       bool
	readQueue      []string
}

func (r *rMqAutoConsumer) Stop() {

	log.Printf("prepare to stop consumer %v\n", r.readQueue)

	for _, stopConsumeMsg := range r.stopConsumeMsg {
		stopConsumeMsg()
	}
	//if r.stopConsumeMsg != nil {
	//	r.stopConsumeMsg()
	//}

	//for _, msgCh := range r.msgCh {
	//	close(msgCh)
	//}

	//if r.msgCh != nil {
	//	close(r.msgCh)
	//}

	r.stop()

	log.Printf("stop consumer %v complete\n", r.readQueue)
}

func (r *rMqAutoConsumer) SetReadQueue(queue ...string) {
	r.readQueue = queue
}

func (r *rMqAutoConsumer) StartConnection(param *RmqConnectionParam) (c *amqp.Connection, err error) {

	err = r.startConnection(param)
	if err != nil {
		log.Panicln(err.Error())
	}

	c = r.conn

	return
}

//func (r *rMqAutoConsumer) PrepareConsumeChannel() {
//	r.msgCh = make(chan amqp.Delivery) // prepare channel to store message
//}

func (r *rMqAutoConsumer) listenQueueOnChannel() (err error) {

	// make sure that only one message at one time
	err = r.GetRqChannel().Qos(
		1,     //prefetch count
		0,     //prefetch size
		false, //global
	)
	if err != nil {
		r.ch.Close()
		r.conn.Close()
		log.Panicln(err.Error())
	}

	for _, readQueue := range r.readQueue {

		log.Printf("listen to queue %s\n", readQueue)

		r.deliveryCh[readQueue], err = r.ch.Consume(
			readQueue, //queue
			//config.RqNotifQueue(), //name
			"",    //consumer
			false, //auto ack
			false, //exclusive
			false, //no local
			false, //no wait
			nil,   //args
		)
		if err != nil {
			log.Panicln(err.Error())
		}
	}

	r.isBroken = false

	return
}

func (r *rMqAutoConsumer) ConsumeMessage() (err error) {

	r.deliveryCh = map[string]<-chan amqp.Delivery{}
	r.msgCh = map[string]chan amqp.Delivery{}
	r.ctxConsumeMsg = map[string]context.Context{}
	r.stopConsumeMsg = map[string]context.CancelFunc{}

	r.listenQueueOnChannel()

	for _, readQueue := range r.readQueue {

		log.Printf("create context with cancel, delivery chanel, message chanel on queue : %s\n", readQueue)

		// prepare context
		r.ctxConsumeMsg[readQueue], r.stopConsumeMsg[readQueue] = context.WithCancel(context.Background())

		// prepare chanel
		r.msgCh[readQueue] = make(chan amqp.Delivery)

		go func(readQueue string) {

			//defer close(r.msgCh[readQueue])

			for {

				if r.isBroken {
					<-time.After(time.Duration(1) * time.Second)
					continue
				}

				select {
				case <-r.ctxConsumeMsg[readQueue].Done():
					close(r.msgCh[readQueue])
					log.Printf("stop consuming message on %s\n", readQueue)
					return

				case <-time.After(time.Duration(1) * time.Second):

				case delivery := <-r.deliveryCh[readQueue]:
					//log.Println(delivery)
					//if !r.isBroken {
					log.Printf("receive new data \"%s\" on %s\n", delivery.Body, readQueue)
					//func() {
					//	defer func() { recover() }() //anticipate if chanel has been closed
					r.msgCh[readQueue] <- delivery
					//}()

					//}

				}

			}
		}(readQueue)
	}

	return
}

func (r *rMqAutoConsumer) GetMessageChanel(queue string) <-chan amqp.Delivery {
	return r.msgCh[queue]
}

func (r *rMqAutoConsumer) beforeReconnect() { // implement template pattern
	r.isBroken = true
	//r.stopConsumeMsg()
}

func (r *rMqAutoConsumer) afterReconnect() { // implement template pattern
	//r.ConsumeMessage()
	r.listenQueueOnChannel()
}

// CreateRqConsumer is function to create rabbit mq auto connect for consumer
func CreateRqConsumer() (r IRqAutoConsumer) {
	rmq := new(rMqAutoConsumer)
	rmq.rq = rmq

	//rmq.msgCh = make(chan amqp.Delivery) // prepare channel to store message

	return rmq
}
