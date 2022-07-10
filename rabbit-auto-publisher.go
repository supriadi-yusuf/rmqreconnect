package rmqreconnect

import (
	"log"

	"github.com/streadway/amqp"
)

// IRqAutoPublisher is interface defining method of rabbit mq auto connect for publisher
type IRqAutoPublisher interface {
	IRqAutoConnect
}

type rMqAutoPublisher struct {
	rMqAutoConnect
}

func (r *rMqAutoPublisher) Stop() {
	r.stop()
	log.Println("stop publisher")
}

func (r *rMqAutoPublisher) StartConnection(param *RmqConnectionParam) (c *amqp.Connection, err error) {

	err = r.startConnection(param)
	if err != nil {
		log.Panicln(err.Error())
	}

	c = r.conn

	return
}

func (r *rMqAutoPublisher) beforeReconnect() { // implement template pattern

}

func (r *rMqAutoPublisher) afterReconnect() { // implement template pattern

}

// CreateRqPublisher is function to create rabbit mq auto connect for publisher
func CreateRqPublisher() (r IRqAutoPublisher) {
	rmq := new(rMqAutoPublisher)
	rmq.rq = rmq

	return rmq
}
