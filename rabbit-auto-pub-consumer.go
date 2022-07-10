package rmqreconnect

// IRqAutoPubConsumer is interface defining method of rabbit mq auto connect for publisher and consumer
type IRqAutoPubConsumer interface {
	IRqAutoConsumer
}

type rMqAutoPubConsumer struct {
	rMqAutoConsumer
}

// CreateRqPubConsumer is function to create rabbit mq auto connect for publisher and consumer
func CreateRqPubConsumer() (r IRqAutoPubConsumer) {
	rmq := new(rMqAutoPubConsumer)
	rmq.rq = rmq

	//rmq.msgCh = make(chan amqp.Delivery) // prepare channel to store message

	return rmq
}
