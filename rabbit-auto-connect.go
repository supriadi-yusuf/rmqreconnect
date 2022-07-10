package rmqreconnect

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/streadway/amqp"
)

// IRqAutoConnect is interface defining method of rabbit mq auto connect
type IRqAutoConnect interface {
	StartConnection(param *RmqConnectionParam) (c *amqp.Connection, err error)
	DeclareQueues(queues ...string) (err error)
	GetRqChannel() *amqp.Channel
	Stop()
	beforeReconnect() // implement template pattern
	afterReconnect()  // implement template pattern
}

type rMqAutoConnect struct {
	conn          *amqp.Connection
	ch            *amqp.Channel
	uriConnection string
	//queue         string
	notifCloseCh   chan *amqp.Error
	ctxReconnect   context.Context
	stopReconnect  context.CancelFunc
	rq             IRqAutoConnect // implement template pattern
	declaredQueues []string       // queues
}

func (r *rMqAutoConnect) reset() {
	r.ch.Close()
	r.conn.Close()
}

func (r *rMqAutoConnect) connect(uri string) (c *amqp.Connection, err error) {
	const (
		maxTrialSecond = 3 // 60 second
		maxTrialMinute = 7 // 10 minute
		//maxTrialHour   = 15 // 1 hour
	)

	// connect to rabbit mq
	log.Println("try connecting to rabbit mq ...")

	trial := 0
	for {
		trial++

		r.conn, err = amqp.Dial(uri)
		if err != nil {

			log.Println(err.Error())

			switch {
			case trial <= maxTrialSecond:
				log.Println("try to reconnect in 30 seconds ...")
				<-time.After(time.Duration(30) * time.Second)

			case trial <= maxTrialMinute:
				log.Println("try to reconnect in 10 minutes ...")
				<-time.After(time.Duration(10) * time.Minute)

			default:
				log.Println("try to reconnect in 1 hour ...")
				<-time.After(time.Duration(1) * time.Hour)
			}

			continue
		}

		break
	}

	log.Println("connected to rabbit mq successfully")

	// keep a live
	r.conn.Config.Heartbeat = time.Duration(5) * time.Second

	//declare channel
	log.Println("open channel ...")

	r.ch, err = r.conn.Channel()
	if err != nil {
		r.conn.Close()
		log.Panicln(err.Error())
	}

	log.Println("opening channel succeed")

	return r.conn, nil
}

func (r *rMqAutoConnect) DeclareQueues(queues ...string) (err error) {

	r.declaredQueues = queues

	//declare queues
	for _, queue := range queues {
		log.Printf("declare %s queue ...\n", queue)
		_, err = r.ch.QueueDeclare(
			queue, //name
			//true,  //durable
			false, //durable
			false, //auto delte
			false, //exclusive
			false, //no wait
			func() (out amqp.Table) {
				//out = amqp.Table{
				//	"x-message-ttl": 62000,// we will handle in other place
				//}
				return
			}(), //args
		)
		if err != nil {
			//r.ch.Close()
			//r.conn.Close()
			log.Println(err.Error())
			return
		}

		log.Printf("queue %s is successfully declared\n", queue)

	}

	return
}

func (r *rMqAutoConnect) stop() {

	defer func() {
		if it := recover(); it != nil {
			log.Printf("panic : %v\n", it)
		}
	}()

	if r.stopReconnect != nil {
		r.stopReconnect()
	}

	r.reset()
	//close(r.notifCloseCh)
}

func (r *rMqAutoConnect) GetRqChannel() *amqp.Channel {
	return r.ch
}

func (r *rMqAutoConnect) beforeReconnect() { // implement template pattern
	r.rq.beforeReconnect()
}

func (r *rMqAutoConnect) afterReconnect() { // implement template pattern
	r.rq.afterReconnect()
}

func (r *rMqAutoConnect) startConnection(param *RmqConnectionParam) (err error) {

	// set uri parameter to connect to rabbit mq
	r.uriConnection = fmt.Sprintf("amqp://%s:%s@%s:%s/%s", param.Username, param.Password, param.Host, param.Port, param.Vhost)
	//r.queue = queue

	log.Println(r.uriConnection)

	r.conn, err = r.connect(r.uriConnection)
	if err != nil {
		log.Panicln(err.Error())
	}

	// try to reconnect
	r.reconnect()

	return
}

func (r *rMqAutoConnect) getConnection() *amqp.Connection {
	return r.conn
}

func (r *rMqAutoConnect) reconnect() {

	log.Println("auto reconnect")

	r.ctxReconnect, r.stopReconnect = context.WithCancel(context.Background()) // prepare context

	log.Println("create notif close channel")

	r.notifCloseCh = make(chan *amqp.Error)

	log.Println("notif close channel is created successfully")

	go func() {

		for {

			log.Println("check if rabbit mq connection is closed ...")

			select {
			case <-r.ctxReconnect.Done():
				log.Println("stop reconnect listening queue ...")
				return // exit from this routine

			case <-r.getConnection().NotifyClose(r.notifCloseCh):
				//case chErr := <-r.getConnection().NotifyClose(r.notifCloseCh):
				//if chErr!=nil{

				//}
				//log.Println("error chErr : ", chErr)
				r.beforeReconnect()
				log.Println("connection is closed, try to reconnect to rabbit mq ...")
				r.reset()
				r.connect(r.uriConnection)
				r.DeclareQueues(r.declaredQueues...)
				r.afterReconnect()
				//<-time.After(time.Duration(5) * time.Second)
				//close(r.notifCloseCh)//panic - close of closed channel
				r.notifCloseCh = make(chan *amqp.Error)
			}

		}

	}()
}
