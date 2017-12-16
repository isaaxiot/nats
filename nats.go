package nats

import (
	"encoding/json"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats/encoders/protobuf"
	"github.com/sirupsen/logrus"
)

const connectionRetries = 10

type natsCB func(subj, reply string, msg map[string]interface{})

//NATSReply is a common response format on NATS requests
type NATSReply struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Stack   string      `json:"stack,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

type Client struct {
	mtx      sync.Mutex                    //mutex
	c        *nats.Conn                    //nats client connection
	jsonCon  *nats.EncodedConn             //nats json encoded connection
	protoCon *nats.EncodedConn             //nats proto encoded connection
	subs     map[string]*nats.Subscription //active subscriptions
	cbs      map[string]func(msg *nats.Msg)
	cbse     map[string]natsCB
	log      *logrus.Entry
	url      string
}

// New returns new instance of NATS client
func New(url, instance string) *Client {
	n := &Client{subs: make(map[string]*nats.Subscription)}
	n.url = url
	go n.connect()
	n.subs = make(map[string]*nats.Subscription)
	n.cbs = make(map[string]func(msg *nats.Msg))
	n.cbse = make(map[string]natsCB)
	n.log = logrus.WithField("service", "NATS").WithField("i", instance)
	return n
}

func (n *Client) retry() {
	retry := time.NewTicker(5 * time.Second)
	i := 0
	for {
		select {
		case <-retry.C:
			if natsConnection, err := nats.Connect(n.url); err != nil {
				if i >= connectionRetries {
					n.log.Error("Error connecting to NATS server: ", err.Error())
					i = 0
					// return
				}
			} else {
				retry.Stop()
				n.c = natsConnection
				if n.jsonCon, err = nats.NewEncodedConn(n.c, nats.JSON_ENCODER); err != nil {
					n.log.Error("Error creating nats encoded connection: ", err.Error())
				}
				return
			}
			i++
		}
	}
}

func (n *Client) connect() {
	n.mtx.Lock()
	defer n.mtx.Unlock()

	n.c = &nats.Conn{}

	if natsConnection, err := nats.Connect(n.url); err != nil {
		n.log.Println("Error connecting to NATS server: ", err.Error())
		n.retry()
	} else {
		n.c = natsConnection
		if n.jsonCon, err = nats.NewEncodedConn(n.c, nats.JSON_ENCODER); err != nil {
			n.log.Error("error creating nats json encoded connection: ", err.Error())
		}
		if n.protoCon, err = nats.NewEncodedConn(n.c, protobuf.PROTOBUF_ENCODER); err != nil {
			n.log.Error("error creating nats protobuf encoded connection: ", err.Error())
		}
	}
}

func (n *Client) Publish(topic string, payload interface{}) error {
	if !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	if err := n.jsonCon.Publish(topic, payload); err != nil {
		n.log.Error("error publishing to nats broker:", err.Error())
		return err
	}
	return nil
}

func (n *Client) PublishPlain(topic string, payload []byte) error {
	if !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	if err := n.c.Publish(topic, payload); err != nil {
		n.log.Error("error publishing to nats broker: ", err.Error())
	}
	return nil
}

func (n *Client) PublishRequest(topic, reply string, payload interface{}) error {
	if !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	if err := n.jsonCon.PublishRequest(topic, reply, payload); err != nil {
		n.log.Error("error publishing to nats broker:", err.Error())
		return err
	}
	return nil
}

func (n *Client) Callback(msg *nats.Msg) {
	defer func() {
		if recovery := recover(); recovery != nil {
			n.log.WithField("bt", string(debug.Stack())).Error("Recovered from:", recovery)
		}
	}()
	n.log.WithField("data", string(msg.Data)).WithField("reply", msg.Reply).Debug(msg.Subject)
	if name, err := n.findWildcardSubscription(msg.Subject, n.cbs); err == nil {
		n.cbs[name](msg)
	}
}

func (n *Client) findWildcardSubscription(topic string, list map[string]func(msg *nats.Msg)) (string, error) {
	if _, ok := list[topic]; ok {
		return topic, nil
	}
	for name := range list {
		if strings.HasPrefix(topic, strings.TrimSuffix(name, "*")) {
			return name, nil
		}
	}
	return "", fmt.Errorf("callback for ", topic, " is not set")
}

func (n *Client) CallbackE(subj, reply string, msg map[string]interface{}) {
	defer func() {
		if recovery := recover(); recovery != nil {
			n.log.WithField("bt", string(debug.Stack())).Error("Recovered from:", recovery)
		}
	}()
	n.log.WithField("data", msg).WithField("reply", reply).Debug(subj)
	n.cbse[subj](subj, reply, msg)
}

func (n *Client) SubEncoded(topic string, callback natsCB) error {
	n.mtx.Lock()
	defer n.mtx.Unlock()
	n.log.Debug("SubEncoded: " + topic)
	if !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	if sub, err := n.jsonCon.QueueSubscribe(topic, "distributed-queue-encoded", n.CallbackE); err == nil {
		n.cbse[topic] = callback
		if sub.IsValid() && !n.isSubscribed(topic) {
			n.subs[topic] = sub
		}
	} else {
		n.log.Error(err)
		return err
	}
	return nil
}

func (n *Client) SubProtoEncoded(topic string, callback interface{}) error {
	if !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	if sub, err := n.protoCon.QueueSubscribe(topic, "distributed-queue-proto-encoded", callback); err == nil {
		if sub.IsValid() && !n.isSubscribed(topic) {
			n.subs[topic] = sub
		}
	} else {
		n.log.Error(err)
		return err
	}
	return nil
}

func (n *Client) ProtoRequest(topic string, request interface{}, v interface{}) error {
	if !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	if err := n.protoCon.Request(topic, request, v, 15*time.Second); err != nil {
		n.log.Error(err)
		return err
	}
	return nil
}

func (n *Client) PublishProtoEncoded(topic string, v interface{}) error {
	if !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	if err := n.protoCon.Publish(topic, v); err != nil {
		n.log.Error(err)
		return err
	}
	return nil
}

func (n *Client) Subscribe(topic string, callback func(msg *nats.Msg)) error {
	n.mtx.Lock()
	defer n.mtx.Unlock()
	n.log.Debug("Subscribe: " + topic)
	if !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	if sub, err := n.c.QueueSubscribe(topic, "distributed-queue", n.Callback); err == nil {
		n.cbs[topic] = callback
		if sub.IsValid() && !n.isSubscribed(topic) {
			n.subs[topic] = sub
		}
	} else {
		n.log.Error(err)
		return err
	}
	return nil
}

func (n *Client) isSubscribed(topic string) bool {
	_, ok := n.subs[topic]
	return ok
}

func (n *Client) Unsubscribe(topic string) error {
	n.mtx.Lock()
	defer n.mtx.Unlock()
	if sub, ok := n.subs[topic]; ok {
		if sub.IsValid() {
			if err := sub.Unsubscribe(); err != nil {
				return err
			}
		}
		delete(n.subs, topic)
	}
	return nil
}

func (n *Client) Request(subj string, request interface{}, data interface{}) error {
	n.log.WithField("subj", subj).WithField("request", request).Info("Request")
	if !n.c.IsConnected() {
		time.Sleep(time.Second)
	}
	if !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	response := struct {
		Success bool
		Message string
		Stack   string
		Data    json.RawMessage
	}{}

	if err := n.jsonCon.Request(subj, request, &response, 15*time.Second); err != nil {
		n.log.WithField("subj", subj).Error(err)
		return err
	}
	n.log.WithField("subj", subj).WithField("response", response.Success).WithField("response", response.Message).WithField("data", string(response.Data)).Debug("Response")

	if !response.Success {
		if response.Message == "record not found" {
			return nil
		}
		return fmt.Errorf(response.Message)
	}

	if err := json.Unmarshal(response.Data, data); err != nil {
		return err
	}
	return nil
}

func (n *Client) NATS() *nats.Conn {
	return n.c
}

func (n *Client) IsConnected() bool {
	return n.c.IsConnected()
}
