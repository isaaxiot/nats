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

type natsCB func(topic, reply string, msg map[string]interface{})
type handler interface{}
type plainHandler func(msg *nats.Msg)

type Metrics interface {
	MeasureSince(key string, start time.Time)
}

// NATSReply is a common response format on NATS requests
type NATSReply struct {
	Success bool        `json:"success"`
	Message string      `json:"message,omitempty"`
	Stack   string      `json:"stack,omitempty"`
	Data    interface{} `json:"data,omitempty"`
}

// Client nats client wrapper
type Client struct {
	mtx      sync.Mutex                    //mutex
	c        *nats.Conn                    //nats client connection
	jsonCon  *nats.EncodedConn             //nats json encoded connection
	protoCon *nats.EncodedConn             //nats proto encoded connection
	subs     map[string]*nats.Subscription //active subscriptions
	cbs      map[string]func(msg *nats.Msg)
	cbse     map[string]natsCB
	log      *logrus.Entry
	metrics  Metrics
	opts     []nats.Option
	url      string
}

// New returns new instance of NATS client
func New(url, instance string) *Client {
	n := &Client{subs: make(map[string]*nats.Subscription)}
	n.url = url
	n.opts = append(n.opts, nats.ErrorHandler(func(_ *nats.Conn, s *nats.Subscription, e error) {
		n.log.WithField("subj", s.Subject).Error(e)
	}))
	n.opts = append(n.opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		n.log.WithField("err", nc.LastError()).Warn("Disconnected")
	}))
	n.opts = append(n.opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		n.log.WithField("err", nc.LastError()).Info("Got reconnected to ", nc.ConnectedUrl())
	}))
	go n.connect()
	n.subs = make(map[string]*nats.Subscription)
	n.cbs = make(map[string]func(msg *nats.Msg))
	n.cbse = make(map[string]natsCB)
	n.log = logrus.WithField("service", "NATS").WithField("instance", instance)
	return n
}

// TrackStats injects stats tracker
func (n *Client) TrackStats(m Metrics) {
	n.metrics = m
}

// GetStats returns metrics object
func (n *Client) GetStats() Metrics {
	return n.metrics
}

func (n *Client) measure(key string, start time.Time) {
	if n.metrics != nil {
		n.metrics.MeasureSince(key, start)
	}
}

func (n *Client) retry() {
	retry := time.NewTicker(5 * time.Second)
	i := 0
	for {
		select {
		case <-retry.C:
			if natsConnection, err := nats.Connect(n.url, n.opts...); err != nil {
				if i >= connectionRetries {
					n.log.Error("Error connecting to NATS server: ", err.Error())
					i = 0
				}
			} else {
				retry.Stop()
				n.c = natsConnection
				n.log.WithField("Broker", n.c.ConnectedUrl()).Info("NATS Client connected")
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

	if natsConnection, err := nats.Connect(n.url, n.opts...); err != nil {
		n.log.Println("Error connecting to NATS server: ", err.Error())
		n.retry()
	} else {
		n.c = natsConnection
		n.log.WithField("Broker", n.c.ConnectedUrl()).Info("NATS Client connected")
		if n.jsonCon, err = nats.NewEncodedConn(n.c, nats.JSON_ENCODER); err != nil {
			n.log.Error("error creating nats json encoded connection: ", err.Error())
		}
		if n.protoCon, err = nats.NewEncodedConn(n.c, protobuf.PROTOBUF_ENCODER); err != nil {
			n.log.Error("error creating nats protobuf encoded connection: ", err.Error())
		}
	}
}

func (n *Client) Publish(topic string, payload interface{}) error {
	if n.c == nil || !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	n.log.WithField("topic", topic).WithField("payload", payload).Debug("Publish")
	if err := n.jsonCon.Publish(topic, payload); err != nil {
		n.log.Error("error publishing to nats broker:", err.Error())
		return err
	}
	return nil
}

func (n *Client) PublishPlain(topic string, payload []byte) error {
	if n.c == nil || !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	if err := n.c.Publish(topic, payload); err != nil {
		n.log.Error("error publishing to nats broker: ", err.Error())
	}
	return nil
}

func (n *Client) PublishRequest(topic, reply string, payload interface{}) error {
	if n.c == nil || !n.c.IsConnected() {
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

func (n *Client) CallbackE(topic, reply string, msg map[string]interface{}) {
	defer func() {
		if recovery := recover(); recovery != nil {
			n.log.WithField("bt", string(debug.Stack())).Error("Recovered from:", recovery)
		}
	}()
	n.log.WithField("data", msg).WithField("reply", reply).Debug(topic)
	n.cbse[topic](topic, reply, msg)
}

func (n *Client) SubEncoded(topic string, callback natsCB) error {
	n.mtx.Lock()
	defer n.mtx.Unlock()
	// n.log.Debug("SubEncoded: " + topic)
	if n.c == nil || !n.c.IsConnected() {
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

func (n *Client) PlainSubscribe(topic string, callback plainHandler, extra ...bool) error {
	n.mtx.Lock()
	defer n.mtx.Unlock()
	// n.log.Debug("Subscribe: " + topic)
	if n.c == nil || !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	queue := true
	if len(extra) > 0 {
		queue = extra[0]
	}
	var (
		sub *nats.Subscription
		err error
	)
	if queue {
		sub, err = n.c.QueueSubscribe(topic, "distributed-queue", n.Callback)
	} else {
		sub, err = n.c.Subscribe(topic, n.Callback)
	}
	if err != nil {
		n.log.Error(err)
		return err
	}
	n.cbs[topic] = callback
	if sub.IsValid() && !n.isSubscribed(topic) {
		n.subs[topic] = sub
	}
	return nil
}

func (n *Client) subscribe(topic string, callback handler, protobuf bool) error {
	n.mtx.Lock()
	defer n.mtx.Unlock()
	if n.c == nil || !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	conn := n.jsonCon
	if protobuf {
		conn = n.protoCon
	}
	if sub, err := conn.QueueSubscribe(topic, "distributed-queue-encoded", callback); err == nil {
		if sub.IsValid() && !n.isSubscribed(topic) {
			n.subs[topic] = sub
		}
	} else {
		n.log.Error(err)
		return err
	}
	return nil
}

// SubscribeTo unsafe subscriber with auto unmarshal
func (n *Client) Subscribe(topic string, callback handler) error {
	return n.subscribe(topic, callback, false)
}

func (n *Client) ProtoSubscribe(topic string, callback handler) error {
	return n.subscribe(topic, callback, true)
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
		} else {
			return fmt.Errorf("sub is not valid")
		}
		delete(n.subs, topic)
	}
	return nil
}

func (n *Client) Request(topic string, request interface{}, data interface{}) error {
	defer n.measure(topic, time.Now())
	n.log.WithField("topic", topic).WithField("request", request).Debug("Request")
	if n.c == nil || !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	response := struct {
		Success bool
		Message string
		Stack   string
		Data    json.RawMessage
	}{}

	if err := n.jsonCon.Request(topic, request, &response, 15*time.Second); err != nil {
		n.log.WithField("topic", topic).Error(err)
		return err
	}
	n.log.WithField("topic", topic).WithField("response", response.Success).WithField("response", response.Message).WithField("data", string(response.Data)).Debug("Response")

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
