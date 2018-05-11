package nats

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/go-nats"
	"github.com/nats-io/go-nats/encoders/protobuf"
	"github.com/rollbar/rollbar-go"
	"github.com/sirupsen/logrus"
)

const connectionRetries = 10

type natsCB func(topic, reply string, msg map[string]interface{})
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
	Debug    bool
}

// New returns new instance of NATS client
func New(url, instance string) *Client {
	n := &Client{subs: make(map[string]*nats.Subscription)}
	n.url = url
	n.opts = append(n.opts, nats.ErrorHandler(func(_ *nats.Conn, s *nats.Subscription, e error) {
		n.log.WithField("subj", s.Subject).Error(e)
		if e == nats.ErrSlowConsumer {
			pendingMsgs, _, err := s.Pending()
			if err != nil {
				n.log.Errorf("couldn't get pending messages: %v", err)
				return
			}
			n.log.Errorf("Falling behind with %d pending messages on subject %q.\n",
				pendingMsgs, s.Subject)
		}
	}))
	n.opts = append(n.opts, nats.DisconnectHandler(func(nc *nats.Conn) {
		n.log.WithField("err", nc.LastError()).Warn("disconnected")
	}))
	n.opts = append(n.opts, nats.ReconnectHandler(func(nc *nats.Conn) {
		n.log.WithField("err", nc.LastError()).Info("reconnected to ", nc.ConnectedUrl())
	}))
	go n.connect()
	n.subs = make(map[string]*nats.Subscription)
	n.cbs = make(map[string]func(msg *nats.Msg))
	n.cbse = make(map[string]natsCB)
	n.log = logrus.WithField("service", "NATS").WithField("instance", instance)
	n.Debug = true
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
					n.log.Error("error connecting to NATS server: ", err.Error())
					i = 0
				}
			} else {
				retry.Stop()
				n.c = natsConnection
				n.log.WithField("broker", n.c.ConnectedUrl()).Info("NATS Client connected")

				if n.jsonCon, err = nats.NewEncodedConn(n.c, nats.JSON_ENCODER); err != nil {
					n.log.Error("error creating NATS encoded connection: ", err.Error())
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
		n.log.Error("error connecting to NATS server: ", err.Error())
		n.retry()
	} else {
		n.c = natsConnection
		n.log.WithField("broker", n.c.ConnectedUrl()).Info("NATS client connected")
		if n.jsonCon, err = nats.NewEncodedConn(n.c, nats.JSON_ENCODER); err != nil {
			n.log.Error("error creating nats json encoded connection: ", err.Error())
		}
		if n.protoCon, err = nats.NewEncodedConn(n.c, protobuf.PROTOBUF_ENCODER); err != nil {
			n.log.Error("error creating nats protobuf encoded connection: ", err.Error())
		}
	}
}

func (n *Client) SwitchDebug(enable bool) *Client {
	n.mtx.Lock()
	defer n.mtx.Unlock()
	n.Debug = enable
	return n
}

func (n *Client) debug(topic string, payload interface{}, message string) {
	if !n.Debug {
		return
	}
	n.log.WithField("topic", topic).WithField("payload", payload).Debug(message)
}

func (n *Client) handlePanic() {
	//panic handler
	if err := recover(); err != nil {
		switch val := err.(type) {
		case error:
			rollbar.ErrorWithStackSkip("critical", val, 4)
		default:
			rollbar.Critical(val)
		}
		n.log.Error("recovered from: ", err)
	}
}

func (n *Client) Publish(topic string, payload interface{}) error {
	if n.c == nil || !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}

	n.debug(topic, payload, "publish")

	if err := n.jsonCon.Publish(topic, payload); err != nil {
		n.log.Error("error publishing to nats broker:", err.Error())
		return err
	}
	return nil
}

func (n *Client) ProtoPublish(topic string, v interface{}) error {
	if !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}
	if err := n.protoCon.Publish(topic, v); err != nil {
		n.log.Error(err)
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
	defer n.handlePanic()
	n.debug(msg.Reply, string(msg.Data), msg.Subject)
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
	defer n.handlePanic()
	n.debug(reply, msg, topic)
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

// Internal implementation that all public functions will use.
func (n *Client) subscribe(subject string, cb nats.Handler, protobuf bool) error {
	defer n.handlePanic()
	if CB, ok := cb.(func(*nats.Msg)); ok {
		return n.PlainSubscribe(subject, CB)
	}
	n.mtx.Lock()
	defer n.mtx.Unlock()
	if n.c == nil || !n.c.IsConnected() {
		return fmt.Errorf("NATS client is not connected")
	}

	argType, numArgs := argInfo(cb)

	cbValue := reflect.ValueOf(cb)

	conn := n.jsonCon
	if protobuf {
		conn = n.protoCon
	}

	natsCB := func(m *nats.Msg) {
		defer n.handlePanic()
		var oV []reflect.Value

		var oPtr reflect.Value
		if argType.Kind() != reflect.Ptr {
			oPtr = reflect.New(argType)
		} else {
			oPtr = reflect.New(argType.Elem())
		}

		if err := conn.Enc.Decode(m.Subject, m.Data, oPtr.Interface()); err != nil {
			n.log.WithField("subject", m.Subject).Error(err)
			return
		}
		if argType.Kind() != reflect.Ptr {
			oPtr = reflect.Indirect(oPtr)
		}

		// Callback Arity
		switch numArgs {
		case 1:
			oV = []reflect.Value{oPtr}
		case 2:
			subV := reflect.ValueOf(m.Subject)
			oV = []reflect.Value{subV, oPtr}
		case 3:
			subV := reflect.ValueOf(m.Subject)
			replyV := reflect.ValueOf(m.Reply)
			oV = []reflect.Value{subV, replyV, oPtr}
		}

		cbValue.Call(oV)
	}

	sub, err := n.c.QueueSubscribe(subject, "distributed-queue-encoded", natsCB)
	if err != nil {
		n.log.Error(err)
		return err
	}
	if sub.IsValid() && !n.isSubscribed(subject) {
		n.subs[subject] = sub
	}
	return nil
}

// SubscribeTo unsafe subscriber with auto unmarshal
func (n *Client) Subscribe(topic string, callback nats.Handler) error {
	return n.subscribe(topic, callback, false)
}

func (n *Client) ProtoSubscribe(topic string, callback nats.Handler) error {
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
	n.debug(topic, request, "request")
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
	n.debug(topic, string(response.Data), "response")

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

// Dissect the cb Handler's signature
func argInfo(cb nats.Handler) (reflect.Type, int) {
	cbType := reflect.TypeOf(cb)
	if cbType.Kind() != reflect.Func {
		panic("nats: Handler needs to be a func")
	}
	numArgs := cbType.NumIn()
	if numArgs == 0 {
		return nil, numArgs
	}
	return cbType.In(numArgs - 1), numArgs
}
