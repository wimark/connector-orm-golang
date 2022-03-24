package orm

import (
	"encoding/json"
	"log"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	uuid "github.com/google/uuid"
	cache "github.com/patrickmn/go-cache"
	wimark "github.com/wimark/libwimark"
)

type MQTTData struct {
	BrokerAddr   string
	DbID         string
	SenderModule wimark.Module
	SenderID     string
}

type StatefulORM struct {
	connected bool
	broker    mqtt.Client
	mqtt_info MQTTData
	logger    *log.Logger
	cache     *cache.Cache
	timeout   time.Duration
}

func (self *StatefulORM) Connect(mqtt_info MQTTData, logger *log.Logger, request_timeout time.Duration) error {
	var broker mqtt.Client
	{
		var err error
		broker, err = wimark.MQTTConnectSync(mqtt_info.BrokerAddr)
		if err != nil {
			return err
		}
	}
	{
		var err = wimark.MQTTSubscribeSync(broker, []wimark.Topic{
			wimark.ResponseTopic{
				SenderModule:   wimark.ModuleDB,
				SenderID:       mqtt_info.DbID,
				ReceiverModule: mqtt_info.SenderModule,
				ReceiverID:     mqtt_info.SenderID,
				RequestID:      wimark.MQTT_ANY_WILDCARD,
			},
		}, func(msg mqtt.Message) {
			var t, err = wimark.ParseResponseTopic(msg.Topic())
			if err == nil {
				var cb, ok = self.cache.Get(t.RequestID)
				if ok {
					cb.(func([]byte))(msg.Payload())
				}
			}
		})
		if err != nil {
			return err
		}
	}
	self.mqtt_info = mqtt_info
	self.broker = broker
	self.logger = logger
	self.timeout = request_timeout
	self.cache = cache.New(request_timeout, request_timeout+time.Duration(1)*time.Minute)
	self.connected = true

	return nil
}

func (self *StatefulORM) mqttExec(op wimark.Operation, request interface{}, data_ptr interface{}) []wimark.ModelError {
	var reqid = uuid.New().String()
	var reqTopic = wimark.RequestTopic{
		SenderModule:   self.mqtt_info.SenderModule,
		SenderID:       self.mqtt_info.SenderID,
		ReceiverModule: wimark.ModuleDB,
		ReceiverID:     self.mqtt_info.DbID,
		RequestID:      reqid,
		Operation:      op,
	}
	var rspTopic = reqTopic.ToResponse()
	var data_chan = make(chan []byte)
	{
		self.cache.Set(reqid, func(data []byte) { data_chan <- data }, self.timeout)
		var err = wimark.MQTTSubscribeSync(self.broker, []wimark.Topic{rspTopic}, func(msg mqtt.Message) { data_chan <- msg.Payload() })
		if err != nil {
			return []wimark.ModelError{
				wimark.ModelError{
					Type:        wimark.WimarkErrorCodeMqtt,
					Description: err.Error(),
				},
			}
		}
	}
	defer wimark.MQTTMustUnsubscribeSync(self.broker, []wimark.Topic{rspTopic})
	{
		var _, reqErr = json.Marshal(request)
		if reqErr != nil {
			return []wimark.ModelError{
				wimark.ModelError{
					Type:        wimark.WimarkErrorCodeJson,
					Description: reqErr.Error(),
				},
			}
		}

		var err = wimark.MQTTPublishMsg(self.broker, wimark.MQTTDocumentMessage{
			T: reqTopic,
			D: request,
			R: false,
		})
		if err != nil {
			return []wimark.ModelError{
				wimark.ModelError{
					Type:        wimark.WimarkErrorCodeMqtt,
					Description: err.Error(),
				},
			}
		}
	}
	select {
	case data_raw := <-data_chan:

		var err = json.Unmarshal(data_raw, &data_ptr)
		if err != nil {
			return []wimark.ModelError{
				wimark.ModelError{
					Type:        wimark.WimarkErrorCodeJson,
					Description: err.Error(),
				},
			}
		}

	case <-time.Tick(self.timeout):
		return []wimark.ModelError{
			wimark.ModelError{
				Type:        wimark.WimarkErrorCodeDB,
				Description: "Timed out waiting for DB reply",
			},
		}

	}

	return []wimark.ModelError{}
}

func (self *StatefulORM) Create(request wimark.DBRequestC) wimark.DBResponseC {
	var data wimark.DBResponseC
	data.Errors = self.mqttExec(wimark.OperationCreate, &request, &data)
	return data
}
func (self *StatefulORM) Read(request wimark.DBRequestR) wimark.DBResponseR {
	var data wimark.DBResponseR
	data.Errors = self.mqttExec(wimark.OperationRead, &request, &data)
	return data
}
func (self *StatefulORM) Update(request wimark.DBRequestU) wimark.DBResponseU {
	var data wimark.DBResponseU
	data.Errors = self.mqttExec(wimark.OperationUpdate, &request, &data)
	return data
}
func (self *StatefulORM) Delete(request wimark.DBRequestD) wimark.DBResponseD {
	var data wimark.DBResponseD
	data.Errors = self.mqttExec(wimark.OperationDelete, &request, &data)
	return data
}
