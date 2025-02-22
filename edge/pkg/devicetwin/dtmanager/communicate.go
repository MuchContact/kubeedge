package dtmanager

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"k8s.io/klog/v2"

	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	connect "github.com/kubeedge/kubeedge/edge/pkg/common/cloudconnection"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dtcommon"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dtcontext"
	"github.com/kubeedge/kubeedge/edge/pkg/devicetwin/dttype"
)

var (
	//ActionCallBack map for action to callback
	ActionCallBack map[string]CallBack
)

func init() {
	initActionCallBack()
}

//CommWorker deal app response event
type CommWorker struct {
	Worker
	Group string
}

//Start worker
func (cw CommWorker) Start() {
	for {
		select {
		case msg, ok := <-cw.ReceiverChan:
			klog.V(2).Info("receive msg commModule")
			if !ok {
				return
			}
			if dtMsg, isDTMessage := msg.(*dttype.DTMessage); isDTMessage {
				if fn, exist := ActionCallBack[dtMsg.Action]; exist {
					err := fn(cw.DTContexts, dtMsg.Identity, dtMsg.Msg)
					if err != nil {
						klog.Errorf("CommModule deal %s event failed: %v", dtMsg.Action, err)
					}
				} else {
					klog.Errorf("CommModule deal %s event failed, not found callback", dtMsg.Action)
				}
			}

		case <-time.After(time.Duration(60) * time.Second):
			cw.checkConfirm(cw.DTContexts)
		case v, ok := <-cw.HeartBeatChan:
			if !ok {
				return
			}
			if err := cw.DTContexts.HeartBeat(cw.Group, v); err != nil {
				return
			}
		}
	}
}

func initActionCallBack() {
	ActionCallBack = make(map[string]CallBack)
	ActionCallBack[dtcommon.SendToCloud] = dealSendToCloud
	ActionCallBack[dtcommon.SendToEdge] = dealSendToEdge
	ActionCallBack[dtcommon.LifeCycle] = dealLifeCycle
	ActionCallBack[dtcommon.Confirm] = dealConfirm
}

func dealSendToEdge(context *dtcontext.DTContext, resource string, msg interface{}) error {
	message, ok := msg.(*model.Message)
	if !ok {
		return fmt.Errorf("msg type is %T and not Message type", msg)
	}

	beehiveContext.Send(dtcommon.EventHubModule, *message)
	return nil
}
func dealSendToCloud(context *dtcontext.DTContext, resource string, msg interface{}) error {
	if strings.Compare(context.State, dtcommon.Disconnected) == 0 {
		klog.Infof("Disconnected with cloud, not send msg to cloud")
		return nil
	}
	message, ok := msg.(*model.Message)
	if !ok {
		return errors.New("msg not Message type")
	}
	beehiveContext.Send(dtcommon.HubModule, *message)
	msgID := message.GetID()
	context.ConfirmMap.Store(msgID, &dttype.DTMessage{Msg: message, Action: dtcommon.SendToCloud, Type: dtcommon.CommModule})
	return nil
}
func dealLifeCycle(context *dtcontext.DTContext, resource string, msg interface{}) error {
	klog.V(2).Infof("CONNECTED EVENT")
	message, ok := msg.(*model.Message)
	if !ok {
		return errors.New("msg not Message type")
	}
	connectedInfo, _ := message.Content.(string)
	if strings.Compare(connectedInfo, connect.CloudConnected) == 0 {
		if strings.Compare(context.State, dtcommon.Disconnected) == 0 {
			err := detailRequest(context)
			if err != nil {
				klog.Errorf("detail request: %v", err)
				return err
			}
		}
		context.State = dtcommon.Connected
	} else if strings.Compare(connectedInfo, connect.CloudDisconnected) == 0 {
		context.State = dtcommon.Disconnected
	}
	return nil
}
func dealConfirm(context *dtcontext.DTContext, resource string, msg interface{}) error {
	klog.V(2).Infof("CONFIRM EVENT")
	value, ok := msg.(*model.Message)

	if ok {
		parentMsgID := value.GetParentID()
		klog.Infof("CommModule deal confirm msgID %s", parentMsgID)
		context.ConfirmMap.Delete(parentMsgID)
	} else {
		return errors.New("CommModule deal confirm, type not correct")
	}
	return nil
}

func detailRequest(context *dtcontext.DTContext) error {
	getDetail := dttype.GetDetailNode{
		EventType: "group_membership_event",
		EventID:   uuid.New().String(),
		Operation: "detail",
		GroupID:   context.NodeName,
		TimeStamp: time.Now().UnixNano() / 1000000}
	getDetailJSON, marshalErr := json.Marshal(getDetail)
	if marshalErr != nil {
		klog.Errorf("Marshal request error while request detail, err: %#v", marshalErr)
		return marshalErr
	}

	message := context.BuildModelMessage("resource", "", "membership/detail", "get", string(getDetailJSON))
	klog.V(2).Info("Request detail")
	msgID := message.GetID()
	if message.GetParentID() != "" {
		context.ConfirmMap.Store(msgID, &dttype.DTMessage{Msg: message, Action: dtcommon.SendToCloud, Type: dtcommon.CommModule})
	}
	beehiveContext.Send(dtcommon.HubModule, *message)
	return nil
}

func (cw CommWorker) checkConfirm(context *dtcontext.DTContext) {
	klog.V(2).Info("CheckConfirm")
	context.ConfirmMap.Range(func(key interface{}, value interface{}) bool {
		dtmsg, ok := value.(*dttype.DTMessage)
		klog.V(2).Info("has msg")
		if !ok {
			klog.Warningf("confirm map key %s 's value is not the *DTMessage type", key.(string))
			return true
		}
		klog.V(2).Info("redo task due to no recv")
		if fn, exist := ActionCallBack[dtmsg.Action]; exist {
			if err := fn(cw.DTContexts, dtmsg.Identity, dtmsg.Msg); err != nil {
				klog.Errorf("CommModule deal %s event failed: %v", dtmsg.Action, err)
			}
		} else {
			klog.Errorf("CommModule deal %s event failed, not found callback", dtmsg.Action)
		}
		return true
	})
}
