package application

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	authorizationv1 "k8s.io/api/authorization/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metainternalversion "k8s.io/apimachinery/pkg/apis/meta/internalversion"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	apirequest "k8s.io/apiserver/pkg/endpoints/request"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/dynamic/dynamicinformer"
	authorizationv1client "k8s.io/client-go/kubernetes/typed/authorization/v1"
	"k8s.io/client-go/rest"
	"k8s.io/klog/v2"

	beehiveContext "github.com/kubeedge/beehive/pkg/core/context"
	"github.com/kubeedge/beehive/pkg/core/model"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/client"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/messagelayer"
	"github.com/kubeedge/kubeedge/cloud/pkg/common/modules"
	"github.com/kubeedge/kubeedge/cloud/pkg/dynamiccontroller/filter"
	commontypes "github.com/kubeedge/kubeedge/common/types"
	"github.com/kubeedge/kubeedge/edge/pkg/common/message"
	edgemodule "github.com/kubeedge/kubeedge/edge/pkg/common/modules"
	metaserverconfig "github.com/kubeedge/kubeedge/edge/pkg/metamanager/metaserver/config"
	kefeatures "github.com/kubeedge/kubeedge/pkg/features"
	"github.com/kubeedge/kubeedge/pkg/metaserver"
)

// used to set Message.Route
const (
	MetaServerSource    = "metaserver"
	ApplicationResource = "Application"
	ApplicationResp     = "applicationResponse"
	Ignore              = "ignore"
)

type applicationStatus string

const (
	// set by agent
	PreApplying applicationStatus = "PreApplying" // application is waiting to be sent to cloud
	InApplying  applicationStatus = "InApplying"  // application is sending to cloud

	// set by center
	InProcessing applicationStatus = "InProcessing" // application is in processing by cloud
	Approved     applicationStatus = "Approved"     // application is approved by cloud
	Rejected     applicationStatus = "Rejected"     // application is rejected by cloud

	// both
	Failed    applicationStatus = "Failed"    // failed to get application resp from cloud
	Completed applicationStatus = "Completed" // application is completed and waiting to be recycled
)

type applicationVerb string

const (
	Get          applicationVerb = "get"
	List         applicationVerb = "list"
	Watch        applicationVerb = "watch"
	Create       applicationVerb = "create"
	Delete       applicationVerb = "delete"
	Update       applicationVerb = "update"
	UpdateStatus applicationVerb = "updatestatus"
	Patch        applicationVerb = "patch"
)

type PatchInfo struct {
	Name         string
	PatchType    types.PatchType
	Data         []byte
	Options      metav1.PatchOptions
	Subresources []string
}

// Application record the resources that are in applying for requesting to be transferred down from the cloud, please:
// 0.use Agent.Generate to generate application
// 1.use Agent.Apply to apply application( generate msg and send it to cloud dynamiccontroller)
type Application struct {
	ID          string
	Key         string // group version resource namespaces name
	Verb        applicationVerb
	Nodename    string
	Status      applicationStatus
	Reason      string // why in this status
	Option      []byte //
	ReqBody     []byte // better a k8s api instance
	RespBody    []byte
	Subresource string
	Error       apierrors.StatusError
	Token       string
	ctx         context.Context // to end app.Wait
	cancel      context.CancelFunc

	count     uint64 // count the number of current citations
	countLock sync.Mutex
	timestamp time.Time // record the last closing time of application, only make sense when count == 0
}

func newApplication(ctx context.Context, key string, verb applicationVerb, nodename, subresource string, option interface{}, reqBody interface{}) (*Application, error) {
	var v1 metav1.ListOptions
	if internal, ok := option.(metainternalversion.ListOptions); ok {
		err := metainternalversion.Convert_internalversion_ListOptions_To_v1_ListOptions(&internal, &v1, nil)
		if err != nil {
			// error here won't happen, log in case
			klog.Errorf("failed to transfer internalListOption to v1ListOption, force set to empty")
		}
		option = v1
	}
	token, ok := ctx.Value(commontypes.AuthorizationKey).(string)
	if !ok {
		klog.Errorf("unsupported Token type :%T", ctx.Value(commontypes.AuthorizationKey))
		return nil, fmt.Errorf("unsupported Token type :%T", ctx.Value(commontypes.AuthorizationKey))
	}
	ctx2, cancel := context.WithCancel(ctx)
	app := &Application{
		Key:         key,
		Verb:        verb,
		Nodename:    nodename,
		Subresource: subresource,
		Status:      PreApplying,
		Option:      toBytes(option),
		ReqBody:     toBytes(reqBody),
		Token:       token,
		ctx:         ctx2,
		cancel:      cancel,
		count:       0,
		countLock:   sync.Mutex{},
		timestamp:   time.Time{},
	}
	app.add()
	return app, nil
}

func (a *Application) Identifier() string {
	if a.ID != "" {
		return a.ID
	}
	b := []byte(a.Nodename)
	b = append(b, []byte(a.Key)...)
	b = append(b, []byte(a.Verb)...)
	b = append(b, a.Option...)
	b = append(b, a.ReqBody...)
	b = append(b, []byte(a.Subresource)...)
	b = append(b, []byte(a.Token)...)
	a.ID = fmt.Sprintf("%x", sha256.Sum256(b))
	return a.ID
}
func (a *Application) String() string {
	return fmt.Sprintf("(NodeName=%v;Key=%v;Verb=%v;Status=%v;Reason=%v)", a.Nodename, a.Key, a.Verb, a.Status, a.Reason)
}
func (a *Application) ReqContent() interface{} {
	return a.ReqBody
}
func (a *Application) RespContent() interface{} {
	return a.RespBody
}

func (a *Application) ToListener(option metav1.ListOptions) *SelectorListener {
	gvr, namespace, _ := metaserver.ParseKey(a.Key)
	selector := NewSelector(option.LabelSelector, option.FieldSelector)
	if namespace != "" {
		selector.Field = fields.AndSelectors(selector.Field, fields.OneTermEqualSelector("metadata.namespace", namespace))
	}
	l := NewSelectorListener(a.Nodename, gvr, selector)
	return l
}

// OptionTo convert application option. Remember `i` must be a pointer to the initialized variable
func (a *Application) OptionTo(i interface{}) error {
	err := json.Unmarshal(a.Option, i)
	if err != nil {
		return fmt.Errorf("failed to prase Option bytes, %v", err)
	}
	return nil
}

func (a *Application) ReqBodyTo(i interface{}) error {
	err := json.Unmarshal(a.ReqBody, i)
	if err != nil {
		return fmt.Errorf("failed to parse ReqBody bytes, %v", err)
	}
	return nil
}

func (a *Application) RespBodyTo(i interface{}) error {
	err := json.Unmarshal(a.RespBody, i)
	if err != nil {
		return fmt.Errorf("failed to parse RespBody bytes, %v", err)
	}
	return nil
}

func (a *Application) GVR() schema.GroupVersionResource {
	gvr, _, _ := metaserver.ParseKey(a.Key)
	return gvr
}
func (a *Application) Namespace() string {
	_, ns, _ := metaserver.ParseKey(a.Key)
	return ns
}

func (a *Application) Call() {
	if a.cancel != nil {
		a.cancel()
	}
}

func (a *Application) getStatus() applicationStatus {
	return a.Status
}

// Wait the result of application after it is applied by application agent
func (a *Application) Wait() {
	if a.ctx != nil {
		<-a.ctx.Done()
	}
}

func (a *Application) Reset() {
	if a.ctx != nil && a.cancel != nil {
		a.cancel()
	}
	a.ctx, a.cancel = context.WithCancel(beehiveContext.GetContext())
	a.Reason = ""
	a.RespBody = []byte{}
}

func (a *Application) add() {
	a.countLock.Lock()
	a.count++
	a.countLock.Unlock()
}

func (a *Application) getCount() uint64 {
	a.countLock.Lock()
	c := a.count
	a.countLock.Unlock()
	return c
}

// Close must be called when applicant no longer using application
func (a *Application) Close() {
	a.countLock.Lock()
	defer a.countLock.Unlock()
	if a.count == 0 {
		return
	}

	a.timestamp = time.Now()
	a.count--
	if a.count == 0 {
		a.Status = Completed
	}
}

func (a *Application) LastCloseTime() time.Time {
	a.countLock.Lock()
	defer a.countLock.Unlock()
	if a.count == 0 && !a.timestamp.IsZero() {
		return a.timestamp
	}
	return time.Time{}
}

// Agent used for generating application and do apply
type Agent struct {
	Applications sync.Map //store struct application
	nodeName     string
}

// NewApplicationAgent create edge agent for list/watch
func NewApplicationAgent() *Agent {
	defaultAgent := &Agent{nodeName: metaserverconfig.Config.NodeName}

	go wait.Until(func() {
		defaultAgent.GC()
	}, time.Minute*5, beehiveContext.Done())

	return defaultAgent
}

func (a *Agent) Generate(ctx context.Context, verb applicationVerb, option interface{}, obj runtime.Object) (*Application, error) {
	key, err := metaserver.KeyFuncReq(ctx, "")
	if err != nil {
		klog.Errorf("%v", err)
		return nil, err
	}

	info, ok := apirequest.RequestInfoFrom(ctx)
	if !ok || !info.IsResourceRequest {
		klog.Errorf("no request info in context")
		return nil, fmt.Errorf("no request info in context")
	}
	app, err := newApplication(ctx, key, verb, a.nodeName, info.Subresource, option, obj)
	if err != nil {
		return nil, err
	}
	store, ok := a.Applications.LoadOrStore(app.Identifier(), app)
	if ok {
		app = store.(*Application)
		app.add()
		return app, nil
	}
	return app, nil
}

func (a *Agent) Apply(app *Application) error {
	store, ok := a.Applications.Load(app.Identifier())
	if !ok {
		return fmt.Errorf("application %v has not been registered to agent", app.String())
	}
	app = store.(*Application)
	switch app.getStatus() {
	case PreApplying:
		go a.doApply(app)
	case Completed:
		app.Reset()
		go a.doApply(app)
	case Rejected:
		return &app.Error
	case Failed:
		return errors.New(app.Reason)
	case Approved:
		return nil
	case InApplying:
		//continue
	}
	app.Wait()
	if app.getStatus() == Rejected {
		return &app.Error
	}
	if app.getStatus() != Approved {
		return errors.New(app.Reason)
	}
	return nil
}

func (a *Agent) doApply(app *Application) {
	defer app.Call()
	// encapsulate as a message
	app.Status = InApplying
	msg := model.NewMessage("").SetRoute(MetaServerSource, modules.DynamicControllerModuleGroup).FillBody(app)
	msg.SetResourceOperation("null", "null")
	resp, err := beehiveContext.SendSync(edgemodule.EdgeHubModuleName, *msg, 10*time.Second)
	if err != nil {
		app.Status = Failed
		app.Reason = fmt.Sprintf("failed to access cloud Application center: %v", err)
		return
	}
	retApp, err := msgToApplication(resp)
	if err != nil {
		app.Status = Failed
		app.Reason = fmt.Sprintf("failed to get Application from resp msg: %v", err)
		return
	}

	//merge returned application to local application
	app.Status = retApp.Status
	app.Reason = retApp.Reason
	app.Error = retApp.Error
	app.RespBody = retApp.RespBody
}

func (a *Agent) GC() {
	a.Applications.Range(func(key, value interface{}) bool {
		app := value.(*Application)
		lastCloseTime := app.LastCloseTime()
		if !lastCloseTime.IsZero() && time.Since(lastCloseTime) >= time.Minute*5 {
			a.Applications.Delete(key)
		}
		return true
	})
}

type Center struct {
	Applications sync.Map
	HandlerCenter
	messageLayer messagelayer.MessageLayer
	authConfig   *rest.Config
}

func NewApplicationCenter(dynamicSharedInformerFactory dynamicinformer.DynamicSharedInformerFactory) *Center {
	a := &Center{
		HandlerCenter: NewHandlerCenter(dynamicSharedInformerFactory),
		authConfig:    client.GetAuthConfig(),
		messageLayer:  messagelayer.DynamicControllerMessageLayer(),
	}
	return a
}

func toBytes(i interface{}) (bytes []byte) {
	if i == nil {
		return
	}

	if bytes, ok := i.([]byte); ok {
		return bytes
	}

	var err error
	if bytes, err = json.Marshal(i); err != nil {
		klog.Errorf("marshal content to []byte failed, err: %v", err)
	}
	return
}

// extract application in message's Content
func msgToApplication(msg model.Message) (*Application, error) {
	var app = new(Application)
	contentData, err := msg.GetContentData()
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(contentData, app)
	if err != nil {
		return nil, err
	}
	return app, nil
}

// Process translate msg to application , process and send resp to edge
// TODO: upgrade to parallel process
func (c *Center) Process(msg model.Message) {
	app, err := msgToApplication(msg)
	if err != nil {
		klog.Errorf("failed to translate msg to Application: %v", err)
		return
	}

	klog.Infof("[metaserver/ApplicationCenter] get a Application %v", app.String())

	resp, err := c.ProcessApplication(app)
	if err != nil {
		c.Response(app, msg.GetID(), Rejected, err, nil)
		klog.Errorf("[metaserver/applicationCenter]failed to process Application(%+v), %v", app, err)
		return
	}
	c.Response(app, msg.GetID(), Approved, nil, resp)
	klog.Infof("[metaserver/applicationCenter]successfully to process Application(%+v)", app)
}

func (c *Center) generateNewConfig(raw string) (*rest.Config, error) {
	parts := strings.SplitN(raw, " ", 3)
	if len(parts) < 2 || strings.ToLower(parts[0]) != "bearer" || len(parts[1]) <= 0 {
		return nil, fmt.Errorf("invalid request token format or length: %v", len(parts))
	}
	authConfig := rest.CopyConfig(c.authConfig)
	authConfig.BearerToken = parts[1]
	return authConfig, nil
}

func (c *Center) createAuthClient(app *Application) (authorizationv1client.AuthorizationV1Interface, error) {
	authConfig, err := c.generateNewConfig(app.Token)
	if err != nil {
		return nil, err
	}
	return authorizationv1client.NewForConfigOrDie(authConfig), nil
}

func (c *Center) createKubeClient(app *Application) (dynamic.Interface, error) {
	if !kefeatures.DefaultFeatureGate.Enabled(kefeatures.RequireAuthorization) {
		return client.GetDynamicClient(), nil
	}
	authConfig, err := c.generateNewConfig(app.Token)
	if err != nil {
		return nil, err
	}
	return dynamic.NewForConfigOrDie(authConfig), nil
}

func (c *Center) authorizeApplication(app *Application, gvr schema.GroupVersionResource, namespace string, name string) error {
	if !kefeatures.DefaultFeatureGate.Enabled(kefeatures.RequireAuthorization) {
		return nil
	}
	tmpAuthClient, err := c.createAuthClient(app)
	if err != nil {
		return err
	}
	sar := &authorizationv1.SelfSubjectAccessReview{
		Spec: authorizationv1.SelfSubjectAccessReviewSpec{
			ResourceAttributes: &authorizationv1.ResourceAttributes{
				Namespace:   namespace,
				Verb:        string(app.Verb),
				Group:       gvr.Group,
				Resource:    gvr.Resource,
				Name:        name,
				Subresource: app.Subresource,
			},
		},
	}
	response, err := tmpAuthClient.SelfSubjectAccessReviews().Create(context.TODO(), sar, metav1.CreateOptions{})
	if err != nil {
		return err
	}
	if response.Status.Allowed {
		return nil
	}
	var errMsg = fmt.Sprintf("resource %v authorize failed.", gvr)
	if len(response.Status.Reason) > 0 {
		errMsg += fmt.Sprintf("reason: %v.", response.Status.Reason)
	}
	if len(response.Status.EvaluationError) > 0 {
		errMsg += fmt.Sprintf("evaluation error: %v.", response.Status.EvaluationError)
	}
	return fmt.Errorf(errMsg)
}

// ProcessApplication processes application by re-translating it to kube-api request with kube client,
// which will be processed and responded by apiserver eventually.
// Specially if app.verb == watch, it transforms app to a listener and register it to HandlerCenter, rather
// than request to apiserver directly. Listener will then continuously listen kube-api change events and
// push them to edge node.
func (c *Center) ProcessApplication(app *Application) (interface{}, error) {
	app.Status = InProcessing
	gvr, ns, name := metaserver.ParseKey(app.Key)
	var kubeClient dynamic.Interface
	var err error
	if app.Verb != Watch {
		kubeClient, err = c.createKubeClient(app)
		if err != nil {
			klog.Errorf("create kube client error: %v", err)
			return nil, err
		}
	} else {
		err := c.authorizeApplication(app, gvr, ns, name)
		if err != nil {
			klog.Errorf("authorize application error: %v", err)
			return nil, err
		}
	}

	switch app.Verb {
	case List:
		var option = new(metav1.ListOptions)
		if err := app.OptionTo(option); err != nil {
			return nil, err
		}
		list, err := kubeClient.Resource(gvr).Namespace(ns).List(context.TODO(), *option)
		if err != nil {
			return nil, fmt.Errorf("get current list error: %v", err)
		}
		return list, nil
	case Watch:
		var option = new(metav1.ListOptions)
		if err := app.OptionTo(option); err != nil {
			return nil, err
		}
		if err := c.HandlerCenter.AddListener(app.ToListener(*option)); err != nil {
			return nil, fmt.Errorf("failed to add listener, %v", err)
		}
		return nil, nil
	case Get:
		var option = new(metav1.GetOptions)
		if err := app.OptionTo(option); err != nil {
			return nil, err
		}
		retObj, err := kubeClient.Resource(gvr).Namespace(ns).Get(context.TODO(), name, *option)
		if err != nil {
			return nil, err
		}
		return retObj, nil
	case Create:
		var option = new(metav1.CreateOptions)
		if err := app.OptionTo(option); err != nil {
			return nil, err
		}
		var obj = new(unstructured.Unstructured)
		if err := app.ReqBodyTo(obj); err != nil {
			return nil, err
		}
		var retObj interface{}
		var err error
		if app.Subresource == "" {
			retObj, err = kubeClient.Resource(gvr).Namespace(ns).Create(context.TODO(), obj, *option)
		} else {
			retObj, err = kubeClient.Resource(gvr).Namespace(ns).Create(context.TODO(), obj, *option, app.Subresource)
		}
		if err != nil {
			return nil, err
		}
		return retObj, err
	case Delete:
		var option = new(metav1.DeleteOptions)
		if err := app.OptionTo(&option); err != nil {
			return nil, err
		}
		if err := kubeClient.Resource(gvr).Namespace(ns).Delete(context.TODO(), name, *option); err != nil {
			return nil, err
		}
		return nil, nil
	case Update:
		var option = new(metav1.UpdateOptions)
		if err := app.OptionTo(option); err != nil {
			return nil, err
		}
		var obj = new(unstructured.Unstructured)
		if err := app.ReqBodyTo(obj); err != nil {
			return nil, err
		}
		var retObj interface{}
		var err error
		if app.Subresource == "" {
			retObj, err = kubeClient.Resource(gvr).Namespace(ns).Update(context.TODO(), obj, *option)
		} else {
			retObj, err = kubeClient.Resource(gvr).Namespace(ns).Update(context.TODO(), obj, *option, app.Subresource)
		}
		if err != nil {
			return nil, err
		}
		return retObj, nil
	case UpdateStatus:
		var option = new(metav1.UpdateOptions)
		if err := app.OptionTo(option); err != nil {
			return nil, err
		}
		var obj = new(unstructured.Unstructured)
		if err := app.ReqBodyTo(obj); err != nil {
			return nil, err
		}
		retObj, err := kubeClient.Resource(gvr).Namespace(ns).UpdateStatus(context.TODO(), obj, *option)
		if err != nil {
			return nil, err
		}
		return retObj, nil
	case Patch:
		var pi = new(PatchInfo)
		if err := app.OptionTo(pi); err != nil {
			return nil, err
		}
		retObj, err := kubeClient.Resource(gvr).Namespace(ns).Patch(context.TODO(), pi.Name, pi.PatchType, pi.Data, pi.Options, pi.Subresources...)
		if err != nil {
			return nil, err
		}
		return retObj, nil
	default:
		return nil, fmt.Errorf("unsupported Application Verb type :%v", app.Verb)
	}
}

// Response update application, generate and send resp message to edge
func (c *Center) Response(app *Application, parentID string, status applicationStatus, err error, respContent interface{}) {
	app.Status = status
	if err != nil {
		apierr, ok := err.(apierrors.APIStatus)
		if ok {
			app.Error = apierrors.StatusError{ErrStatus: apierr.Status()}
		} else {
			app.Reason = err.Error()
		}
	}
	if respContent != nil {
		if app.Verb == List || app.Verb == Get {
			filter.MessageFilter(respContent, app.Nodename)
		}
		app.RespBody = toBytes(respContent)
	}

	resource, err := messagelayer.BuildResource(app.Nodename, Ignore, ApplicationResource, Ignore)
	if err != nil {
		klog.Warningf("built message resource failed with error: %s", err)
		return
	}
	msg := model.NewMessage(parentID).
		BuildRouter(modules.DynamicControllerModuleName, message.ResourceGroupName, resource, ApplicationResp).
		FillBody(app)

	if err := c.messageLayer.Response(*msg); err != nil {
		klog.Warningf("send message failed with error: %s, operation: %s, resource: %s", err, msg.GetOperation(), msg.GetResource())
		return
	}
	klog.V(4).Infof("send message successfully, operation: %s, resource: %s", msg.GetOperation(), msg.GetResource())
}

func (c *Center) GC() {

}
