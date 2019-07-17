package main

import (
	"context"
	"io"

	// "github.com/k0kubun/pp"
	"github.com/mongodb/ftdc/bsonx"
	"github.com/mongodb/grip"
	"github.com/mongodb/jasper"
	"github.com/pkg/errors"
	"github.com/tychoish/mongorpc"
	mongorpcBson "github.com/tychoish/mongorpc/bson"
	"github.com/tychoish/mongorpc/mongowire"
	"gopkg.in/mgo.v2/bson"
)

type Service struct {
	manager jasper.Manager
}

func NewManagerService(m jasper.Manager) *Service {
	return &Service{
		manager: m,
	}
}

func handleIsMaster(ctx context.Context, w io.Writer, msg mongowire.Message) {
	ok := bsonx.EC.Int32("ok", 1)
	doc := bsonx.NewDocument(ok)
	grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to ismaster"))
}

func handleBuildInfo(ctx context.Context, w io.Writer, msg mongowire.Message) {
	version := bsonx.EC.String("version", "0.0.0")
	doc := bsonx.NewDocument(version)
	grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to buildinfo"))
}

func handleGetLog(ctx context.Context, w io.Writer, msg mongowire.Message) {
	logs := bsonx.EC.ArrayFromElements("log", bsonx.VC.ArrayFromValues(bsonx.VC.String("hello")))
	ok := bsonx.EC.Int32("ok", 1)
	doc := bsonx.NewDocument(ok, logs)
	grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to getLog"))
}

func handleGetFreeMonitoringStatus(ctx context.Context, w io.Writer, msg mongowire.Message) {
	ok := bsonx.EC.Int32("ok", 0)
	doc := bsonx.NewDocument(ok)
	grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to getFreeMonitoringStatus"))
}

func handleReplSetGetStatus(ctx context.Context, w io.Writer, msg mongowire.Message) {
	ok := bsonx.EC.Int32("ok", 0)
	doc := bsonx.NewDocument(ok)
	grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to replSetGetStatus"))
}

func handleListCollections(ctx context.Context, w io.Writer, msg mongowire.Message) {
	ok := bsonx.EC.Int32("ok", 0)
	doc := bsonx.NewDocument(ok)
	grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to listCollections"))
}

func (s *Service) handleList(ctx context.Context, w io.Writer, msg mongowire.Message) {
	cmdMsg, ok := msg.(*mongowire.CommandMessage)
	if !ok {
		grip.Error(errors.New("received unexpected mongo message"))
		return
	}
	cmdMsgDoc, err := bsonx.ReadDocument(cmdMsg.CommandArgs.BSON)
	if err != nil {
		grip.Error(errors.New("received unexpected mongo message"))
		return
	}
	cmdListArgs := cmdMsgDoc.Lookup("list")
	listString, ok := cmdListArgs.StringValueOK()
	if !ok {
		grip.Error(errors.New("received unexpected mongo message"))
		return
	}
	list, err := s.manager.List(ctx, jasper.Filter(listString))
	if err != nil {
		grip.Error(errors.New("received unexpected mongo message"))
		return
	}
	array := bsonx.MakeArray(len(list))
	for _, proc := range list {
		processBSON, err := bson.Marshal(proc.Info(ctx))
		if err != nil {
			grip.Error(err)
			return
		}
		processDoc, err := bsonx.ReadDocument(processBSON)
		if err != nil {
			grip.Error(err)
			return
		}
		processValueDoc := bsonx.VC.Document(processDoc)
		array.Append(processValueDoc)
	}
	responseOk := bsonx.EC.Int32("ok", 1)
	arrayDoc := bsonx.EC.Array("processes", array)
	doc := bsonx.NewDocument(responseOk, arrayDoc)
	grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to list"))
}

func (s *Service) handleCreateProcess(ctx context.Context, w io.Writer, msg mongowire.Message) {
	cmdMsg, ok := msg.(*mongowire.CommandMessage)
	if !ok {
		grip.Error(errors.New("received unexpected mongo message"))
		return
	}
	cmdMsgDoc, err := bsonx.ReadDocument(cmdMsg.CommandArgs.BSON)
	if err != nil {
		grip.Error(errors.New("received unexpected mongo message"))
		return
	}
	cmdMessageCreateProcessArgs := cmdMsgDoc.Lookup("createProcess")
	subDoc, subDocOk := cmdMessageCreateProcessArgs.MutableDocumentOK()
	if !subDocOk {
		grip.Error(errors.New("could not parse document from createProcess argument"))
		return
	}
	byteArray, err := subDoc.MarshalBSON()
	if err != nil {
		grip.Error(errors.New("couldn't marshall bson"))
		return
	}
	options := jasper.CreateOptions{}
	err = bson.Unmarshal(byteArray, &options)
	if err != nil {
		grip.Error(err)
		return
	}
	process, err := s.manager.CreateProcess(ctx, &options)
	if err != nil {
		grip.Error(err)
		return
	}
	processBSON, err := bson.Marshal(process.Info(ctx))
	if err != nil {
		grip.Error(err)
		return
	}
	responseOk := bsonx.EC.Int32("ok", 1)
	processDoc, err := bsonx.ReadDocument(processBSON)
	if err != nil {
		grip.Error(err)
		return
	}
	processSubDoc := bsonx.EC.SubDocument("info", processDoc)
	doc := bsonx.NewDocument(responseOk, processSubDoc)
	grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to createProcess"))
}

func writeReply(doc *bsonx.Document, w io.Writer) error {
	resp, err := doc.MarshalBSON()
	if err != nil {
		return errors.Wrap(err, "problem marshalling response")
	}
	respDoc := mongorpcBson.Simple{BSON: resp, Size: int32(len(resp))}

	reply := mongowire.NewReply(int64(0), int32(0), int32(0), int32(1), []mongorpcBson.Simple{respDoc})
	_, err = w.Write(reply.Serialize())
	return errors.Wrap(err, "could not write response")
}

func (s *Service) RegisterHandlers(host string, port int) (*mongorpc.Service, error) {
	srv := mongorpc.NewService(host, port)

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "admin",
		Command: "isMaster",
	}, handleIsMaster); err != nil {
		return nil, errors.Wrap(err, "could not register handler for isMaster")
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "test",
		Command: "isMaster",
	}, handleIsMaster); err != nil {
		return nil, errors.Wrap(err, "could not register handler for isMaster")
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "admin",
		Command: "whatsmyuri",
	}, func(ctx context.Context, w io.Writer, msg mongowire.Message) {
		uri := bsonx.EC.String("you", "localhost:12345")
		doc := bsonx.NewDocument(uri)
		grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to whatsmyuri"))
	}); err != nil {
		return nil, errors.Wrap(err, "could not register handler for whatsmyuri")
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "admin",
		Command: "buildinfo",
	}, handleBuildInfo); err != nil {
		return nil, errors.Wrap(err, "could not register handler for buildinfo")
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "test",
		Command: "buildInfo",
	}, handleBuildInfo); err != nil {
		return nil, errors.Wrap(err, "could not register handler for buildinfo")
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "admin",
		Command: "getLog",
	}, handleGetLog); err != nil {
		return nil, errors.Wrap(err, "could not register handler for getLog")
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "test",
		Command: "getLog",
	}, handleGetLog); err != nil {
		return nil, errors.Wrap(err, "could not register handler for getLog")
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "admin",
		Command: "getFreeMonitoringStatus",
	}, handleGetFreeMonitoringStatus); err != nil {
		return nil, errors.Wrap(err, "could not register handler for getFreeMonitoringStatus")
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "admin",
		Command: "replSetGetStatus",
	}, handleReplSetGetStatus); err != nil {
		return nil, errors.Wrap(err, "could not register handler for replSetGetStatus")
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "test",
		Command: "listCollections",
	}, handleListCollections); err != nil {
		return nil, errors.Wrap(err, "could not register handler for listCollections")
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "test",
		Command: "createProcess",
	}, s.handleCreateProcess); err != nil {
		return nil, errors.Wrap(err, "could not register handler for createProcess")
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "test",
		Command: "list",
	}, s.handleList); err != nil {
		return nil, errors.Wrap(err, "could not register handler for createProcess")
	}

	return srv, nil
}

func main() {
	mgr, err := jasper.NewLocalManager(false)
	if err != nil {
		grip.Error(err)
		return
	}
	srv := NewManagerService(mgr)
	runningSrv, err := srv.RegisterHandlers("localhost", 12345)
	if err != nil {
		grip.Error(err)
		return
	}
	ctx, cancel := context.WithCancel(context.Background())
	grip.Error(runningSrv.Run(ctx))
	defer cancel()
}
