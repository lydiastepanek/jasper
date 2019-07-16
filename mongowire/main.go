package main

import (
	"context"
	"io"

	"github.com/k0kubun/pp"
	"github.com/mongodb/ftdc/bsonx"
	"github.com/mongodb/grip"
	// "github.com/mongodb/jasper"
	"github.com/pkg/errors"
	"github.com/tychoish/mongorpc"
	"github.com/tychoish/mongorpc/bson"
	"github.com/tychoish/mongorpc/mongowire"
)

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
	logs := bsonx.EC.String("logs", "hello")
	doc := bsonx.NewDocument(logs)
	grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to getLog"))
}

func handleGetFreeMonitoringStatus(ctx context.Context, w io.Writer, msg mongowire.Message) {
	ok := bsonx.EC.Int32("ok", 0)
	doc := bsonx.NewDocument(ok)
	grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to getFreeMonitoringStatus"))
}

func handleStartProcess(ctx context.Context, w io.Writer, msg mongowire.Message) {
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
	cmdMessageStartProcessArgs := cmdMsgDoc.Lookup("startProcess")
	// convert cmdMessageStartProcessArgs which is a value to a document
	subDoc, subDocOk := bsonx.MutableDocumentOK(cmdMessageStartProcessArgs)
	if subDocOk {
	}
	if cmdMessageStartProcessArgs Document
	err = bson.Unmarshal(cmdMessageStartProcessArgs, &CreateOptions)
	// if err != nil {
	// return err
	// }
	// unmarshall data and grab {myFakeData: "hi"} and convert it to CreateOptions
	// pass CreateOptions to CreateProcess
	responseOk := bsonx.EC.Int32("ok", 0)
	doc := bsonx.NewDocument(responseOk)
	grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to getFreeMonitoringStatus"))
}

func writeReply(doc *bsonx.Document, w io.Writer) error {
	resp, err := doc.MarshalBSON()
	if err != nil {
		return errors.Wrap(err, "problem marshalling response")
	}
	respDoc := bson.Simple{BSON: resp, Size: int32(len(resp))}

	reply := mongowire.NewReply(int64(0), int32(0), int32(0), int32(1), []bson.Simple{respDoc})
	_, err = w.Write(reply.Serialize())
	return errors.Wrap(err, "could not write response")
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	srv := mongorpc.NewService("localhost", 12345)

	// db.runCommand({whatever})
	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_QUERY,
		Context: "test.$cmd",
	}, func(ctx context.Context, w io.Writer, msg mongowire.Message) {
	}); err != nil {
		grip.Error(err)
		return
	}

	// db.runCommand({isMaster: 1})
	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "admin",
		Command: "isMaster",
	}, handleIsMaster); err != nil {
		grip.Error(errors.Wrap(err, "could not register handler for isMaster"))
		return
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "test",
		Command: "isMaster",
	}, handleIsMaster); err != nil {
		grip.Error(errors.Wrap(err, "could not register handler for isMaster"))
		return
	}

	// db.runCommand({whatsmyuri: 1})
	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "admin",
		Command: "whatsmyuri",
	}, func(ctx context.Context, w io.Writer, msg mongowire.Message) {
		uri := bsonx.EC.String("you", "localhost:12345")
		doc := bsonx.NewDocument(uri)
		grip.Error(errors.Wrap(writeReply(doc, w), "could not make response to whatsmyuri"))
	}); err != nil {
		grip.Error(errors.Wrap(err, "could not register handler for whatsmyuri"))
		return
	}

	// db.runCommand({buildinfo: 1})
	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "admin",
		Command: "buildinfo",
	}, handleBuildInfo); err != nil {
		grip.Error(errors.Wrap(err, "could not register handler for buildinfo"))
		return
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "test",
		Command: "buildInfo",
	}, handleBuildInfo); err != nil {
		grip.Error(errors.Wrap(err, "could not register handler for buildinfo"))
		return
	}

	// db.runCommand({getLog: 1})
	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "admin",
		Command: "getLog",
	}, handleGetLog); err != nil {
		grip.Error(errors.Wrap(err, "could not register handler for getLog"))
		return
	}

	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "test",
		Command: "getLog",
	}, handleGetLog); err != nil {
		grip.Error(errors.Wrap(err, "could not register handler for getLog"))
		return
	}

	// db.runCommand({getFreeMonitoringStatus: 1})
	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "admin",
		Command: "getFreeMonitoringStatus",
	}, handleGetFreeMonitoringStatus); err != nil {
		grip.Error(errors.Wrap(err, "could not register handler for getFreeMonitoringStatus"))
		return
	}

	// db.runCommand({startProcess: {myFakeData: "hi"}})
	if err := srv.RegisterOperation(&mongowire.OpScope{
		Type:    mongowire.OP_COMMAND,
		Context: "test",
		Command: "startProcess",
	}, handleStartProcess); err != nil {
		grip.Error(errors.Wrap(err, "could not register handler for getFreeMonitoringStatus"))
		return
	}

	grip.Error(srv.Run(ctx))
}
