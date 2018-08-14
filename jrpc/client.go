package jrpc

import (
	"context"
	"io"
	"syscall"

	"github.com/pkg/errors"
	"github.com/tychoish/jasper"
	internal "github.com/tychoish/jasper/jrpc/internal"
	grpc "google.golang.org/grpc"
)

type jrpcManager struct {
	client internal.JasperProcessManagerClient
}

// TODO provide some better way of constructing this object

func NewJRPCManager(cc *grpc.ClientConn) jasper.Manager {
	return &jrpcManager{
		client: internal.NewJasperProcessManagerClient(cc),
	}
}

func (m *jrpcManager) Create(ctx context.Context, opts *jasper.CreateOptions) (jasper.Process, error) {
	proc, err := m.client.Create(ctx, internal.ConvertCreateOptions(opts))
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &jrpcProcess{client: m.client, info: proc}, nil
}

func (m *jrpcManager) Register(ctx context.Context, proc jasper.Process) error {
	return errors.New("cannot register extant processes on remote systms")
}

func (m *jrpcManager) List(ctx context.Context, f jasper.Filter) ([]jasper.Process, error) {
	procs, err := m.client.List(ctx, internal.ConvertFilter(f))
	if err != nil {
		return nil, errors.Wrap(err, "problem getting streaming client")
	}

	out := []jasper.Process{}
	for {
		info, err := procs.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "problem getting list")
		}

		out = append(out, &jrpcProcess{
			client: m.client,
			info:   info,
		})
	}

	if len(out) == 0 {
		return nil, errors.New("found no matching processes")
	}

	return out, nil
}

func (m *jrpcManager) Group(ctx context.Context, name string) ([]jasper.Process, error) {
	procs, err := m.client.Group(ctx, &internal.TagName{Value: name})
	if err != nil {
		return nil, errors.Wrap(err, "problem getting streaming client")
	}

	out := []jasper.Process{}
	for {
		info, err := procs.Recv()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, errors.Wrap(err, "problem getting group")
		}

		out = append(out, &jrpcProcess{
			client: m.client,
			info:   info,
		})
	}
	if len(out) == 0 {
		return nil, errors.New("found no matching processes")
	}

	return out, nil
}

func (m *jrpcManager) Get(ctx context.Context, name string) (jasper.Process, error) {
	info, err := m.client.Get(ctx, &internal.JasperProcessID{Value: name})
	if err != nil {
		return nil, errors.Wrap(err, "problem finding process")
	}

	return &jrpcProcess{client: m.client, info: info}, nil
}

func (m *jrpcManager) Close(ctx context.Context) error {
	resp, err := m.client.Close(ctx, nil)
	if err != nil {
		return errors.WithStack(err)
	}
	if resp.Succuess {
		return nil
	}

	return errors.New(resp.Text)
}

type jrpcProcess struct {
	client internal.JasperProcessManagerClient
	info   *internal.ProcessInfo
}

func (p *jrpcProcess) ID() string { return p.info.Id }

func (p *jrpcProcess) Info(ctx context.Context) jasper.ProcessInfo {
	if p.info.Complete {
		return p.info.Export()
	}

	info, err := p.client.Get(ctx, &internal.JasperProcessID{Value: p.info.Id})
	if err != nil {
		return jasper.ProcessInfo{}
	}

	return info.Export()
}
func (p *jrpcProcess) Running(ctx context.Context) bool {
	if p.info.Complete {
		return false
	}

	info, err := p.client.Get(ctx, &internal.JasperProcessID{Value: p.info.Id})
	if err != nil {
		return false
	}
	p.info = info

	return info.Running
}

func (p *jrpcProcess) Complete(ctx context.Context) bool {
	if p.info.Complete {
		return true
	}

	info, err := p.client.Get(ctx, &internal.JasperProcessID{Value: p.info.Id})
	if err != nil {
		return false
	}
	p.info = info

	return info.Complete
}

func (p *jrpcProcess) Signal(ctx context.Context, sig syscall.Signal) error {
	resp, err := p.client.Signal(ctx, &internal.SignalProcess{
		ProcessID: &internal.JasperProcessID{Value: p.info.Id},
		Signal:    internal.ConvertSignal(sig),
	})

	if err != nil {
		return errors.WithStack(err)
	}

	if resp.Succuess {
		return nil
	}

	return errors.New(resp.Text)
}

func (p *jrpcProcess) Wait(ctx context.Context) error {
	resp, err := p.client.Wait(ctx, &internal.JasperProcessID{Value: p.info.Id})

	if err != nil {
		return errors.WithStack(err)
	}

	if resp.Succuess {
		return nil
	}

	return errors.New(resp.Text)
}

func (p *jrpcProcess) RegisterTrigger(ctx context.Context, _ jasper.ProcessTrigger) error {
	return errors.New("cannot register remote triggers")
}

func (p *jrpcProcess) Tag(tag string) {
	_, _ = p.client.TagProcess(context.TODO(), &internal.ProcessTags{
		ProcessID: p.info.Id,
		Tags:      []string{tag},
	})
}
func (p *jrpcProcess) GetTags() []string {
	tags, err := p.client.GetTags(context.TODO(), &internal.JasperProcessID{Value: p.info.Id})
	if err != nil {
		return nil
	}

	return tags.Tags
}
func (p *jrpcProcess) ResetTags() {
	_, _ = p.client.ResetTags(context.TODO(), &internal.JasperProcessID{Value: p.info.Id})
}