package namingsync

import (
	"context"
	"fmt"
	"sort"
	"testing"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"
)

type multiNodeBatchClient struct {
	naming_client.INamingClient
	out chan vo.BatchRegisterInstanceParam
}

func (c multiNodeBatchClient) BatchRegisterInstance(p vo.BatchRegisterInstanceParam) (bool, error) {
	c.out <- p
	return true, nil
}

// Two independent workers reconciling the same direction must publish the same
// complete service snapshot. No node-specific metadata may make them overwrite
// one another with different desired states.
func TestTwoNodesPublishSameDirectionConcurrently(t *testing.T) {
	source := []model.Instance{
		{Ip: "10.10.0.1", Port: 8080, Weight: 1, Enable: true, Healthy: true, Metadata: map[string]string{"version": "v2"}},
		{Ip: "10.10.0.2", Port: 8080, Weight: 2, Enable: true, Healthy: true, Metadata: map[string]string{"version": "v2"}},
	}
	makeNode := func() (*Engine, *runningDir, chan vo.BatchRegisterInstanceParam) {
		out := make(chan vo.BatchRegisterInstanceParam, 1)
		rd := &runningDir{
			d: &direction{ID: "g/src->g/dst/", SrcCID: "src", DstCID: "dst"},
			dstClient: multiNodeBatchClient{out: out},
			registered: map[string]regEntry{},
			failCount: map[string]int{},
			dead: map[string]regEntry{},
		}
		for _, in := range source {
			rd.registered[regKey("ORDER", in)] = regEntry{svc: "ORDER", inst: in}
		}
		return &Engine{logger: zap.NewNop()}, rd, out
	}
	e1, rd1, out1 := makeNode()
	e2, rd2, out2 := makeNode()

	done := make(chan struct{}, 2)
	go func() { e1.registerServiceProxied(context.Background(), rd1, "ORDER"); done <- struct{}{} }()
	go func() { e2.registerServiceProxied(context.Background(), rd2, "ORDER"); done <- struct{}{} }()
	<-done
	<-done

	p1, p2 := <-out1, <-out2
	if len(p1.Instances) != 2 || len(p2.Instances) != 2 {
		t.Fatalf("both nodes must publish the complete two-instance snapshot: %d / %d", len(p1.Instances), len(p2.Instances))
	}
	keys := func(p vo.BatchRegisterInstanceParam) []string {
		out := make([]string, 0, len(p.Instances))
		for _, in := range p.Instances {
			if in.Metadata[metaSyncSource] != "NACOS" || in.Metadata[metaSyncSourceCluster] != "src" {
				t.Fatalf("missing stable ownership metadata: %+v", in.Metadata)
			}
			if _, ok := in.Metadata["syncSourceAgent"]; ok {
				t.Fatalf("node-specific metadata must not be published: %+v", in.Metadata)
			}
			out = append(out, fmt.Sprintf("%s:%d", in.Ip, in.Port))
		}
		sort.Strings(out)
		return out
	}
	if got1, got2 := keys(p1), keys(p2); !reflect.DeepEqual(got1, got2) {
		t.Fatalf("concurrent workers published different desired snapshots: %v != %v", got1, got2)
	}
}

// A rapid restart on the same endpoint with changed instance metadata must be
// treated as a changed source specification and republished by the next poll.
func TestRapidSourceRestartUpdatesChangedSpecification(t *testing.T) {
	old := model.Instance{Ip: "10.10.0.8", Port: 8080, Weight: 1, Enable: true, Healthy: true, Metadata: map[string]string{"build": "old"}}
	restarted := model.Instance{Ip: "10.10.0.8", Port: 8080, Weight: 1, Enable: true, Healthy: true, Metadata: map[string]string{"build": "new"}}
	if sameSourceSpec(old, restarted) {
		t.Fatal("a restarted instance with changed metadata must trigger a new publication")
	}
	if !sameSourceSpec(restarted, restarted) {
		t.Fatal("an unchanged source snapshot must remain idempotent")
	}
}
