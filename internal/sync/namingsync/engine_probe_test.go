package namingsync

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
	"go.uber.org/zap"

	"github.com/cxcm/nacos-sync-backend/internal/nacos"
	"github.com/cxcm/nacos-sync-backend/internal/pool"
)

// TestProbeRecoversDeadInstance 锁定 probe 仅存的职责（2026-10-23 审查修正后）：
// dead 集合中的实例探活成功（假死恢复）→ 清除 dead 标记、重建 registered 登记、触发代注册。
// 前置事实：崩溃判定已移除（TCP 不可达不判崩溃），dead 常规情况无写入者、probe 不再
// 对 registered 存活实例做周期 TCP 探活（原实现对全部实例每秒探活、结果无任何决策作用）。
func TestProbeRecoversDeadInstance(t *testing.T) {
	pl, err := pool.New(4, zap.NewNop())
	if err != nil {
		t.Fatal(err)
	}
	defer pl.Release()

	// 真实监听端口，使源实例 ip:port 的 TCP 探活成功。
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	_, portStr, _ := net.SplitHostPort(l.Addr().String())
	port, _ := strconv.ParseUint(portStr, 10, 16)

	capCh := make(chan vo.RegisterInstanceParam, 1)
	e := &Engine{
		pl:       pl,
		logger:   zap.NewNop(),
		probeSem: make(chan struct{}, 4),
	}
	e.mgr = nacos.NewClientManagerStub("d", "", captureNamingClient{got: capCh})

	in := model.Instance{Ip: "127.0.0.1", Port: port, Weight: 1, Enable: true, Healthy: true}
	rk := regKey("TEST-SVC", in)
	rd := &runningDir{
		d:          &direction{ID: "g/s->g/d/", SrcCID: "s", DstCID: "d"},
		dstClient:  captureNamingClient{got: capCh},
		registered: map[string]regEntry{},
		failCount:  map[string]int{},
		dead:       map[string]regEntry{rk: {svc: "TEST-SVC", inst: in}},
	}

	e.probe(context.Background(), rd) // 同步：内部 wg.Wait 等全部探活完成

	select {
	case p := <-capCh:
		if p.Ip != "127.0.0.1" || p.Port != port {
			t.Fatalf("代注册目标地址错误: %s:%d", p.Ip, p.Port)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("dead 假死恢复应在 2s 内触发代注册")
	}
	rd.mu.Lock()
	if _, ok := rd.registered[rk]; !ok {
		t.Fatal("假死恢复后实例应回到 registered")
	}
	if _, ok := rd.dead[rk]; ok {
		t.Fatal("假死恢复后 dead 标记应被清除")
	}
	rd.mu.Unlock()
}
