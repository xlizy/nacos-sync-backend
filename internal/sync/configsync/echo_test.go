package configsync

import (
	"testing"
	"time"
)

// TestEchoSuppression 校验「回声抑制」语义：
// 我们从 A 推到 B 的内容，当 B→A 方向的监听器再次触发同一内容时，应被判定为回声而跳过，
// 从而既点燃目标通知又杜绝 A→B→A 风暴。
func TestEchoSuppression(t *testing.T) {
	e := &Engine{}
	const content = "app.port=8080"

	// 我们 A→B 推入一条内容。
	e.recordPush("A", "B", "public", "DEFAULT_GROUP", "app.yaml", content)

	// B→A 方向的监听器触发同一内容 → 期望判定为回声（true）。
	if !e.isEcho("B", "A", "public", "DEFAULT_GROUP", "app.yaml", content) {
		t.Fatal("expected echo detection to be true for our own push")
	}

	// 内容不同的变更不应被当作回声（否则会漏掉真实的用户变更）。
	if e.isEcho("B", "A", "public", "DEFAULT_GROUP", "app.yaml", content+"\n# user edit") {
		t.Fatal("different content must not be treated as echo")
	}

	// 无关 dataId 不应误判。
	if e.isEcho("B", "A", "public", "DEFAULT_GROUP", "other.yaml", content) {
		t.Fatal("different dataId must not be treated as echo")
	}
}

// TestEchoTableStoresHashNotContent 校验回声表只存内容哈希，不持有完整配置内容，
// 避免大配置对象长期常驻内存（即便 GC 尚未回收残留 key，占用也可忽略）。
func TestEchoTableStoresHashNotContent(t *testing.T) {
	e := &Engine{}
	const content = "very-long-config-content-should-not-be-retained-verbatim"
	e.recordPush("A", "B", "ns", "g", "d.yaml", content)

	v, ok := e.pushed.Load(pushKey("A", "B", "ns", "g", "d.yaml"))
	if !ok {
		t.Fatal("echo entry missing")
	}
	rec, ok := v.(pushRec)
	if !ok {
		t.Fatalf("unexpected value type %T", v)
	}
	if rec.hash == content {
		t.Fatal("echo entry must not store raw content")
	}
	if rec.hash != hashContent(content) {
		t.Fatal("echo entry hash mismatch")
	}
}

// TestEchoOneShotConsumption 回声记录命中即删除（一次性消费）：
// 否则 TTL 内内容改回相同值的真实用户变更会被当作回声反复误吞（P2）。
// 消费后如再收到同内容事件，应被当作真实变更转发（发布相同内容幂等，无风暴风险）。
func TestEchoOneShotConsumption(t *testing.T) {
	e := &Engine{}
	const content = "app.port=9090"

	e.recordPush("A", "B", "public", "DEFAULT_GROUP", "app.yaml", content)

	// 第一次：我们的回声 → 抑制。
	if !e.isEcho("B", "A", "public", "DEFAULT_GROUP", "app.yaml", content) {
		t.Fatal("first identical event should be suppressed as echo")
	}
	// 第二次：真实改回相同内容 → 必须放行，不得再次误吞。
	if e.isEcho("B", "A", "public", "DEFAULT_GROUP", "app.yaml", content) {
		t.Fatal("second identical event must be forwarded (one-shot consumption)")
	}
}

// TestEchoTTLExpiry 校验超过 echoTTL 的回声条目会失效并被删除，
// 避免陈旧回声长期命中导致真实变更被误跳过。
func TestEchoTTLExpiry(t *testing.T) {
	e := &Engine{}
	key := pushKey("A", "B", "ns", "g", "d.yaml")
	// 注入一条已过期（超过 echoTTL）的条目。
	e.pushed.Store(key, pushRec{hash: hashContent("old"), at: time.Now().Add(-2 * echoTTL)})

	if e.isEcho("B", "A", "ns", "g", "d.yaml", "old") {
		t.Fatal("expired echo must not suppress the change")
	}
	if _, still := e.pushed.Load(key); still {
		t.Fatal("expired echo entry should have been deleted")
	}
}
