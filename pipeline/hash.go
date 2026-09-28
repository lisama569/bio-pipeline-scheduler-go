package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// shortHash 计算数据的短 SHA-256 指纹（前 16 个十六进制字符）。
func shortHash(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])[:16]
}

// hashEvent 根据上一条事件的哈希与本条事件内容计算哈希链节点。
func hashEvent(prevHash string, payload []byte) string {
	h := sha256.New()
	h.Write([]byte(prevHash))
	h.Write([]byte{':'})
	h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

// eventPayload 是参与哈希计算的事件字段（不含 Hash 自身）。
func eventPayload(ev *Event) []byte {
	b, _ := json.Marshal(struct {
		Seq        int        `json:"seq"`
		Step       string     `json:"step"`
		Transition Transition `json:"transition"`
		Attempt    int        `json:"attempt"`
		Detail     string     `json:"detail"`
		At         string     `json:"at"`
		PrevHash   string     `json:"prev_hash"`
	}{ev.Seq, ev.Step, ev.Transition, ev.Attempt, ev.Detail, ev.At, ev.PrevHash})
	return b
}
