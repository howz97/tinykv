package runner

import (
	"encoding/hex"
	"time"

	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/message"
	"github.com/pingcap-incubator/tinykv/kv/raftstore/util"
	"github.com/pingcap-incubator/tinykv/kv/util/codec"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/kv/util/worker"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
)

type SplitCheckTask struct {
	Region *metapb.Region
}

type splitCheckHandler struct {
	engine  *badger.DB
	router  message.RaftRouter
	checker *sizeSplitChecker
}

func NewSplitCheckHandler(engine *badger.DB, router message.RaftRouter, conf *config.Config) *splitCheckHandler {
	runner := &splitCheckHandler{
		engine:  engine,
		router:  router,
		checker: newSizeSplitChecker(conf.RegionMaxSize, conf.RegionSplitSize),
	}
	return runner
}

/// run checks a region with split checkers to produce split keys and generates split admin command.
func (r *splitCheckHandler) Handle(t worker.Task) {
	spCheckTask := t.(*SplitCheckTask)
	region := spCheckTask.Region
	regionId := region.Id
	log.Infof("splitCheckHandler executing split check worker.Task: [regionId: %d, startKey: %s, endKey: %s]", regionId,
		hex.EncodeToString(region.StartKey), hex.EncodeToString(region.EndKey))
	key := r.splitCheck(regionId, region.StartKey, region.EndKey)
	apprxMsg := message.Msg{
		Type: message.MsgTypeRegionApproximateSize,
		Data: r.checker.currentSize,
	}
	if key == nil {
		log.Infof("splitCheckHandler no need to send, split key not found: [regionId: %v]", regionId)
	} else if r.splitByKey(region, key) {
		apprxMsg.Data = r.checker.splitSize
	} else {
		log.Infof("splitCheckHandler failed to split key=%s", string(key))
	}
	r.router.Send(regionId, apprxMsg)
}

func (r *splitCheckHandler) splitByKey(region *metapb.Region, key []byte) bool {
	_, userKey, err := codec.DecodeBytes(key)
	if err == nil {
		// It's not a raw key.
		// To make sure the keys of same user key locate in one Region, decode and then encode to truncate the timestamp
		key = codec.EncodeBytes(userKey)
	}
	log.Infof("split key found %s: region=%d,[%s,%s)", string(key), region.Id, string(region.StartKey), string(region.EndKey))
	cb := message.NewCallback()
	msg := message.Msg{
		Type:     message.MsgTypeSplitRegion,
		RegionID: region.Id,
		Data: &message.MsgSplitRegion{
			RegionEpoch: region.GetRegionEpoch(),
			SplitKey:    key,
			Callback:    cb,
		},
	}
	err = r.router.Send(region.Id, msg)
	if err != nil {
		log.Errorf("failed to send check result: [regionId: %d, err: %v]", region.Id, err)
		return false
	}
	resp := cb.WaitRespWithTimeout(3 * time.Second)
	if resp == nil {
		return false
	}
	if resp.Header.Error != nil {
		return false
	}
	log.Infof("splitCheckHandler finished split by key %s", string(key))
	return true
}

/// SplitCheck gets the split keys by scanning the range.
func (r *splitCheckHandler) splitCheck(regionID uint64, startKey, endKey []byte) []byte {
	txn := r.engine.NewTransaction(false)
	defer txn.Discard()

	r.checker.reset()
	it := engine_util.NewCFIterator(engine_util.CfDefault, txn)
	defer it.Close()
	for it.Seek(startKey); it.Valid(); it.Next() {
		item := it.Item()
		key := item.Key()
		if engine_util.ExceedEndKey(key, endKey) {
			break
		}
		if r.checker.onKv(key, item) {
			log.Infof("region %d splitCheck passed currentSize = %d, keyRange=[%s,%s)", regionID, r.checker.currentSize, string(startKey), string(endKey))
			break
		}
	}
	log.Infof("region %d splitCheck currentSize = %d, keyRange=[%s,%s)", regionID, r.checker.currentSize, string(startKey), string(endKey))
	return r.checker.getSplitKey()
}

type sizeSplitChecker struct {
	maxSize   uint64
	splitSize uint64

	currentSize uint64
	splitKey    []byte
}

func newSizeSplitChecker(maxSize, splitSize uint64) *sizeSplitChecker {
	return &sizeSplitChecker{
		maxSize:   maxSize,
		splitSize: splitSize,
	}
}

func (checker *sizeSplitChecker) reset() {
	checker.currentSize = 0
	checker.splitKey = nil
}

func (checker *sizeSplitChecker) onKv(key []byte, item engine_util.DBItem) bool {
	valueSize := uint64(item.ValueSize())
	size := uint64(len(key)) + valueSize
	checker.currentSize += size
	if checker.currentSize > checker.splitSize && checker.splitKey == nil {
		checker.splitKey = util.SafeCopy(key)
	}
	return checker.currentSize > checker.maxSize
}

func (checker *sizeSplitChecker) getSplitKey() []byte {
	// Make sure not to split when less than maxSize for last part
	if checker.currentSize < checker.maxSize {
		checker.splitKey = nil
	}
	return checker.splitKey
}
