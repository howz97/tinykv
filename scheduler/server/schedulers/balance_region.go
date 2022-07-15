// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package schedulers

import (
	"sort"
	"time"

	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/metapb"
	"github.com/pingcap-incubator/tinykv/scheduler/server/core"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/operator"
	"github.com/pingcap-incubator/tinykv/scheduler/server/schedule/opt"
)

func init() {
	schedule.RegisterSliceDecoderBuilder("balance-region", func(args []string) schedule.ConfigDecoder {
		return func(v interface{}) error {
			return nil
		}
	})
	schedule.RegisterScheduler("balance-region", func(opController *schedule.OperatorController, storage *core.Storage, decoder schedule.ConfigDecoder) (schedule.Scheduler, error) {
		return newBalanceRegionScheduler(opController), nil
	})
}

const (
	// balanceRegionRetryLimit is the limit to retry schedule for selected store.
	balanceRegionRetryLimit = 10
	balanceRegionName       = "balance-region-scheduler"
)

type balanceRegionScheduler struct {
	*baseScheduler
	name         string
	opController *schedule.OperatorController
}

// newBalanceRegionScheduler creates a scheduler that tends to keep regions on
// each store balanced.
func newBalanceRegionScheduler(opController *schedule.OperatorController, opts ...BalanceRegionCreateOption) schedule.Scheduler {
	base := newBaseScheduler(opController)
	s := &balanceRegionScheduler{
		baseScheduler: base,
		opController:  opController,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// BalanceRegionCreateOption is used to create a scheduler with an option.
type BalanceRegionCreateOption func(s *balanceRegionScheduler)

func (s *balanceRegionScheduler) GetName() string {
	if s.name != "" {
		return s.name
	}
	return balanceRegionName
}

func (s *balanceRegionScheduler) GetType() string {
	return "balance-region"
}

func (s *balanceRegionScheduler) IsScheduleAllowed(cluster opt.Cluster) bool {
	return s.opController.OperatorCount(operator.OpRegion) < cluster.GetRegionScheduleLimit()
}

func (s *balanceRegionScheduler) Schedule(cluster opt.Cluster) *operator.Operator {
	var stores []*core.StoreInfo
	for _, store := range cluster.GetStores() {
		if store.GetState() != metapb.StoreState_Up {
			continue
		}
		if store.IsBlocked() {
			continue
		}
		if time.Since(store.GetLastHeartbeatTS()) > cluster.GetMaxStoreDownTime() {
			continue
		}
		stores = append(stores, store)
	}
	if len(stores) <= cluster.GetMaxReplicas() {
		return nil
	}
	sort.Sort(core.StoreSlice(stores))
	src := stores[len(stores)-1]
	region := cluster.RandPendingRegion(src.GetID())
	if region == nil {
		region = cluster.RandFollowerRegion(src.GetID())
		if region == nil {
			region = cluster.RandLeaderRegion(src.GetID())
			if region == nil {
				return nil
			}
		}
	}
	log.Debugf("balanceRegionScheduler selected region %v", region.GetMeta().String())
	var dst *core.StoreInfo
	for i := range stores {
		dst = stores[i]
		if region.GetStorePeer(dst.GetID()) == nil {
			// only single replica of a region on each store
			break
		}
	}
	if dst == nil {
		return nil
	}
	if dst.GetAvailable()-src.GetAvailable() < 2*uint64(region.GetApproximateSize()) {
		return nil
	}

	op, err := operator.CreateMovePeerOperator("balance region", cluster, region, operator.OpBalance, src.GetID(), dst.GetID(), region.GetStorePeer(src.GetID()).Id)
	if err != nil {
		return nil
	}
	return op
}
