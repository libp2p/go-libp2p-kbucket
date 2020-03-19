package kbucket

import (
	"context"
	"time"

	"github.com/libp2p/go-libp2p-core/peer"
)

func (rt *RoutingTable) cleanup() {
	validatePeerF := func(p peer.ID) bool {
		queryCtx, cancel := context.WithTimeout(rt.ctx, rt.peerValidationTimeout)
		defer cancel()
		return rt.peerValidationFnc(queryCtx, p)
	}

	cleanupTickr := time.NewTicker(rt.tableCleanupInterval)
	defer cleanupTickr.Stop()
	for {
		select {
		case <-cleanupTickr.C:
			ps := rt.peersToValidate()
			for _, pinfo := range ps {
				// TODO This is racy
				// A peer could disconnect immediately after we validate it & would thus be in missing state again
				// which means we would wrongly mark it as active here. The longer term solution is to
				// handle all peer related events in a single event loop in the RT or more fingrained locking at bucket/peer level.
				// See https://github.com/libp2p/go-libp2p-kbucket/issues/60
				if validatePeerF(pinfo.Id) {
					rt.tabLock.Lock()
					// add it back/mark it as active ONLY if it is still in the RT
					// to avoid adding it back if it's been marked as dead
					i := rt.bucketIdForPeer(pinfo.Id)
					if rt.buckets[i].getPeer(pinfo.Id) != nil {
						log.Infof("successfully validated missing peer=%s, marking it as active", pinfo.Id)
						rt.addPeer(pinfo.Id)
					}
					rt.tabLock.Unlock()
					continue
				}

				// peer does not seem to be alive, let's try to replace it
				// evict missing peer & request replacement ONLY if it's NOT marked as active to avoid removing a peer that connected after
				// the failed validation
				rt.tabLock.Lock()
				i := rt.bucketIdForPeer(pinfo.Id)
				p := rt.buckets[i].getPeer(pinfo.Id)
				if p != nil && p.State != PeerStateActive {
					log.Infof("failed to validate missing peer=%s, evicting it from the RT & requesting a replace", pinfo.Id)
					rt.removePeer(pinfo.Id)
				}
				rt.tabLock.Unlock()
			}
		case <-rt.ctx.Done():
			return
		}
	}
}

// replaces a peer using a valid peer from the replacement cache
func (rt *RoutingTable) startPeerReplacement() {
	validatePeerF := func(p peer.ID) bool {
		queryCtx, cancel := context.WithTimeout(rt.ctx, rt.peerValidationTimeout)
		defer cancel()
		return rt.peerValidationFnc(queryCtx, p)
	}

	for {
		select {
		case p := <-rt.peerReplaceCh:
			// keep trying replacement candidates till we get a successful validation or
			// we run out of candidates
			cpl := uint(CommonPrefixLen(ConvertPeerID(p), rt.local))
			c, notEmpty := rt.cplReplacementCache.pop(cpl)
			for notEmpty {
				if validatePeerF(c) {
					log.Infof("successfully validated candidate=%s for peer=%s", c, p)
					// TODO There is a race here. The peer could disconnect from us or stop supporting the DHT
					// protocol after the validation which means we should not be adding it to the RT here.
					// See https://github.com/libp2p/go-libp2p-kbucket/issues/60
					rt.tabLock.Lock()
					rt.addPeer(c)
					rt.tabLock.Unlock()
					break
				}
				log.Infof("failed to validated candidate=%s", c)
				c, notEmpty = rt.cplReplacementCache.pop(cpl)
			}

			if !notEmpty {
				log.Infof("failed to replace missing peer=%s as all candidates were invalid", p)
			}
		case <-rt.ctx.Done():
			return
		}
	}
}
