//! Shared push-target selection logic for repair and scaling.
//!
//! Prefers non-provider peers (those NOT in `known_providers`) over providers,
//! falling back to providers if no non-providers are available.

use std::sync::Arc;

use libp2p::PeerId;
use tokio::sync::Mutex;

use crate::peer_scorer::PeerScorer;

/// Select the best push target, preferring non-providers over providers.
///
/// 1. Rank all known peers by score (highest first).
/// 2. Pick the highest-ranked peer NOT in `known_providers` and not `local_peer_id`.
/// 3. If none, fall back to the highest-ranked provider (not `local_peer_id`).
/// 4. If no peers at all, return `None`.
pub fn select_push_target(
    local_peer_id: &PeerId,
    known_providers: &[PeerId],
    peer_scorer: &Option<Arc<Mutex<PeerScorer>>>,
) -> Option<PeerId> {
    let scorer = peer_scorer.as_ref()?;
    let scorer_guard = scorer.try_lock().ok()?;

    let all_peers: Vec<PeerId> = scorer_guard.iter().map(|(p, _)| *p).collect();
    if all_peers.is_empty() {
        return None;
    }

    let ranked = scorer_guard.rank_peers(&all_peers);

    // Prefer non-providers
    let non_provider = ranked
        .iter()
        .find(|p| **p != *local_peer_id && !known_providers.contains(p));

    if let Some(p) = non_provider {
        return Some(*p);
    }

    // Fall back to any provider that isn't us
    ranked.iter().find(|p| **p != *local_peer_id).copied()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::peer_scorer::PeerScorer;
    use std::time::Duration;

    fn make_scorer(peers: &[(PeerId, f64)]) -> Option<Arc<Mutex<PeerScorer>>> {
        let mut scorer = PeerScorer::new();
        for (peer, score) in peers {
            // Record successes to build score. More successes = higher score.
            let count = (*score * 10.0) as usize;
            for _ in 0..count {
                scorer.record_success(peer, Duration::from_millis(10));
            }
        }
        Some(Arc::new(Mutex::new(scorer)))
    }

    #[test]
    fn test_prefers_non_provider() {
        let local = PeerId::random();
        let provider = PeerId::random();
        let non_provider = PeerId::random();

        let scorer = make_scorer(&[
            (provider, 1.0),
            (non_provider, 0.5),
        ]);

        let target = select_push_target(&local, &[provider], &scorer);
        assert_eq!(target, Some(non_provider));
    }

    #[test]
    fn test_falls_back_to_provider() {
        let local = PeerId::random();
        let provider = PeerId::random();

        let scorer = make_scorer(&[(provider, 1.0)]);

        let target = select_push_target(&local, &[provider], &scorer);
        assert_eq!(target, Some(provider));
    }

    #[test]
    fn test_returns_none_when_no_peers() {
        let local = PeerId::random();
        let scorer = make_scorer(&[]);

        let target = select_push_target(&local, &[], &scorer);
        assert_eq!(target, None);
    }

    #[test]
    fn test_returns_none_without_scorer() {
        let local = PeerId::random();
        let target = select_push_target(&local, &[], &None);
        assert_eq!(target, None);
    }

    #[test]
    fn test_excludes_local_peer() {
        let local = PeerId::random();
        let scorer = make_scorer(&[(local, 1.0)]);

        let target = select_push_target(&local, &[], &scorer);
        assert_eq!(target, None);
    }
}
