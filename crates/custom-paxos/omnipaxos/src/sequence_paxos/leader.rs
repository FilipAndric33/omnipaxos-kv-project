use super::super::{ballot_leader_election::Ballot, util::LeaderState};
use crate::util::WRITE_ERROR_MSG;

use super::*;

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    /// Handle a new leader. Should be called when the leader election has elected a new leader with the ballot `n`
    /*** Leader ***/
    pub(crate) async fn handle_leader(&mut self, n: Ballot) {
        if n <= self.leader_state.n_leader || n <= self.internal_storage.lock().await.get_promise()
        {
            return;
        }
        if self.pid == n.pid {
            self.leader_state =
                LeaderState::with(n, self.leader_state.max_pid, self.leader_state.quorum);
            // Flush any pending writes
            // Don't have to handle flushed entries here because we will sync with followers
            let _ = self
                .internal_storage
                .lock()
                .await
                .flush_batch()
                .expect(WRITE_ERROR_MSG);
            self.internal_storage
                .lock()
                .await
                .set_promise(n)
                .expect(WRITE_ERROR_MSG);
            /* insert my promise */

            /* initialise longest chosen sequence and update state */
            self.state = (Role::Leader, Phase::Prepare);
        } else {
            self.become_follower();
        }
    }

    pub(crate) fn become_follower(&mut self) {
        self.state.0 = Role::Follower;
    }
}
