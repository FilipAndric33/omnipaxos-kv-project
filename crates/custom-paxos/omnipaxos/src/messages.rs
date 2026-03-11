use crate::{
    messages::{ballot_leader_election::BLEMessage, sequence_paxos::PaxosMessage},
    storage::Entry,
    util::NodeId,
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// Internal component for log replication
pub mod sequence_paxos {
    use crate::{
        ballot_leader_election::Ballot,
        storage::Entry,
        util::{LogSync, NodeId},
    };
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};
    use std::fmt::Debug;

    /// Message sent by a follower on crash-recovery or dropped messages to request its leader to re-prepare them.
    #[derive(Copy, Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct PrepareReq {
        /// The current round.
        pub n: Ballot,
    }

    /// Promise message sent by a follower in response to a [`Prepare`] sent by the leader.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct Promise<T>
    where
        T: Entry,
    {
        /// The current round.
        pub n: Ballot,
        /// The latest round in which an entry was accepted.
        pub n_accepted: Ballot,
        /// The decided index of this follower.
        pub decided_idx: usize,
        /// The log length of this follower.
        pub accepted_idx: usize,
        /// The log update which the leader applies to its log in order to sync
        /// with this follower (if the follower is more up-to-date).
        pub log_sync: Option<LogSync<T>>,
    }

    /// An enum for all the different message types.
    #[allow(missing_docs)]
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum PaxosMsg<T>
    where
        T: Entry,
    {
        Ack(usize, u64, Option<Option<Option<String>>>, usize), // First option is weather we do speculate, second is if the command returns something by definition (for instance a write returns None), third is if weather the action returned something (for instance reading a non-existing variable returns none).
        Confirm(T, usize),
    }

    /// A struct for a Paxos message that also includes sender and receiver.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct PaxosMessage<T>
    where
        T: Entry,
    {
        /// Sender of `msg`.
        pub from: NodeId,
        /// Receiver of `msg`.
        pub to: NodeId,
        /// The message content.
        pub msg: PaxosMsg<T>,
    }
}

/// The different messages BLE uses to communicate with other servers.
pub mod ballot_leader_election {

    use crate::{ballot_leader_election::Ballot, util::NodeId};
    #[cfg(feature = "serde")]
    use serde::{Deserialize, Serialize};

    /// An enum for all the different BLE message types.
    #[allow(missing_docs)]
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub enum HeartbeatMsg {
        Request(HeartbeatRequest),
        Reply(HeartbeatReply),
    }

    /// Requests a reply from all the other servers.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct HeartbeatRequest {
        /// Number of the current round.
        pub round: u32,
    }

    /// Replies
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct HeartbeatReply {
        /// Number of the current heartbeat round.
        pub round: u32,
        /// Ballot of replying server.
        pub ballot: Ballot,
        /// Leader this server is following
        pub leader: Ballot,
        /// Whether the replying server sees a need for a new leader
        pub happy: bool,
    }

    /// A struct for a Paxos message that also includes sender and receiver.
    #[derive(Clone, Debug)]
    #[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
    pub struct BLEMessage {
        /// Sender of `msg`.
        pub from: NodeId,
        /// Receiver of `msg`.
        pub to: NodeId,
        /// The message content.
        pub msg: HeartbeatMsg,
    }
}

#[allow(missing_docs)]
/// Message in OmniPaxos. Can be either a `SequencePaxos` message (for log replication) or `BLE` message (for leader election)
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Message<T>
where
    T: Entry,
{
    SequencePaxos(PaxosMessage<T>),
    BLE(BLEMessage),
}

impl<T> Message<T>
where
    T: Entry,
{
    /// Get the sender id of the message
    pub fn get_sender(&self) -> NodeId {
        match self {
            Message::SequencePaxos(p) => p.from,
            Message::BLE(b) => b.from,
        }
    }

    /// Get the receiver id of the message
    pub fn get_receiver(&self) -> NodeId {
        match self {
            Message::SequencePaxos(p) => p.to,
            Message::BLE(b) => b.to,
        }
    }
}
