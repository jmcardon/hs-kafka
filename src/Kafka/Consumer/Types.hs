{-# language
    BangPatterns
  , OverloadedStrings
  #-}

-- | Consumer types.
module Kafka.Consumer.Types
  ( -- * Consumer handle
    KafkaConsumer(..)
  , ConsumerConfig(..)
  , defaultConsumerConfig
    -- * Records
  , ConsumerRecord(..)
    -- * Offsets
  , TopicPartition(..)
  , OffsetSpec(..)
    -- * Rebalance
  , RebalanceEvent(..)
    -- * Group state
  , ConsumerGroupState(..)
  , JoinState(..)
  , ConsumerMode(..)
  ) where

import Control.Concurrent.Async (Async)
import Control.Concurrent.STM (TVar, TBQueue)
import Data.ByteString (ByteString)
import Data.Int (Int16, Int32, Int64)
import Data.Map.Strict (Map)
import Data.Set (Set)

import Kafka.Common (TopicName, GroupName, KafkaException)
import Kafka.Client (KafkaClient)
import Kafka.Internal.Config (ClientConfig)
import Kafka.Producer.Types (Header, Offset(..))

-- | A consumed message.
data ConsumerRecord = ConsumerRecord
  { crTopic     :: !TopicName
  , crPartition :: {-# UNPACK #-} !Int32
  , crOffset    :: {-# UNPACK #-} !Int64
  , crTimestamp :: {-# UNPACK #-} !Int64
  , crKey       :: !(Maybe ByteString)
  , crValue     :: !(Maybe ByteString)
  , crHeaders   :: ![Header]
  } deriving (Eq, Show)

-- | A topic-partition with offset.
data TopicPartition = TopicPartition
  { tpTopic     :: !TopicName
  , tpPartition :: {-# UNPACK #-} !Int32
  , tpOffset    :: {-# UNPACK #-} !Int64
  } deriving (Eq, Show, Ord)

-- | Where to start consuming.
data OffsetSpec
  = OffsetEarliest
  | OffsetLatest
  | OffsetAt {-# UNPACK #-} !Int64
  deriving (Eq, Show)

-- | Rebalance event delivered to the callback.
data RebalanceEvent
  = PartitionsAssigned [TopicPartition]
  | PartitionsRevoked [TopicPartition]
  deriving (Eq, Show)

-- | Consumer group join state machine.
data JoinState
  = JoinInit
  | JoinWaiting
  | JoinSyncing
  | JoinSteady
  deriving (Eq, Show)

-- | Mutable consumer group state.
data ConsumerGroupState = ConsumerGroupState
  { cgsJoinState    :: !JoinState
  , cgsMemberId     :: !(Maybe ByteString)
  , cgsGenerationId :: {-# UNPACK #-} !Int32
  , cgsAssignment   :: ![TopicPartition]
    -- ^ Currently assigned partitions.
  , cgsOffsets      :: !(Map (TopicName, Int32) Int64)
    -- ^ Current fetch offsets per partition.
  , cgsCommitted    :: !(Map (TopicName, Int32) Int64)
    -- ^ Last committed offsets.
  } deriving (Eq, Show)

-- | Consumer configuration.
data ConsumerConfig = ConsumerConfig
  { ccClientConfig     :: !ClientConfig
  , ccGroupId          :: !GroupName
  , ccTopics           :: ![TopicName]
    -- ^ Topics to subscribe to.
  , ccOffsetReset      :: !OffsetSpec
    -- ^ Where to start if no committed offset. Default: OffsetLatest.
  , ccAutoCommit       :: !Bool
    -- ^ Auto-commit offsets. Default: True.
  , ccAutoCommitMs     :: {-# UNPACK #-} !Int
    -- ^ Auto-commit interval in ms. Default: 5000.
  , ccMaxPollRecords   :: {-# UNPACK #-} !Int
    -- ^ Max records per poll. Default: 500.
  , ccFetchMaxBytes    :: {-# UNPACK #-} !Int32
    -- ^ Max bytes per fetch response. Default: 52428800 (50MB).
  , ccFetchWaitMs      :: {-# UNPACK #-} !Int
    -- ^ Max wait time for fetch in ms. Default: 500.
  , ccSessionTimeoutMs :: {-# UNPACK #-} !Int
    -- ^ Session timeout. Default: 45000.
  , ccHeartbeatMs      :: {-# UNPACK #-} !Int
    -- ^ Heartbeat interval. Default: 3000.
  , ccRebalanceCallback :: !(Maybe (RebalanceEvent -> IO ()))
    -- ^ Called on rebalance (assign/revoke).
  }

defaultConsumerConfig :: ClientConfig -> GroupName -> [TopicName] -> ConsumerConfig
defaultConsumerConfig cfg groupId topics = ConsumerConfig
  { ccClientConfig = cfg
  , ccGroupId = groupId
  , ccTopics = topics
  , ccOffsetReset = OffsetLatest
  , ccAutoCommit = True
  , ccAutoCommitMs = 5000
  , ccMaxPollRecords = 500
  , ccFetchMaxBytes = 52428800
  , ccFetchWaitMs = 500
  , ccSessionTimeoutMs = 45000
  , ccHeartbeatMs = 3000
  , ccRebalanceCallback = Nothing
  }

-- | Consumer handle.
data KafkaConsumer = KafkaConsumer
  { consClient       :: !KafkaClient
  , consConfig       :: !ConsumerConfig
  , consGroupState   :: !(TVar ConsumerGroupState)
  , consFetchQueue   :: !(TBQueue ConsumerRecord)
    -- ^ Messages from fetch responses, ready for poll.
  , consShutdown     :: !(TVar Bool)
  , consPaused       :: !(TVar (Set (TopicName, Int32)))
    -- ^ Paused partitions (fetch skips these).
  , consMode         :: !(TVar ConsumerMode)
    -- ^ Subscribe mode (group) or assign mode (direct).
  , consHeartbeat    :: !(TVar (Maybe (Async ())))
    -- ^ Current heartbeat thread. Tracked so we can cancel on rejoin.
  }

-- | Consumer operating mode.
data ConsumerMode
  = ModeSubscribe   -- ^ Using consumer group (join/sync/heartbeat)
  | ModeAssign      -- ^ Manual partition assignment (no group)
  deriving (Eq, Show)
