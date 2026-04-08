{-# language
    BangPatterns
  , OverloadedStrings
  #-}

-- | Producer types: ProducerRecord, DeliveryReport, partitioning.
module Kafka.Producer.Types
  ( ProducerRecord(..)
  , ProducePartition(..)
  , DeliveryReport(..)
  , Offset(..)
  , Headers
  , Header(..)
  , DeliveryEntry(..)
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int16, Int32, Int64)
import Kafka.Common (TopicName)

-- | A message to be produced to Kafka.
data ProducerRecord = ProducerRecord
  { prTopic     :: !TopicName
  , prPartition :: !ProducePartition
  , prKey       :: !(Maybe ByteString)
  , prValue     :: !(Maybe ByteString)
  , prHeaders   :: !Headers
  } deriving (Eq, Show)

-- | Partition selection strategy.
data ProducePartition
  = SpecifiedPartition {-# UNPACK #-} !Int32
  | UnassignedPartition
  deriving (Eq, Show, Ord)

-- | Offset assigned by the broker.
newtype Offset = Offset { unOffset :: Int64 }
  deriving (Eq, Show, Ord)

-- | The result of producing a message.
-- Matches librdkafka's rd_kafka_message_t delivery report fields.
data DeliveryReport
  = DeliverySuccess
      { drRecord    :: !ProducerRecord
      , drOffset    :: !Offset
      , drPartition :: {-# UNPACK #-} !Int32
        -- ^ Partition the message was produced to.
      , drBrokerId  :: {-# UNPACK #-} !Int32
        -- ^ Node ID of the broker that accepted the message (-1 if unknown).
      , drLatencyUs :: {-# UNPACK #-} !Int64
        -- ^ End-to-end latency in microseconds (enqueue to delivery report).
      , drTimestamp :: {-# UNPACK #-} !Int64
        -- ^ Broker-assigned timestamp (logAppendTime), or -1 if unavailable.
      }
  | DeliveryFailure
      { drRecord    :: !ProducerRecord
      , drError     :: !ByteString
      , drErrorCode :: {-# UNPACK #-} !Int16
        -- ^ Kafka error code (0 if not a protocol error).
      }
  deriving (Eq, Show)

-- | Record header (KIP-82).
data Header = Header
  { hdrKey   :: !ByteString
  , hdrValue :: !(Maybe ByteString)
  } deriving (Eq, Show)

type Headers = [Header]

-- | Internal: a delivery report plus its optional per-message callback.
-- The broker thread pushes these to the delivery queue. The poller
-- thread drains the queue, invokes callbacks, and returns the reports.
data DeliveryEntry = DeliveryEntry
  { deReport   :: !DeliveryReport
  , deCallback :: !(Maybe (DeliveryReport -> IO ()))
    -- ^ Per-message callback, invoked by the poller thread (NOT broker thread).
  }
