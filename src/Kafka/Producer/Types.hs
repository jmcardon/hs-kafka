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
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int32, Int64)
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
    -- ^ Send to this specific partition.
  | UnassignedPartition
    -- ^ Let the producer choose (round-robin, or key-hash if key present).
  deriving (Eq, Show, Ord)

-- | Offset assigned by the broker on successful produce.
newtype Offset = Offset { unOffset :: Int64 }
  deriving (Eq, Show, Ord)

-- | The result of producing a message, delivered asynchronously.
data DeliveryReport
  = DeliverySuccess !ProducerRecord !Offset
    -- ^ Message was successfully produced at this offset.
  | DeliveryFailure !ProducerRecord !ByteString
    -- ^ Message could not be produced. The ByteString is the error description.
  deriving (Eq, Show)

-- | Record header: a key-value pair attached to a message (KIP-82).
data Header = Header
  { hdrKey   :: !ByteString
  , hdrValue :: !(Maybe ByteString)
  } deriving (Eq, Show)

-- | List of record headers.
type Headers = [Header]
