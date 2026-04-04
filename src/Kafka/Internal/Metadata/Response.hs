module Kafka.Internal.Metadata.Response
  ( MetadataResponse(..)
  , MetadataBroker(..)
  , MetadataTopic(..)
  , MetadataPartition(..)
  , getMetadataResponse
  , parseMetadataResponse
  , parseMetadataResponseV12
  ) where

import Control.Concurrent.STM (TVar)
import Data.ByteString (ByteString)
import Data.Int (Int16, Int32)
import Data.Primitive.ByteArray (ByteArray)
import System.IO (Handle)

import Kafka.Internal.Combinator
import Kafka.Common
import Kafka.Internal.Response

data MetadataResponse = MetadataResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , brokers :: [MetadataBroker]
  , clusterId :: !(Maybe ByteString)
  , controllerId :: {-# UNPACK #-} !Int32
  , topics :: [MetadataTopic]
  } deriving (Eq, Show)

data MetadataBroker = MetadataBroker
  { nodeId :: {-# UNPACK #-} !Int32
  , host :: !ByteString
  , port :: {-# UNPACK #-} !Int32
  , rack :: !(Maybe ByteString)
  } deriving (Eq, Show)

data MetadataTopic = MetadataTopic
  { errorCode :: {-# UNPACK #-} !Int16
  , name :: !TopicName
  , isInternal :: !Bool
  , partitions :: [MetadataPartition]
  } deriving (Eq, Show)

data MetadataPartition = MetadataPartition
  { partitionErrorCode :: {-# UNPACK #-} !Int16
  , partitionIndex :: {-# UNPACK #-} !Int32
  , leaderId :: {-# UNPACK #-} !Int32
  , leaderEpoch :: {-# UNPACK #-} !Int32
  , replicaNodes :: [Int32]
  , isrNodes :: [Int32]
  , offlineReplicas :: [Int32]
  } deriving (Eq, Show)

-- | Parse Metadata v7 response (legacy encoding).
parseMetadataResponse :: Parser MetadataResponse
parseMetadataResponse = do
  _correlationId <- int32 "correlation id"
  MetadataResponse
    <$> (int32 "throttle time")
    <*> (array parseMetadataBrokerLegacy <?> "brokers")
    <*> (fmap (fmap byteArrayToByteString) (nullableByteArray) <?> "cluster id")
    <*> (int32 "controller id")
    <*> (array parseMetadataTopicLegacy <?> "topics")

parseMetadataBrokerLegacy :: Parser MetadataBroker
parseMetadataBrokerLegacy = do
  nid <- int32 "node id"
  h <- bytearray
  p <- int32 "port"
  r <- nullableByteArray
  pure (MetadataBroker nid (byteArrayToByteString h) p (fmap byteArrayToByteString r))

parseMetadataTopicLegacy :: Parser MetadataTopic
parseMetadataTopicLegacy = do
  ec <- int16 "error code"
  tn <- topicName
  internal <- bool "is internal"
  parts <- array parseMetadataPartitionLegacy
  pure (MetadataTopic ec tn internal parts)

parseMetadataPartitionLegacy :: Parser MetadataPartition
parseMetadataPartitionLegacy = do
  ec <- int16 "error code"
  idx <- int32 "partition index"
  leader <- int32 "leader id"
  epoch <- int32 "leader epoch"
  _replicas <- int32 "replica nodes"
  _isrs <- int32 "isr nodes"
  _offline <- int32 "offline replicas"
  pure (MetadataPartition ec idx leader epoch [] [] [])

-- | Parse Metadata v12+ response (flexible/compact encoding).
-- Response header v1: correlation_id + tagged_fields (KIP-482).
parseMetadataResponseV12 :: Parser MetadataResponse
parseMetadataResponseV12 = do
  _correlationId <- int32 "correlation id"
  skipTaggedFields  -- response header v1 tagged fields
  throttle <- int32 "throttle time"
  brokersL <- compactArray parseMetadataBrokerV12
  clId <- compactNullableString
  ctrl <- int32 "controller id"
  topicsL <- compactArray parseMetadataTopicV12
  skipTaggedFields  -- body tagged fields
  pure (MetadataResponse throttle brokersL clId ctrl topicsL)

parseMetadataBrokerV12 :: Parser MetadataBroker
parseMetadataBrokerV12 = do
  nid <- int32 "node id"
  h <- compactString
  p <- int32 "port"
  r <- compactNullableString
  skipTaggedFields
  pure (MetadataBroker nid h p r)

parseMetadataTopicV12 :: Parser MetadataTopic
parseMetadataTopicV12 = do
  ec <- int16 "error code"
  tn <- TopicName <$> compactString
  _topicId <- int64 "topic id high" >> int64 "topic id low"  -- UUID: 16 bytes
  internal <- bool "is internal"
  parts <- compactArray parseMetadataPartitionV12
  _topicAuthorizedOps <- int32 "topic authorized operations"
  skipTaggedFields
  pure (MetadataTopic ec tn internal parts)

parseMetadataPartitionV12 :: Parser MetadataPartition
parseMetadataPartitionV12 = do
  ec <- int16 "error code"
  idx <- int32 "partition index"
  leader <- int32 "leader id"
  epoch <- int32 "leader epoch"
  replicas <- compactArray (int32 "replica")
  isrs <- compactArray (int32 "isr")
  offline <- compactArray (int32 "offline")
  skipTaggedFields
  pure (MetadataPartition ec idx leader epoch replicas isrs offline)

getMetadataResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String MetadataResponse))
getMetadataResponse = fromKafkaResponse parseMetadataResponse
