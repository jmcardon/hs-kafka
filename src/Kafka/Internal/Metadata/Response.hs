module Kafka.Internal.Metadata.Response
  ( MetadataResponse(..)
  , MetadataBroker(..)
  , MetadataTopic(..)
  , MetadataPartition(..)
  , parseMetadataResponse
  , parseMetadataResponseV12
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int16, Int32)

import Kafka.Common (TopicName(..))
import Kafka.Internal.Wire

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
parseMetadataResponse :: Wire MetadataResponse
parseMetadataResponse = do
  _correlationId <- int32
  throttle <- int32
  brokersL <- legacyArray parseMetadataBrokerLegacy
  clId <- legacyNullableByteArray
  ctrl <- int32
  topicsL <- legacyArray parseMetadataTopicLegacy
  pure (MetadataResponse throttle brokersL (fmap fst clId) ctrl topicsL)
  where
    legacyNullableByteArray = do
      len <- int16
      if len < 0
        then pure Nothing
        else Just <$> ((,()) <$> takeBytes (fromIntegral len))

parseMetadataBrokerLegacy :: Wire MetadataBroker
parseMetadataBrokerLegacy = do
  nid <- int32
  hLen <- int16
  h <- takeBytes (fromIntegral hLen)
  p <- int32
  rLen <- int16
  r <- if rLen < 0 then pure Nothing else Just <$> takeBytes (fromIntegral rLen)
  pure (MetadataBroker nid h p r)

parseMetadataTopicLegacy :: Wire MetadataTopic
parseMetadataTopicLegacy = do
  ec <- int16
  tLen <- int16
  tn <- takeBytes (fromIntegral tLen)
  internal <- parseBool
  parts <- legacyArray parseMetadataPartitionLegacy
  pure (MetadataTopic ec (TopicName tn) internal parts)

parseMetadataPartitionLegacy :: Wire MetadataPartition
parseMetadataPartitionLegacy = do
  ec <- int16
  idx <- int32
  leader <- int32
  epoch <- int32
  _replicas <- int32  -- skip replica count (legacy)
  _isrs <- int32      -- skip ISR count (legacy)
  _offline <- int32   -- skip offline count (legacy)
  pure (MetadataPartition ec idx leader epoch [] [] [])

-- | Parse Metadata v12+ response (flexible/compact encoding).
-- Response header v1: correlation_id + tagged_fields (KIP-482).
parseMetadataResponseV12 :: Wire MetadataResponse
parseMetadataResponseV12 = do
  _correlationId <- int32
  skipTaggedFields  -- response header v1
  throttle <- int32
  brokersL <- compactArray parseMetadataBrokerV12
  clId <- compactNullableString
  ctrl <- int32
  topicsL <- compactArray parseMetadataTopicV12
  skipTaggedFields  -- body
  pure (MetadataResponse throttle brokersL clId ctrl topicsL)

parseMetadataBrokerV12 :: Wire MetadataBroker
parseMetadataBrokerV12 = do
  nid <- int32
  h <- compactString
  p <- int32
  r <- compactNullableString
  skipTaggedFields
  pure (MetadataBroker nid h p r)
{-# INLINE parseMetadataBrokerV12 #-}

parseMetadataTopicV12 :: Wire MetadataTopic
parseMetadataTopicV12 = do
  ec <- int16
  tn <- compactString
  skip 16  -- topicId UUID
  internal <- parseBool
  parts <- compactArray parseMetadataPartitionV12
  _topicAuthorizedOps <- int32
  skipTaggedFields
  pure (MetadataTopic ec (TopicName tn) internal parts)
{-# INLINE parseMetadataTopicV12 #-}

parseMetadataPartitionV12 :: Wire MetadataPartition
parseMetadataPartitionV12 = do
  ec <- int16
  idx <- int32
  leader <- int32
  epoch <- int32
  replicas <- compactArray int32
  isrs <- compactArray int32
  offline <- compactArray int32
  skipTaggedFields
  pure (MetadataPartition ec idx leader epoch replicas isrs offline)
{-# INLINE parseMetadataPartitionV12 #-}

