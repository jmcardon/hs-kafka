module Kafka.Internal.OffsetCommit.Response
  ( OffsetCommitPartition(..)
  , OffsetCommitResponse(..)
  , OffsetCommitTopic(..)
  , parseOffsetCommitResponse
  ) where

import Data.Int (Int16, Int32)

import Kafka.Common (TopicName(..))
import Kafka.Internal.Wire

data OffsetCommitResponse = OffsetCommitResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , topics :: [OffsetCommitTopic]
  } deriving (Eq, Show)

data OffsetCommitTopic = OffsetCommitTopic
  { topic :: {-# UNPACK #-} !TopicName
  , partitions :: [OffsetCommitPartition]
  } deriving (Eq, Show)

data OffsetCommitPartition = OffsetCommitPartition
  { partitionIndex :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  } deriving (Eq, Show)

parseOffsetCommitResponse :: Wire OffsetCommitResponse
parseOffsetCommitResponse = do
  _correlationId <- int32
  OffsetCommitResponse
    <$> int32
    <*> legacyArray parseOffsetCommitTopic

parseOffsetCommitTopic :: Wire OffsetCommitTopic
parseOffsetCommitTopic = OffsetCommitTopic
  <$> (TopicName <$> legacyString)
  <*> legacyArray parseOffsetCommitPartitions
{-# INLINE parseOffsetCommitTopic #-}

parseOffsetCommitPartitions :: Wire OffsetCommitPartition
parseOffsetCommitPartitions = OffsetCommitPartition <$> int32 <*> int16
{-# INLINE parseOffsetCommitPartitions #-}
