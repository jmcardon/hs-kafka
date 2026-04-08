module Kafka.Internal.OffsetFetch.Response
  ( OffsetFetchResponse(..)
  , OffsetFetchTopic(..)
  , OffsetFetchPartition(..)
  , parseOffsetFetchResponse
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int16, Int32, Int64)

import Kafka.Common (TopicName(..))
import Kafka.Internal.Wire

data OffsetFetchResponse = OffsetFetchResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , topics :: [OffsetFetchTopic]
  , errorCode :: !Int16
  } deriving (Eq, Show)

data OffsetFetchTopic = OffsetFetchTopic
  { topic :: {-# UNPACK #-} !TopicName
  , partitions :: [OffsetFetchPartition]
  } deriving (Eq, Show)

data OffsetFetchPartition = OffsetFetchPartition
  { partitionIndex :: {-# UNPACK #-} !Int32
  , offset :: {-# UNPACK #-} !Int64
  , leaderEpoch :: {-# UNPACK #-} !Int32
  , metadata :: !(Maybe ByteString)
  , partitionErrorCode :: {-# UNPACK #-} !Int16
  } deriving (Eq, Show)

parseOffsetFetchResponse :: Wire OffsetFetchResponse
parseOffsetFetchResponse = do
  _correlationId <- int32
  OffsetFetchResponse
    <$> int32
    <*> legacyArray parseOffsetFetchTopic
    <*> int16

parseOffsetFetchTopic :: Wire OffsetFetchTopic
parseOffsetFetchTopic = OffsetFetchTopic
  <$> (TopicName <$> legacyString)
  <*> legacyArray parseOffsetFetchPartition
{-# INLINE parseOffsetFetchTopic #-}

parseOffsetFetchPartition :: Wire OffsetFetchPartition
parseOffsetFetchPartition = OffsetFetchPartition
  <$> int32 <*> int64 <*> int32
  <*> legacyNullableString
  <*> int16
{-# INLINE parseOffsetFetchPartition #-}
