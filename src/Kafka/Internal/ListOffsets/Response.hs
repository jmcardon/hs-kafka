module Kafka.Internal.ListOffsets.Response
  ( ListOffsetsResponse(..)
  , ListOffsetsTopic(..)
  , ListOffsetPartition(..)
  , parseListOffsetsResponse
  ) where

import Data.Int (Int16, Int32, Int64)

import Kafka.Common (TopicName(..))
import Kafka.Internal.Wire

data ListOffsetsResponse = ListOffsetsResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , topics :: [ListOffsetsTopic]
  } deriving (Eq, Show)

data ListOffsetsTopic = ListOffsetsTopic
  { topic :: {-# UNPACK #-} !TopicName
  , partitions :: [ListOffsetPartition]
  } deriving (Eq, Show)

data ListOffsetPartition = ListOffsetPartition
  { partition :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  , timestamp :: {-# UNPACK #-} !Int64
  , offset :: {-# UNPACK #-} !Int64
  , leaderEpoch :: {-# UNPACK #-} !Int32
  } deriving (Eq, Show)

parseListOffsetsResponse :: Wire ListOffsetsResponse
parseListOffsetsResponse = do
  _correlationId <- int32
  ListOffsetsResponse
    <$> int32
    <*> legacyArray parseListOffsetsTopic

parseListOffsetsTopic :: Wire ListOffsetsTopic
parseListOffsetsTopic = ListOffsetsTopic
  <$> (TopicName <$> legacyString)
  <*> legacyArray parseListOffsetPartition
{-# INLINE parseListOffsetsTopic #-}

parseListOffsetPartition :: Wire ListOffsetPartition
parseListOffsetPartition = ListOffsetPartition
  <$> int32 <*> int16 <*> int64 <*> int64 <*> int32
{-# INLINE parseListOffsetPartition #-}
