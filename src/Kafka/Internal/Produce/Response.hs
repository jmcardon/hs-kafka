{-# language OverloadedStrings #-}

module Kafka.Internal.Produce.Response
  ( ProducePartitionResponse(..)
  , ProduceResponse(..)
  , ProduceResponseMessage(..)
  , parseProduceResponse
  , parseProduceResponseV9
  ) where

import Data.Int (Int16, Int32, Int64)

import Kafka.Common (TopicName(..))
import Kafka.Internal.Wire

data ProduceResponse = ProduceResponse
  { produceResponseMessages :: [ProduceResponseMessage]
  , throttleTimeMs :: !Int32
  } deriving (Eq, Show)

data ProduceResponseMessage = ProduceResponseMessage
  { prMessageTopic :: !TopicName
  , prPartitionResponses :: [ProducePartitionResponse]
  } deriving (Eq, Show)

data ProducePartitionResponse = ProducePartitionResponse
  { prResponsePartition :: !Int32
  , prResponseErrorCode :: !Int16
  , prResponseBaseOffset :: !Int64
  , prResponseLogAppendTime :: !Int64
  , prResponseLogStartTime :: !Int64
  } deriving (Eq, Show)

-- | Parse Produce v0-v8 response (legacy encoding).
parseProduceResponse :: Wire ProduceResponse
parseProduceResponse = do
  _correlationId <- int32
  responsesCount <- int32
  msgs <- replicateM (fromIntegral responsesCount) parseProduceResponseMessage
  throttle <- int32
  pure (ProduceResponse msgs throttle)

parseProduceResponseMessage :: Wire ProduceResponseMessage
parseProduceResponseMessage = do
  tlen <- int16
  t <- takeBytes (fromIntegral tlen)
  prc <- int32
  resps <- replicateM (fromIntegral prc) parseProducePartitionResponse
  pure (ProduceResponseMessage (TopicName t) resps)

parseProducePartitionResponse :: Wire ProducePartitionResponse
parseProducePartitionResponse = ProducePartitionResponse
  <$> int32 <*> int16 <*> int64 <*> int64 <*> int64
{-# INLINE parseProducePartitionResponse #-}

-- | Parse Produce v9+ response (flexible/compact encoding).
-- Response header v1: correlation_id + tagged_fields (KIP-482).
parseProduceResponseV9 :: Wire ProduceResponse
parseProduceResponseV9 = do
  _correlationId <- int32
  skipTaggedFields  -- response header v1
  msgs <- compactArray parseProduceResponseMessageV9
  throttle <- int32
  skipTaggedFields  -- body
  pure (ProduceResponse msgs throttle)

parseProduceResponseMessageV9 :: Wire ProduceResponseMessage
parseProduceResponseMessageV9 = do
  topicBS <- compactString
  resps <- compactArray parseProducePartitionResponseV9
  skipTaggedFields
  pure (ProduceResponseMessage (TopicName topicBS) resps)
{-# INLINE parseProduceResponseMessageV9 #-}

parseProducePartitionResponseV9 :: Wire ProducePartitionResponse
parseProducePartitionResponseV9 = do
  resp <- ProducePartitionResponse
    <$> int32 <*> int16 <*> int64 <*> int64 <*> int64
  skipTaggedFields
  pure resp
{-# INLINE parseProducePartitionResponseV9 #-}
