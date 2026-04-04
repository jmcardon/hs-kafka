{-# language
    BangPatterns
  , LambdaCase
  , OverloadedStrings
  #-}

module Kafka.Internal.Produce.Response
  ( ProducePartitionResponse(..)
  , ProduceResponse(..)
  , ProduceResponseMessage(..)
  , getProduceResponse
  , parseProduceResponse
  , parseProduceResponseV9
  ) where

import Control.Concurrent.STM (TVar)
import Data.Int (Int16, Int32, Int64)
import System.IO (Handle)

import Kafka.Internal.Combinator
import Kafka.Common (Kafka, KafkaException(..), TopicName(..))
import Kafka.Internal.Response (fromKafkaResponse)

import qualified Data.Bytes as B
import qualified Data.Bytes.Parser as Smith

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
parseProduceResponse :: Parser ProduceResponse
parseProduceResponse = do
  _correlationId <- int32 "correlationId"
  responsesCount <- int32 "responses count"
  ProduceResponse
    <$> (count responsesCount parseProduceResponseMessage <?> "response messages")
    <*> (int32 "throttle time")

parseProduceResponseMessage :: Parser ProduceResponseMessage
parseProduceResponseMessage = do
  tlen <- int16 "topic length"
  t <- Smith.take "topic name" (fromIntegral tlen)
  let top = B.toByteString t
  prc <- int32 "partition response count"
  resps <- count prc parseProducePartitionResponse
  pure (ProduceResponseMessage (TopicName top) resps)

parseProducePartitionResponse :: Parser ProducePartitionResponse
parseProducePartitionResponse = ProducePartitionResponse
  <$> int32 "partition"
  <*> int16 "error code"
  <*> int64 "base offset"
  <*> int64 "log append time"
  <*> int64 "log start time"

-- | Parse Produce v9+ response (flexible/compact encoding).
-- Response header v1: correlation_id + tagged_fields (KIP-482).
parseProduceResponseV9 :: Parser ProduceResponse
parseProduceResponseV9 = do
  _correlationId <- int32 "correlationId"
  skipTaggedFields  -- response header v1 tagged fields
  msgs <- compactArray parseProduceResponseMessageV9
  throttle <- int32 "throttle time"
  skipTaggedFields  -- body tagged fields
  pure (ProduceResponse msgs throttle)

parseProduceResponseMessageV9 :: Parser ProduceResponseMessage
parseProduceResponseMessageV9 = do
  topicBS <- compactString
  resps <- compactArray parseProducePartitionResponseV9
  skipTaggedFields  -- topic tagged fields
  pure (ProduceResponseMessage (TopicName topicBS) resps)

parseProducePartitionResponseV9 :: Parser ProducePartitionResponse
parseProducePartitionResponseV9 = do
  resp <- ProducePartitionResponse
    <$> int32 "partition"
    <*> int16 "error code"
    <*> int64 "base offset"
    <*> int64 "log append time"
    <*> int64 "log start time"
  skipTaggedFields  -- partition tagged fields
  pure resp

getProduceResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String ProduceResponse))
getProduceResponse = fromKafkaResponse parseProduceResponse
