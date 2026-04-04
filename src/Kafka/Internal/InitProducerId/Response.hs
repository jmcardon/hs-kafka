module Kafka.Internal.InitProducerId.Response
  ( InitProducerIdResponse(..)
  , parseInitProducerIdResponse
  , parseInitProducerIdResponseV4
  , getInitProducerIdResponse
  ) where

import Control.Concurrent.STM (TVar)
import Data.Int (Int16, Int32, Int64)
import System.IO (Handle)

import Kafka.Common
import Kafka.Internal.Combinator
import Kafka.Internal.Response

data InitProducerIdResponse = InitProducerIdResponse
  { ipThrottleTimeMs :: {-# UNPACK #-} !Int32
  , ipErrorCode      :: {-# UNPACK #-} !Int16
  , ipProducerId     :: {-# UNPACK #-} !Int64
  , ipProducerEpoch  :: {-# UNPACK #-} !Int16
  } deriving (Eq, Show)

-- | Parse InitProducerId v0-v3 response (legacy encoding).
parseInitProducerIdResponse :: Parser InitProducerIdResponse
parseInitProducerIdResponse = do
  _correlationId <- int32 "correlation id"
  InitProducerIdResponse
    <$> int32 "throttle time"
    <*> int16 "error code"
    <*> int64 "producer id"
    <*> int16 "producer epoch"

-- | Parse InitProducerId v4+ response (flexible/compact encoding).
-- Response header v1: correlation_id + tagged_fields (KIP-482).
parseInitProducerIdResponseV4 :: Parser InitProducerIdResponse
parseInitProducerIdResponseV4 = do
  _correlationId <- int32 "correlation id"
  skipTaggedFields  -- response header v1 tagged fields
  resp <- InitProducerIdResponse
    <$> int32 "throttle time"
    <*> int16 "error code"
    <*> int64 "producer id"
    <*> int16 "producer epoch"
  skipTaggedFields  -- body tagged fields
  pure resp

getInitProducerIdResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String InitProducerIdResponse))
getInitProducerIdResponse = fromKafkaResponse parseInitProducerIdResponse
