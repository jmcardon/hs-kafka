module Kafka.Internal.InitProducerId.Response
  ( InitProducerIdResponse(..)
  , parseInitProducerIdResponse
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

parseInitProducerIdResponse :: Parser InitProducerIdResponse
parseInitProducerIdResponse = do
  _correlationId <- int32 "correlation id"
  InitProducerIdResponse
    <$> int32 "throttle time"
    <*> int16 "error code"
    <*> int64 "producer id"
    <*> int16 "producer epoch"

getInitProducerIdResponse ::
     Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String InitProducerIdResponse))
getInitProducerIdResponse = fromKafkaResponse parseInitProducerIdResponse
