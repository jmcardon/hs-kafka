module Kafka.Internal.InitProducerId.Response
  ( InitProducerIdResponse(..)
  , parseInitProducerIdResponse
  , parseInitProducerIdResponseV4
  ) where

import Data.Int (Int16, Int32, Int64)

import Kafka.Internal.Wire

data InitProducerIdResponse = InitProducerIdResponse
  { ipThrottleTimeMs :: {-# UNPACK #-} !Int32
  , ipErrorCode      :: {-# UNPACK #-} !Int16
  , ipProducerId     :: {-# UNPACK #-} !Int64
  , ipProducerEpoch  :: {-# UNPACK #-} !Int16
  } deriving (Eq, Show)

-- | Parse InitProducerId v0-v3 response (legacy encoding).
parseInitProducerIdResponse :: Wire InitProducerIdResponse
parseInitProducerIdResponse = do
  _correlationId <- int32
  InitProducerIdResponse
    <$> int32 <*> int16 <*> int64 <*> int16

-- | Parse InitProducerId v4+ response (flexible/compact encoding).
-- Response header v1: correlation_id + tagged_fields (KIP-482).
parseInitProducerIdResponseV4 :: Wire InitProducerIdResponse
parseInitProducerIdResponseV4 = do
  _correlationId <- int32
  skipTaggedFields  -- response header v1
  resp <- InitProducerIdResponse
    <$> int32 <*> int16 <*> int64 <*> int16
  skipTaggedFields  -- body
  pure resp
