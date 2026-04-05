module Kafka.Internal.Heartbeat.Response
  ( HeartbeatResponse(..)
  , parseHeartbeatResponse
  ) where

import Data.Int (Int16, Int32)

import Kafka.Internal.Wire

data HeartbeatResponse = HeartbeatResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  } deriving (Eq, Show)

parseHeartbeatResponse :: Wire HeartbeatResponse
parseHeartbeatResponse = do
  _correlationId <- int32
  HeartbeatResponse <$> int32 <*> int16
