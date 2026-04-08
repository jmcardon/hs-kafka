module Kafka.Internal.LeaveGroup.Response
  ( LeaveGroupResponse(..)
  , parseLeaveGroupResponse
  ) where

import Data.Int (Int16, Int32)

import Kafka.Internal.Wire

data LeaveGroupResponse = LeaveGroupResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  } deriving (Eq, Show)

parseLeaveGroupResponse :: Wire LeaveGroupResponse
parseLeaveGroupResponse = do
  _correlationId <- int32
  LeaveGroupResponse <$> int32 <*> int16
