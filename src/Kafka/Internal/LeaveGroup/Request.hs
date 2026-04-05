{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.LeaveGroup.Request
  ( leaveGroupRequest
  ) where

import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int16, Int32)

import Kafka.Common
import Kafka.Internal.Writer

leaveGroupApiVersion :: Int16
leaveGroupApiVersion = 2

leaveGroupApiKey :: Int16
leaveGroupApiKey = 13

leaveGroupRequest ::
     GroupMember
  -> BSL.ByteString
leaveGroupRequest (GroupMember (GroupName gid) mid) =
  buildRequest $
    int16 leaveGroupApiKey
    <> int16 leaveGroupApiVersion
    <> int32 correlationId
    <> string clientId
    <> string gid
    <> maybe
        (int16 0)
        (\m -> bytearray m)
        mid
