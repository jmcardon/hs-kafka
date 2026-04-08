{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.Heartbeat.Request
  ( heartbeatRequest
  ) where

import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int16)

import Kafka.Common
import Kafka.Internal.Writer

heartbeatApiVersion :: Int16
heartbeatApiVersion = 2

heartbeatApiKey :: Int16
heartbeatApiKey = 12

heartbeatRequest ::
     GroupMember
  -> GenerationId
  -> BSL.ByteString
heartbeatRequest (GroupMember (GroupName gid) mid) (GenerationId genId) =
  buildRequest $
    int16 heartbeatApiKey
    <> int16 heartbeatApiVersion
    <> int32 correlationId
    <> string clientId
    <> string gid
    <> int32 genId
    <> maybe
        (int16 0)
        (\m -> bytearray m)
        mid
