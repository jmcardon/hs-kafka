{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.Heartbeat.Request
  ( heartbeatRequest
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BSL
import Data.Bytes.Types (Bytes(Bytes))
import qualified Data.Bytes
import Data.Int (Int16, Int32)
import Data.Primitive.ByteArray (ByteArray, sizeofByteArray)

import Kafka.Common
import Kafka.Internal.Writer

heartbeatApiVersion :: Int16
heartbeatApiVersion = 2

heartbeatApiKey :: Int16
heartbeatApiKey = 12

baToBS :: ByteArray -> ByteString
baToBS ba = Data.Bytes.toByteString (Bytes ba 0 (sizeofByteArray ba))

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
        (\m -> bytearray (baToBS m))
        mid
