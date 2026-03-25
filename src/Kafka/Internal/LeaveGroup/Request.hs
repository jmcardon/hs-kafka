{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.LeaveGroup.Request
  ( leaveGroupRequest
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BSL
import Data.Bytes.Types (Bytes(Bytes))
import qualified Data.Bytes
import Data.Int (Int16, Int32)
import Data.Primitive.ByteArray (ByteArray, sizeofByteArray)

import Kafka.Common
import Kafka.Internal.Writer

leaveGroupApiVersion :: Int16
leaveGroupApiVersion = 2

leaveGroupApiKey :: Int16
leaveGroupApiKey = 13

baToBS :: ByteArray -> ByteString
baToBS ba = Data.Bytes.toByteString (Bytes ba 0 (sizeofByteArray ba))

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
        (\m -> bytearray (baToBS m))
        mid
