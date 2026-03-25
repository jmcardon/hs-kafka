{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.JoinGroup.Request
  ( joinGroupRequest
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Bytes.Types (Bytes(Bytes))
import qualified Data.Bytes
import Data.Coerce (coerce)
import Data.Int (Int16, Int32)
import Data.Primitive.ByteArray (ByteArray, sizeofByteArray)

import Kafka.Common
import Kafka.Internal.Writer

joinGroupApiVersion :: Int16
joinGroupApiVersion = 4

joinGroupApiKey :: Int16
joinGroupApiKey = 11

defaultSessionTimeout :: Int32
defaultSessionTimeout = 30000

defaultRebalanceTimeout :: Int32
defaultRebalanceTimeout = 30000

defaultProtocolData :: TopicName -> BuildR
defaultProtocolData topic =
  int32 1 -- 1 protocol
  <> string "range" -- protocol name
  <> int32 (12 + fromIntegral topicSize) -- metadata bytes length
  <> int16 0 -- version
  <> int32 1 -- number of subscriptions
  <> topicName topic -- topic name
  <> int32 0 -- userdata bytes length
  where
    topicSize = BS.length (coerce topic :: ByteString)

baToBS :: ByteArray -> ByteString
baToBS ba = Data.Bytes.toByteString (Bytes ba 0 (sizeofByteArray ba))

joinGroupRequest ::
     TopicName
  -> GroupMember
  -> BSL.ByteString
joinGroupRequest topic (GroupMember (GroupName gid) mid) =
  buildRequest $
    int16 joinGroupApiKey
    <> int16 joinGroupApiVersion
    <> int32 correlationId
    <> string clientId
    <> string gid
    <> int32 defaultSessionTimeout
    <> int32 defaultRebalanceTimeout
    <> maybe
        (int16 0)
        (\m -> bytearray (baToBS m))
        mid
    <> string "consumer"
    <> defaultProtocolData topic
