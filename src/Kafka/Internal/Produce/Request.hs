{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.Produce.Request
  ( ProduceRequestParams(..)
  , buildProduceRequest
  ) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Int (Int16, Int32, Int64)

import Kafka.Common (TopicName(..))
import Kafka.Internal.Compression (compressBatch)
import Kafka.Internal.Config (Compression(..))
import Kafka.Internal.RecordBatch (RecordInput(..), buildRecordBatch, buildRecords, wrapRecordBatch)
import Kafka.Internal.Writer

produceApiVersion :: Int16
produceApiVersion = 9

produceApiKey :: Int16
produceApiKey = 0

-- | Parameters for building a produce request.
data ProduceRequestParams = ProduceRequestParams
  { prpCorrId        :: {-# UNPACK #-} !Int32
  , prpAcks          :: {-# UNPACK #-} !Int16
  , prpClientId      :: !ByteString
  , prpTimeoutMs     :: {-# UNPACK #-} !Int
  , prpTopic         :: !TopicName
  , prpPartition     :: {-# UNPACK #-} !Int32
  , prpProducerId    :: {-# UNPACK #-} !Int64
  , prpProducerEpoch :: {-# UNPACK #-} !Int16
  , prpBaseSequence  :: {-# UNPACK #-} !Int32
  , prpCompression   :: !Compression
  , prpFirstTs       :: {-# UNPACK #-} !Int64
  }

-- | Build a Produce v9 request as a strict ByteString.
-- Single BuildR materialization — no intermediate BSL concatenation.
buildProduceRequest :: ProduceRequestParams -> [RecordInput] -> ByteString
buildProduceRequest !params records =
  let
    !n = length records

    !batchBS = case prpCompression params of
      NoCompression ->
        buildRecordBatch (prpProducerId params) (prpProducerEpoch params)
          (prpBaseSequence params) 0 (prpFirstTs params) records
      _ ->
        let !rawRecords = buildRecords records
            (!compRecords, !attr) = compressBatch (prpCompression params) rawRecords
        in if attr == 0
          then buildRecordBatch (prpProducerId params) (prpProducerEpoch params)
                 (prpBaseSequence params) 0 (prpFirstTs params) records
          else wrapRecordBatch (prpProducerId params) (prpProducerEpoch params)
                 (prpBaseSequence params) attr n (prpFirstTs params) compRecords

    !batchLen = BS.length batchBS
    TopicName !tn = prpTopic params
    !topicLen = BS.length tn

    !prefixBuilder =
      int16 produceApiKey
      <> int16 produceApiVersion
      <> int32 (prpCorrId params)
      <> string (prpClientId params)
      <> taggedFields
      <> compactNullableString Nothing
      <> int16 (prpAcks params)
      <> int32 (fromIntegral (prpTimeoutMs params))
      <> unsignedVarInt 2
      <> compactString tn
      <> unsignedVarInt 2
      <> int32 (prpPartition params)
      <> unsignedVarInt (batchLen + 1)

    !suffixBuilder = taggedFields <> taggedFields <> taggedFields

    -- Compute body size arithmetically
    !prefixSize = 24 + BS.length (prpClientId params)
                + uvarSize (topicLen + 1) + topicLen
                + uvarSize (batchLen + 1)
    !bodySize = fromIntegral (prefixSize + batchLen + 3) :: Int32

    -- Single BuildR, single materialization
    !fullRequest = int32 bodySize <> prefixBuilder <> bs batchBS <> suffixBuilder

  in BSL.toStrict (toLazyByteString fullRequest)

uvarSize :: Int -> Int
uvarSize n
  | n < 0x80       = 1
  | n < 0x4000     = 2
  | n < 0x200000   = 3
  | n < 0x10000000 = 4
  | otherwise       = 5
{-# INLINE uvarSize #-}
