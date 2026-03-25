{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.Produce.Request
  ( produceRequest
  ) where

import Control.Monad.ST (runST)
import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Bytes.Types (Bytes(Bytes))
import qualified Data.Bytes
import Data.Coerce (coerce)
import Data.Foldable
import Data.Int (Int8, Int16, Int32, Int64)
import Data.Primitive.ByteArray (ByteArray, byteArrayFromList, newByteArray, sizeofByteArray,
  unsafeFreezeByteArray, writeByteArray)
-- No longer using Data.Primitive.Slice; CRC is computed via fold over UnliftedArray
import Data.Primitive.Unlifted.Array
import Data.Word (Word8)

import qualified Crc32c as CRC

import Kafka.Common
import Kafka.Internal.Writer
import Kafka.Internal.Zigzag (zigzag)

-- idk what this is
magic :: Int8
magic = 2

produceApiVersion :: Int16
produceApiVersion = 7

produceApiKey :: Int16
produceApiKey = 0

defaultBaseOffset :: Int64
defaultBaseOffset = 0

defaultPartitionLeaderEpoch :: Int32
defaultPartitionLeaderEpoch = 0

defaultRecordAttributes :: Int8
defaultRecordAttributes = 0

defaultTimestampDelta :: Int
defaultTimestampDelta = 0

defaultFirstTimestamp :: Int64
defaultFirstTimestamp = 0

defaultMaxTimestamp :: Int64
defaultMaxTimestamp = 0

defaultProducerId :: Int64
defaultProducerId = -1

defaultProducerEpoch :: Int16
defaultProducerEpoch = -1

defaultBaseSequence :: Int32
defaultBaseSequence = -1

defaultRecordBatchAttributes :: Int16
defaultRecordBatchAttributes = 0

-- | Convert a ByteArray to a ByteString.
baToBS :: ByteArray -> ByteString
baToBS ba = Data.Bytes.toByteString (Bytes ba 0 (sizeofByteArray ba))

-- | Convert a ByteString to a ByteArray.
bsToBA :: ByteString -> ByteArray
bsToBA bstr = byteArrayFromList (BS.unpack bstr)

-- | Materialize a BuildR to a ByteArray (for CRC computation).
buildToBA :: BuildR -> ByteArray
buildToBA b = bsToBA (BSL.toStrict (toLazyByteString b))

makeRecordMetadata :: Int -> ByteArray -> ByteArray
makeRecordMetadata index content =
  let
    -- plus one is for the trailing null byte
    recordLength = zigzag (sizeofByteArray metadataContent + sizeofByteArray content + 1)
    metadataContent = fold
      [ byteArrayFromList [defaultRecordAttributes]
      , zigzag defaultTimestampDelta
      , zigzag index -- offsetDelta
      , zigzag (-1) -- keyLength
      , zigzag (sizeofByteArray content) -- valueLen
      ]
  in
    recordLength <> metadataContent

sumSizes :: UnliftedArray ByteArray -> Int
sumSizes = foldrUnliftedArray (\e acc -> acc + sizeofByteArray e) 0

produceRequestRecordBatchMetadata ::
     UnliftedArray ByteArray
  -> Int
  -> Int
  -> ByteArray
produceRequestRecordBatchMetadata payloadsSectionChunks payloadCount payloadsSectionSize =
  let
    crc = crcOverChunks
        (CRC.bytes 0 (Bytes postCrc 0 postCrcLength))
        payloadsSectionChunks 0 (3*payloadCount)
    crcOverChunks !acc _arr !ix !end
      | ix >= end = acc
      | otherwise =
          let ba = indexUnliftedArray payloadsSectionChunks ix
          in crcOverChunks (CRC.bytes acc (Bytes ba 0 (sizeofByteArray ba)))
               payloadsSectionChunks (ix + 1) end
    batchLength = fromIntegral $
        preCrcLength
      + postCrcLength
      + payloadsSectionSize
    preCrcLength = 9
    preCrc = buildToBA $
      int64 defaultBaseOffset
      <> int32 batchLength
      <> int32 defaultPartitionLeaderEpoch
      <> int8 magic
      <> int32 (fromIntegral crc)
    postCrcLength = 40
    postCrc = buildToBA $
      int16 defaultRecordBatchAttributes
      <> int32 (fromIntegral (payloadCount - 1))
      <> int64 defaultFirstTimestamp
      <> int64 defaultMaxTimestamp
      <> int64 defaultProducerId
      <> int16 defaultProducerEpoch
      <> int32 defaultBaseSequence
      <> int32 (fromIntegral payloadCount)
  in
    preCrc <> postCrc

makeRequestMetadata :: ()
  => Int16 -- ^ acks value
  -> ByteString -- ^ client ID
  -> Int -- ^ record batch section size
  -> Int -- ^ timeout (microseconds)
  -> TopicName -- ^ topic name
  -> Int32 -- ^ partition
  -> ByteArray
makeRequestMetadata !acksVal !cid !rbss !timeout tn !partition = buildToBA $
  int32 size
  <> int16 produceApiKey
  <> int16 produceApiVersion
  <> int32 correlationId
  <> string cid
  <> int16 (-1) -- transactional_id length
  <> int16 acksVal -- acks
  <> int32 (fromIntegral timeout) -- timeout in ms
  <> int32 1 -- following array length
  <> topicName tn -- topic_data topic
  <> int32 1 -- following array [data] length
  <> int32 partition -- partition
  <> int32 (fromIntegral rbss) -- record_set length
  where
    minimumSize = 36
    cidLen = BS.length cid
    topicNameSize = BS.length (coerce tn :: ByteString)
    size = fromIntegral $ 0
      + minimumSize
      + cidLen
      + topicNameSize
      + rbss

produceRequest ::
     Int16 -- ^ acks
  -> ByteString -- ^ client ID
  -> Int -- ^ timeout (ms)
  -> TopicName
  -> Int32
  -> UnliftedArray ByteArray
  -> BSL.ByteString
produceRequest acksVal cid timeout topic partition payloads =
  let
    payloadCount = sizeofUnliftedArray payloads
    zero = runST $ do
      ba <- newByteArray 1
      writeByteArray ba 0 (0 :: Word8)
      unsafeFreezeByteArray ba
    recordBatchSectionSize = 0
      + sumSizes payloadsSectionChunks
      + sizeofByteArray recordBatchMetadata
    requestMetadata = makeRequestMetadata
      acksVal
      cid
      recordBatchSectionSize
      timeout
      topic
      partition
    recordBatchMetadata =
      produceRequestRecordBatchMetadata
        payloadsSectionChunks
        payloadCount
        (sumSizes payloadsSectionChunks)
    payloadsSectionChunks = runUnliftedArray $ do
      arr <- newUnliftedArray (3 * payloadCount) zero
      itraverseUnliftedArray_
        (\i payload -> do
          writeUnliftedArray arr (i * 3) (makeRecordMetadata i payload)
          writeUnliftedArray arr (i * 3 + 1) payload
          writeUnliftedArray arr (i * 3 + 2) zero)
        payloads
      pure arr
    finalArr = runUnliftedArray $ do
      arr <- newUnliftedArray (3 * payloadCount + 2) zero
      writeUnliftedArray arr 0 requestMetadata
      writeUnliftedArray arr 1 recordBatchMetadata
      copyUnliftedArray arr 2 payloadsSectionChunks 0 (3 * payloadCount)
      pure arr
  in gatherToLBS finalArr

-- | Gather an UnliftedArray ByteArray into a lazy ByteString.
-- Concatenates all ByteArray chunks into a single contiguous ByteArray
-- first (one allocation, one copy), then converts to ByteString.
gatherToLBS :: UnliftedArray ByteArray -> BSL.ByteString
gatherToLBS chunks =
  let gathered = foldrUnliftedArray (<>) mempty chunks
  in BSL.fromStrict (baToBS gathered)
