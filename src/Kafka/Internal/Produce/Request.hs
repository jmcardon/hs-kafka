{-# LANGUAGE BangPatterns #-}
{-# LANGUAGE OverloadedStrings #-}

module Kafka.Internal.Produce.Request
  ( produceRequest
  , produceRequestIdempotent
  , produceRequestCompressed
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
import Kafka.Internal.Compression (compressBatch)
import Kafka.Internal.Config (Compression(..))
import Kafka.Internal.Writer
import Kafka.Internal.Zigzag (zigzag)

-- Record batch magic byte (message format v2)
magic :: Int8
magic = 2

produceApiVersion :: Int16
produceApiVersion = 9

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

-- | Convert a ByteArray to a ByteString.
baToBS :: ByteArray -> ByteString
baToBS ba = Data.Bytes.toByteString (Bytes ba 0 (sizeofByteArray ba))

-- | Convert a ByteString to a ByteArray (single memcpy).
bsToBA :: ByteString -> ByteArray
bsToBA bs = Data.Bytes.toByteArrayClone (Data.Bytes.fromByteString bs)

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

-- | Build record batch metadata with CRC over postCrc + records.
-- The records ByteArray is the (possibly compressed) records section.
recordBatchMetadataWithRecords ::
     Int64 -- ^ producerId (-1 for non-idempotent)
  -> Int16 -- ^ producerEpoch (-1 for non-idempotent)
  -> Int32 -- ^ baseSequence (-1 for non-idempotent)
  -> Int16 -- ^ record batch attributes (compression bits)
  -> Int   -- ^ record count
  -> ByteArray -- ^ records section (possibly compressed)
  -> ByteArray
recordBatchMetadataWithRecords !producerId !producerEpoch !baseSeq !batchAttrs
    !recordCount records =
  let
    recordsSize = sizeofByteArray records
    crc = CRC.bytes (CRC.bytes 0 (Bytes postCrc 0 postCrcLength))
            (Bytes records 0 recordsSize)
    batchLength = fromIntegral $
        preCrcLength
      + postCrcLength
      + recordsSize
    preCrcLength = 9
    preCrc = buildToBA $
      int64 defaultBaseOffset
      <> int32 batchLength
      <> int32 defaultPartitionLeaderEpoch
      <> int8 magic
      <> int32 (fromIntegral crc)
    postCrcLength = 40
    postCrc = buildToBA $
      int16 batchAttrs
      <> int32 (fromIntegral (recordCount - 1))
      <> int64 defaultFirstTimestamp
      <> int64 defaultMaxTimestamp
      <> int64 producerId
      <> int16 producerEpoch
      <> int32 baseSeq
      <> int32 (fromIntegral recordCount)
  in
    preCrc <> postCrc

-- | Build the Produce v9 request prefix (everything before the record batch bytes).
-- Uses flexible encoding (compact strings/arrays + tagged fields).
-- Layout: header v2 | body fields | topic array header | partition fields | records length
-- After this, the caller appends: record batch bytes | partition TF | topic TF | body TF
makeRequestPrefix ::
     Int16 -- ^ acks value
  -> ByteString -- ^ client ID
  -> Int -- ^ record batch section size
  -> Int -- ^ timeout (ms)
  -> TopicName -- ^ topic name
  -> Int32 -- ^ partition
  -> BuildR
makeRequestPrefix !acksVal !cid !rbss !timeout (TopicName tn) !partition =
  -- Request header v2 (clientId is always legacy INT16 string per KIP-482)
  int16 produceApiKey
  <> int16 produceApiVersion
  <> int32 correlationId
  <> string cid                         -- clientId (legacy INT16 string, even in flexible)
  <> taggedFields                       -- header tagged fields (0 = none)
  -- Produce v9 body fields
  <> compactNullableString Nothing      -- transactional_id (null = non-transactional)
  <> int16 acksVal                      -- acks
  <> int32 (fromIntegral timeout)       -- timeout_ms
  <> unsignedVarInt 2                   -- compact array: 1 topic (count+1)
  -- TopicProduceData
  <> compactString tn                   -- topic name (COMPACT_STRING)
  <> unsignedVarInt 2                   -- compact array: 1 partition (count+1)
  -- PartitionProduceData
  <> int32 partition                    -- partition index
  <> unsignedVarInt (rbss + 1)          -- records length (compact bytes: UVARINT(len+1))

-- | Gather payload section chunks into a single ByteArray.
gatherChunks :: UnliftedArray ByteArray -> ByteArray
gatherChunks = foldrUnliftedArray (<>) mempty

-- | Build the full produce payload for Produce v9 (flexible encoding).
--
-- Wire layout:
--   INT32 total_body_size
--   [request header v2]
--   [produce body: transactional_id, acks, timeout, topic_data array]
--     [TopicProduceData: name, partition_data array]
--       [PartitionProduceData: index, INT32 records_length, record_batch_bytes, taggedFields]
--     [topic taggedFields]
--   [body taggedFields]
buildProducePayload ::
     Int16 -- ^ acks
  -> ByteString -- ^ client ID
  -> Int -- ^ timeout (ms)
  -> TopicName
  -> Int32 -- ^ partition
  -> Int64 -- ^ producerId
  -> Int16 -- ^ producerEpoch
  -> Int32 -- ^ baseSequence
  -> Compression -- ^ compression codec
  -> UnliftedArray ByteArray -- ^ payloads
  -> BSL.ByteString
buildProducePayload acksVal cid timeout topic partition
    producerId producerEpoch baseSeq compression payloads =
  let
    payloadCount = sizeofUnliftedArray payloads
    zero = runST $ do
      ba <- newByteArray 1
      writeByteArray ba 0 (0 :: Word8)
      unsafeFreezeByteArray ba

    -- Build the records section (metadata + payload + zero per record)
    payloadsSectionChunks = runUnliftedArray $ do
      arr <- newUnliftedArray (3 * payloadCount) zero
      itraverseUnliftedArray_
        (\i payload -> do
          writeUnliftedArray arr (i * 3) (makeRecordMetadata i payload)
          writeUnliftedArray arr (i * 3 + 1) payload
          writeUnliftedArray arr (i * 3 + 2) zero)
        payloads
      pure arr

    -- Gather records into single ByteArray, then optionally compress
    rawRecords = gatherChunks payloadsSectionChunks
    (!records, !compressionAttr) = compressBatch compression rawRecords

    -- Build record batch metadata (preCrc + postCrc with CRC over postCrc + records)
    rbMeta = recordBatchMetadataWithRecords
      producerId producerEpoch baseSeq compressionAttr
      payloadCount records

    -- Record batch section = rbMeta + records
    recordBatchSection = rbMeta <> records
    recordBatchSectionSize = sizeofByteArray recordBatchSection

    -- Build prefix (everything before record batch bytes)
    prefixBuilder = makeRequestPrefix acksVal cid recordBatchSectionSize timeout topic partition
    -- Suffix: trailing tagged fields (partition, topic, body)
    suffixBuilder = taggedFields <> taggedFields <> taggedFields

    -- Assemble the full body (after size prefix)
    prefixBytes = toLazyByteString prefixBuilder
    recordBatchBS = BSL.fromStrict (baToBS recordBatchSection)
    suffixBytes = toLazyByteString suffixBuilder
    fullBody = prefixBytes <> recordBatchBS <> suffixBytes

    -- Size prefix
    bodySize = fromIntegral (BSL.length fullBody) :: Int32

  in toLazyByteString (int32 bodySize) <> fullBody

-- | Build a produce request (non-idempotent, no compression).
-- PID/epoch/sequence are set to -1.
produceRequest ::
     Int16 -- ^ acks
  -> ByteString -- ^ client ID
  -> Int -- ^ timeout (ms)
  -> TopicName
  -> Int32
  -> UnliftedArray ByteArray
  -> BSL.ByteString
produceRequest acksVal cid timeout topic partition payloads =
  buildProducePayload acksVal cid timeout topic partition
    (-1) (-1) (-1) NoCompression payloads

-- | Build a produce request with idempotent producer state.
-- PID/epoch/baseSequence are provided from the idempotent state.
produceRequestIdempotent ::
     Int16 -- ^ acks (should be -1 for idempotent)
  -> ByteString -- ^ client ID
  -> Int -- ^ timeout (ms)
  -> TopicName
  -> Int32 -- ^ partition
  -> Int64 -- ^ producerId
  -> Int16 -- ^ producerEpoch
  -> Int32 -- ^ baseSequence for this partition
  -> UnliftedArray ByteArray -- ^ payloads
  -> BSL.ByteString
produceRequestIdempotent acksVal cid timeout topic partition
    producerId producerEpoch baseSeq payloads =
  buildProducePayload acksVal cid timeout topic partition
    producerId producerEpoch baseSeq NoCompression payloads

-- | Build a produce request with compression and optional idempotent state.
produceRequestCompressed ::
     Int16 -- ^ acks
  -> ByteString -- ^ client ID
  -> Int -- ^ timeout (ms)
  -> TopicName
  -> Int32 -- ^ partition
  -> Int64 -- ^ producerId (-1 for non-idempotent)
  -> Int16 -- ^ producerEpoch (-1 for non-idempotent)
  -> Int32 -- ^ baseSequence (-1 for non-idempotent)
  -> Compression -- ^ compression codec
  -> UnliftedArray ByteArray -- ^ payloads
  -> BSL.ByteString
produceRequestCompressed = buildProducePayload
