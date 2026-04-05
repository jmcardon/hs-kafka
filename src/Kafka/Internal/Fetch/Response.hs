{-# language
    BangPatterns
  , MagicHash
  , OverloadedStrings
  , RecordWildCards
  , UnboxedSums
  , UnboxedTuples
  #-}

module Kafka.Internal.Fetch.Response
  ( FetchResponse(..)
  , FetchTopic(..)
  , Header(..)
  , PartitionHeader(..)
  , FetchPartition(..)
  , Record(..)
  , RecordBatch(..)
  , parseFetchResponse
  , partitionLastSeenOffset
  ) where

import Data.ByteString (ByteString)
import Data.Int (Int8, Int16, Int32, Int64)
import Data.List (find, intercalate)
import Data.List.NonEmpty (nonEmpty)
import Data.Maybe (mapMaybe)

import qualified Data.Foldable as F

import GHC.Exts (Addr#, eqAddr#)
import GHC.ForeignPtr (ForeignPtrContents)

import Kafka.Common (TopicName(..))
import Kafka.Internal.Wire

fetchResponseContents :: FetchResponse -> [ByteString]
fetchResponseContents = id
  . mapMaybe recordValue
  . concatMap records
  . concat
  . mapMaybe recordSet
  . concatMap partitions
  . topics

instance Show FetchResponse where
  show resp =
    "Fetch (error_code "
    <> show (errorCode resp)
    <> ", partition_header errors "
    <> intercalate ","
       [ show x
       | t <- topics resp
       , p <- partitions t
       , let x = partitionHeaderErrorCode (partitionHeader p)
       , x /= 0
       ]
    <> ", "
    <> show (length (fetchResponseContents resp))
    <> " records)"

data FetchResponse = FetchResponse
  { throttleTimeMs :: {-# UNPACK #-} !Int32
  , errorCode :: {-# UNPACK #-} !Int16
  , sessionId :: {-# UNPACK #-} !Int32
  , topics :: [FetchTopic]
  }

data FetchTopic = FetchTopic
  { topic :: !TopicName
  , partitions :: [FetchPartition]
  } deriving (Eq, Show)

data FetchPartition = FetchPartition
  { partitionHeader :: !PartitionHeader
  , recordSet :: !(Maybe [RecordBatch])
  } deriving (Eq, Show)

data PartitionHeader = PartitionHeader
  { partition :: {-# UNPACK #-} !Int32
  , partitionHeaderErrorCode :: {-# UNPACK #-} !Int16
  , highWatermark :: {-# UNPACK #-} !Int64
  , lastStableOffset :: {-# UNPACK #-} !Int64
  , logStartOffset :: {-# UNPACK #-} !Int64
  , abortedTransactions :: [AbortedTransaction]
  } deriving (Eq, Show)

data AbortedTransaction = AbortedTransaction
  { abortedTransactionProducerId :: {-# UNPACK #-} !Int64
  , firstOffset :: {-# UNPACK #-} !Int64
  } deriving (Eq, Show)

data RecordBatch = RecordBatch
  { baseOffset :: {-# UNPACK #-} !Int64
  , batchLength :: {-# UNPACK #-} !Int32
  , partitionLeaderEpoch :: {-# UNPACK #-} !Int32
  , recordBatchMagic :: {-# UNPACK #-} !Int8
  , crc :: {-# UNPACK #-} !Int32
  , attributes :: {-# UNPACK #-} !Int16
  , lastOffsetDelta :: {-# UNPACK #-} !Int32
  , firstTimestamp :: {-# UNPACK #-} !Int64
  , maxTimestamp :: {-# UNPACK #-} !Int64
  , producerId :: {-# UNPACK #-} !Int64
  , producerEpoch :: {-# UNPACK #-} !Int16
  , baseSequence :: {-# UNPACK #-} !Int32
  , records :: [Record]
  } deriving (Eq, Show)

data Record = Record
  { recordLength :: {-# UNPACK #-} !Int
  , recordAttributes :: {-# UNPACK #-} !Int8
  , recordTimestampDelta :: {-# UNPACK #-} !Int
  , recordOffsetDelta :: {-# UNPACK #-} !Int
  , recordKey :: !(Maybe ByteString)
  , recordValue :: !(Maybe ByteString)
  , recordHeaders :: [Header]
  } deriving (Eq, Show)

data Header = Header
  { headerKey :: !(Maybe ByteString)
  , headerValue :: !(Maybe ByteString)
  } deriving (Eq, Show)

parseFetchResponse :: Wire FetchResponse
parseFetchResponse = do
  _correlationId <- int32
  FetchResponse
    <$> int32 <*> int16 <*> int32
    <*> legacyNullableArray parseFetchTopic

parseFetchTopic :: Wire FetchTopic
parseFetchTopic = FetchTopic
  <$> (TopicName <$> legacyString)
  <*> legacyNullableArray parseFetchPartition
{-# INLINE parseFetchTopic #-}

parseFetchPartition :: Wire FetchPartition
parseFetchPartition = FetchPartition
  <$> parsePartitionHeader
  <*> parseNullableRecordBatches
{-# INLINE parseFetchPartition #-}

parsePartitionHeader :: Wire PartitionHeader
parsePartitionHeader = PartitionHeader
  <$> int32 <*> int16 <*> int64 <*> int64 <*> int64
  <*> legacyNullableArray parseAbortedTransaction
{-# INLINE parsePartitionHeader #-}

parseAbortedTransaction :: Wire AbortedTransaction
parseAbortedTransaction = AbortedTransaction <$> int64 <*> int64
{-# INLINE parseAbortedTransaction #-}

-- | Nullable record set: INT32 length, then record batches until length consumed.
-- Uses a sub-parse on the bytes slice.
parseNullableRecordBatches :: Wire (Maybe [RecordBatch])
parseNullableRecordBatches = do
  len <- int32
  if len <= 0
    then pure Nothing
    else do
      batchBytes <- takeBytes (fromIntegral len)
      case runWire (parseMany parseRecordBatch) batchBytes of
        Nothing -> pure Nothing
        Just rbs -> pure (Just rbs)

-- | Parse as many items as possible until input exhausted.
parseMany :: Wire a -> Wire [a]
parseMany (Wire p) = Wire $ \fpc pos end -> go fpc pos end []
  where
    go fpc pos end !acc = case eqAddr# pos end of
      1# -> OK# (reverse acc) pos
      _  -> case p fpc pos end of
        OK# a pos' -> go fpc pos' end (a : acc)
        _          -> OK# (reverse acc) pos  -- stop on failure (partial batch)

parseRecordBatch :: Wire RecordBatch
parseRecordBatch = RecordBatch
  <$> int64 <*> int32 <*> int32 <*> int8 <*> int32
  <*> int16 <*> int32 <*> int64 <*> int64 <*> int64 <*> int16 <*> int32
  <*> legacyNullableArray parseRecord

parseRecord :: Wire Record
parseRecord = do
  recordLength <- signedVarInt
  recordAttributes <- int8
  recordTimestampDelta <- signedVarInt
  recordOffsetDelta <- signedVarInt
  recordKey <- varintNullableBytes
  recordValue <- varintNullableBytes
  recordHeaders <- varintArray parseHeader
  pure (Record {..})

varintNullableBytes :: Wire (Maybe ByteString)
varintNullableBytes = do
  len <- signedVarInt
  if len < 0 then pure Nothing else Just <$> takeBytes len
{-# INLINE varintNullableBytes #-}

varintArray :: Wire a -> Wire [a]
varintArray p = do
  n <- signedVarInt
  count n p
{-# INLINE varintArray #-}

parseHeader :: Wire Header
parseHeader = Header <$> varintNullableBytes <*> varintNullableBytes
{-# INLINE parseHeader #-}

-- Lookups

partitionLastSeenOffset :: FetchResponse -> TopicName -> Int32 -> Maybe Int64
partitionLastSeenOffset fetchResponse t partitionId = do
  ftopic <- find (\resp -> t == topic resp) (topics fetchResponse)
  fpart <- find (\resp -> partitionId == partition (partitionHeader resp)) (partitions ftopic)
  set <- recordSet fpart
  maxMaybe (fmap recordBatchLastOffset set)
  where
    recordBatchLastOffset rb =
      baseOffset rb + fromIntegral (lastOffsetDelta rb) + 1
    maxMaybe xs = fmap (F.foldr1 max) (nonEmpty xs)
