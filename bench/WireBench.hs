{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Criterion.Main
import Data.ByteString (ByteString)
import qualified Data.ByteString.Lazy as BSL
import Data.Int
import Data.Primitive.ByteArray (ByteArray)
import Control.DeepSeq (NFData(..))

import qualified Data.Bytes
import qualified Data.Bytes.Parser as Smith

import Kafka.Internal.Wire (Wire)
import qualified Kafka.Internal.Wire as Wire
import Kafka.Internal.Combinator (Parser)
import qualified Kafka.Internal.Combinator as C
import Kafka.Internal.Writer (BuildR, toLazyByteString)
import qualified Kafka.Internal.Writer as W

------------------------------------------------------------------------
-- Build a realistic ProduceResponse v9 payload
------------------------------------------------------------------------

-- 10 topics, 4 partitions each = 40 partition responses
buildProduceResponseV9 :: Int -> Int -> ByteString
buildProduceResponseV9 numTopics numPartitions =
  BSL.toStrict $ toLazyByteString $
    W.int32 42                         -- correlationId
    <> W.unsignedVarInt 0              -- header v1 tagged fields
    <> W.unsignedVarInt (numTopics + 1)  -- compact array of topics
    <> mconcat (map buildTopic [0 .. numTopics - 1])
    <> W.int32 0                       -- throttle time
    <> W.unsignedVarInt 0              -- body tagged fields
  where
    buildTopic :: Int -> BuildR
    buildTopic i =
      W.compactString (topicNames !! (i `mod` length topicNames))
      <> W.unsignedVarInt (numPartitions + 1)
      <> mconcat (map buildPartition [0 .. numPartitions - 1])
      <> W.unsignedVarInt 0            -- topic tagged fields

    buildPartition :: Int -> BuildR
    buildPartition p =
      W.int32 (fromIntegral p)         -- partition
      <> W.int16 0                     -- errorCode (success)
      <> W.int64 (fromIntegral (p * 100))  -- baseOffset
      <> W.int64 (-1)                  -- logAppendTime
      <> W.int64 0                     -- logStartOffset
      <> W.unsignedVarInt 0            -- partition tagged fields

    topicNames = ["topic-alpha", "topic-beta", "topic-gamma", "topic-delta"
                 , "topic-epsilon", "topic-zeta", "topic-eta", "topic-theta"
                 , "topic-iota", "topic-kappa"]

------------------------------------------------------------------------
-- Wire parser for ProduceResponse v9
------------------------------------------------------------------------

data BenchPartResp = BenchPartResp
  { bpPartition :: !Int32
  , bpErrorCode :: !Int16
  , bpBaseOffset :: !Int64
  , bpLogAppendTime :: !Int64
  , bpLogStartOffset :: !Int64
  } deriving (Eq, Show)

instance NFData BenchPartResp where rnf !_ = ()

data BenchTopicResp = BenchTopicResp
  { btTopic :: !ByteString
  , btPartitions :: [BenchPartResp]
  } deriving (Eq, Show)

instance NFData BenchTopicResp where rnf (BenchTopicResp t ps) = rnf t `seq` rnf ps

data BenchProduceResp = BenchProduceResp
  { brTopics :: [BenchTopicResp]
  , brThrottle :: !Int32
  } deriving (Eq, Show)

instance NFData BenchProduceResp where rnf (BenchProduceResp ts t) = rnf ts `seq` rnf t `seq` ()

wireParseProduceResp :: Wire BenchProduceResp
wireParseProduceResp = do
  _corrId <- Wire.int32
  Wire.skipTaggedFields
  topics <- Wire.compactArray $ do
    topic <- Wire.compactString
    parts <- Wire.compactArray $ do
      p <- BenchPartResp
        <$> Wire.int32
        <*> Wire.int16
        <*> Wire.int64
        <*> Wire.int64
        <*> Wire.int64
      Wire.skipTaggedFields
      pure p
    Wire.skipTaggedFields
    pure (BenchTopicResp topic parts)
  throttle <- Wire.int32
  Wire.skipTaggedFields
  pure (BenchProduceResp topics throttle)

------------------------------------------------------------------------
-- Bytesmith parser for the same structure
------------------------------------------------------------------------

smithParseProduceResp :: Parser BenchProduceResp
smithParseProduceResp = do
  _corrId <- C.int32 "corrId"
  C.skipTaggedFields
  topics <- C.compactArray $ do
    topic <- C.compactString
    parts <- C.compactArray $ do
      p <- BenchPartResp
        <$> C.int32 "partition"
        <*> C.int16 "errorCode"
        <*> C.int64 "baseOffset"
        <*> C.int64 "logAppendTime"
        <*> C.int64 "logStartOffset"
      C.skipTaggedFields
      pure p
    C.skipTaggedFields
    pure (BenchTopicResp topic parts)
  throttle <- C.int32 "throttle"
  C.skipTaggedFields
  pure (BenchProduceResp topics throttle)

fromByteString :: ByteString -> ByteArray
fromByteString bs = Data.Bytes.toByteArrayClone (Data.Bytes.fromByteString bs)

------------------------------------------------------------------------
-- Main
------------------------------------------------------------------------

main :: IO ()
main = do
  let small   = buildProduceResponseV9 1 1
      medium  = buildProduceResponseV9 4 4
      large   = buildProduceResponseV9 10 4
      smallBA  = fromByteString small
      mediumBA = fromByteString medium
      largeBA  = fromByteString large

  -- Sanity check
  case Wire.runWire wireParseProduceResp small of
    Nothing -> error "Wire parser failed on small input"
    Just r  -> putStrLn $ "Wire OK: " ++ show (length (brTopics r)) ++ " topics"

  case Smith.parseByteArray smithParseProduceResp smallBA of
    Smith.Failure e -> error $ "Smith parser failed: " ++ e
    Smith.Success (Smith.Slice _ _ r) ->
      putStrLn $ "Smith OK: " ++ show (length (brTopics r)) ++ " topics"

  let smithParse ba = case Smith.parseByteArray smithParseProduceResp ba of
        Smith.Failure _ -> Nothing
        Smith.Success (Smith.Slice _ _ r) -> Just r

  defaultMain
    [ bgroup "ProduceResponse v9 parse"
      [ bgroup "small (1 topic, 1 partition)"
        [ bench "Wire"  $ nf (Wire.runWire wireParseProduceResp) small
        , bench "Smith" $ nf smithParse smallBA
        ]
      , bgroup "medium (4 topics, 4 partitions)"
        [ bench "Wire"  $ nf (Wire.runWire wireParseProduceResp) medium
        , bench "Smith" $ nf smithParse mediumBA
        ]
      , bgroup "large (10 topics, 4 partitions)"
        [ bench "Wire"  $ nf (Wire.runWire wireParseProduceResp) large
        , bench "Smith" $ nf smithParse largeBA
        ]
      ]
    ]
