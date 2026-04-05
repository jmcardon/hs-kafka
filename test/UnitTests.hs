{-# LANGUAGE OverloadedStrings #-}

module Main (main) where

import Data.Int
import Data.Primitive.ByteArray (ByteArray, byteArrayFromList, sizeofByteArray)
import Data.Word
import Test.Tasty
import Test.Tasty.Golden
import Test.Tasty.HUnit

import qualified Data.ByteString as B
import qualified Data.ByteString.Char8 as BC8
import qualified Data.ByteString.Lazy as BL
import qualified Data.IntMap as IM

import Kafka.Common
import Kafka.Consumer (merge)
import Kafka.Internal.Compression (compressBatch, decompressBatch)
import Kafka.Internal.Config (Compression(..))
import Kafka.Internal.Fetch.Request
import Kafka.Internal.JoinGroup.Request
import Kafka.Internal.ListOffsets.Request
import Kafka.Internal.Produce.Request (buildProduceRequest)
import Kafka.Internal.Produce.Response
import Kafka.Internal.Wire (Wire, runWire)
import Kafka.Internal.Zigzag
import qualified Kafka.Internal.Fetch.Response as Fetch
import Kafka.Internal.ApiVersions.Response (ApiVersionsResponse(..), ApiVersionEntry(..),
  parseApiVersionsResponse, parseApiVersionsResponseV3)
import Kafka.Internal.InitProducerId.Response (InitProducerIdResponse(..),
  parseInitProducerIdResponse, parseInitProducerIdResponseV4)
import Kafka.Internal.Writer (BuildR, toLazyByteString)
import qualified Kafka.Internal.Writer as W

import WireTests (wireTests)

main :: IO ()
main = defaultMain (testGroup "Tests" [unitTests, goldenTests, wireTests])

unitTests :: TestTree
unitTests = testGroup "Unit tests"
  [ zigzagTests
  , apiVersionsTests
  , responseParserTests
  , consumerTests
  , errorCodeTests
  , compressionTests
  , idempotentProduceTests
  ]

zigzagTests :: TestTree
zigzagTests = testGroup "zigzag"
  [ testCase
      "zigzag 0 is 0"
      (zigzag 0 @=? byteArrayFromList [0 :: Word8])
  , testCase
      "zigzag -1 is 1"
      (zigzag (-1) @=? byteArrayFromList [1 :: Word8])
  , testCase
      "zigzag 1 is 2"
      (zigzag 1 @=? byteArrayFromList [2 :: Word8])
  , testCase
      "zigzag -2 is 3"
      (zigzag (-2) @=? byteArrayFromList [3 :: Word8])
  , testCase
      "zigzag 100 is [200, 1]"
      (zigzag 100 @?= byteArrayFromList [200, 1 :: Word8])
  , testCase
      "zigzag 150 is [172, 2]"
      (zigzag 150 @?= byteArrayFromList [172, 2 :: Word8])
  ]

fromByteString :: B.ByteString -> ByteArray
fromByteString = byteArrayFromList . B.unpack

-- | Build a ByteString from a BuildR (for test response construction).
buildBS :: BuildR -> B.ByteString
buildBS = BL.toStrict . toLazyByteString

-- | Parse a Wire parser on BuildR output.
wireParse :: Wire a -> BuildR -> Maybe a
wireParse p = runWire p . buildBS

apiVersionsTests :: TestTree
apiVersionsTests = testGroup "ApiVersions + InitProducerId"
  [ testCase "parse ApiVersionsResponse v0 (legacy) with 2 entries" $
      let responseBytes = buildBS $
            W.int32 0           -- correlationId
            <> W.int16 0        -- errorCode (no error)
            <> W.int32 2        -- array length: 2 entries
            <> W.int16 0        -- apiKey: Produce
            <> W.int16 0        -- minVersion
            <> W.int16 7        -- maxVersion
            <> W.int16 1        -- apiKey: Fetch
            <> W.int16 0        -- minVersion
            <> W.int16 10       -- maxVersion
          expected = ApiVersionsResponse 0
            [ ApiVersionEntry 0 0 7
            , ApiVersionEntry 1 0 10
            ] 0
      in case runWire parseApiVersionsResponse responseBytes of
        Just resp -> resp @?= expected
        Nothing -> assertFailure "parse failed"
  , testCase "parse ApiVersionsResponse v3 (flexible) with tagged fields" $
      let responseBytes = buildBS $
            W.int32 42          -- correlationId
            <> W.int16 0        -- errorCode
            -- compact array: 2 entries → varint(3)
            <> W.unsignedVarInt 3
            -- entry 1
            <> W.int16 0        -- apiKey: Produce
            <> W.int16 0 <> W.int16 9  -- minVersion, maxVersion
            <> W.unsignedVarInt 0      -- entry tagged fields
            -- entry 2
            <> W.int16 18       -- apiKey: ApiVersions
            <> W.int16 0 <> W.int16 3  -- minVersion, maxVersion
            <> W.unsignedVarInt 0      -- entry tagged fields
            -- throttle time
            <> W.int32 100
            -- body tagged fields
            <> W.unsignedVarInt 0
          expected = ApiVersionsResponse 0
            [ ApiVersionEntry 0 0 9
            , ApiVersionEntry 18 0 3
            ] 100
      in case runWire parseApiVersionsResponseV3 responseBytes of
        Just resp -> resp @?= expected
        Nothing -> assertFailure "parse v3 failed"
  , testCase "parse ApiVersionsResponse v3 with error code" $
      let responseBytes = buildBS $
            W.int32 0           -- correlationId
            <> W.int16 35       -- errorCode (UnsupportedVersion)
            <> W.unsignedVarInt 1  -- empty compact array
            <> W.int32 0        -- throttle time
            <> W.unsignedVarInt 0  -- body tagged fields
      in case runWire parseApiVersionsResponseV3 responseBytes of
        Just resp -> avErrorCode resp @?= 35
        Nothing -> assertFailure "parse failed"
  , testCase "parse InitProducerIdResponse v0 (legacy)" $
      let responseBytes = buildBS $
            W.int32 0           -- correlationId
            <> W.int32 0        -- throttleTimeMs
            <> W.int16 0        -- errorCode
            <> W.int64 12345    -- producerId
            <> W.int16 0        -- producerEpoch
          expected = InitProducerIdResponse 0 0 12345 0
      in case runWire parseInitProducerIdResponse responseBytes of
        Just resp -> resp @?= expected
        Nothing -> assertFailure "parse failed"
  , testCase "parse InitProducerIdResponse v4 (flexible)" $
      let responseBytes = buildBS $
            W.int32 99          -- correlationId
            <> W.unsignedVarInt 0  -- header v1 tagged fields
            <> W.int32 50       -- throttleTimeMs
            <> W.int16 0        -- errorCode (no error)
            <> W.int64 999      -- producerId
            <> W.int16 5        -- producerEpoch
            <> W.unsignedVarInt 0  -- body tagged fields
          expected = InitProducerIdResponse 50 0 999 5
      in case runWire parseInitProducerIdResponseV4 responseBytes of
        Just resp -> resp @?= expected
        Nothing -> assertFailure "parse v4 failed"
  , testCase "parse InitProducerIdResponse v4 with error" $
      let responseBytes = buildBS $
            W.int32 0
            <> W.unsignedVarInt 0  -- header v1 tagged fields
            <> W.int32 0        -- throttleTimeMs
            <> W.int16 45       -- errorCode (OutOfOrderSequenceNumber)
            <> W.int64 (-1)     -- producerId
            <> W.int16 (-1)     -- producerEpoch
            <> W.unsignedVarInt 0
      in case runWire parseInitProducerIdResponseV4 responseBytes of
        Just resp -> ipErrorCode resp @?= 45
        Nothing -> assertFailure "parse failed"
  ]

responseParserTests :: TestTree
responseParserTests = testGroup "Response parsers"
  [ produceResponseTest
  , produceResponseV9Test
  , fetchResponseTest
  ]

------------------------------------------------------------------------
-- Error code tests
------------------------------------------------------------------------

errorCodeTests :: TestTree
errorCodeTests = testGroup "Error codes"
  [ testCase "fromErrorCode 0 is None" $
      fromErrorCode 0 @?= Just None
  , testCase "fromErrorCode 5 is LeaderNotAvailable" $
      fromErrorCode 5 @?= Just LeaderNotAvailable
  , testCase "fromErrorCode 6 is NotLeaderForPartition" $
      fromErrorCode 6 @?= Just NotLeaderForPartition
  , testCase "fromErrorCode 999 is Nothing" $
      fromErrorCode 999 @?= Nothing
  , testCase "isRetriable LeaderNotAvailable" $
      isRetriable LeaderNotAvailable @?= True
  , testCase "isRetriable NotLeaderForPartition" $
      isRetriable NotLeaderForPartition @?= True
  , testCase "isRetriable RequestTimedOut" $
      isRetriable RequestTimedOut @?= True
  , testCase "isRetriable NotEnoughReplicas" $
      isRetriable NotEnoughReplicas @?= True
  , testCase "isRetriable NotEnoughReplicasAfterAppend" $
      isRetriable NotEnoughReplicasAfterAppend @?= True
  , testCase "not isRetriable MessageTooLarge" $
      isRetriable MessageTooLarge @?= False
  , testCase "not isRetriable InvalidRequiredAcks" $
      isRetriable InvalidRequiredAcks @?= False
  , testCase "not isRetriable TopicAuthorizationFailed" $
      isRetriable TopicAuthorizationFailed @?= False
  , testCase "not isRetriable OutOfOrderSequenceNumber" $
      isRetriable OutOfOrderSequenceNumber @?= False
  ]

------------------------------------------------------------------------
-- Compression tests
------------------------------------------------------------------------

compressionTests :: TestTree
compressionTests = testGroup "Compression"
  [ testCase "NoCompression passthrough" $ do
      let payload = "hello world"
          (result, attr) = compressBatch NoCompression payload
      attr @?= 0
      result @?= payload
  , compressionRoundTrip "Gzip" Gzip 1
  , compressionRoundTrip "Snappy" Snappy 2
  , compressionRoundTrip "Lz4" Lz4 3
  , compressionRoundTrip "Zstd" Zstd 4
  , compressionFallback "Gzip" Gzip
  , compressionFallback "Snappy" Snappy
  , compressionFallback "Lz4" Lz4
  , compressionFallback "Zstd" Zstd
  , testCase "decompressBatch 0 is passthrough" $
      decompressBatch 0 "hello" @?= Right "hello"
  , testCase "decompressBatch unknown codec returns error" $
      case decompressBatch 99 "data" of
        Left _ -> pure ()
        Right _ -> assertFailure "expected error for unknown codec"
  ]

-- | Round-trip test: compress repetitive data, verify attribute, decompress, compare.
compressionRoundTrip :: String -> Compression -> Int16 -> TestTree
compressionRoundTrip name codec expectedAttr = testCase (name ++ " round-trips") $ do
  let rawBS = B.concat (replicate 100 "hello world, this is a test payload for compression. ")
      (compressed, attr) = compressBatch codec rawBS
  attr @?= expectedAttr
  case decompressBatch (fromIntegral attr) compressed of
    Left err -> assertFailure ("decompression failed: " ++ err)
    Right decompressed -> decompressed @?= rawBS

compressionFallback :: String -> Compression -> TestTree
compressionFallback name codec = testCase (name ++ " falls back for tiny data") $ do
  let (_, attr) = compressBatch codec "hi"
  attr @?= 0

------------------------------------------------------------------------
-- Idempotent produce tests
------------------------------------------------------------------------

idempotentProduceTests :: TestTree
idempotentProduceTests = testGroup "Idempotent produce"
  [ testCase "idempotent request encodes PID in record batch" $ do
      let req = buildProduceRequest 0 (-1) "kafka-native" 30000
                  "test-topic" 0 42 1 0 NoCompression ["test message"]
      assertBool "request should be non-empty" (B.length req > 0)
      let nonIdem = buildProduceRequest 0 (-1) "kafka-native" 30000
                      "test-topic" 0 (-1) (-1) (-1) NoCompression ["test message"]
      assertBool "idempotent request should differ from non-idempotent"
        (req /= nonIdem)
  , testCase "NoCompression is deterministic" $ do
      let r1 = buildProduceRequest 0 1 "ruko" 30000 "test" 0 (-1) (-1) (-1) NoCompression ["test"]
          r2 = buildProduceRequest 0 1 "ruko" 30000 "test" 0 (-1) (-1) (-1) NoCompression ["test"]
      r1 @?= r2
  , testCase "Gzip compression produces different (shorter) bytes" $ do
      let payload = B.concat (replicate 50 "repetitive data for compression ")
          compressed = buildProduceRequest 0 1 "test" 30000 "test" 0 (-1) (-1) (-1) Gzip [payload]
          plain = buildProduceRequest 0 1 "test" 30000 "test" 0 (-1) (-1) (-1) NoCompression [payload]
      assertBool "compressed should differ" (compressed /= plain)
      assertBool "compressed should be shorter" (B.length compressed < B.length plain)
  ]

goldenTests :: TestTree
goldenTests = testGroup "Golden tests"
  [ testGroup "Produce"
      [ goldenVsString
          "One payload"
          "test/golden/produce-one-payload-request"
          produceTest
      , goldenVsString
          "Many payloads"
          "test/golden/produce-many-payloads-request"
          multipleProduceTest
      ]
  , testGroup "Fetch"
      [ goldenVsString
          "One partition"
          "test/golden/fetch-one-partition-request"
          fetchTest
      , goldenVsString
          "Many partitions"
          "test/golden/fetch-many-partitions-request"
          multipleFetchTest
      ]
  , testGroup "ListOffsets"
      [ goldenVsString
          "No partitions"
          "test/golden/listoffsets-no-partitions-request"
          (listOffsetsTest [])
      , goldenVsString
          "One partition"
          "test/golden/listoffsets-one-partition-request"
          (listOffsetsTest [0])
      , goldenVsString
          "Many partitions"
          "test/golden/listoffsets-many-partitions-request"
          (listOffsetsTest [0,1,2,3,4,5])
      ]
  , testGroup "JoinGroup"
      [ goldenVsString
          "null member id"
          "test/golden/joingroup-null-member-id-request"
          (joinGroupTest
            (GroupMember "test-group" Nothing))
      , goldenVsString
          "with member id"
          "test/golden/joingroup-with-member-id-request"
          (joinGroupTest
            (GroupMember
              ("test-group")
              (Just "test-member-id")))
      ]
  ]

-- Produce golden tests use buildProduceRequest with corrId=0xbeef (legacy default).
produceTest :: IO BL.ByteString
produceTest = pure $ BL.fromStrict $ buildProduceRequest
  0xbeef 1 "ruko" 30000 "test" 0 (-1) (-1) (-1) NoCompression
  ["\"im not owned! im not owned!!\", i continue to insist as i slowlyshrink and transform into a corn cob"]

multipleProduceTest :: IO BL.ByteString
multipleProduceTest = pure $ BL.fromStrict $ buildProduceRequest
  0xbeef 1 "ruko" 30000 "test" 0 (-1) (-1) (-1) NoCompression
  [ "i'm dying"
  , "is it blissful?"
  , "it's like a dream"
  , "i want to dream"
  ]

fetchTest :: IO BL.ByteString
fetchTest = pure (sessionlessFetchRequest 30000 "test" [PartitionOffset 0 0] 30000000)

multipleFetchTest :: IO BL.ByteString
multipleFetchTest = pure (sessionlessFetchRequest 30000 "test" [PartitionOffset 0 0, PartitionOffset 1 0, PartitionOffset 2 0] 30000000)

listOffsetsTest :: [Int32] -> IO BL.ByteString
listOffsetsTest partitions = pure (listOffsetsRequest "test" partitions Latest)

joinGroupTest :: GroupMember -> IO BL.ByteString
joinGroupTest groupMember = pure (joinGroupRequest "test" groupMember)

produceResponseTest :: TestTree
produceResponseTest = testGroup "Produce"
  [ testCase
      "One message"
      (runWire parseProduceResponse oneMsgProduceResponseBytes @?= Just oneMsgProduceResponse)
  , testCase
      "Two messages"
      (runWire parseProduceResponse twoMsgProduceResponseBytes @?= Just twoMsgProduceResponse)
  ]

oneMsgProduceResponseBytes :: B.ByteString
oneMsgProduceResponseBytes = buildBS $
  W.int32 0
  <> W.int32 1
  <> W.string "topic-name"
  <> W.int32 1
  <> W.int32 10
  <> W.int16 11
  <> W.int64 12
  <> W.int64 13
  <> W.int64 14
  <> W.int32 1

twoMsgProduceResponseBytes :: B.ByteString
twoMsgProduceResponseBytes = buildBS $
  W.int32 0
  <> W.int32 1
  <> W.string "topic-name"
  <> W.int32 2
  <> W.int32 10
  <> W.int16 11
  <> W.int64 12
  <> W.int64 13
  <> W.int64 14
  <> W.int32 20
  <> W.int16 21
  <> W.int64 22
  <> W.int64 23
  <> W.int64 24
  <> W.int32 1

oneMsgProduceResponse :: ProduceResponse
oneMsgProduceResponse =
  ProduceResponse
    { produceResponseMessages =
        [ ProduceResponseMessage
            { prMessageTopic = "topic-name"
            , prPartitionResponses =
                [ ProducePartitionResponse
                    { prResponsePartition = 10
                    , prResponseErrorCode = 11
                    , prResponseBaseOffset = 12
                    , prResponseLogAppendTime = 13
                    , prResponseLogStartTime = 14
                    }
                ]
            }
        ]
    , throttleTimeMs = 1
    }

twoMsgProduceResponse :: ProduceResponse
twoMsgProduceResponse =
  ProduceResponse
    { produceResponseMessages =
        [ ProduceResponseMessage
            { prMessageTopic = "topic-name"
            , prPartitionResponses =
                [ ProducePartitionResponse
                    { prResponsePartition = 10
                    , prResponseErrorCode = 11
                    , prResponseBaseOffset = 12
                    , prResponseLogAppendTime = 13
                    , prResponseLogStartTime = 14
                    }
                , ProducePartitionResponse
                    { prResponsePartition = 20
                    , prResponseErrorCode = 21
                    , prResponseBaseOffset = 22
                    , prResponseLogAppendTime = 23
                    , prResponseLogStartTime = 24
                    }
                ]
            }
        ]
    , throttleTimeMs = 1
    }

-- | Test Produce v9 response parsing (flexible/compact encoding).
produceResponseV9Test :: TestTree
produceResponseV9Test = testGroup "Produce v9 (flexible)"
  [ testCase "parse v9 response with success" $
      let responseBytes = buildBS $
            W.int32 0           -- correlationId
            <> W.unsignedVarInt 0  -- header v1 tagged fields
            -- compact array of topic responses: 1 topic → varint(2)
            <> W.unsignedVarInt 2
            -- TopicProduceResponse
            <> W.compactString "my-topic"
            -- compact array of partition responses: 1 partition → varint(2)
            <> W.unsignedVarInt 2
            -- PartitionProduceResponse
            <> W.int32 0        -- partition
            <> W.int16 0        -- errorCode (success)
            <> W.int64 100      -- baseOffset
            <> W.int64 (-1)     -- logAppendTime
            <> W.int64 0        -- logStartOffset
            <> W.unsignedVarInt 0  -- partition tagged fields
            <> W.unsignedVarInt 0  -- topic tagged fields
            -- throttle time
            <> W.int32 0
            -- body tagged fields
            <> W.unsignedVarInt 0
          expected = ProduceResponse
            { produceResponseMessages =
                [ ProduceResponseMessage
                    { prMessageTopic = "my-topic"
                    , prPartitionResponses =
                        [ ProducePartitionResponse 0 0 100 (-1) 0
                        ]
                    }
                ]
            , throttleTimeMs = 0
            }
      in case runWire parseProduceResponseV9 responseBytes of
        Just resp -> resp @?= expected
        Nothing -> assertFailure "parse v9 failed"
  , testCase "parse v9 response with error code" $
      let responseBytes = buildBS $
            W.int32 0
            <> W.unsignedVarInt 0  -- header v1 tagged fields
            <> W.unsignedVarInt 2  -- 1 topic
            <> W.compactString "test"
            <> W.unsignedVarInt 2  -- 1 partition
            <> W.int32 0           -- partition
            <> W.int16 6           -- errorCode: NotLeaderForPartition
            <> W.int64 (-1) <> W.int64 (-1) <> W.int64 (-1)
            <> W.unsignedVarInt 0  -- partition tf
            <> W.unsignedVarInt 0  -- topic tf
            <> W.int32 0           -- throttle
            <> W.unsignedVarInt 0  -- body tf
      in case runWire parseProduceResponseV9 responseBytes of
        Just resp -> do
          let partResp = head (prPartitionResponses (head (produceResponseMessages resp)))
          prResponseErrorCode partResp @?= 6
        Nothing -> assertFailure "parse v9 failed"
  , testCase "parse v9 response with multiple partitions" $
      let responseBytes = buildBS $
            W.int32 0
            <> W.unsignedVarInt 0  -- header v1 tagged fields
            <> W.unsignedVarInt 2  -- 1 topic
            <> W.compactString "multi-part"
            <> W.unsignedVarInt 4  -- 3 partitions
            -- partition 0: success
            <> W.int32 0 <> W.int16 0 <> W.int64 10 <> W.int64 (-1) <> W.int64 0
            <> W.unsignedVarInt 0
            -- partition 1: error
            <> W.int32 1 <> W.int16 5 <> W.int64 (-1) <> W.int64 (-1) <> W.int64 (-1)
            <> W.unsignedVarInt 0
            -- partition 2: success
            <> W.int32 2 <> W.int16 0 <> W.int64 20 <> W.int64 (-1) <> W.int64 0
            <> W.unsignedVarInt 0
            <> W.unsignedVarInt 0  -- topic tf
            <> W.int32 0
            <> W.unsignedVarInt 0  -- body tf
      in case runWire parseProduceResponseV9 responseBytes of
        Just resp -> do
          let parts = prPartitionResponses (head (produceResponseMessages resp))
          length parts @?= 3
          prResponseErrorCode (parts !! 0) @?= 0
          prResponseErrorCode (parts !! 1) @?= 5
          prResponseErrorCode (parts !! 2) @?= 0
        Nothing -> assertFailure "parse v9 failed"
  ]

fetchResponseTest :: TestTree
fetchResponseTest = testGroup "Fetch"
  [ goldenVsString
      "Many batches"
      "test/golden/fetch-response-parsed"
      (do
        rawBytes <- B.readFile "test/golden/fetch-response-bytes"
        case runWire Fetch.parseFetchResponse rawBytes of
          Nothing -> fail "Parse failed"
          Just res -> pure (BL.fromStrict (BC8.pack (show res)))
      )
  ]

consumerTests :: TestTree
consumerTests = testGroup "Consumer"
  [ testCase
      "merge replaces -1 and keeps other values"
      mergeTest
  ]
  where
  mergeTest =
    let actual =
          merge
            (IM.fromList [(0, 5), (1, 6), (2, 7)])
            (IM.fromList [(0, -1), (1, 3), (2, -1)])
    in  actual @=? (IM.fromList [(0, 5), (1, 3), (2, 7)])
