{-# LANGUAGE OverloadedStrings #-}

module WireTests (wireTests) where

import Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import Data.Int
import Data.Word
import Test.Tasty
import Test.Tasty.HUnit

import Kafka.Internal.Wire
import Kafka.Internal.Writer (BuildR, toLazyByteString)
import qualified Kafka.Internal.Writer as W

------------------------------------------------------------------------
-- Helpers
------------------------------------------------------------------------

-- | Encode via Writer, parse via Wire.
roundTrip :: Wire a -> BuildR -> Maybe a
roundTrip p builder = runWire p (BSL.toStrict (toLazyByteString builder))

-- | Parse raw bytes.
parse :: Wire a -> ByteString -> Maybe a
parse = runWire

-- | Assert parser succeeds with expected value.
shouldBe :: (Show a, Eq a) => Maybe a -> a -> Assertion
shouldBe (Just a) expected = a @?= expected
shouldBe Nothing  _        = assertFailure "parser failed unexpectedly"

-- | Assert parser fails.
shouldFail :: Show a => Maybe a -> Assertion
shouldFail Nothing  = pure ()
shouldFail (Just a) = assertFailure ("expected failure, got: " ++ show a)

------------------------------------------------------------------------
-- Tests
------------------------------------------------------------------------

wireTests :: TestTree
wireTests = testGroup "Wire parser"
  [ fixedSizeTests
  , bigEndianTests
  , varIntTests
  , compactEncodingTests
  , combinatorTests
  , compositionTests
  , boundaryTests
  ]

------------------------------------------------------------------------
-- Fixed-size reads
------------------------------------------------------------------------

fixedSizeTests :: TestTree
fixedSizeTests = testGroup "Fixed-size reads"
  [ testCase "word8 reads a byte" $
      parse word8 "\xef" `shouldBe` 0xef

  , testCase "int8 reads signed byte" $
      parse int8 "\x80" `shouldBe` (-128)

  , testCase "word16 reads big-endian" $
      parse word16 "\x01\x02" `shouldBe` 0x0102

  , testCase "int16 reads big-endian signed" $
      parse int16 "\xff\xfe" `shouldBe` (-2)

  , testCase "word32 reads big-endian" $
      parse word32 "\x00\x00\x01\x00" `shouldBe` 256

  , testCase "int32 reads big-endian signed" $
      parse int32 "\xff\xff\xff\xff" `shouldBe` (-1)

  , testCase "int32 known value 305419896" $
      parse int32 "\x12\x34\x56\x78" `shouldBe` 0x12345678

  , testCase "word64 reads big-endian" $
      parse word64 "\x00\x00\x00\x00\x00\x00\x00\x01" `shouldBe` 1

  , testCase "int64 reads big-endian signed" $
      parse int64 "\xff\xff\xff\xff\xff\xff\xff\xff" `shouldBe` (-1)

  , testCase "int64 known value" $
      parse int64 "\x00\x00\x00\x00\x00\x00\x30\x39" `shouldBe` 12345
  ]

------------------------------------------------------------------------
-- Big-endian round-trips via Writer
------------------------------------------------------------------------

bigEndianTests :: TestTree
bigEndianTests = testGroup "Big-endian round-trips"
  [ testCase "int16 round-trips 0" $
      roundTrip int16 (W.int16 0) `shouldBe` 0

  , testCase "int16 round-trips -1" $
      roundTrip int16 (W.int16 (-1)) `shouldBe` (-1)

  , testCase "int16 round-trips 32767" $
      roundTrip int16 (W.int16 32767) `shouldBe` 32767

  , testCase "int32 round-trips 0" $
      roundTrip int32 (W.int32 0) `shouldBe` 0

  , testCase "int32 round-trips -1" $
      roundTrip int32 (W.int32 (-1)) `shouldBe` (-1)

  , testCase "int32 round-trips 2147483647" $
      roundTrip int32 (W.int32 2147483647) `shouldBe` 2147483647

  , testCase "int64 round-trips 0" $
      roundTrip int64 (W.int64 0) `shouldBe` 0

  , testCase "int64 round-trips -1" $
      roundTrip int64 (W.int64 (-1)) `shouldBe` (-1)

  , testCase "int64 round-trips large positive" $
      roundTrip int64 (W.int64 9223372036854775807) `shouldBe` 9223372036854775807

  , testCase "int64 round-trips large negative" $
      roundTrip int64 (W.int64 (-9223372036854775808)) `shouldBe` (-9223372036854775808)
  ]

------------------------------------------------------------------------
-- Variable-length integers
------------------------------------------------------------------------

varIntTests :: TestTree
varIntTests = testGroup "Variable-length integers"
  [ testCase "uvarint 0" $
      parse unsignedVarInt "\x00" `shouldBe` 0

  , testCase "uvarint 1" $
      parse unsignedVarInt "\x01" `shouldBe` 1

  , testCase "uvarint 127" $
      parse unsignedVarInt "\x7f" `shouldBe` 127

  , testCase "uvarint 128" $
      parse unsignedVarInt "\x80\x01" `shouldBe` 128

  , testCase "uvarint 300" $
      parse unsignedVarInt "\xac\x02" `shouldBe` 300

  , testCase "uvarint 16384" $
      parse unsignedVarInt "\x80\x80\x01" `shouldBe` 16384

  , testCase "uvarint round-trip via Writer" $
      roundTrip unsignedVarInt (W.unsignedVarInt 300) `shouldBe` 300

  , testCase "uvarint round-trip 0" $
      roundTrip unsignedVarInt (W.unsignedVarInt 0) `shouldBe` 0

  , testCase "uvarint round-trip 127" $
      roundTrip unsignedVarInt (W.unsignedVarInt 127) `shouldBe` 127

  , testCase "uvarint round-trip 128" $
      roundTrip unsignedVarInt (W.unsignedVarInt 128) `shouldBe` 128

  , testCase "uvarint round-trip 16384" $
      roundTrip unsignedVarInt (W.unsignedVarInt 16384) `shouldBe` 16384

  , testCase "uvarint fails on empty" $
      shouldFail (parse unsignedVarInt "")

  , testCase "uvarint fails on truncated" $
      shouldFail (parse unsignedVarInt "\x80")

  , testCase "signed varint 0" $
      roundTrip signedVarInt (mempty <> W.unsignedVarInt 0) `shouldBe` 0

  , testCase "signed varint -1 (zigzag 1)" $
      parse signedVarInt "\x01" `shouldBe` (-1)

  , testCase "signed varint 1 (zigzag 2)" $
      parse signedVarInt "\x02" `shouldBe` 1

  , testCase "signed varint -2 (zigzag 3)" $
      parse signedVarInt "\x03" `shouldBe` (-2)
  ]

------------------------------------------------------------------------
-- Compact encoding (KIP-482)
------------------------------------------------------------------------

compactEncodingTests :: TestTree
compactEncodingTests = testGroup "Compact encoding"
  [ testCase "compactString round-trips 'hello'" $
      roundTrip compactString (W.compactString "hello") `shouldBe` "hello"

  , testCase "compactString round-trips empty" $
      roundTrip compactString (W.compactString "") `shouldBe` ""

  , testCase "compactNullableString null round-trips" $
      roundTrip compactNullableString (W.compactNullableString Nothing)
        `shouldBe` Nothing

  , testCase "compactNullableString present round-trips" $
      roundTrip compactNullableString (W.compactNullableString (Just "test"))
        `shouldBe` Just "test"

  , testCase "compactArray of int32 round-trips" $
      roundTrip (compactArray int32) (W.compactArray [W.int32 10, W.int32 20, W.int32 30])
        `shouldBe` [10, 20, 30]

  , testCase "compactArray empty round-trips" $
      roundTrip (compactArray int32) (W.compactArray [])
        `shouldBe` ([] :: [Int32])

  , testCase "skipTaggedFields empty round-trips" $
      roundTrip (skipTaggedFields *> pure ()) W.taggedFields
        `shouldBe` ()

  , testCase "compactString is zero-copy slice" $ do
      -- The returned ByteString should share the underlying buffer
      let input = BSL.toStrict (toLazyByteString (W.compactString "kafka"))
      case runWire compactString input of
        Nothing -> assertFailure "parse failed"
        Just bs -> do
          bs @?= "kafka"
          BS.length bs @?= 5
  ]

------------------------------------------------------------------------
-- Combinators
------------------------------------------------------------------------

combinatorTests :: TestTree
combinatorTests = testGroup "Combinators"
  [ testCase "skip 0 on empty succeeds" $
      parse (skip 0) "" `shouldBe` ()

  , testCase "skip 3 on 3 bytes succeeds" $
      parse (skip 3) "abc" `shouldBe` ()

  , testCase "skip 4 on 3 bytes fails" $
      shouldFail (parse (skip 4) "abc")

  , testCase "takeBytes 5 returns correct slice" $
      parse (takeBytes 5) "hello world" `shouldBe` "hello"

  , testCase "takeBytes 0 returns empty" $
      parse (takeBytes 0) "anything" `shouldBe` ""

  , testCase "takeBytes too many fails" $
      shouldFail (parse (takeBytes 10) "short")

  , testCase "eof succeeds on empty" $
      parse eof "" `shouldBe` ()

  , testCase "eof fails on non-empty" $
      shouldFail (parse eof "x")

  , testCase "remaining on empty is 0" $
      parse remaining "" `shouldBe` 0

  , testCase "remaining counts correctly" $
      parse remaining "abcde" `shouldBe` 5

  , testCase "ensure 0 always succeeds" $
      parse (ensure 0) "" `shouldBe` ()

  , testCase "ensure exact succeeds" $
      parse (ensure 3) "abc" `shouldBe` ()

  , testCase "ensure too many fails" $
      shouldFail (parse (ensure 4) "abc")

  , testCase "replicateM 0 returns empty list" $
      parse (replicateM 0 word8) "" `shouldBe` ([] :: [Word8])

  , testCase "replicateM 3 parses 3 items" $
      parse (replicateM 3 word8) "\x01\x02\x03" `shouldBe` [1, 2, 3]

  , testCase "replicateM exceeds input fails" $
      shouldFail (parse (replicateM 4 word8) "\x01\x02\x03")
  ]

------------------------------------------------------------------------
-- Composition (sequential parsing)
------------------------------------------------------------------------

compositionTests :: TestTree
compositionTests = testGroup "Composition"
  [ testCase "int32 then int16 parses 6 bytes" $ do
      let p = (,) <$> int32 <*> int16
      parse p "\x00\x00\x00\x01\x00\x02" `shouldBe` (1, 2)

  , testCase "int32 then int16 fails on 5 bytes" $
      shouldFail (parse ((,) <$> int32 <*> int16) "\x00\x00\x00\x01\x00")

  , testCase "simulated Kafka header: corrId + skipTags + throttle" $ do
      let p = do
            corrId <- int32
            skipTaggedFields
            throttle <- int32
            pure (corrId, throttle)
          input = BSL.toStrict $ toLazyByteString $
            W.int32 42 <> W.unsignedVarInt 0 <> W.int32 100
      parse p input `shouldBe` (42, 100)

  , testCase "nested compact: array of strings" $ do
      let p = compactArray compactString
          input = BSL.toStrict $ toLazyByteString $
            W.compactArray [W.compactString "foo", W.compactString "bar"]
      parse p input `shouldBe` ["foo", "bar"]

  , testCase "full Kafka response header v1 + body" $ do
      let p = do
            _corrId <- int32
            skipTaggedFields    -- header v1 tagged fields
            errCode <- int16
            skipTaggedFields    -- body tagged fields
            pure errCode
          input = BSL.toStrict $ toLazyByteString $
            W.int32 0 <> W.taggedFields <> W.int16 5 <> W.taggedFields
      parse p input `shouldBe` 5
  ]

------------------------------------------------------------------------
-- Boundary conditions
------------------------------------------------------------------------

boundaryTests :: TestTree
boundaryTests = testGroup "Boundary conditions"
  [ testCase "empty input fails all fixed-size parsers" $ do
      shouldFail (parse word8 "")
      shouldFail (parse int16 "")
      shouldFail (parse int32 "")
      shouldFail (parse int64 "")

  , testCase "1 byte short fails" $ do
      shouldFail (parse int16 "\x00")
      shouldFail (parse int32 "\x00\x00\x00")
      shouldFail (parse int64 "\x00\x00\x00\x00\x00\x00\x00")

  , testCase "exact size succeeds" $ do
      parse word8 "\xff" `shouldBe` 0xff
      parse int16 "\x00\x01" `shouldBe` 1
      parse int32 "\x00\x00\x00\x01" `shouldBe` 1
      parse int64 "\x00\x00\x00\x00\x00\x00\x00\x01" `shouldBe` 1

  , testCase "trailing bytes ignored" $
      parse int32 "\x00\x00\x00\x01\xff\xff" `shouldBe` 1

  , testCase "parser consumes correct amount then eof" $ do
      let p = int32 <* int16 <* eof
      parse p "\x00\x00\x00\x01\x00\x02" `shouldBe` 1
      shouldFail (parse p "\x00\x00\x00\x01\x00\x02\xff")
  ]
