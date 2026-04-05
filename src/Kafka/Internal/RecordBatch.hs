{-# language BangPatterns, MagicHash, UnboxedTuples #-}

-- | Single-allocation record batch builder.
--
-- Writes the entire record batch (61-byte header + all records) directly
-- into a pinned buffer via Ptr arithmetic. The result IS a ByteString —
-- no freeze, no copy, no ByteArray anywhere in the hot path.
--
-- CRC32C via @digest@ (hardware-accelerated on ARM64/x86 SSE4.2).
module Kafka.Internal.RecordBatch
  ( buildRecordBatch
  , buildRecords
  , wrapRecordBatch
  ) where

import Data.Bits ((.&.), (.|.), shiftR, shiftL, xor)
import Data.ByteString (ByteString)
import Data.ByteString.Internal (unsafeCreate)
import Data.Digest.CRC32C (crc32cUpdate)
import Data.Int (Int16, Int32, Int64)
import Data.Primitive.ByteArray (ByteArray(ByteArray), sizeofByteArray)
import Data.Primitive.Unlifted.Array (UnliftedArray, sizeofUnliftedArray, indexUnliftedArray)
import Data.Word (Word8, Word16, Word32, Word64, byteSwap16, byteSwap32, byteSwap64)
import Foreign.Ptr (Ptr, plusPtr, castPtr)
import Foreign.Storable (poke)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BSU

import GHC.Exts (Int(..), Int#, ByteArray#, copyByteArrayToAddr#)
import GHC.ForeignPtr (ForeignPtr(ForeignPtr))
import GHC.IO (IO(IO))
import GHC.Ptr (Ptr(Ptr))

------------------------------------------------------------------------
-- Public API
------------------------------------------------------------------------

-- | Build a complete record batch as a ByteString.
-- Single pinned allocation — header + records in one buffer.
buildRecordBatch ::
     Int64 -> Int16 -> Int32 -> Int16
  -> UnliftedArray ByteArray -> ByteString
buildRecordBatch !pid !epoch !baseSeq !comprAttr payloads =
  let !n = sizeofUnliftedArray payloads
      !recordsSize = computeRecordsSize payloads n
      !totalSize = 61 + recordsSize
  in unsafeCreate totalSize $ \ptr -> do
    writeAllRecords (ptr `plusPtr` 61) payloads n
    writePostCrc ptr pid epoch baseSeq comprAttr n
    -- CRC over postCrc(40 bytes at offset 21) + records
    let !crcSlice = BSI.BS (ptrToFP (ptr `plusPtr` 21)) (40 + recordsSize)
        !crc = crc32cUpdate 0 crcSlice
    writePreCrc ptr recordsSize crc
{-# INLINE buildRecordBatch #-}

-- | Build just the records portion (no header). For compression:
-- build records → compress → wrapRecordBatch.
buildRecords :: UnliftedArray ByteArray -> ByteString
buildRecords payloads =
  let !n = sizeofUnliftedArray payloads
      !size = computeRecordsSize payloads n
  in unsafeCreate size $ \ptr -> writeAllRecords ptr payloads n

-- | Wrap compressed records with the 61-byte header.
wrapRecordBatch ::
     Int64 -> Int16 -> Int32 -> Int16 -> Int
  -> ByteString -> ByteString
wrapRecordBatch !pid !epoch !baseSeq !comprAttr !recordCount records =
  let !recordsSize = BS.length records
      !totalSize = 61 + recordsSize
  in unsafeCreate totalSize $ \ptr -> do
    BSU.unsafeUseAsCStringLen records $ \(srcPtr, len) ->
      BSI.memcpy (ptr `plusPtr` 61) (castPtr srcPtr) len
    writePostCrc ptr pid epoch baseSeq comprAttr recordCount
    let !crcSlice = BSI.BS (ptrToFP (ptr `plusPtr` 21)) (40 + recordsSize)
        !crc = crc32cUpdate 0 crcSlice
    writePreCrc ptr recordsSize crc

------------------------------------------------------------------------
-- Header: preCrc (21 bytes at offset 0)
------------------------------------------------------------------------

writePreCrc :: Ptr Word8 -> Int -> Word32 -> IO ()
writePreCrc !p !recordsSize !crc = do
  poke64BE p 0                                   -- baseOffset
  poke32BE (p `plusPtr` 8) batchLen               -- batchLength
  poke32BE (p `plusPtr` 12) 0                     -- partitionLeaderEpoch
  poke (p `plusPtr` 16) (2 :: Word8)              -- magic
  poke32BE (p `plusPtr` 17) (fromIntegral crc)    -- CRC32C
  where
    !batchLen = fromIntegral (49 + recordsSize) :: Int32
{-# INLINE writePreCrc #-}

------------------------------------------------------------------------
-- Header: postCrc (40 bytes at offset 21)
------------------------------------------------------------------------

writePostCrc :: Ptr Word8 -> Int64 -> Int16 -> Int32 -> Int16 -> Int -> IO ()
writePostCrc !base !pid !epoch !baseSeq !comprAttr !recordCount = do
  let !p = base `plusPtr` 21
  poke16BE p comprAttr                                       -- attributes
  poke32BE (p `plusPtr` 2) (fromIntegral (recordCount - 1))  -- lastOffsetDelta
  poke64BE (p `plusPtr` 6) 0                                 -- firstTimestamp
  poke64BE (p `plusPtr` 14) 0                                -- maxTimestamp
  poke64BE (p `plusPtr` 22) pid                              -- producerId
  poke16BE (p `plusPtr` 30) epoch                            -- producerEpoch
  poke32BE (p `plusPtr` 32) baseSeq                          -- baseSequence
  poke32BE (p `plusPtr` 36) (fromIntegral recordCount)       -- recordCount
{-# INLINE writePostCrc #-}

------------------------------------------------------------------------
-- Records: Ptr arithmetic, zero intermediate allocations
------------------------------------------------------------------------

writeAllRecords :: Ptr Word8 -> UnliftedArray ByteArray -> Int -> IO ()
writeAllRecords !startPtr payloads !n = go startPtr 0
  where
    go !_ !i | i >= n = pure ()
    go !p !i = do
      p' <- writeRecord p i (indexUnliftedArray payloads i)
      go p' (i + 1)
{-# INLINE writeAllRecords #-}

writeRecord :: Ptr Word8 -> Int -> ByteArray -> IO (Ptr Word8)
writeRecord !p !index !payload = do
  let !payloadLen = sizeofByteArray payload
      !bodySize = 1 + 1 + zigzagSize index + 1 + zigzagSize payloadLen + payloadLen + 1
  p1 <- pokeZigzag p bodySize       -- record length
  poke p1 (0 :: Word8)              -- attributes
  poke (p1 `plusPtr` 1) (0 :: Word8) -- timestampDelta (zigzag 0)
  p2 <- pokeZigzag (p1 `plusPtr` 2) index  -- offsetDelta
  poke p2 (1 :: Word8)              -- keyLength (zigzag -1)
  p3 <- pokeZigzag (p2 `plusPtr` 1) payloadLen  -- valueLength
  copyBAToPtr payload 0 p3 payloadLen  -- value bytes
  let !p4 = p3 `plusPtr` payloadLen
  poke p4 (0 :: Word8)              -- headerCount (zigzag 0)
  pure (p4 `plusPtr` 1)
{-# INLINE writeRecord #-}

copyBAToPtr :: ByteArray -> Int -> Ptr Word8 -> Int -> IO ()
copyBAToPtr (ByteArray ba#) (I# off#) (Ptr addr#) (I# len#) =
  IO $ \s -> case copyByteArrayToAddr# ba# off# addr# len# s of
    s' -> (# s', () #)
{-# INLINE copyBAToPtr #-}

------------------------------------------------------------------------
-- Size computation (pure, no allocations)
------------------------------------------------------------------------

computeRecordsSize :: UnliftedArray ByteArray -> Int -> Int
computeRecordsSize payloads !n = go 0 0
  where
    go !acc !i | i >= n = acc
    go !acc !i =
      let !payloadLen = sizeofByteArray (indexUnliftedArray payloads i)
          !bodySize = 1 + 1 + zigzagSize i + 1 + zigzagSize payloadLen + payloadLen + 1
      in go (acc + zigzagSize bodySize + bodySize) (i + 1)
{-# INLINE computeRecordsSize #-}

------------------------------------------------------------------------
-- Zigzag varint — Ptr writes
------------------------------------------------------------------------

pokeZigzag :: Ptr Word8 -> Int -> IO (Ptr Word8)
pokeZigzag !p !n = pokeUvarint p (zigzagEncode n)
{-# INLINE pokeZigzag #-}

pokeUvarint :: Ptr Word8 -> Int -> IO (Ptr Word8)
pokeUvarint !p !n
  | n < 0x80 = do
      poke p (fromIntegral n :: Word8)
      pure (p `plusPtr` 1)
  | otherwise = do
      poke p (fromIntegral (n .&. 0x7F .|. 0x80) :: Word8)
      pokeUvarint (p `plusPtr` 1) (n `shiftR` 7)
{-# INLINE pokeUvarint #-}

zigzagEncode :: Int -> Int
zigzagEncode x
  | x >= 0   = x `shiftL` 1
  | otherwise = (x `shiftL` 1) `xor` (-1)
{-# INLINE zigzagEncode #-}

zigzagSize :: Int -> Int
zigzagSize = uvarintSize . zigzagEncode
{-# INLINE zigzagSize #-}

uvarintSize :: Int -> Int
uvarintSize n
  | n < 0x80       = 1
  | n < 0x4000     = 2
  | n < 0x200000   = 3
  | n < 0x10000000 = 4
  | otherwise       = 5
{-# INLINE uvarintSize #-}

------------------------------------------------------------------------
-- Big-endian poke: single store + byteswap instruction
------------------------------------------------------------------------

poke16BE :: Ptr Word8 -> Int16 -> IO ()
poke16BE p v = poke (castPtr p :: Ptr Word16) (byteSwap16 (fromIntegral v))
{-# INLINE poke16BE #-}

poke32BE :: Ptr Word8 -> Int32 -> IO ()
poke32BE p v = poke (castPtr p :: Ptr Word32) (byteSwap32 (fromIntegral v))
{-# INLINE poke32BE #-}

poke64BE :: Ptr Word8 -> Int64 -> IO ()
poke64BE p v = poke (castPtr p :: Ptr Word64) (byteSwap64 (fromIntegral v))
{-# INLINE poke64BE #-}

------------------------------------------------------------------------
-- ForeignPtr helper
------------------------------------------------------------------------

-- | Wrap a Ptr as a ForeignPtr with no finalizer.
-- UNSAFE: only valid within unsafeCreate's callback where the
-- enclosing ForeignPtr keeps the memory alive.
ptrToFP :: Ptr Word8 -> ForeignPtr Word8
ptrToFP (Ptr addr#) = ForeignPtr addr# (error "ptrToFP: touched finalizer")
{-# INLINE ptrToFP #-}
