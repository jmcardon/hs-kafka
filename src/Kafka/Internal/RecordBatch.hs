{-# language BangPatterns, MagicHash, UnboxedTuples #-}

-- | Single-allocation record batch builder.
--
-- Writes the entire record batch (61-byte header + all records) into a
-- pinned buffer. The result IS a ByteString — one allocation total.
-- CRC32C computed directly on the Ptr via FFI (no ByteString wrapper).
--
-- All inputs and outputs are ByteString. No ByteArray in the hot path.
module Kafka.Internal.RecordBatch
  ( buildRecordBatch
  , buildRecords
  , wrapRecordBatch
  ) where

import Data.Bits ((.&.), (.|.), shiftR, shiftL, xor)
import Data.ByteString (ByteString)
import Data.ByteString.Internal (unsafeCreate)
import Data.Int (Int16, Int32, Int64)
import Data.Word (Word8, Word16, Word32, Word64, byteSwap16, byteSwap32, byteSwap64)
import Foreign.C.Types (CSize(..))
import Foreign.Ptr (Ptr, plusPtr, castPtr)
import Foreign.Storable (poke)
import System.IO.Unsafe (unsafeDupablePerformIO)

import qualified Data.ByteString as BS
import qualified Data.ByteString.Internal as BSI
import qualified Data.ByteString.Unsafe as BSU

import GHC.IO (IO(IO))

------------------------------------------------------------------------
-- Public API
------------------------------------------------------------------------

-- | Build a complete record batch (header + records) as a ByteString.
-- Single pinned allocation.
buildRecordBatch ::
     Int64 -> Int16 -> Int32 -> Int16
  -> [ByteString]  -- ^ message payloads
  -> ByteString
buildRecordBatch !pid !epoch !baseSeq !comprAttr payloads =
  let !(# recordsSize, payloadCount #) = computeRecordsSizeAndCount payloads
      !totalSize = 61 + recordsSize
  in unsafeCreate totalSize $ \ptr -> do
    writeAllRecords (ptr `plusPtr` 61) payloads 0
    writePostCrc ptr pid epoch baseSeq comprAttr payloadCount
    let !crc = crc32cPtr (ptr `plusPtr` 21) (40 + recordsSize)
    writePreCrc ptr recordsSize crc
{-# INLINE buildRecordBatch #-}

-- | Build just the records portion (no header).
-- Used for compression: build records → compress → wrapRecordBatch.
buildRecords :: [ByteString] -> ByteString
buildRecords payloads =
  let !(# size, _ #) = computeRecordsSizeAndCount payloads
  in unsafeCreate size $ \ptr -> writeAllRecords ptr payloads 0

-- | Wrap compressed records with the 61-byte batch header.
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
    let !crc = crc32cPtr (ptr `plusPtr` 21) (40 + recordsSize)
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
  poke16BE p comprAttr
  poke32BE (p `plusPtr` 2) (fromIntegral (recordCount - 1))
  poke64BE (p `plusPtr` 6) 0
  poke64BE (p `plusPtr` 14) 0
  poke64BE (p `plusPtr` 22) pid
  poke16BE (p `plusPtr` 30) epoch
  poke32BE (p `plusPtr` 32) baseSeq
  poke32BE (p `plusPtr` 36) (fromIntegral recordCount)
{-# INLINE writePostCrc #-}

------------------------------------------------------------------------
-- Records
------------------------------------------------------------------------

writeAllRecords :: Ptr Word8 -> [ByteString] -> Int -> IO ()
writeAllRecords !_ [] !_ = pure ()
writeAllRecords !p (payload : rest) !i = do
  p' <- writeRecord p i payload
  writeAllRecords p' rest (i + 1)
{-# INLINE writeAllRecords #-}

writeRecord :: Ptr Word8 -> Int -> ByteString -> IO (Ptr Word8)
writeRecord !p !index !payload = do
  let !payloadLen = BS.length payload
      !bodySize = 1 + 1 + zigzagSize index + 1 + zigzagSize payloadLen + payloadLen + 1
  p1 <- pokeZigzag p bodySize          -- record length
  poke p1 (0 :: Word8)                 -- attributes
  poke (p1 `plusPtr` 1) (0 :: Word8)   -- timestampDelta (zigzag 0)
  p2 <- pokeZigzag (p1 `plusPtr` 2) index  -- offsetDelta
  poke p2 (1 :: Word8)                 -- keyLength (zigzag -1)
  p3 <- pokeZigzag (p2 `plusPtr` 1) payloadLen  -- valueLength
  -- Copy payload bytes: use unsafeUseAsCStringLen to get Ptr, then memcpy
  BSU.unsafeUseAsCStringLen payload $ \(srcPtr, len) ->
    BSI.memcpy p3 (castPtr srcPtr) len
  let !p4 = p3 `plusPtr` payloadLen
  poke p4 (0 :: Word8)                 -- headerCount (zigzag 0)
  pure (p4 `plusPtr` 1)
{-# INLINE writeRecord #-}

------------------------------------------------------------------------
-- Size computation (pure, no allocations)
------------------------------------------------------------------------

-- | Returns (# totalRecordsSize, payloadCount #) in a single pass.
computeRecordsSizeAndCount :: [ByteString] -> (# Int, Int #)
computeRecordsSizeAndCount = go 0 0
  where
    go !acc !i [] = (# acc, i #)
    go !acc !i (payload : rest) =
      let !payloadLen = BS.length payload
          !bodySize = 1 + 1 + zigzagSize i + 1 + zigzagSize payloadLen + payloadLen + 1
      in go (acc + zigzagSize bodySize + bodySize) (i + 1) rest
{-# INLINE computeRecordsSizeAndCount #-}

------------------------------------------------------------------------
-- Zigzag varint
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
-- Big-endian poke: single store + BSWAP
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
-- CRC32C — direct FFI on Ptr
------------------------------------------------------------------------

foreign import ccall unsafe "crc32c/crc32c.h crc32c_extend"
  c_crc32c_extend :: Word32 -> Ptr Word8 -> CSize -> IO Word32

crc32cPtr :: Ptr Word8 -> Int -> Word32
crc32cPtr ptr len = unsafeDupablePerformIO $
  c_crc32c_extend 0 ptr (fromIntegral len)
{-# INLINE crc32cPtr #-}
