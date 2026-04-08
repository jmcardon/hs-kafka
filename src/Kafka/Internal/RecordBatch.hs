{-# language BangPatterns, MagicHash, UnboxedTuples #-}

-- | Single-allocation record batch builder.
--
-- Writes the entire record batch (61-byte header + all records) into a
-- pinned buffer. The result IS a ByteString — one allocation total.
-- CRC32C computed directly on the Ptr via FFI.
--
-- Supports full record format v2: key, value, headers, timestamp deltas.
module Kafka.Internal.RecordBatch
  ( RecordInput(..)
  , buildRecordBatch
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
import qualified Data.ByteString.Unsafe as BSU
import Foreign.Marshal.Utils (copyBytes)

import Kafka.Producer.Types (Header(..))

------------------------------------------------------------------------
-- Input type
------------------------------------------------------------------------

-- | A single record to be written into the batch.
data RecordInput = RecordInput
  { riKey     :: !(Maybe ByteString)
  , riValue   :: !(Maybe ByteString)
  , riHeaders :: ![Header]
  , riTimestampDelta :: {-# UNPACK #-} !Int64
    -- ^ Milliseconds relative to the batch's firstTimestamp.
    -- 0 if the batch uses broker-assigned timestamps.
  }

------------------------------------------------------------------------
-- Public API
------------------------------------------------------------------------

-- | Build a complete record batch (header + records) as a ByteString.
buildRecordBatch ::
     Int64 -> Int16 -> Int32 -> Int16
  -> Int64           -- ^ firstTimestamp (epoch ms, 0 for broker-assigned)
  -> [RecordInput]
  -> ByteString
buildRecordBatch !pid !epoch !baseSeq !comprAttr !firstTs records =
  let !(# recordsSize, recordCount #) = computeRecordsSizeAndCount records
      !totalSize = 61 + recordsSize
  in unsafeCreate totalSize $ \ptr -> do
    writeAllRecords (ptr `plusPtr` 61) records 0
    writePostCrc ptr pid epoch baseSeq comprAttr firstTs recordCount
    let !crc = crc32cPtr (ptr `plusPtr` 21) (40 + recordsSize)
    writePreCrc ptr recordsSize crc
{-# INLINE buildRecordBatch #-}

-- | Build just the records portion (no header). For compression.
buildRecords :: [RecordInput] -> ByteString
buildRecords records =
  let !(# size, _ #) = computeRecordsSizeAndCount records
  in unsafeCreate size $ \ptr -> writeAllRecords ptr records 0

-- | Wrap compressed records with the 61-byte batch header.
wrapRecordBatch ::
     Int64 -> Int16 -> Int32 -> Int16 -> Int
  -> Int64 -> ByteString -> ByteString
wrapRecordBatch !pid !epoch !baseSeq !comprAttr !recordCount !firstTs records =
  let !recordsSize = BS.length records
      !totalSize = 61 + recordsSize
  in unsafeCreate totalSize $ \ptr -> do
    BSU.unsafeUseAsCStringLen records $ \(srcPtr, len) ->
      copyBytes (ptr `plusPtr` 61) (castPtr srcPtr) len
    writePostCrc ptr pid epoch baseSeq comprAttr firstTs recordCount
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

writePostCrc :: Ptr Word8 -> Int64 -> Int16 -> Int32 -> Int16 -> Int64 -> Int -> IO ()
writePostCrc !base !pid !epoch !baseSeq !comprAttr !firstTs !recordCount = do
  let !p = base `plusPtr` 21
  poke16BE p comprAttr
  poke32BE (p `plusPtr` 2) (fromIntegral (recordCount - 1))
  poke64BE (p `plusPtr` 6) firstTs                -- firstTimestamp
  poke64BE (p `plusPtr` 14) firstTs               -- maxTimestamp (= firstTs for now)
  poke64BE (p `plusPtr` 22) pid
  poke16BE (p `plusPtr` 30) epoch
  poke32BE (p `plusPtr` 32) baseSeq
  poke32BE (p `plusPtr` 36) (fromIntegral recordCount)
{-# INLINE writePostCrc #-}

------------------------------------------------------------------------
-- Records
------------------------------------------------------------------------

writeAllRecords :: Ptr Word8 -> [RecordInput] -> Int -> IO ()
writeAllRecords !_ [] !_ = pure ()
writeAllRecords !p (r : rest) !i = do
  p' <- writeRecord p i r
  writeAllRecords p' rest (i + 1)
{-# INLINE writeAllRecords #-}

writeRecord :: Ptr Word8 -> Int -> RecordInput -> IO (Ptr Word8)
writeRecord !p !index (RecordInput mKey mValue hdrs tsDelta) = do
  let !keyLen = maybe (-1) BS.length mKey
      !valLen = maybe (-1) BS.length mValue
      !hdrsSize = headersWireSize hdrs
      !bodySize = 1                             -- attributes
               + zigzagSize (fromIntegral tsDelta) -- timestampDelta
               + zigzagSize index               -- offsetDelta
               + zigzagSize keyLen              -- keyLength
               + (if keyLen >= 0 then keyLen else 0) -- key bytes
               + zigzagSize valLen              -- valueLength
               + (if valLen >= 0 then valLen else 0) -- value bytes
               + hdrsSize                       -- headers (count varint + each header)
  -- Record length
  p1 <- pokeZigzag p bodySize
  -- Attributes (always 0 for individual records in v2)
  poke p1 (0 :: Word8)
  -- Timestamp delta
  p2 <- pokeZigzag (p1 `plusPtr` 1) (fromIntegral tsDelta)
  -- Offset delta
  p3 <- pokeZigzag p2 index
  -- Key
  p4 <- pokeZigzag p3 keyLen
  p5 <- pokeOptionalBytes p4 mKey
  -- Value
  p6 <- pokeZigzag p5 valLen
  p7 <- pokeOptionalBytes p6 mValue
  -- Headers
  p8 <- pokeHeaders p7 hdrs
  pure p8
{-# INLINE writeRecord #-}

-- | Write optional bytes (Nothing = no bytes written, Just = write content).
pokeOptionalBytes :: Ptr Word8 -> Maybe ByteString -> IO (Ptr Word8)
pokeOptionalBytes !p Nothing = pure p
pokeOptionalBytes !p (Just bs) =
  BSU.unsafeUseAsCStringLen bs $ \(srcPtr, len) -> do
    copyBytes p (castPtr srcPtr) len
    pure (p `plusPtr` len)
{-# INLINE pokeOptionalBytes #-}

-- | Write record headers: varint(count) then each header.
pokeHeaders :: Ptr Word8 -> [Header] -> IO (Ptr Word8)
pokeHeaders !p [] = do
  poke p (0 :: Word8)  -- headerCount = 0 (zigzag 0)
  pure (p `plusPtr` 1)
pokeHeaders !p hdrs = do
  p1 <- pokeZigzag p (length hdrs)
  foldlM' pokeHeader p1 hdrs
{-# INLINE pokeHeaders #-}

pokeHeader :: Ptr Word8 -> Header -> IO (Ptr Word8)
pokeHeader !p (Header key mVal) = do
  let !keyLen = BS.length key
      !valLen = maybe (-1) BS.length mVal
  p1 <- pokeZigzag p keyLen
  p2 <- BSU.unsafeUseAsCStringLen key $ \(srcPtr, len) -> do
    copyBytes p1 (castPtr srcPtr) len
    pure (p1 `plusPtr` len)
  p3 <- pokeZigzag p2 valLen
  pokeOptionalBytes p3 mVal
{-# INLINE pokeHeader #-}

-- Strict left fold over a list with monadic accumulator.
foldlM' :: (a -> b -> IO a) -> a -> [b] -> IO a
foldlM' _ !acc [] = pure acc
foldlM' f !acc (x:xs) = do
  !acc' <- f acc x
  foldlM' f acc' xs
{-# INLINE foldlM' #-}

------------------------------------------------------------------------
-- Size computation
------------------------------------------------------------------------

computeRecordsSizeAndCount :: [RecordInput] -> (# Int, Int #)
computeRecordsSizeAndCount = go 0 0
  where
    go !acc !i [] = (# acc, i #)
    go !acc !i (RecordInput mKey mValue hdrs tsDelta : rest) =
      let !keyLen = maybe (-1) BS.length mKey
          !valLen = maybe (-1) BS.length mValue
          !hdrsSize = headersWireSize hdrs
          !bodySize = 1
                   + zigzagSize (fromIntegral tsDelta)
                   + zigzagSize i
                   + zigzagSize keyLen
                   + max 0 keyLen
                   + zigzagSize valLen
                   + max 0 valLen
                   + hdrsSize
      in go (acc + zigzagSize bodySize + bodySize) (i + 1) rest
{-# INLINE computeRecordsSizeAndCount #-}

-- | Wire size of the headers section: varint(count) + sum of each header.
headersWireSize :: [Header] -> Int
headersWireSize [] = 1  -- zigzag(0) = 1 byte
headersWireSize hdrs = zigzagSize (length hdrs) + sum (map headerWireSize hdrs)
{-# INLINE headersWireSize #-}

headerWireSize :: Header -> Int
headerWireSize (Header key mVal) =
  let !keyLen = BS.length key
      !valLen = maybe (-1) BS.length mVal
  in zigzagSize keyLen + keyLen + zigzagSize valLen + max 0 valLen
{-# INLINE headerWireSize #-}

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
-- Big-endian poke
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
-- CRC32C
------------------------------------------------------------------------

foreign import ccall unsafe "crc32c/crc32c.h crc32c_extend"
  c_crc32c_extend :: Word32 -> Ptr Word8 -> CSize -> IO Word32

crc32cPtr :: Ptr Word8 -> Int -> Word32
crc32cPtr ptr len = unsafeDupablePerformIO $
  c_crc32c_extend 0 ptr (fromIntegral len)
{-# INLINE crc32cPtr #-}
