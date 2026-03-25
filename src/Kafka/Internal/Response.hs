{-# language
    LambdaCase
  , RankNTypes
  #-}

module Kafka.Internal.Response
  ( fromKafkaResponse
  , getKafkaResponse
  , getResponseSizeHeader
  , tryParse
  ) where

import Control.Concurrent.STM (TVar)
import Control.Exception (try, IOException)
import Data.Int (Int32)
import Data.Primitive.ByteArray (ByteArray)
import qualified Data.Bytes
import System.IO (Handle, hPutStr, hFlush)

import qualified Data.ByteString as BS
import qualified Data.Bytes.Parser as Smith
import qualified Network.Socket.ByteString as NBS

import Kafka.Common
import Kafka.Internal.Combinator

-- | Receive exactly n bytes from a socket, or fail.
recvExact :: Kafka -> Int -> IO (Either KafkaException BS.ByteString)
recvExact kafka n = do
  result <- try (go n [])
  case result of
    Left (e :: IOException) -> pure (Left (KafkaIOError (show e)))
    Right bs -> pure (Right bs)
  where
    go 0 acc = pure (BS.concat (reverse acc))
    go remaining acc = do
      chunk <- NBS.recv (getSocket kafka) remaining
      if BS.null chunk
        then ioError (userError "connection closed by peer")
        else go (remaining - BS.length chunk) (chunk : acc)

-- | Convert a strict ByteString to a ByteArray (one copy, no intermediate list).
bsToByteArray :: BS.ByteString -> ByteArray
bsToByteArray bs =
  let bytes = Data.Bytes.fromByteString bs
  in Data.Bytes.toByteArrayClone bytes

-- | Read a full Kafka response (size header + body) as a ByteArray.
getKafkaResponse ::
     Kafka
  -> TVar Bool  -- ignored (was for sockets interruption, kept for API compat)
  -> IO (Either KafkaException ByteArray)
getKafkaResponse kafka _interrupt = do
  getResponseSizeHeader kafka _interrupt >>= \case
    Right byteCount -> do
      result <- recvExact kafka byteCount
      pure (bsToByteArray <$> result)
    Left e -> pure (Left e)

getResponseSizeHeader ::
     Kafka
  -> TVar Bool
  -> IO (Either KafkaException Int)
getResponseSizeHeader kafka _interrupt = do
  result <- recvExact kafka 4
  case result of
    Left e -> pure (Left e)
    Right bs ->
      let b0 = fromIntegral (BS.index bs 0) :: Int
          b1 = fromIntegral (BS.index bs 1) :: Int
          b2 = fromIntegral (BS.index bs 2) :: Int
          b3 = fromIntegral (BS.index bs 3) :: Int
      in pure (Right (b0 * 16777216 + b1 * 65536 + b2 * 256 + b3))

logMaybe :: Show a => a -> Maybe Handle -> IO ()
logMaybe a = \case
  Nothing -> pure ()
  Just h -> do
    hPutStr h (show a ++ "\n\n")
    hFlush h

fromKafkaResponse :: (Show a)
  => Parser a
  -> Kafka
  -> TVar Bool
  -> Maybe Handle
  -> IO (Either KafkaException (Either String a))
fromKafkaResponse parser kafka interrupt debugHandle =
  getKafkaResponse kafka interrupt >>= \case
    Right bytes -> do
      let res = Smith.parseByteArray parser bytes
      logMaybe res debugHandle
      case res of
        Smith.Failure e -> pure (Right (Left e))
        Smith.Success (Smith.Slice _ _ a) -> pure (Right (Right a))
    Left err -> pure (Left err)

tryParse :: Either KafkaException (Either String a) -> Either KafkaException a
tryParse = \case
  Right (Right parsed) -> Right parsed
  Right (Left parseError) -> Left (KafkaParseException parseError)
  Left networkError -> Left networkError
