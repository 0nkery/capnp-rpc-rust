use std::io;
use std::io::Error;

use capnp::message::Allocator;
use capnp::message::Reader;

use tokio_core::io::Codec;
use tokio_core::io::EasyBuf;

use super::codec::CapnpCodec;
use super::codec::WORD_SIZE;
use super::codec::OwnedSegments;


const ZERO_WORD: [u8; WORD_SIZE] = [0, 0, 0, 0, 0, 0, 0, 0];


enum ReadState {
    // Waiting for one-byte tag
    Initial,
    // Inspecting one-byte tag
    Tag(u8),
    // Waiting for N unpacked words
    Unpacked(u8),
    // Waiting for packed word with N content bytes
    Packed(u8),
}

struct PackedCapnpCodec<A>
    where A: Allocator
{
    codec: CapnpCodec<A>,
    read_state: ReadState,
    out_buf: EasyBuf,
}

impl<A: Allocator> Codec for PackedCapnpCodec<A> {
    type In = Reader<OwnedSegments>;
    type Out = ();

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, Error> {
        match self.read_state {
            ReadState::Initial => {
                let tag = self.drain_byte(buf);
                self.read_state = self.inspect_tag(buf, tag)?;
            }
            ReadState::Tag(tag) => {
                self.read_state = self.inspect_tag(buf, tag)?;
            }
            ReadState::Unpacked(words_count) => {
                self.read_state = self.try_read_unpacked(buf, words_count)?;
            }
            ReadState::Packed(tag) => {
                self.read_state = self.try_read_packed(buf, tag)?;
            }
        }

        let needed_buf_len = match self.read_state {
            ReadState::Initial => 1,
            ReadState::Tag(tag) => {
                match tag {
                    0x00 => 1,
                    0xFF => WORD_SIZE + 1,
                    t => t.count_ones() as usize,
                }
            }
            ReadState::Unpacked(words_count) => (words_count as usize) * WORD_SIZE,
            ReadState::Packed(bytes_count) => bytes_count as usize,
        };

        if needed_buf_len <= buf.len() {
            return self.decode(buf);
        }

        self.codec.decode(&mut self.out_buf)
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        Ok(())
    }
}

impl<A: Allocator> PackedCapnpCodec<A> {
    #[inline]
    fn drain_byte(&mut self, buf: &mut EasyBuf) -> u8 {
        buf.drain_to(1).as_slice()[0]
    }

    fn inspect_tag(&mut self, buf: &mut EasyBuf, tag: u8) -> Result<ReadState, Error> {
        match tag {
            0x00 => {
                if buf.len() == 0 {
                    return Ok(ReadState::Tag(tag));
                }

                let zero_words_count = self.drain_byte(buf) as usize;
                self.out_buf.get_mut().reserve(zero_words_count * WORD_SIZE);
                for _ in 0..zero_words_count {
                    self.out_buf.get_mut().extend_from_slice(&ZERO_WORD);
                }

                Ok(ReadState::Initial)
            }
            0xFF => {
                // word + byte with count of unpacked words
                if buf.len() < WORD_SIZE + 1 {
                    return Ok(ReadState::Tag(tag));
                }

                let word = buf.drain_to(WORD_SIZE);
                self.out_buf.get_mut().extend_from_slice(word.as_slice());

                let unpacked_words_count = self.drain_byte(buf);

                self.try_read_unpacked(buf, unpacked_words_count)
            }
            t => self.try_read_packed(buf, t),
        }
    }

    fn try_read_packed(&mut self, buf: &mut EasyBuf, tag: u8) -> Result<ReadState, Error> {
        let content_bytes_count = tag.count_ones() as usize;

        if buf.len() < content_bytes_count {
            return Ok(ReadState::Packed(tag));
        }

        let mut word = [0; 8];
        for i in 0..8 {
            let is_one = tag & (1u8 << i) != 0;
            if is_one {
                word[i] = self.drain_byte(buf);
            }
        }

        self.out_buf.get_mut().extend_from_slice(&word);

        Ok(ReadState::Initial)
    }

    fn try_read_unpacked(&mut self,
                         buf: &mut EasyBuf,
                         words_count: u8)
                         -> Result<ReadState, Error> {
        let needed_buf_len = (words_count as usize) * WORD_SIZE;
        if buf.len() < needed_buf_len {
            return Ok(ReadState::Unpacked(words_count));
        }

        let words = buf.drain_to(needed_buf_len);
        self.out_buf.get_mut().extend_from_slice(words.as_slice());

        Ok(ReadState::Initial)
    }
}
