use std::io;
use std::io::Error;

use capnp::message::Allocator;
use capnp::message::Builder;
use capnp::message::Reader;
use capnp::message::ReaderOptions;

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

impl<A: Allocator> PackedCapnpCodec<A> {
    pub fn new(options: ReaderOptions) -> Self {
        PackedCapnpCodec {
            codec: CapnpCodec::new(options),
            read_state: ReadState::Initial,
            out_buf: EasyBuf::new(),
        }
    }
}

impl<A: Allocator> Codec for PackedCapnpCodec<A> {
    type In = Reader<OwnedSegments>;
    type Out = Builder<A>;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, Error> {
        self.unpack(buf)?;
        self.codec.decode(&mut self.out_buf)
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        let mut unpacked = Vec::new();
        self.codec.encode(msg, &mut unpacked)?;
        self.pack(&unpacked, buf);

        Ok(())
    }
}

impl<A: Allocator> PackedCapnpCodec<A> {
    fn unpack(&mut self, buf: &mut EasyBuf) -> Result<(), Error> {
        if buf.len() == 0 {
            return Ok(());
        }

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

        if buf.len() >= needed_buf_len {
            self.unpack(buf)?
        }

        Ok(())
    }

    fn pack(&self, unpacked: &[u8], buf: &mut Vec<u8>) {
        let mut read_pos = 0;
        let mut tag_pos;

        while read_pos + WORD_SIZE <= unpacked.len() {
            let mut word = &unpacked[read_pos..read_pos + WORD_SIZE];
            let mut tag: u8 = 0;

            // place for tag
            tag_pos = buf.len();
            buf.push(0);

            for (i, byte) in word.iter().enumerate() {
                if *byte != 0 {
                    tag |= 1u8 << i;
                    buf.push(*byte);
                }
            }

            buf[tag_pos] = tag;
            read_pos += WORD_SIZE;

            if tag == 0x00 {
                let mut zero_words_count = 0;

                while read_pos + WORD_SIZE <= unpacked.len() {
                    word = &unpacked[read_pos..read_pos + WORD_SIZE];

                    if word != ZERO_WORD {
                        break;
                    }

                    zero_words_count += 1;

                    if zero_words_count == 255 {
                        break;
                    }

                    read_pos += WORD_SIZE;
                }

                buf.push(zero_words_count);

            } else if tag == 0xFF {
                let mut non_zero_words_count = 0;
                let start = read_pos;

                while read_pos + WORD_SIZE <= unpacked.len() {
                    word = &unpacked[read_pos..read_pos + WORD_SIZE];

                    let zeros_count = word.iter()
                        .map(|b| *b == 0)
                        .filter(|chk| *chk == true)
                        .count();

                    if zeros_count >= 2 {
                        break;
                    }

                    non_zero_words_count += 1;
                    if non_zero_words_count == 255 {
                        break;
                    }

                    read_pos += WORD_SIZE;
                }

                buf.push(non_zero_words_count);
                buf.extend_from_slice(&unpacked[start..read_pos]);
            }
        }
    }

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

                let zero_words_count = (self.drain_byte(buf) as usize) + 1;
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


#[cfg(test)]
mod test {

    use capnp::message::ReaderOptions;
    use capnp::message::HeapAllocator;

    use tokio_core::io::EasyBuf;

    use super::PackedCapnpCodec;

    fn check_packing(unpacked: &[u8], packed: &[u8]) {
        let mut codec = PackedCapnpCodec::<HeapAllocator>::new(ReaderOptions::new());
        // encode
        let mut buf = Vec::new();
        codec.pack(unpacked, &mut buf);

        assert_eq!(buf, packed);

        // decode
        let mut buf = EasyBuf::from(buf);
        let res = codec.unpack(&mut buf);
        if let Err(msg) = res {
            panic!("{}", msg);
        }
        assert_eq!(codec.out_buf.as_slice(), unpacked);
    }

    #[test]
    fn simple_packing() {
        check_packing(&[], &[]);
        check_packing(&[0; 8], &[0, 0]);
        check_packing(&[0, 0, 12, 0, 0, 34, 0, 0], &[0x24, 12, 34]);
        check_packing(&[1, 3, 2, 4, 5, 7, 6, 8], &[0xff, 1, 3, 2, 4, 5, 7, 6, 8, 0]);
        check_packing(&[0, 0, 0, 0, 0, 0, 0, 0, 1, 3, 2, 4, 5, 7, 6, 8],
                      &[0, 0, 0xff, 1, 3, 2, 4, 5, 7, 6, 8, 0]);
        check_packing(&[0, 0, 12, 0, 0, 34, 0, 0, 1, 3, 2, 4, 5, 7, 6, 8],
                      &[0x24, 12, 34, 0xff, 1, 3, 2, 4, 5, 7, 6, 8, 0]);
        check_packing(&[1, 3, 2, 4, 5, 7, 6, 8, 8, 6, 7, 4, 5, 2, 3, 1],
                      &[0xff, 1, 3, 2, 4, 5, 7, 6, 8, 1, 8, 6, 7, 4, 5, 2, 3, 1]);
        check_packing(&[1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8,
                        1, 2, 3, 4, 5, 6, 7, 8, 0, 2, 4, 0, 9, 0, 5, 1],
                      &[0xff, 1, 2, 3, 4, 5, 6, 7, 8, 3, 1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5,
                        6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 0xd6, 2, 4, 9, 5, 1]);
        check_packing(&[1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8, 6, 2, 4, 3, 9, 0, 5, 1,
                        1, 2, 3, 4, 5, 6, 7, 8, 0, 2, 4, 0, 9, 0, 5, 1],
                      &[0xff, 1, 2, 3, 4, 5, 6, 7, 8, 3, 1, 2, 3, 4, 5, 6, 7, 8, 6, 2, 4, 3, 9,
                        0, 5, 1, 1, 2, 3, 4, 5, 6, 7, 8, 0xd6, 2, 4, 9, 5, 1]);
        check_packing(&[8, 0, 100, 6, 0, 1, 1, 2, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 2, 0, 3, 1],
                      &[0xed, 8, 100, 6, 1, 1, 2, 0, 2, 0xd4, 1, 2, 3, 1]);
        check_packing(&[0; 16], &[0, 1]);
        check_packing(&[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
                      &[0, 2]);
    }

}
