use std::io;
use std::io::ErrorKind;
use std::io::Error;
use std::io::SeekFrom;
use std::io::Write;
use std::io::Seek;
use std::mem;
use std::marker::PhantomData;

use capnp::message::Allocator;
use capnp::message::Builder;
use capnp::message::Reader;
use capnp::message::ReaderOptions;
use capnp::message::ReaderSegments;
use capnp::Word;

use tokio_core::io::Codec;
use tokio_core::io::EasyBuf;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;


const SEGMENTS_COUNT_OFFSET: usize = 4;
const SEGMENTS_LEN_OFFSET: usize = 4;
const SEGMENTS_COUNT_MAX: usize = 512;
const WORD_SIZE: usize = 8;

enum ReadState {
    Initial,
    KnowsSegmentsCount,
    KnowsSegmentsMeta,
    Done,
}

#[derive(Default)]
struct ReadData {
    segments_count: usize,
    segments_slices: Vec<(usize, usize)>,
    segments: Vec<Word>,
}

struct OwnedSegments {
    segments: Vec<Word>,
    slices: Vec<(usize, usize)>,
}

impl ReaderSegments for OwnedSegments {
    fn get_segment<'a>(&'a self, id: u32) -> Option<&'a [Word]> {
        let id = id as usize;
        if id < self.slices.len() {
            let (a, b) = self.slices[id];
            Some(&self.segments[a..b])
        } else {
            None
        }
    }
}

struct CapnpCodec<A>
    where A: Allocator
{
    read_state: ReadState,
    read_data: ReadData,
    read_options: ReaderOptions,
    phantom: PhantomData<A>,
}

impl<A: Allocator> CapnpCodec<A> {
    pub fn new(options: ReaderOptions) -> Self {
        CapnpCodec {
            read_state: ReadState::Initial,
            read_data: Default::default(),
            read_options: options,
            phantom: Default::default(),
        }
    }
}

impl<A: Allocator> Codec for CapnpCodec<A> {
    type In = Reader<OwnedSegments>;
    type Out = Builder<A>;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, Error> {
        match self.read_state {
            ReadState::Initial => {
                self.read_state = self.read_segments_count(buf)?;
            }
            ReadState::KnowsSegmentsCount => {
                self.read_state = self.read_segments_meta(buf)?;
            }
            ReadState::KnowsSegmentsMeta => {
                self.read_state = self.read_segments(buf)?;
            }
            ReadState::Done => {
                let segments = OwnedSegments {
                    slices: mem::replace(&mut self.read_data.segments_slices, Default::default()),
                    segments: mem::replace(&mut self.read_data.segments, Default::default()),
                };
                return Ok(Some(Reader::new(segments, self.read_options)));
            }
        }

        Ok(None)
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        let segments = msg.get_segments_for_output();

        buf.write_u32::<LittleEndian>(segments.len() as u32)?;
        for segment in segments.iter() {
            buf.write_u32::<LittleEndian>(segment.len() as u32)?;
        }
        for segment in segments.iter() {
            buf.write(Word::words_to_bytes(segment))?;
        }

        Ok(())
    }
}

// Decode functions
impl<A: Allocator> CapnpCodec<A> {
    fn read_segments_count(&mut self, buf: &mut EasyBuf) -> Result<ReadState, Error> {
        if buf.len() >= SEGMENTS_COUNT_OFFSET {
            // From Capnp docs: The number of segments, minus one
            // (since there is always at least one segment).
            // We need to have full count, therefore `wrapping_add`.
            let segments_count = self.read_u32(buf.split_off(SEGMENTS_COUNT_OFFSET))?
                .wrapping_add(1) as usize;

            if segments_count >= SEGMENTS_COUNT_MAX {
                return Err(Error::new(ErrorKind::InvalidInput,
                                      format!("Too many segments: {}", segments_count)));
            } else if segments_count == 0 {
                return Err(Error::new(ErrorKind::InvalidInput,
                                      format!("Too few segments: {}", segments_count)));
            }

            self.read_data.segments_count = segments_count;
            self.read_data.segments_slices.reserve_exact(segments_count);

            Ok(ReadState::KnowsSegmentsCount)
        } else {
            Ok(ReadState::Initial)
        }
    }

    fn read_segments_meta(&mut self, buf: &mut EasyBuf) -> Result<ReadState, Error> {
        let seg_count = self.read_data.segments_count;

        let needed_buf_len = seg_count * SEGMENTS_LEN_OFFSET;

        if needed_buf_len < buf.len() {
            return Ok(ReadState::KnowsSegmentsCount);
        }

        let mut prev_seg_len = 0;

        for _ in 0..seg_count {
            let seg_len = self.read_u32(buf.split_off(SEGMENTS_LEN_OFFSET))? as usize;
            self.read_data.segments_slices.push((prev_seg_len, prev_seg_len + seg_len));
            prev_seg_len = seg_len;
        }

        let total_words = self.read_data.segments_slices.len() * SEGMENTS_LEN_OFFSET;
        if total_words as u64 > self.read_options.traversal_limit_in_words {
            return Err(Error::new(ErrorKind::InvalidData,
                                  format!("Message has {} words, which is too large. To \
                                           increase the limit on the receiving end, see \
                                           capnp::message::ReaderOptions.",
                                          total_words)));
        }

        // Reserving hereafter we ensure we don't exceed the limit (^).
        self.read_data.segments.reserve_exact(seg_count);

        // Checking padding up to the next word boundary.
        let read_bytes = SEGMENTS_COUNT_OFFSET + needed_buf_len;
        let padding = read_bytes % WORD_SIZE;
        if padding == 4 {
            buf.drain_to(SEGMENTS_LEN_OFFSET);
        }

        Ok(ReadState::KnowsSegmentsMeta)
    }

    fn read_segments(&mut self, buf: &mut EasyBuf) -> Result<ReadState, Error> {
        let segments = &mut self.read_data.segments;
        let slices = &self.read_data.segments_slices;
        let ready_segments = segments.len();
        let total_segments = self.read_data.segments_count;

        let mut writer = io::Cursor::new(Word::words_to_bytes_mut(segments));
        let (start, _) = slices[ready_segments];
        writer.seek(SeekFrom::Start(start as u64))?;

        for (_, &(start, end)) in (ready_segments..total_segments).zip(slices) {

            let needed_buf_len = (end - start) * WORD_SIZE;
            if buf.len() < needed_buf_len {
                return Ok(ReadState::KnowsSegmentsMeta);
            }

            writer.write(buf.split_off(needed_buf_len).as_slice())?;
        }

        Ok(ReadState::Done)
    }
}

// Helper functions
impl<A: Allocator> CapnpCodec<A> {
    #[inline]
    fn read_u32(&self, buf: EasyBuf) -> Result<u32, Error> {
        let mut rdr = io::Cursor::new(buf);
        rdr.read_u32::<LittleEndian>()
    }
}


#[cfg(test)]
pub mod test {

    use std::io::Error;

    use capnp::message::ReaderOptions;
    use capnp::message::HeapAllocator;

    use tokio_core::io::EasyBuf;

    use super::CapnpCodec;
    use super::ReadData;

    fn decode(buf: Vec<u8>) -> Result<ReadData, Error> {
        let mut codec = CapnpCodec::<HeapAllocator>::new(ReaderOptions::new());
        let mut buf = EasyBuf::from(buf);
        codec.read_segments_count(&mut buf)?;
        codec.read_segments_meta(&mut buf)?;
        codec.read_segments(&mut buf)?;

        Ok(codec.read_data)
    }

    fn prepare_buf(input: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(input.iter().cloned());
        buf
    }

    fn prepare_buf2(input1: &[u8], input2: &[u8]) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend(input1.iter().cloned());
        buf.extend(input2.iter().cloned());
        buf
    }

    fn check_decoded<CF>(d: Result<ReadData, Error>, check_fn: CF) where CF: FnOnce(ReadData) {
        match d {
            Err(msg) => panic!("{}", msg),
            Ok(read_data) => check_fn(read_data)
        };
    }

    #[test]
    fn test_decode() {
        // 1 segment, 0 length
        let buf = prepare_buf(&[0, 0, 0, 0, 0, 0, 0, 0]);
        check_decoded(decode(buf), |rd| {
            assert!(rd.segments_count == 0);
            assert!(vec![(0, 0)] == rd.segments_slices);
        });

        // 1 segment, 1 length
        let buf = prepare_buf(&[0, 0, 0, 0, 1, 0, 0, 0]);
        check_decoded(decode(buf), |rd| {
            assert!(rd.segments_count == 1);
            assert!(vec![(0, 1)] == rd.segments_slices);
        });

        // 2 segments, 1 length, 1 length, padding
        let buf = prepare_buf(&[1, 0, 0, 0,
                                1, 0, 0, 0,
                                1, 0, 0, 0,
                                0, 0, 0, 0]);
        check_decoded(decode(buf), |rd| {
            assert!(rd.segments_count == 2);
            assert!(vec![(0, 1), (1, 2)] == rd.segments_slices);
        });

        // 3 segments, 1 length, 1 length, 256 length
        let buf = prepare_buf(&[2, 0, 0, 0,
                                1, 0, 0, 0,
                                1, 0, 0, 0,
                                0, 1, 0, 0]);
        check_decoded(decode(buf), |rd| {
            assert!(rd.segments_count == 258);
            assert!(vec![(0, 1), (1, 2), (2, 258)] == rd.segments_slices);
        });

        // 4 segments, 77 length, 23 length, 1 length, 99 length, padding
        let buf = prepare_buf(&[3, 0, 0, 0,
                                77, 0, 0, 0,
                                23, 0, 0, 0,
                                1, 0, 0, 0,
                                99, 0, 0, 0,
                                0, 0, 0, 0]);
        check_decoded(decode(buf), |rd| {
            assert!(rd.segments_count == 200);
            assert!(vec![(0, 77), (77, 100), (100, 101), (101, 200)] == rd.segments_slices);
        });
    }

    #[test]
    fn test_invalid_decode() {
        // 513 segments
        let buf = prepare_buf2(&[0, 2, 0, 0], &[0; 513 * 4]);
        assert!(decode(buf).is_err());

        // 1 segments
        let buf = prepare_buf(&[0, 0, 0, 0]);
        assert!(decode(buf).is_err());

        // 1 segments
        let buf = prepare_buf2(&[0, 0, 0, 0], &[0; 3]);
        assert!(decode(buf).is_err());

        // 0 segments
        let buf = prepare_buf(&[255, 255, 255, 255]);
        assert!(decode(buf).is_err());
    }
}
