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
use capnp::OutputSegments;

use tokio_core::io::Codec;
use tokio_core::io::EasyBuf;

use byteorder::LittleEndian;
use byteorder::ReadBytesExt;
use byteorder::WriteBytesExt;


const SEGMENTS_COUNT_OFFSET: usize = 4;
const SEGMENTS_LEN_OFFSET: usize = 4;
const SEGMENTS_COUNT_MAX: usize = 512;
pub const WORD_SIZE: usize = 8;

#[derive(Debug, PartialEq)]
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
    words: Vec<Word>,
}

pub struct OwnedSegments {
    words: Vec<Word>,
    slices: Vec<(usize, usize)>,
}

impl ReaderSegments for OwnedSegments {
    fn get_segment(&self, id: u32) -> Option<&[Word]> {
        let id = id as usize;
        if id < self.slices.len() {
            let (a, b) = self.slices[id];
            Some(&self.words[a..b])
        } else {
            None
        }
    }
}

pub struct CapnpCodec<A>
    where A: Allocator
{
    read_state: ReadState,
    read_data: ReadData,
    read_options: ReaderOptions,
    prev_buf_len: usize,
    phantom: PhantomData<A>,
}

// Interface
impl<A: Allocator> CapnpCodec<A> {
    pub fn new(options: ReaderOptions) -> Self {
        CapnpCodec {
            read_state: ReadState::Initial,
            read_options: options,
            read_data: Default::default(),
            phantom: Default::default(),
            prev_buf_len: Default::default(),
        }
    }
}

impl<A: Allocator> Codec for CapnpCodec<A> {
    type In = Reader<OwnedSegments>;
    type Out = Builder<A>;

    fn decode(&mut self, buf: &mut EasyBuf) -> Result<Option<Self::In>, Error> {
        let no_new_data_available = self.prev_buf_len == buf.len();
        if no_new_data_available {
            return Err(Error::new(ErrorKind::InvalidData, "Insufficient data"));
        }

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
                    words: mem::replace(&mut self.read_data.words, Default::default()),
                };
                return Ok(Some(Reader::new(segments, self.read_options)));
            }
        };

        Ok(None)
    }

    fn encode(&mut self, msg: Self::Out, buf: &mut Vec<u8>) -> io::Result<()> {
        let segments = msg.get_segments_for_output();
        self.construct_segment_table(buf, &segments)?;
        for segment in segments.iter() {
            buf.write_all(Word::words_to_bytes(segment))?;
        }

        Ok(())
    }
}

// Decode and encode functions
impl<A: Allocator> CapnpCodec<A> {
    fn read_segments_count(&mut self, buf: &mut EasyBuf) -> Result<ReadState, Error> {
        if buf.len() < SEGMENTS_COUNT_OFFSET {
            self.prev_buf_len = buf.len();
            return Ok(ReadState::Initial);
        }

        // From Capnp docs: The number of segments, minus one
        // (since there is always at least one segment).
        // We need to have full count, therefore `wrapping_add`.
        let segments_count = self.read_u32(buf.drain_to(SEGMENTS_COUNT_OFFSET))?
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
    }

    fn read_segments_meta(&mut self, buf: &mut EasyBuf) -> Result<ReadState, Error> {
        let seg_count = self.read_data.segments_count;
        let needed_buf_len = seg_count * SEGMENTS_LEN_OFFSET;

        if buf.len() < needed_buf_len {
            self.prev_buf_len = buf.len();
            return Ok(ReadState::KnowsSegmentsCount);
        }

        let mut total_words = 0;

        for _ in 0..seg_count {
            let tmp_buf = buf.drain_to(SEGMENTS_LEN_OFFSET);
            let seg_len = self.read_u32(tmp_buf)? as usize;
            self.read_data.segments_slices.push((total_words, total_words + seg_len));
            total_words += seg_len;
        }

        if total_words as u64 > self.read_options.traversal_limit_in_words {
            return Err(Error::new(ErrorKind::InvalidData,
                                  format!("Message has {} words, which is too large. To \
                                           increase the limit on the receiving end, see \
                                           capnp::message::ReaderOptions.",
                                          total_words)));
        }

        // Reserving hereafter we ensure we don't exceed the limit (^).
        self.read_data.words.reserve_exact(seg_count);

        // Checking padding up to the next word boundary.
        let read_bytes = SEGMENTS_COUNT_OFFSET + needed_buf_len;
        let padding = read_bytes % WORD_SIZE;
        if padding == 4 {
            buf.drain_to(SEGMENTS_LEN_OFFSET);
        }

        Ok(ReadState::KnowsSegmentsMeta)
    }

    fn read_segments(&mut self, buf: &mut EasyBuf) -> Result<ReadState, Error> {
        let words = &mut self.read_data.words;
        let slices = &self.read_data.segments_slices;
        let ready_segments = words.len();
        let total_segments = self.read_data.segments_count;

        let mut writer = io::Cursor::new(Word::words_to_bytes_mut(words));
        let (start, _) = slices[ready_segments];
        writer.seek(SeekFrom::Start(start as u64))?;

        for (_, &(start, end)) in (ready_segments..total_segments).zip(slices) {

            let needed_buf_len = (end - start) * WORD_SIZE;
            if buf.len() < needed_buf_len {
                self.prev_buf_len = buf.len();
                return Ok(ReadState::KnowsSegmentsMeta);
            }

            writer.write_all(buf.drain_to(needed_buf_len).as_slice())?;
        }

        Ok(ReadState::Done)
    }

    fn construct_segment_table(&self,
                               buf: &mut Vec<u8>,
                               segments: &OutputSegments)
                               -> io::Result<()> {
        let segments_count = segments.len().checked_sub(1).unwrap_or(0);
        buf.write_u32::<LittleEndian>(segments_count as u32)?;

        for segment in segments.iter() {
            buf.write_u32::<LittleEndian>(segment.len() as u32)?;
        }

        let written = buf.len();
        let padding = written % WORD_SIZE;
        if padding == 4 {
            buf.write_u32::<LittleEndian>(0)?;
        }

        Ok(())
    }

    // Helpers
    #[inline]
    fn read_u32(&self, buf: EasyBuf) -> Result<u32, Error> {
        let mut rdr = io::Cursor::new(buf);
        rdr.read_u32::<LittleEndian>()
    }
}


#[cfg(test)]
mod test {

    use std::io::Error;

    use capnp::message::ReaderOptions;
    use capnp::message::HeapAllocator;
    use capnp::Word;
    use capnp::OutputSegments;

    use tokio_core::io::EasyBuf;
    use tokio_core::io::Codec;

    use super::CapnpCodec;
    use super::ReadData;
    use super::ReadState;

    fn decode_segment_table(buf: Vec<u8>) -> Result<ReadData, Error> {
        let mut codec = CapnpCodec::<HeapAllocator>::new(ReaderOptions::new());
        let mut buf = EasyBuf::from(buf);
        while codec.read_state != ReadState::KnowsSegmentsMeta {
            codec.decode(&mut buf)?;
        }
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

    fn check_decoded<CF>(d: Result<ReadData, Error>, check_fn: CF)
        where CF: FnOnce(ReadData)
    {
        match d {
            Err(msg) => panic!("{}", msg),
            Ok(read_data) => check_fn(read_data),
        };
    }

    fn check_segment_table<CF>(segments: &OutputSegments, check_fn: CF)
        where CF: FnOnce(Vec<u8>)
    {
        let mut buf = Vec::new();
        let codec = CapnpCodec::<HeapAllocator>::new(ReaderOptions::new());
        codec.construct_segment_table(&mut buf, &segments)
            .unwrap_or_else(|msg: Error| panic!("{}", msg));
        println!("buf - {:?}", buf);
        check_fn(buf);
    }

    #[test]
    fn test_decode_segment_table() {
        // 1 segment, 0 length
        let buf = prepare_buf(&[0, 0, 0, 0, 0, 0, 0, 0]);
        check_decoded(decode_segment_table(buf), |rd| {
            assert!(rd.segments_count == 1);
            assert!(vec![(0, 0)] == rd.segments_slices);
        });

        // 1 segment, 1 length
        let buf = prepare_buf(&[0, 0, 0, 0, 1, 0, 0, 0]);
        check_decoded(decode_segment_table(buf), |rd| {
            assert!(rd.segments_count == 1);
            assert!(vec![(0, 1)] == rd.segments_slices);
        });

        // 2 segments, 1 length, 1 length, padding
        let buf = prepare_buf(&[1, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0]);
        check_decoded(decode_segment_table(buf), |rd| {
            assert!(rd.segments_count == 2);
            assert!(vec![(0, 1), (1, 2)] == rd.segments_slices);
        });

        // 3 segments, 1 length, 1 length, 256 length
        let buf = prepare_buf(&[2, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 1, 0, 0]);
        check_decoded(decode_segment_table(buf), |rd| {
            assert!(rd.segments_count == 3);
            assert!(vec![(0, 1), (1, 2), (2, 258)] == rd.segments_slices);
        });

        // 4 segments, 77 length, 23 length, 1 length, 99 length, padding
        let buf = prepare_buf(&[3, 0, 0, 0, 77, 0, 0, 0, 23, 0, 0, 0, 1, 0, 0, 0, 99, 0, 0, 0, 0,
                                0, 0, 0]);
        check_decoded(decode_segment_table(buf), |rd| {
            assert!(rd.segments_count == 4);
            assert!(vec![(0, 77), (77, 100), (100, 101), (101, 200)] == rd.segments_slices);
        });
    }

    #[test]
    fn test_invalid_decode_segment_table() {
        // 513 segments
        let buf = prepare_buf2(&[0, 2, 0, 0], &[0; 513 * 4]);
        assert!(decode_segment_table(buf).is_err());

        // 1 segments
        let buf = prepare_buf(&[0, 0, 0, 0]);
        assert!(decode_segment_table(buf).is_err());

        // 1 segments
        let buf = prepare_buf2(&[0, 0, 0, 0], &[0; 3]);
        assert!(decode_segment_table(buf).is_err());

        // 0 segments
        let buf = prepare_buf(&[255, 255, 255, 255]);
        assert!(decode_segment_table(buf).is_err());
    }

    #[test]
    fn test_construct_table() {
        let segment_0 = [];
        let segment_1 = [Word { raw_content: 1 }];
        let segment_199 = [Word { raw_content: 199 }; 199];

        // 1 segment, 0 length
        check_segment_table(&OutputSegments::MultiSegment(vec![&segment_0]), |buf| {
            assert!(&[0, 0, 0, 0, 0, 0, 0, 0] == &buf[..]);
        });

        // 1 segment, 1 length
        check_segment_table(&OutputSegments::MultiSegment(vec![&segment_1[..]]), |buf| {
            assert!(&[0, 0, 0, 0, 1, 0, 0, 0] == &buf[..]);
        });

        // 1 segment, 199 length
        check_segment_table(&OutputSegments::MultiSegment(vec![&segment_199[..]]), |buf| {
            assert!(&[0, 0, 0, 0, 199, 0, 0, 0] == &buf[..]);
        });

        // 2 segments, 0 length, 1 length, padding
        let segments = OutputSegments::MultiSegment(vec![&segment_0[..], &segment_1[..]]);
        check_segment_table(&segments, |buf| {
            assert!(&[1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0] == &buf[..]);
        });

        // 4 segments, 199 length, 1 length, 199 length, 0 length, padding
        let segments = OutputSegments::MultiSegment(vec![&segment_199[..],
                                                         &segment_1[..],
                                                         &segment_199[..],
                                                         &segment_0[..]]);
        check_segment_table(&segments, |buf| {
            assert!(&[3, 0, 0, 0, 199, 0, 0, 0, 1, 0, 0, 0, 199, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
                      0] == &buf[..]);
        });

        // 5 segments, 199 length, 1 length, 199 length, 0 length, 1 length
        let segments = OutputSegments::MultiSegment(vec![&segment_199[..],
                                                         &segment_1[..],
                                                         &segment_199[..],
                                                         &segment_0[..],
                                                         &segment_1[..]]);
        check_segment_table(&segments, |buf| {
            assert!(&[4, 0, 0, 0, 199, 0, 0, 0, 1, 0, 0, 0, 199, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
                      0] == &buf[..]);
        });
    }
}
