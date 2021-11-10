use crate::frame::{self, Frame};

use bytes::{Buf, Bytes, BytesMut};
use futures::future::{BoxFuture, Future};
use std::convert::TryInto;
use std::fmt;
use std::io::{self, Cursor};
use std::num::TryFromIntError;
use std::string::FromUtf8Error;
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufWriter};
use tokio::net::TcpStream;

/// Send and receive `Frame` values from a remote peer.
///
/// When implementing networking protocols, a message on that protocol is
/// often composed of several smaller messages known as frames. The purpose of
/// `Connection` is to read and write frames on the underlying `TcpStream`.
///
/// To read frames, the `Connection` uses an internal buffer, which is filled
/// up until there are enough bytes to create a full frame. Once this happens,
/// the `Connection` creates the frame and returns it to the caller.
///
/// When sending frames, the frame is first encoded into the write buffer.
/// The contents of the write buffer are then written to the socket.
#[derive(Debug)]
pub struct Connection {
    // The `TcpStream`. It is decorated with a `BufWriter`, which provides write
    // level buffering. The `BufWriter` implementation provided by Tokio is
    // sufficient for our needs.
    stream: BufWriter<TcpStream>,

    // The buffer for reading frames.
    buffer: BytesMut,
}

#[derive(Debug)]
pub struct Bufio {
    bufpos: usize,
    cur: Cursor<Vec<u8>>,
}

impl Bufio {
    async fn get_byte<'fut>(&mut self, connection: &'fut mut Connection) -> Result<u8, MyError> {
        if self.bufpos >= self.cur.position() as usize {
            // read more
            // first read into bytes mut
            let buffer = &mut connection.buffer;
            let sz = connection.stream.read_buf(buffer).await?;

            if sz <= 0 {
                println!("Read 0 bytes");
                return Err(MyError::Incomplete);
            }
            self.cur.write(&buffer[self.bufpos..]).await?;
        }

        self.bufpos += 1;
        Ok(self.cur.get_ref()[self.bufpos - 1])
    }
    // TODO peek byte may need fixing
    async fn peek_byte<'a>(&mut self, connection: &'a mut Connection) -> Result<u8, MyError> {
        if self.bufpos >= self.cur.position() as usize {
            // read more
            // first read into bytes mut
            let buffer = &mut connection.buffer;
            let sz = connection.stream.read_buf(buffer).await?;

            if sz <= 0 {
                println!("Read 0 bytes");
                return Err(MyError::Incomplete);
            }
            self.cur.write(&buffer[self.bufpos..]).await?;
        }
        Ok(self.cur.get_ref()[self.bufpos])
    }

    fn get_slice(&mut self, start: usize, end: usize) -> &[u8] {
        &self.cur.get_ref()[start..end]
    }

    fn remaining(&mut self) -> usize {
        self.cur.position() as usize - self.bufpos
    }

    fn get_line<'buf>(
        &'buf mut self,
        connection: &'buf mut Connection,
    ) -> impl Future<Output = Result<&'buf [u8], MyError>> {
        async move {
            let start = self.bufpos;
            let mut curr_char = 0;
            while curr_char != b'\n' {
                curr_char = self.get_byte(connection).await?;
            }
            Ok(self.get_slice(start, self.bufpos - 2))
        }
    }

    // fn my_get_line<'buf>(
    //     &'buf mut self,
    //     cur: &'buf mut Cursor<Vec<u8>>,
    // ) -> impl Future<Output = Result<&'buf [u8], MyError>> {
    //     async move {
    //         let start = cur.position() as usize;
    //         let mut curr_char = 0;
    //         let buffer = &mut [0; 1][..];
    //         while curr_char != b'\n' {
    //             // currently panics, can change as needed
    //             curr_char = self.read_byte(cur, buffer).await?[0];
    //         }
    //         let end_pos = (cur.position() - 2) as usize;

    //         return Ok(&cur.get_ref()[start..end_pos]);
    //     }
    // }
}

#[derive(Debug)]
pub enum MyError {
    /// Not enough data is available to parse a message
    Incomplete,

    /// Invalid message encoding
    Other(crate::Error),
}

impl From<String> for MyError {
    fn from(src: String) -> MyError {
        MyError::Other(src.into())
    }
}

impl From<&str> for MyError {
    fn from(src: &str) -> MyError {
        src.to_string().into()
    }
}

impl From<FromUtf8Error> for MyError {
    fn from(_src: FromUtf8Error) -> MyError {
        "protocol error; invalid frame format".into()
    }
}

impl From<TryFromIntError> for MyError {
    fn from(_src: TryFromIntError) -> MyError {
        "protocol error; invalid frame format".into()
    }
}

impl std::error::Error for MyError {}

impl std::convert::From<std::io::Error> for MyError {
    fn from(_src: std::io::Error) -> MyError {
        "protocol error".into()
    }
}

impl fmt::Display for MyError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MyError::Incomplete => "stream ended early".fmt(fmt),
            MyError::Other(err) => err.fmt(fmt),
        }
    }
}

impl Connection {
    /// Create a new `Connection`, backed by `socket`. Read and write buffers
    /// are initialized.
    pub fn new(socket: TcpStream) -> Connection {
        Connection {
            stream: BufWriter::new(socket),
            // Default to a 4KB read buffer. For the use case of mini redis,
            // this is fine. However, real applications will want to tune this
            // value to their specific use case. There is a high likelihood that
            // a larger read buffer will work better.
            buffer: BytesMut::with_capacity(4 * 1024),
        }
    }

    /// Read a single `Frame` value from the underlying stream.
    ///
    /// The function waits until it has retrieved enough data to parse a frame.
    /// Any data remaining in the read buffer after the frame has been parsed is
    /// kept there for the next call to `read_frame`.
    ///
    /// # Returns
    ///
    /// On success, the received frame is returned. If the `TcpStream`
    /// is closed in a way that doesn't break a frame in half, it returns
    /// `None`. Otherwise, an error is returned.
    pub async fn read_frame(&mut self) -> crate::Result<Option<Frame>> {
        loop {
            // Attempt to parse a frame from the buffered data. If enough data
            // has been buffered, the frame is returned.
            if let Some(frame) = self.parse_frame()? {
                return Ok(Some(frame));
            }

            // There is not enough buffered data to read a frame. Attempt to
            // read more data from the socket.
            //
            // On success, the number of bytes is returned. `0` indicates "end
            // of stream".
            if 0 == self.stream.read_buf(&mut self.buffer).await? {
                // The remote closed the connection. For this to be a clean
                // shutdown, there should be no data in the read buffer. If
                // there is, this means that the peer closed the socket while
                // sending a frame.
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err("connection reset by peer".into());
                }
            }
        }
    }

    // fn my_get_u8(&mut self, src: &mut Cursor<Vec<u8>>) -> Result<u8, MyError> {
    //     if !src.has_remaining() {
    //         return Err(MyError::Incomplete);
    //     }
    //     Ok(src.get_u8())
    // }

    // fn read_byte<'cur>(
    //     &'cur mut self,
    //     cur: &'cur mut Cursor<Vec<u8>>,
    //     buffer: &'cur mut [u8],
    // ) -> impl Future<Output = Result<&mut [u8], MyError>> {
    //     async move {
    //         println!("cursor ref: {:?}", cur.get_ref());
    //         println!("buffer: {:?}", self.buffer[..].to_vec());
    //         // nothing remaining
    //         if cur.remaining() == 0 {
    //             // read more
    //             println!("reading more");
    //             // println!("len before: {}", self.buffer.len());
    //             // println!("cur len before: {}", cur.get_ref().len());
    //             let sz = self.stream.read_buf(&mut self.buffer).await?;
    //             println!("read size: {}", sz);
    //             // println!("len after: {}", self.buffer.len());
    //             println!("cursor position before: {}", cur.position());
    //             let before = cur.position();
    //             cur.write(&self.buffer[(before as usize)..]).await?;
    //             cur.set_position(before);
    //             println!("cursor position after: {}", cur.position());
    //             println!("cur len after {}", cur.get_ref().len());
    //             println!("buffer len after {}", self.buffer[..].to_vec().len());

    //             println!("cursor after: {:?}", cur.get_ref());
    //             println!("buffer after: {:?}", self.buffer[..].to_vec());
    //             // println!("wrote to cursor");
    //             if sz <= 0 {
    //                 println!("Error");
    //                 return Err(MyError::Incomplete);
    //             }
    //         }
    //         println!("reading exact");
    //         cur.read_exact(buffer).await?;
    //         println!("done reading");
    //         println!("value read: {}", buffer[0] as char);
    //         return Ok(buffer);
    //     }
    // }

    // fn my_get_line<'buf>(
    //     &'buf mut self,
    //     cur: &'buf mut Cursor<Vec<u8>>,
    // ) -> impl Future<Output = Result<&'buf [u8], MyError>> {
    //     async move {
    //         let start = cur.position() as usize;
    //         let mut curr_char = 0;
    //         let buffer = &mut [0; 1][..];
    //         while curr_char != b'\n' {
    //             // currently panics, can change as needed
    //             curr_char = self.read_byte(cur, buffer).await?[0];
    //         }
    //         let end_pos = (cur.position() - 2) as usize;

    //         return Ok(&cur.get_ref()[start..end_pos]);
    //     }
    // }

    // fn my_peek_u8(&mut self, src: &mut Cursor<Vec<u8>>) -> Result<u8, MyError> {
    //     if !src.has_remaining() {
    //         return Err(MyError::Incomplete);
    //     }
    //     Ok(src.chunk()[0])
    // }

    // fn my_skip(&mut self, src: &mut Cursor<Vec<u8>>, n: usize) -> Result<(), MyError> {
    //     if src.remaining() < n {
    //         return Err(MyError::Incomplete);
    //     }
    //     src.advance(n);
    //     Ok(())
    // }

    pub fn my_parse<'fut>(
        &'fut mut self,
        buf: &'fut mut Bufio,
    ) -> BoxFuture<'fut, Result<Frame, MyError>> {
        Box::pin(async move {
            match buf.get_byte(self).await? {
                b'+' => {
                    let line = buf.get_line(self).await?.to_vec();
                    let string = String::from_utf8(line)?;
                    return Ok(Frame::Simple(string));
                }
                b'-' => {
                    let line = buf.get_line(self).await?.to_vec();
                    let string = String::from_utf8(line)?;
                    return Ok(Frame::Simple(string));
                }
                b':' => {
                    use atoi::atoi;
                    let line = buf.get_line(self).await?;
                    let len =
                        atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format")?;
                    return Ok(Frame::Integer(len));
                }
                b'$' => {
                    if b'-' == buf.peek_byte(self).await? {
                        let line = buf.get_line(self).await?;
                        if line != b"-1" {
                            return Err("protcol error; invalid frame format".into());
                        }
                        return Ok(Frame::Null);
                    } else {
                        use atoi::atoi;
                        let line = buf.get_line(self).await?;
                        let len = atoi::<u64>(line)
                            .ok_or_else(|| "protcol error; invalid frame format")?;
                        // .try_into()?;
                        let n = (len + 2) as usize;
                        if buf.remaining() < n {
                            return Err(MyError::Incomplete);
                        }
                        let chunk = buf.get_slice(buf.bufpos, buf.bufpos + len as usize);
                        // let string = String::from_utf8(chunk.to_vec())?;
                        let data = Bytes::copy_from_slice(chunk);
                        // self.my_skip(buf, n)?;
                        buf.bufpos += n;
                        return Ok(Frame::Bulk(data));
                    }
                }
                b'*' => {
                    use atoi::atoi;
                    let line = buf.get_line(self).await?;
                    // let string = String::from_utf8(line.to_vec())?;
                    let len = atoi::<u64>(line)
                        .ok_or_else(|| "protcol error; invalid frame format")?
                        .try_into()?;
                    let mut out = Vec::with_capacity(len);
                    for _ in 0..len {
                        out.push(self.my_parse(buf).await?);
                    }
                    return Ok(Frame::Array(out));
                }
                actual => {
                    Err(format!("protocol error; invalid frame type byte `{}`", actual).into())
                }
            }
        })
    }

    pub async fn my_parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        if 0 == self.stream.read_buf(&mut self.buffer).await? {
            // The remote closed the connection. For this to be a clean
            // shutdown, there should be no data in the read buffer. If
            // there is, this means that the peer closed the socket while
            // sending a frame.
            if self.buffer.is_empty() {
                return Ok(None);
            } else {
                return Err("connection reset by peer".into());
            }
        }
        let v = self.buffer[..].to_vec();
        let l = v.len();
        let bufio = &mut Bufio {
            bufpos: 0,
            cur: Cursor::new(v),
        };

        // let cur = &mut Cursor::new(v);
        bufio.cur.set_position(l as u64);
        let frame = self.my_parse(bufio).await?;
        let len = bufio.bufpos;

        self.buffer.advance(len);
        Ok(Some(frame))
    }

    /// Tries to parse a frame from the buffer. If the buffer contains enough
    /// data, the frame is returned and the data removed from the buffer. If not
    /// enough data has been buffered yet, `Ok(None)` is returned. If the
    /// buffered data does not represent a valid frame, `Err` is returned.
    fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        use frame::Error::Incomplete;

        // Cursor is used to track the "current" location in the
        // buffer. Cursor also implements `Buf` from the `bytes` crate
        // which provides a number of helpful utilities for working
        // with bytes.
        let mut buf = Cursor::new(&self.buffer[..]);

        // The first step is to check if enough data has been buffered to parse
        // a single frame. This step is usually much faster than doing a full
        // parse of the frame, and allows us to skip allocating data structures
        // to hold the frame data unless we know the full frame has been
        // received.
        match Frame::check(&mut buf) {
            Ok(_) => {
                // The `check` function will have advanced the cursor until the
                // end of the frame. Since the cursor had position set to zero
                // before `Frame::check` was called, we obtain the length of the
                // frame by checking the cursor position.
                let len = buf.position() as usize;

                // Reset the position to zero before passing the cursor to
                // `Frame::parse`.
                buf.set_position(0);

                // Parse the frame from the buffer. This allocates the necessary
                // structures to represent the frame and returns the frame
                // value.
                //
                // If the encoded frame representation is invalid, an error is
                // returned. This should terminate the **current** connection
                // but should not impact any other connected client.
                let frame = Frame::parse(&mut buf)?;

                // Discard the parsed data from the read buffer.
                //
                // When `advance` is called on the read buffer, all of the data
                // up to `len` is discarded. The details of how this works is
                // left to `BytesMut`. This is often done by moving an internal
                // cursor, but it may be done by reallocating and copying data.
                self.buffer.advance(len);

                // Return the parsed frame to the caller.
                Ok(Some(frame))
            }
            // There is not enough data present in the read buffer to parse a
            // single frame. We must wait for more data to be received from the
            // socket. Reading from the socket will be done in the statement
            // after this `match`.
            //
            // We do not want to return `Err` from here as this "error" is an
            // expected runtime condition.
            Err(Incomplete) => Ok(None),
            // An error was encountered while parsing the frame. The connection
            // is now in an invalid state. Returning `Err` from here will result
            // in the connection being closed.
            Err(e) => Err(e.into()),
        }
    }

    /// Write a single `Frame` value to the underlying stream.
    ///
    /// The `Frame` value is written to the socket using the various `write_*`
    /// functions provided by `AsyncWrite`. Calling these functions directly on
    /// a `TcpStream` is **not** advised, as this will result in a large number of
    /// syscalls. However, it is fine to call these functions on a *buffered*
    /// write stream. The data will be written to the buffer. Once the buffer is
    /// full, it is flushed to the underlying socket.
    pub async fn write_frame(&mut self, frame: &Frame) -> io::Result<()> {
        // Arrays are encoded by encoding each entry. All other frame types are
        // considered literals. For now, mini-redis is not able to encode
        // recursive frame structures. See below for more details.
        match frame {
            Frame::Array(val) => {
                // Encode the frame type prefix. For an array, it is `*`.
                self.stream.write_u8(b'*').await?;

                // Encode the length of the array.
                self.write_decimal(val.len() as u64).await?;

                // Iterate and encode each entry in the array.
                for entry in &**val {
                    self.write_value(entry).await?;
                }
            }
            // The frame type is a literal. Encode the value directly.
            _ => self.write_value(frame).await?,
        }

        // Ensure the encoded frame is written to the socket. The calls above
        // are to the buffered stream and writes. Calling `flush` writes the
        // remaining contents of the buffer to the socket.
        self.stream.flush().await
    }

    /// Write a frame literal to the stream
    async fn write_value(&mut self, frame: &Frame) -> io::Result<()> {
        match frame {
            Frame::Simple(val) => {
                self.stream.write_u8(b'+').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Error(val) => {
                self.stream.write_u8(b'-').await?;
                self.stream.write_all(val.as_bytes()).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            Frame::Integer(val) => {
                self.stream.write_u8(b':').await?;
                self.write_decimal(*val).await?;
            }
            Frame::Null => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
            Frame::Bulk(val) => {
                let len = val.len();

                self.stream.write_u8(b'$').await?;
                self.write_decimal(len as u64).await?;
                self.stream.write_all(val).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            // Encoding an `Array` from within a value cannot be done using a
            // recursive strategy. In general, async fns do not support
            // recursion. Mini-redis has not needed to encode nested arrays yet,
            // so for now it is skipped.
            Frame::Array(_val) => unreachable!(),
        }

        Ok(())
    }

    /// Write a decimal frame to the stream
    async fn write_decimal(&mut self, val: u64) -> io::Result<()> {
        use std::io::Write;

        // Convert the value to a string
        let mut buf = [0u8; 20];
        let mut buf = Cursor::new(&mut buf[..]);
        write!(&mut buf, "{}", val)?;

        let pos = buf.position() as usize;
        self.stream.write_all(&buf.get_ref()[..pos]).await?;
        self.stream.write_all(b"\r\n").await?;

        Ok(())
    }
}
