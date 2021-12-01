use crate::frame::Frame;

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

/**
 * A FIFO-style buffer with a read end and write end.
 * The read end is managed by `bufpos`, through which a frame is read and parsed.
 * The write end is managed by a `cur`, a Cursor, which simply appends bytes to its
 * underlying Vec<u8> as they are received by the client.
 * */
#[derive(Debug)]
pub struct Bufio {
    bufpos: usize,
    cur: Cursor<Vec<u8>>,
}

/**
 * Function implementations for Bufio that handle the reading, writing, and parsing of frames.
 * */
impl Bufio {
    /**
     * Reads more bytes from the stream and writes them to the buffer, which
     * are used later for parsing frames.
     * */
    async fn read_more(&mut self, connection: &mut Connection) -> Result<usize, MyError> {
        // Read bytes into the buffer of the Connection
        let buffer = &mut connection.buffer;
        let sz = connection.stream.read_buf(buffer).await?;

        // No bytes are read
        if sz < 1 {
            return Err(MyError::Incomplete);
        }
        // Write the bytes into Bufio's underlyikng Cursor
        self.cur.write(&buffer[self.bufpos..]).await?;
        Ok(sz)
    }

    /**
     * Attemtps to read a byte from Bufio at the index of `bufpos`. If no bytes are
     * available to read, more bytes are read into the cursor.
     * */
    async fn get_byte<'fut>(&mut self, connection: &'fut mut Connection) -> Result<u8, MyError> {
        // Read more bytes if the read end is at the same position as the write end
        // of the buffer
        if self.bufpos >= self.cur.position() as usize {
            let _read_sz = self.read_more(connection).await?;
        }
        // Increment the read end and return the byte
        self.bufpos += 1;
        Ok(self.cur.get_ref()[self.bufpos - 1])
    }

    /**
     * Attempts to read a byte from Bufio at the index of `bufpos` without "consuming" it.
     * Keeps the read end at the same position and just returns the byte. If no bytes are
     * available to read, more bytes are read into the cursor.
     * */
    async fn peek_byte<'a>(&mut self, connection: &'a mut Connection) -> Result<u8, MyError> {
        // Read more bytes if the read end is at the same position as the write end
        // of the buffer
        if self.bufpos >= self.cur.position() as usize {
            let _read_sz = self.read_more(connection).await?;
        }
        // return the read byte
        Ok(self.cur.get_ref()[self.bufpos])
    }

    /**
     * Returns a slice of the buffer starting at `start` and including all bytes up to but
     * not including `end`.
     * */
    fn get_slice(&mut self, start: usize, end: usize) -> &[u8] {
        &self.cur.get_ref()[start..end]
    }

    /**
     * Returns a chunk of bytes representing a frame (such that a new line character is reached).
     * Asynchronously calls get_byte one at a time, mainting its position in the buffer, until a
     * complete frame is reached. This approach should theoretically save time compared to the
     * approach of reading a complete frame at a time, which also assumes that complete frames
     * exist in the buffer.
     * */
    fn get_line<'buf>(
        &'buf mut self,
        connection: &'buf mut Connection,
    ) -> impl Future<Output = Result<&'buf [u8], MyError>> {
        async move {
            let start = self.bufpos;
            let mut curr_char = 0;
            // loop through the buffer until a new line is reached, representing the end of a frame.
            while curr_char != b'\n' {
                curr_char = self.get_byte(connection).await?;
            }
            // return all the bytes until the carriage return of the frame.
            Ok(self.get_slice(start, self.bufpos - 2))
        }
    }
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

    /**
     * Parses the request sent by the client and returns its corresponding frame.
     **/
    pub fn parse_line<'fut>(
        &'fut mut self,
        buf: &'fut mut Bufio,
    ) -> BoxFuture<'fut, Result<Frame, MyError>> {
        Box::pin(async move {
            // Determine the type of request
            match buf.get_byte(self).await? {
                b'+' => {
                    // Simple String, get line and return as a Frame.
                    let line = buf.get_line(self).await?.to_vec();
                    let string = String::from_utf8(line)?;
                    return Ok(Frame::Simple(string));
                }
                b'-' => {
                    // Error, get line and return as a Frame.
                    let line = buf.get_line(self).await?.to_vec();
                    let string = String::from_utf8(line)?;
                    return Ok(Frame::Simple(string));
                }
                b':' => {
                    // Integer, get line, convert to an Integer, and
                    // return as a Frame.
                    use atoi::atoi;
                    let line = buf.get_line(self).await?;
                    let len =
                        atoi::<u64>(line).ok_or_else(|| "protocol error; invalid frame format")?;
                    return Ok(Frame::Integer(len));
                }
                b'$' => {
                    // Bulk String
                    // First check if the first byte is an error
                    if b'-' == buf.peek_byte(self).await? {
                        let line = buf.get_line(self).await?;
                        if line != b"-1" {
                            return Err("protcol error; invalid frame format".into());
                        }
                        return Ok(Frame::Null);
                    } else {
                        // Return the line as a Bulk String Frame.
                        buf.get_line(self).await?;
                        let chunk = buf.get_line(self).await?;
                        let data = Bytes::copy_from_slice(chunk);
                        return Ok(Frame::Bulk(data));
                    }
                }
                b'*' => {
                    // Array
                    // Determine the length of the array from the fist line
                    use atoi::atoi;
                    let line = buf.get_line(self).await?;
                    let len = atoi::<u64>(line)
                        .ok_or_else(|| "protcol error; invalid frame format")?
                        .try_into()?;
                    let mut out = Vec::with_capacity(len);

                    // Loop through the length of the array and recursively parse
                    // a frame the specified number of times, storing them in a
                    // Vector.
                    for _ in 0..len {
                        out.push(self.parse_line(buf).await?);
                    }

                    // Return the Vector as a Frame.
                    return Ok(Frame::Array(out));
                }
                // Not a valid request
                actual => {
                    Err(format!("protocol error; invalid frame type byte `{}`", actual).into())
                }
            }
        })
    }

    /**
     * Parses a frame from the bytes in the stream, using an asynchronous,
     * one pass approach, through which complete frames are not necessary, since a
     * read end and write end is used.
     **/
    pub async fn parse_frame(&mut self) -> crate::Result<Option<Frame>> {
        // Read bytes from the stream
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

        // Create a Bufio struct from the bytes read, storing the bytes as a Vector
        // and the position at which to read byte by byte.
        let v = self.buffer[..].to_vec();
        let l = v.len();
        let bufio = &mut Bufio {
            bufpos: 0,
            cur: Cursor::new(v),
        };
        // set the position at the end of the buffer to represent the write end
        // and parse the frame.
        bufio.cur.set_position(l as u64);
        let frame = self.parse_line(bufio).await?;

        // Advance the connection's buffer by the amount of bytes read and
        // return the frame.
        let len = bufio.bufpos;
        self.buffer.advance(len);
        Ok(Some(frame))
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
