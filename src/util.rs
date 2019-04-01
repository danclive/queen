use std::mem;

#[inline]
pub fn get_length(buf: &[u8], start: usize) -> usize {
    (
        i32::from(buf[start + 0]) |
        i32::from(buf[start + 1]) << 8 |
        i32::from(buf[start + 2]) << 16 |
        i32::from(buf[start + 3]) << 24
    ) as usize
}

pub fn split_message(buffer: &mut Vec<u8>, buf: &[u8]) -> Vec<Vec<u8>>{
    let mut messages = Vec::new();

    let size = buf.len();
    let buffer_len = buffer.len();
    let buffer_is_empty = buffer_len == 0;

    if buffer_is_empty && size < 4 {
        buffer.extend_from_slice(&buf[0..size]);
        return messages
    }

    if buffer_is_empty {
        let message_len = get_length(&buf, 0);

        if size == message_len {
            let data = buf.to_vec();
            messages.push(data);
        } else if size > message_len {
            let mut start = 0;
            let mut end = message_len;
                
            loop {
                let data = buf[start..end].to_vec();
                messages.push(data);

                if size - end < 4 {
                    buffer.extend_from_slice(&buf[end..size]);
                    return messages
                }

                let message_len = get_length(&buf, end);
                if message_len > size - end {
                    buffer.extend_from_slice(&buf[end..size]);
                    return messages;
                } else {
                    start = end;
                    end = end + message_len;
                }
            }
        } else {
            // size < message length
            buffer.extend_from_slice(&buf[0..size]);
        }
    } else {
        let message_len = get_length(buffer, 0) - buffer_len;

        if size == message_len {
            buffer.extend_from_slice(&buf);
            let data = mem::replace(buffer, Vec::with_capacity(4 * 1024));
            messages.push(data);
        } else if size > message_len {
            let mut start = 0;
            let mut end = message_len;
            let mut first = true;

            loop {
                if first {
                    buffer.extend_from_slice(&buf[start..end]);
                    let data = mem::replace(buffer, Vec::with_capacity(4 * 1024));
                    messages.push(data);
                    first = false;
                } else {
                    let data = buf[start..end].to_vec();
                    messages.push(data);
                }

                if size - end < 4 {
                    buffer.extend_from_slice(&buf[end..size]);
                    return messages
                }

                let message_len = get_length(&buf, end);
                if message_len > size - end {
                    buffer.extend_from_slice(&buf[end..size]);
                    return messages
                } else {
                    start = end;
                    end = end + message_len;
                }
            }
        } else {
            buffer.extend_from_slice(&buf);
        }
    }

    messages
}
