use std::{
    convert::TryFrom,
    fmt::{
        self,
        Display,
        Formatter,
    },
    marker::PhantomData,
    str::FromStr,
};

use serde::{
    Deserialize,
    Serialize,
};

pub trait SizedInt {
    const MAX: usize;
    const MIN: usize;
    const NAME: &'static str;
}

#[derive(Clone, PartialEq, Eq)]
pub struct SizeError<T: SizedInt>(PhantomData<T>);
impl<T: SizedInt> SizeError<T> {
    fn new() -> Self {
        Self(PhantomData)
    }
}
impl<T> std::fmt::Display for SizeError<T>
where
    T: SizedInt,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "{} must be greater than {} and less than {}",
            T::NAME,
            T::MIN,
            T::MAX,
        )
    }
}
impl<T> std::fmt::Debug for SizeError<T>
where
    T: SizedInt,
{
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "SizeError({})", T::NAME)
    }
}
impl<T> std::error::Error for SizeError<T> where T: SizedInt {}

macro_rules! sized_uint {
    ($type:ident, $closest_int:ident, $max:expr, $min:expr) => {
        #[derive(Copy,Clone,Debug,PartialEq,Eq,Hash,PartialOrd,Ord,Serialize,Deserialize)]
        #[serde(try_from = "usize")]
        #[serde(into = "usize")]
        pub struct $type($closest_int);

        impl SizedInt for $type {
            const MAX: usize = $max;
            const MIN: usize = $min;
            const NAME: &'static str = stringify!{$type};
        }
        impl PartialEq<$closest_int> for $type {
            fn eq(&self, other: &$closest_int) -> bool {
                PartialEq::eq(&self.0, other)
            }
        }
        impl FromStr for $type {
            type Err = <Self as TryFrom<$closest_int>>::Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                match <$closest_int as std::str::FromStr>::from_str(s) {
                    Ok(i) => Ok(std::convert::TryInto::try_into(i)?),
                    Err(_) => Err(SizeError::new()),
                }
            }
        }
        impl Display for $type {
            fn fmt(&self, f: &mut Formatter) -> fmt::Result {
                fmt::Display::fmt(&self.0, f)
            }
        }
        sized_uint!(
            @Into<
                u8,
                u16,
                u32,
                u64,
                usize,
                i8,
                i16,
                i32,
                i64,
                isize
            >
            for $type
        );
        sized_uint!(
            @TryFrom<
                u8,
                u16,
                u32,
                usize,
                i8,
                i16,
                i32,
                isize
            >
            for $type as $closest_int
        );
    };
    (@Into<$($int:path),*> for $type:ident) => {
        $(
            impl From<$type> for $int {
                fn from(i: $type) -> Self {
                    i.0 as $int
                }
            }
        )*
    };
    (@TryFrom<$($int:path),*> for $type:ident as $closest_int:ident) => {
        $(
            impl TryFrom<$int> for $type {
                type Error = SizeError<$type>;
                fn try_from(i: $int) -> Result<Self, Self::Error> {
                    match i as usize {
                        i if i > $type::MAX  => Err(SizeError::new()),
                        i if i < $type::MIN => Err(SizeError::new()),
                        _ => Ok(Self(i as $closest_int)),
                    }
                }
            }
        )*
    };
}

sized_uint!(ChunkSize, u8, 32, 10);
impl Default for ChunkSize {
    fn default() -> Self {
        ChunkSize(20)
    }
}
sized_uint!(DataChunkCount, u8, 256, 1);
impl Default for DataChunkCount {
    fn default() -> Self {
        DataChunkCount(3)
    }
}
sized_uint!(ParityChunkCount, u8, 256, 0);
impl Default for ParityChunkCount {
    fn default() -> Self {
        ParityChunkCount(2)
    }
}
sized_uint!(ChunkCount, u8, 256, 0);
impl ChunkCount {
    pub fn none() -> ChunkCount {
        ChunkCount(0)
    }
}
