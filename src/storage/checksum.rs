// Stub — will be filled in by Task 4 (Storage Module)

use aws_sdk_s3::types::ChecksumAlgorithm;

pub trait Checksum {
    fn new(full_object_checksum: bool) -> Self
    where
        Self: Sized;
    fn update(&mut self, data: &[u8]);
    fn finalize(&mut self) -> String;
    fn finalize_all(&mut self) -> String;
}

pub struct AdditionalChecksum {
    checksum: Box<dyn Checksum + Send + Sync + 'static>,
}
