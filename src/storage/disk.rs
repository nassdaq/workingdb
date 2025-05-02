use std::fs::{File,OpenOptions};
use std::io::{Read,Seek,SeekFrom,Write};
use std::os::unix::fs::OpenOptionsExt;
use std::path::Path;
use std::os::fd::AsRawFd;
use nix::fcntl;
use nix::sys::stat::Mode;
use nix::libc::{O_DIRECT,O_DSYNC};


pub struct NvmeAccess {
  path: String,

  block_size: usize,

  file: Option<File>,

}

impl NvmeAccess {
  pub fn new<P: AsRef<Path>>(device_path: P) -> Result<Self,std::io::Error>{

    let path_str = device_path.as_ref()
    .to_str()
    .ok_or_else(|| std::io::Error::new(
      std::io::ErrorKind::InvalidInput,
      "Invalid device path"
    ))?
    .to_string();

  letblock_size = 4096;
  
  Ok(Self { 
    path: path_str,
    block_size,
    file: None,
  })
  }

  pub fn open(&mut self) -> Result<(),std::io::Error>{

    let file = OpenOptions::new()
      .read(true)
      .write(true)
      .custom_flags(O_DIRECT|O_DSYNC)
      .open(&self.path)?;
    self.file = Some(file);
    Ok(())


  }
  pub fn close(&mut self){
    self.file =None;
  }

  
}



