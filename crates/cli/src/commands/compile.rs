use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::sync::Arc;
use ff_core::{
    config::loader::read_config,
    parser::parse_models
};
use common::error::FFError;
use common::types::{Identifier, Materialize};
use ff_core::dag::ModelDag;
use ff_core::macros::build_jinja_env;


pub fn compile(compile_path: String) -> Result<(), FFError> {

    }

    Ok(())
}