// src/query/mod.rs - QUERY ROUTER
// Entry point for query processing

pub mod parser;
pub mod executor;

use std::sync::Arc;
use crate::core::state::GlobalState;

/// SQL query processor
pub struct QueryProcessor {
    // Database state
    state: Arc<GlobalState>,
}

impl QueryProcessor {
    /// Create new query processor
    pub fn new(state: Arc<GlobalState>) -> Self {
        Self { state }
    }
    
    /// Execute SQL query
    pub fn execute(&self, query: &str) -> Result<QueryResult, QueryError> {
        // Parse query
        let parsed = self.parse(query)?;
        
        // Plan execution
        let plan = self.plan(parsed)?;
        
        // Execute plan
        self.execute_plan(plan)
    }
    
    /// Parse SQL query into abstract syntax tree
    fn parse(&self, query: &str) -> Result<parser::ParsedQuery, QueryError> {
        parser::parse_query(query).map_err(|e| QueryError::ParseError(e))
    }
    
    /// Create execution plan for query
    fn plan(&self, query: parser::ParsedQuery) -> Result<executor::ExecutionPlan, QueryError> {
        // In a full implementation, this would involve query optimization
        // For now, just create a basic plan
        Ok(executor::ExecutionPlan::from_parsed_query(query))
    }
    
    /// Execute plan and produce result
    fn execute_plan(&self, plan: executor::ExecutionPlan) -> Result<QueryResult, QueryError> {
        // Execute the plan
        executor::execute_plan(plan, self.state.clone())
            .map_err(|e| QueryError::ExecutionError(e))
    }
}

/// Query result types
pub enum QueryResult {
    // SELECT result
    Rows {
        columns: Vec<String>,
        rows: Vec<Vec<Value>>,
        affected_rows: usize,
    },
    
    // INSERT/UPDATE/DELETE result
    Modified {
        affected_rows: usize,
    },
    
    // DDL result (CREATE/ALTER/DROP)
    Schema,
}

/// Value types for query results
#[derive(Clone, Debug)]
pub enum Value {
    Null,
    Integer(i64),
    Float(f64),
    Text(String),
    Binary(Vec<u8>),
}

/// Query error types
#[derive(Debug)]
pub enum QueryError {
    ParseError(String),
    PlanningError(String),
    ExecutionError(String),
    StorageError(String),
}

impl std::fmt::Display for QueryError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            QueryError::ParseError(msg) => write!(f, "Parse error: {}", msg),
            QueryError::PlanningError(msg) => write!(f, "Planning error: {}", msg),
            QueryError::ExecutionError(msg) => write!(f, "Execution error: {}", msg),
            QueryError::StorageError(msg) => write!(f, "Storage error: {}", msg),
        }
    }
}

impl std::error::Error for QueryError {}

