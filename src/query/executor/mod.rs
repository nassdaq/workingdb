// Query execution engine with runtime optimization

use std::sync::Arc;

use crate::core::state::GlobalState;
use crate::query::parser::{ParsedQuery, Expr, Literal};
use crate::query::{QueryResult, Value};

/// Execution plan for a query
#[derive(Debug, Clone)]
pub struct ExecutionPlan {
    // Plan steps
    steps: Vec<ExecutionStep>,
    
    // Output schema
    output_columns: Vec<String>,
}

/// Step in execution plan
#[derive(Debug, Clone)]
enum ExecutionStep {
    // Table scan
    Scan {
        table: String,
        filter: Option<CompiledExpression>,
    },
    
    // Project specific columns
    Project {
        columns: Vec<String>,
    },
    
    // Insert data
    Insert {
        table: String,
        columns: Vec<String>,
        values: Vec<Vec<Literal>>,
    },
    
    // Update data
    Update {
        table: String,
        assignments: Vec<(String, Literal)>,
    },
    
    // Delete data
    Delete {
        table: String,
    },
    
    // Sort results
    Sort {
        columns: Vec<(String, bool)>, // (column, is_ascending)
    },
    
    // Limit results
    Limit {
        count: usize,
    },
}

/// Compiled expression for efficient evaluation
#[derive(Debug, Clone)]
enum CompiledExpression {
    // Constant
    Constant(bool),
    
    // Column equals value
    ColumnEqValue {
        column: String,
        value: Literal,
    },
    
    // Column comparison
    ColumnCompare {
        column: String,
        op: ComparisonOp,
        value: Literal,
    },
    
    // Logical AND of compiled expressions
    And {
        left: Box<CompiledExpression>,
        right: Box<CompiledExpression>,
    },
    
    // Logical OR of compiled expressions
    Or {
        left: Box<CompiledExpression>,
        right: Box<CompiledExpression>,
    },
    
    // More complex expression requiring runtime evaluation
    Complex {
        expr: Expr,
    },
}

/// Comparison operators
#[derive(Debug, Clone, Copy)]
enum ComparisonOp {
    Eq,
    Ne,
    Lt,
    Lte,
    Gt,
    Gte,
}

impl ExecutionPlan {
    /// Create execution plan from parsed query
    pub fn from_parsed_query(query: ParsedQuery) -> Self {
        match query {
            ParsedQuery::Select { columns, table, where_clause, limit } => {
                // Build steps for SELECT
                let mut steps = Vec::new();
                
                // Scan table with filter if WHERE present
                let filter = where_clause.map(|wc| compile_expression(&wc.expr));
                steps.push(ExecutionStep::Scan { 
                    table, 
                    filter,
                });
                
                // Project columns
                steps.push(ExecutionStep::Project { columns: columns.clone() });
                
                // Add LIMIT if present
                if let Some(limit_count) = limit {
                    steps.push(ExecutionStep::Limit { count: limit_count });
                }
                
                ExecutionPlan {
                    steps,
                    output_columns: columns,
                }
            }
            
            ParsedQuery::Insert { table, columns, values } => {
                // Build step for INSERT
                let steps = vec![
                    ExecutionStep::Insert { 
                        table, 
                        columns: columns.clone(),
                        values,
                    },
                ];
                
                ExecutionPlan {
                    steps,
                    output_columns: vec!["affected_rows".to_string()],
                }
            }
            
            ParsedQuery::Update { table, assignments, where_clause } => {
                // Build steps for UPDATE
                let mut steps = Vec::new();
                
                // Scan table with filter if WHERE present
                let filter = where_clause.map(|wc| compile_expression(&wc.expr));
                steps.push(ExecutionStep::Scan { 
                    table: table.clone(), 
                    filter,
                });
                
                // Update step
                steps.push(ExecutionStep::Update { 
                    table, 
                    assignments,
                });
                
                ExecutionPlan {
                    steps,
                    output_columns: vec!["affected_rows".to_string()],
                }
            }
            
            ParsedQuery::Delete { table, where_clause } => {
                // Build steps for DELETE
                let mut steps = Vec::new();
                
                // Scan table with filter if WHERE present
                let filter = where_clause.map(|wc| compile_expression(&wc.expr));
                steps.push(ExecutionStep::Scan { 
                    table: table.clone(), 
                    filter,
                });
                
                // Delete step
                steps.push(ExecutionStep::Delete { 
                    table,
                });
                
                ExecutionPlan {
                    steps,
                    output_columns: vec!["affected_rows".to_string()],
                }
            }
            
            ParsedQuery::CreateTable { table: _, columns: _ } => {
                // CRITICAL FIX: Changed variable names to _
                // In a real implementation, we'd handle DDL here
                // For now, just return empty plan
                ExecutionPlan {
                    steps: vec![],
                    output_columns: vec![],
                }
            }
        }
    }
}

/// Compile expression into optimized form
fn compile_expression(expr: &Expr) -> CompiledExpression {
    match expr {
        Expr::Literal(Literal::Integer(1)) => {
            // Optimize "1" (constant true)
            CompiledExpression::Constant(true)
        }
        
        Expr::Literal(Literal::Integer(0)) => {
            // Optimize "0" (constant false)
            CompiledExpression::Constant(false)
        }
        
        // CRITICAL FIX: Changed box patterns to match on refs
        Expr::BinaryOp { 
            left, 
            op: crate::query::parser::BinaryOperator::Equal,
            right,
        } if matches!(**left, Expr::Column(_)) && matches!(**right, Expr::Literal(_)) => {
            // Get column name and literal value
            let col = if let Expr::Column(name) = &**left {
                name.clone()
            } else {
                unreachable!()
            };
            
            let val = if let Expr::Literal(lit) = &**right {
                lit.clone()
            } else {
                unreachable!()
            };
            
            // Optimize "column = value"
            CompiledExpression::ColumnEqValue {
                column: col,
                value: val,
            }
        }
        
        // CRITICAL FIX: Changed box patterns to match on refs
        Expr::BinaryOp {
            left,
            op,
            right,
        } if matches!(**left, Expr::Column(_)) && matches!(**right, Expr::Literal(_)) => {
            // Get column name and literal value
            let col = if let Expr::Column(name) = &**left {
                name.clone()
            } else {
                unreachable!()
            };
            
            let val = if let Expr::Literal(lit) = &**right {
                lit.clone()
            } else {
                unreachable!()
            };
            
            // Optimize column comparisons
            let comparison_op = match op {
                crate::query::parser::BinaryOperator::Equal => ComparisonOp::Eq,
                crate::query::parser::BinaryOperator::NotEqual => ComparisonOp::Ne,
                crate::query::parser::BinaryOperator::LessThan => ComparisonOp::Lt,
                crate::query::parser::BinaryOperator::LessThanOrEqual => ComparisonOp::Lte,
                crate::query::parser::BinaryOperator::GreaterThan => ComparisonOp::Gt,
                crate::query::parser::BinaryOperator::GreaterThanOrEqual => ComparisonOp::Gte,
                _ => return CompiledExpression::Complex { expr: expr.clone() },
            };
            
            CompiledExpression::ColumnCompare {
                column: col,
                op: comparison_op,
                value: val,
            }
        }
        
        Expr::BinaryOp {
            left,
            op: crate::query::parser::BinaryOperator::And,
            right,
        } => {
            // Optimize AND expressions
            let left_compiled = compile_expression(left);
            let right_compiled = compile_expression(right);
            
            CompiledExpression::And {
                left: Box::new(left_compiled),
                right: Box::new(right_compiled),
            }
        }
        
        Expr::BinaryOp {
            left,
            op: crate::query::parser::BinaryOperator::Or,
            right,
        } => {
            // Optimize OR expressions
            let left_compiled = compile_expression(left);
            let right_compiled = compile_expression(right);
            
            CompiledExpression::Or {
                left: Box::new(left_compiled),
                right: Box::new(right_compiled),
            }
        }
        
        // For more complex expressions, just wrap them for runtime evaluation
        _ => CompiledExpression::Complex { expr: expr.clone() },
    }
}

/// Execute a query plan
pub fn execute_plan(
    plan: ExecutionPlan,
    state: Arc<GlobalState>
) -> Result<QueryResult, String> {
    // In a real implementation, this would execute the plan steps
    // For now, just return placeholder result
    
    // For SELECT queries
    if plan.steps.iter().any(|step| matches!(step, ExecutionStep::Project { .. })) {
        let columns = plan.output_columns;
        
        // Create dummy row for demo
        let row = columns.iter()
            .map(|col| Value::Text(format!("Value for {}", col)))
            .collect();
            
        let rows = vec![row];
        
        // CRITICAL FIX: Fixed moved value error by cloning rows before use
        Ok(QueryResult::Rows {
            columns,
            rows: rows.clone(),
            affected_rows: rows.len(),
        })
    } else {
        // For modification queries
        Ok(QueryResult::Modified {
            affected_rows: 1, // Placeholder
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::parser;
    
    #[test]
    fn test_execution_plan() {
        // Create a simple SELECT query
        let query = parser::ParsedQuery::Select {
            columns: vec!["id".to_string(), "name".to_string()],
            table: "users".to_string(),
            where_clause: None,
            limit: None,
        };
        
        // Create execution plan
        let plan = ExecutionPlan::from_parsed_query(query);
        
        // Verify plan structure
        assert_eq!(plan.output_columns, vec!["id".to_string(), "name".to_string()]);
        assert_eq!(plan.steps.len(), 2);
        
        // Check steps
        match &plan.steps[0] {
            ExecutionStep::Scan { table, filter } => {
                assert_eq!(table, "users");
                assert!(filter.is_none());
            }
            _ => panic!("Expected Scan step"),
        }
        
        match &plan.steps[1] {
            ExecutionStep::Project { columns } => {
                assert_eq!(columns, &vec!["id".to_string(), "name".to_string()]);
            }
            _ => panic!("Expected Project step"),
        }
    }
}