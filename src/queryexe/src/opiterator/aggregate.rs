use super::OpIterator;
use crate::Managers;
use common::bytecode_expr::ByteCodeExpr;
use common::datatypes::f_decimal;
use common::{AggOp, CrustyError, Field, TableSchema, Tuple};
// use std::cmp::{max, min};
use std::collections::HashMap;

/// Aggregate operator. (You can add any other fields that you think are neccessary)
pub struct Aggregate {
    // Static objects (No need to reset on close)
    #[allow(dead_code)]
    managers: &'static Managers,

    // Parameters (No need to reset on close)
    /// Output schema of the form [groupby_field attributes ..., agg_field attributes ...]).
    schema: TableSchema,
    /// Group by fields
    groupby_expr: Vec<ByteCodeExpr>,
    /// Aggregated fields.
    agg_expr: Vec<ByteCodeExpr>,
    /// Aggregation operations.
    ops: Vec<AggOp>,
    /// Child operator to get the data from.
    child: Box<dyn OpIterator>,
    /// If true, then the operator will be rewinded in the future.
    will_rewind: bool,

    // States (Need to reset on close)
    hashmap: HashMap<Vec<Field>, Vec<Vec<Field>>>,
    open: bool,
    current_index: Option<usize>,
}

impl Aggregate {
    pub fn new(
        managers: &'static Managers,
        groupby_expr: Vec<ByteCodeExpr>,
        agg_expr: Vec<ByteCodeExpr>,
        ops: Vec<AggOp>,
        schema: TableSchema,
        child: Box<dyn OpIterator>,
    ) -> Self {
        assert!(ops.len() == agg_expr.len());
        Self {
            managers,
            schema,
            groupby_expr,
            agg_expr,
            ops,
            child,
            will_rewind: true,
            hashmap: HashMap::new(),
            open: false,
            current_index: None,
        }
    }

    pub fn merge_tuple_into_group(&mut self, tuple: &Tuple) {
        let mut groupkey: Vec<Field> = Vec::new();
        for expr in &self.groupby_expr {
            let group = expr.eval(tuple);
            groupkey.push(group);
        }
        let hashmap = &mut self.hashmap;

        let firsttime = !hashmap.contains_key(&groupkey);
        if firsttime {
            let temp_vec = Vec::new();
            hashmap.insert(groupkey.clone(), temp_vec);
        }

        let vec = hashmap.get_mut(&groupkey).unwrap();

        for (idx, expr) in self.agg_expr.iter().enumerate() {
            let tuple_val = expr.eval(tuple);
            if firsttime {
                let exprresult_vec = Vec::new();
                vec.push(exprresult_vec);
            }
            match self.ops[idx] {
                // check what kind of aggregate is requested
                AggOp::Min => {
                    if vec[idx].is_empty() {
                        vec[idx].push(tuple_val);
                    } else if tuple_val < vec[idx][0] {
                        vec[idx][0] = tuple_val;
                    }
                }
                AggOp::Max => {
                    if vec[idx].is_empty() {
                        vec[idx].push(tuple_val);
                    } else if tuple_val > vec[idx][0] {
                        vec[idx][0] = tuple_val;
                    }
                }
                AggOp::Count => {
                    if vec[idx].is_empty() {
                        vec[idx].push(common::Field::Int(0));
                    }
                    match vec[idx][0] {
                        Field::Int(count) => {
                            vec[idx][0] = common::Field::Int(count + 1);
                            // break;
                        }
                        _ => {
                            panic!("Count did not evaluate to an Int");
                        }
                    }
                }
                AggOp::Sum => {
                    let val = tuple_val;
                    if vec[idx].is_empty() {
                        vec[idx].push(val);
                    } else {
                        let cur_sum = &vec[idx][0];
                        vec[idx][0] = (val + cur_sum.clone()).expect("err");
                    }
                }
                AggOp::Avg => {
                    if vec[idx].is_empty() {
                        vec[idx].push(common::Field::Int(1));
                        vec[idx].push(tuple_val);
                    } else {
                        match vec[idx][0] {
                            Field::Int(count) => {
                                vec[idx][0] = common::Field::Int(count + 1);
                            }
                            _ => {
                                panic!("Count did not evaluate to an Int");
                            }
                        }
                        let val = tuple_val;
                        let cur_sum = &vec[idx][1];
                        vec[idx][1] = (val + cur_sum.clone()).expect("err");
                    }
                }
            }
        }
    }
}

impl OpIterator for Aggregate {
    fn configure(&mut self, will_rewind: bool) {
        self.will_rewind = will_rewind;
        self.child.configure(false); // child of a aggregate will never be rewinded
                                     // because aggregate will buffer all the tuples from the child
    }

    fn open(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            self.child.open()?;
            self.open = true;

            // populate hashmap here
            while let Some(cur_tuple) = &self.child.next()? {
                self.merge_tuple_into_group(cur_tuple);
            }
            self.current_index = Some(0);
        }
        Ok(())
    }

    fn next(&mut self) -> Result<Option<Tuple>, CrustyError> {
        if !self.open {
            panic!("Iterator is not open");
        }
        let hashmap = &mut self.hashmap;
        let mut iter = hashmap.iter();
        let hash_idx = self.current_index.unwrap();
        if let Some((groupkey, val)) = iter.nth(hash_idx) {
            // create vector with final calculated results
            let mut result_vec: Vec<Field> = Vec::new();
            for colval in groupkey {
                result_vec.push(colval.clone());
            }
            for agg_result in val {
                if agg_result.len() > 1 {
                    // then the aggregate was AVG
                    let count = &agg_result[0];
                    let proper_count: Field = match count {
                        Field::Int(c) => f_decimal(*c as f64),
                        _ => {
                            panic!("count is not an Int and should be");
                        }
                    };
                    let sum = &agg_result[1];
                    let proper_sum: Field = match sum {
                        Field::Int(s) => f_decimal(*s as f64),
                        _ => {
                            panic!("count is not an Int and should be");
                        }
                    };
                    let avg = (proper_sum / proper_count).expect("error");
                    result_vec.push(avg.clone());
                } else if !agg_result.is_empty() {
                    let result = &agg_result[0];
                    result_vec.push(result.clone());
                }
            }
            // create a tuple from the result vector and return
            let t = Tuple::new(result_vec);
            self.current_index = Some(hash_idx + 1);
            return Ok(Some(t));
        }

        Ok(None)
    }

    fn close(&mut self) -> Result<(), CrustyError> {
        self.child.close()?;
        self.open = false;
        self.hashmap.clear();
        self.current_index = None;
        Ok(())
    }

    fn rewind(&mut self) -> Result<(), CrustyError> {
        if !self.open {
            panic!("Can't rewind if not open");
        }
        self.current_index = Some(0);
        Ok(())
    }

    fn get_schema(&self) -> &TableSchema {
        &self.schema
    }
}

#[cfg(test)]
mod test {
    use super::super::TupleIterator;
    use super::*;
    use crate::testutil::{execute_iter, new_test_managers, TestTuples};
    use common::{
        bytecode_expr::colidx_expr,
        datatypes::{f_int, f_str},
    };

    fn get_iter(
        groupby_expr: Vec<ByteCodeExpr>,
        agg_expr: Vec<ByteCodeExpr>,
        ops: Vec<AggOp>,
    ) -> Box<dyn OpIterator> {
        let setup = TestTuples::new("");
        let managers = new_test_managers();
        let dummy_schema = TableSchema::new(vec![]);
        let mut iter = Box::new(Aggregate::new(
            managers,
            groupby_expr,
            agg_expr,
            ops,
            dummy_schema,
            Box::new(TupleIterator::new(
                setup.tuples.clone(),
                setup.schema.clone(),
            )),
        ));
        iter.configure(false);
        iter
    }

    fn run_aggregate(
        groupby_expr: Vec<ByteCodeExpr>,
        agg_expr: Vec<ByteCodeExpr>,
        ops: Vec<AggOp>,
    ) -> Vec<Tuple> {
        let mut iter = get_iter(groupby_expr, agg_expr, ops);
        execute_iter(&mut *iter, true).unwrap()
    }

    mod aggregation_test {
        use super::*;

        #[test]
        fn test_empty_group() {
            let group_by = vec![];
            let agg = vec![colidx_expr(0), colidx_expr(1), colidx_expr(2)];
            let ops = vec![AggOp::Count, AggOp::Max, AggOp::Avg];
            let t = run_aggregate(group_by, agg, ops);
            assert_eq!(t.len(), 1);
            assert_eq!(t[0], Tuple::new(vec![f_int(6), f_int(2), f_decimal(4.0)]));
        }

        #[test]
        fn test_empty_aggregation() {
            let group_by = vec![colidx_expr(2)];
            let agg = vec![];
            let ops = vec![];
            let t = run_aggregate(group_by, agg, ops);
            assert_eq!(t.len(), 3);
            assert_eq!(t[0], Tuple::new(vec![f_int(3)]));
            assert_eq!(t[1], Tuple::new(vec![f_int(4)]));
            assert_eq!(t[2], Tuple::new(vec![f_int(5)]));
        }

        #[test]
        fn test_count() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G
            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(0)];
            let ops = vec![AggOp::Count];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 2
            // 1 4 1
            // 2 4 1
            // 2 5 2
            assert_eq!(t.len(), 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_int(2)]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_int(1)]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_int(1)]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_int(2)]));
        }

        #[test]
        fn test_sum() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(0)];
            let ops = vec![AggOp::Sum];
            let tuples = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 3
            // 1 4 3
            // 2 4 4
            // 2 5 11
            assert_eq!(tuples.len(), 4);
            assert_eq!(tuples[0], Tuple::new(vec![f_int(1), f_int(3), f_int(3)]));
            assert_eq!(tuples[1], Tuple::new(vec![f_int(1), f_int(4), f_int(3)]));
            assert_eq!(tuples[2], Tuple::new(vec![f_int(2), f_int(4), f_int(4)]));
            assert_eq!(tuples[3], Tuple::new(vec![f_int(2), f_int(5), f_int(11)]));
        }

        #[test]
        fn test_max() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(3)];
            let ops = vec![AggOp::Max];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 G
            // 1 4 A
            // 2 4 G
            // 2 5 G
            assert_eq!(t.len(), 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_str("G")]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_str("A")]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_str("G")]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_str("G")]));
        }

        #[test]
        fn test_min() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G

            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(3)];
            let ops = vec![AggOp::Min];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 E
            // 1 4 A
            // 2 4 G
            // 2 5 G
            assert!(t.len() == 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_str("E")]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_str("A")]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_str("G")]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_str("G")]));
        }

        #[test]
        fn test_avg() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G
            let group_by = vec![colidx_expr(1), colidx_expr(2)];
            let agg = vec![colidx_expr(0)];
            let ops = vec![AggOp::Avg];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // 1 3 1.5
            // 1 4 3.0
            // 2 4 4.0
            // 2 5 5.5
            assert_eq!(t.len(), 4);
            assert_eq!(t[0], Tuple::new(vec![f_int(1), f_int(3), f_decimal(1.5)]));
            assert_eq!(t[1], Tuple::new(vec![f_int(1), f_int(4), f_decimal(3.0)]));
            assert_eq!(t[2], Tuple::new(vec![f_int(2), f_int(4), f_decimal(4.0)]));
            assert_eq!(t[3], Tuple::new(vec![f_int(2), f_int(5), f_decimal(5.5)]));
        }

        #[test]
        fn test_multi_column_aggregation() {
            // Input:
            // 1 1 3 E
            // 2 1 3 G
            // 3 1 4 A
            // 4 2 4 G
            // 5 2 5 G
            // 6 2 5 G
            let group_by = vec![colidx_expr(3)];
            let agg = vec![colidx_expr(0), colidx_expr(1), colidx_expr(2)];
            let ops = vec![AggOp::Count, AggOp::Max, AggOp::Avg];
            let t = run_aggregate(group_by, agg, ops);
            // Output:
            // A 1 1 4.0
            // E 1 1 3.0
            // G 4 2 4.25
            assert_eq!(t.len(), 3);
            assert_eq!(
                t[0],
                Tuple::new(vec![f_str("A"), f_int(1), f_int(1), f_decimal(4.0)])
            );
            assert_eq!(
                t[1],
                Tuple::new(vec![f_str("E"), f_int(1), f_int(1), f_decimal(3.0)])
            );
            assert_eq!(
                t[2],
                Tuple::new(vec![f_str("G"), f_int(4), f_int(2), f_decimal(4.25)])
            );
        }

        #[test]
        #[should_panic]
        fn test_merge_tuples_not_int() {
            let group_by = vec![];
            let agg = vec![colidx_expr(3)];
            let ops = vec![AggOp::Avg];
            let _ = run_aggregate(group_by, agg, ops);
        }
    }

    mod opiterator_test {
        use super::*;

        #[test]
        #[should_panic]
        fn test_next_not_open() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            let _ = iter.next();
        }

        #[test]
        #[should_panic]
        fn test_rewind_not_open() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            let _ = iter.rewind();
        }

        #[test]
        fn test_open() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            iter.open().unwrap();
        }

        #[test]
        fn test_close() {
            let mut iter = get_iter(vec![], vec![], vec![]);
            iter.open().unwrap();
            iter.close().unwrap();
        }

        #[test]
        fn test_rewind() {
            let mut iter = get_iter(vec![colidx_expr(2)], vec![colidx_expr(0)], vec![AggOp::Max]);
            iter.configure(true); // if we will rewind in the future, then we set will_rewind to true
            let t_before = execute_iter(&mut *iter, true).unwrap();
            iter.rewind().unwrap();
            let t_after = execute_iter(&mut *iter, true).unwrap();
            assert_eq!(t_before, t_after);
        }
    }
}
