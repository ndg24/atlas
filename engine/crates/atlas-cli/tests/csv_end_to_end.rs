use std::process::Command;

fn run_query(sql: &str) -> String {
    let output = Command::new(env!("CARGO_BIN_EXE_atlas-cli"))
        .args([
            "query",
            "--file",
            concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/patients.csv"),
            "--sql",
            sql,
        ])
        .output()
        .expect("failed to run atlas-cli");
    assert!(
        output.status.success(),
        "atlas-cli exited with {:?}, stderr: {}",
        output.status,
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("stdout was not valid UTF-8")
}

#[test]
fn where_and_order_by_and_limit() {
    let out = run_query(
        "SELECT patient_id, diagnosis, age FROM t WHERE age > 50 AND age < 70 ORDER BY age LIMIT 5",
    );
    let expected = "\
+------------+--------------+-----+
| patient_id | diagnosis    | age |
+------------+--------------+-----+
| 14         | asthma       | 51  |
| 42         | diabetes     | 52  |
| 5          | flu          | 53  |
| 33         | hypertension | 54  |
| 24         | asthma       | 56  |
+------------+--------------+-----+
";
    assert_eq!(out, expected);
}

#[test]
fn group_by_with_all_five_aggregates() {
    let out = run_query(
        "SELECT diagnosis, SUM(cost) as total, AVG(cost) as avg_cost, MIN(age) as min_age, MAX(age) as max_age FROM t GROUP BY diagnosis ORDER BY diagnosis",
    );
    let expected = "\
+--------------+--------+----------+---------+---------+
| diagnosis    | total  | avg_cost | min_age | max_age |
+--------------+--------+----------+---------+---------+
| asthma       | 2777.5 | 277.75   | 21      | 81      |
| cold         | 2222.5 | 222.25   | 25      | 80      |
| diabetes     | 2407.5 | 240.75   | 22      | 82      |
| flu          | 2962.5 | 296.25   | 23      | 73      |
| hypertension | 2592.5 | 259.25   | 19      | 79      |
+--------------+--------+----------+---------+---------+
";
    assert_eq!(out, expected);
}

#[test]
fn where_with_or_on_strings() {
    let out = run_query(
        "SELECT patient_id, diagnosis FROM t WHERE diagnosis = 'flu' OR diagnosis = 'cold' ORDER BY patient_id LIMIT 6",
    );
    let expected = "\
+------------+-----------+
| patient_id | diagnosis |
+------------+-----------+
| 1          | cold      |
| 5          | flu       |
| 6          | cold      |
| 10         | flu       |
| 11         | cold      |
| 15         | flu       |
+------------+-----------+
";
    assert_eq!(out, expected);
}

#[test]
fn count_star_group_by_nullable_column() {
    let out =
        run_query("SELECT insurance, COUNT(*) as n FROM t GROUP BY insurance ORDER BY insurance");
    let expected = "\
+--------------+----+
| insurance    | n  |
+--------------+----+
| Aetna        | 10 |
| Cigna        | 10 |
| UnitedHealth | 10 |
|              | 20 |
+--------------+----+
";
    assert_eq!(out, expected);
}

#[test]
fn where_on_float_column_order_desc() {
    let out = run_query("SELECT patient_id, cost FROM t WHERE cost >= 400.0 ORDER BY cost DESC");
    let expected = "\
+------------+-------+
| patient_id | cost  |
+------------+-------+
| 25         | 487.5 |
| 50         | 475.0 |
| 24         | 469.0 |
| 49         | 456.5 |
| 23         | 450.5 |
| 48         | 438.0 |
| 22         | 432.0 |
| 47         | 419.5 |
| 21         | 413.5 |
| 46         | 401.0 |
+------------+-------+
";
    assert_eq!(out, expected);
}
