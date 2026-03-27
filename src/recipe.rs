use crate::types::{RecipeStep, RolloutPhase, RolloutRecipe};

pub fn set_not_null_safe(table: &str, column: &str) -> RolloutRecipe {
    let constraint = format!("{table}_{column}_not_null");
    RolloutRecipe {
        title: format!("Safe SET NOT NULL for \"{table}\".\"{column}\""),
        steps: vec![
            RecipeStep {
                phase: RolloutPhase::Expand,
                description: "Add CHECK constraint with NOT VALID (brief lock, no scan)".into(),
                sql: format!(
                    "ALTER TABLE \"{table}\" ADD CONSTRAINT \"{constraint}\" CHECK (\"{column}\" IS NOT NULL) NOT VALID;"
                ),
                separate_transaction: false,
            },
            RecipeStep {
                phase: RolloutPhase::Validate,
                description: "Validate constraint (SHARE UPDATE EXCLUSIVE, non-blocking)".into(),
                sql: format!("ALTER TABLE \"{table}\" VALIDATE CONSTRAINT \"{constraint}\";"),
                separate_transaction: true,
            },
            RecipeStep {
                phase: RolloutPhase::Switch,
                description: "SET NOT NULL is now instant (PG 12+) as CHECK already proves it".into(),
                sql: format!("ALTER TABLE \"{table}\" ALTER COLUMN \"{column}\" SET NOT NULL;"),
                separate_transaction: true,
            },
            RecipeStep {
                phase: RolloutPhase::Contract,
                description: "Drop the redundant CHECK constraint".into(),
                sql: format!("ALTER TABLE \"{table}\" DROP CONSTRAINT \"{constraint}\";"),
                separate_transaction: false,
            },
        ],
    }
}

pub fn create_index_concurrently(table: &str, columns: &str, index_name: &str) -> RolloutRecipe {
    RolloutRecipe {
        title: format!("Non-blocking index build on \"{table}\""),
        steps: vec![RecipeStep {
            phase: RolloutPhase::Expand,
            description: "Create index concurrently (cannot run inside a transaction block)".into(),
            sql: format!("CREATE INDEX CONCURRENTLY \"{index_name}\" ON \"{table}\" ({columns});"),
            separate_transaction: true,
        }],
    }
}

pub fn drop_index_concurrently(index_name: &str) -> RolloutRecipe {
    RolloutRecipe {
        title: format!("Non-blocking index drop for \"{index_name}\""),
        steps: vec![RecipeStep {
            phase: RolloutPhase::Contract,
            description: "Drop index concurrently (cannot run inside a transaction block)".into(),
            sql: format!("DROP INDEX CONCURRENTLY \"{index_name}\";"),
            separate_transaction: true,
        }],
    }
}

pub fn add_foreign_key_safe(table: &str, constraint_name: &str, fk_definition: &str) -> RolloutRecipe {
    RolloutRecipe {
        title: format!("Safe foreign key on \"{table}\""),
        steps: vec![
            RecipeStep {
                phase: RolloutPhase::Expand,
                description: "Add FK with NOT VALID (brief lock, no validation scan)".into(),
                sql: format!("{fk_definition} NOT VALID;"),
                separate_transaction: false,
            },
            RecipeStep {
                phase: RolloutPhase::Validate,
                description: "Validate FK (SHARE UPDATE EXCLUSIVE, non-blocking scan)".into(),
                sql: format!("ALTER TABLE \"{table}\" VALIDATE CONSTRAINT \"{constraint_name}\";"),
                separate_transaction: true,
            },
        ],
    }
}

pub fn add_check_safe(table: &str, constraint_name: &str, check_expr: &str) -> RolloutRecipe {
    RolloutRecipe {
        title: format!("Safe CHECK constraint on \"{table}\""),
        steps: vec![
            RecipeStep {
                phase: RolloutPhase::Expand,
                description: "Add CHECK with NOT VALID (brief lock, no scan)".into(),
                sql: format!(
                    "ALTER TABLE \"{table}\" ADD CONSTRAINT \"{constraint_name}\" CHECK ({check_expr}) NOT VALID;"
                ),
                separate_transaction: false,
            },
            RecipeStep {
                phase: RolloutPhase::Validate,
                description: "Validate constraint (SHARE UPDATE EXCLUSIVE, non-blocking)".into(),
                sql: format!("ALTER TABLE \"{table}\" VALIDATE CONSTRAINT \"{constraint_name}\";"),
                separate_transaction: true,
            },
        ],
    }
}

pub fn change_column_type(table: &str, column: &str, new_type: &str) -> RolloutRecipe {
    RolloutRecipe {
        title: format!("Expand/migrate/contract for \"{table}\".\"{column}\""),
        steps: vec![
            RecipeStep {
                phase: RolloutPhase::Expand,
                description: "Add new column with target type".into(),
                sql: format!("ALTER TABLE \"{table}\" ADD COLUMN \"{column}_new\" {new_type};"),
                separate_transaction: false,
            },
            RecipeStep {
                phase: RolloutPhase::Backfill,
                description: "Backfill in batches (application-level, not a single UPDATE)".into(),
                sql: format!(
                    "UPDATE \"{table}\" SET \"{column}_new\" = \"{column}\"::{new_type} WHERE \"{column}_new\" IS NULL LIMIT 10000;"
                ),
                separate_transaction: true,
            },
            RecipeStep {
                phase: RolloutPhase::Validate,
                description: "Add trigger to keep columns in sync during migration".into(),
                sql: format!(
                    "CREATE FUNCTION sync_{table}_{column}() RETURNS trigger AS $$ BEGIN NEW.{column}_new := NEW.{column}::{new_type}; RETURN NEW; END $$ LANGUAGE plpgsql;\nCREATE TRIGGER trg_sync_{table}_{column} BEFORE INSERT OR UPDATE ON \"{table}\" FOR EACH ROW EXECUTE FUNCTION sync_{table}_{column}();"
                ),
                separate_transaction: false,
            },
            RecipeStep {
                phase: RolloutPhase::Switch,
                description: "Switch application reads to the new column".into(),
                sql: "-- Application change: update code to read from the new column".into(),
                separate_transaction: false,
            },
            RecipeStep {
                phase: RolloutPhase::Contract,
                description: "Drop old column, trigger, and rename".into(),
                sql: format!(
                    "DROP TRIGGER trg_sync_{table}_{column} ON \"{table}\";\nDROP FUNCTION sync_{table}_{column}();\nALTER TABLE \"{table}\" DROP COLUMN \"{column}\";\nALTER TABLE \"{table}\" RENAME COLUMN \"{column}_new\" TO \"{column}\";"
                ),
                separate_transaction: false,
            },
        ],
    }
}

pub fn drop_column(table: &str, column: &str) -> RolloutRecipe {
    RolloutRecipe {
        title: format!("Safe column removal from \"{table}\""),
        steps: vec![
            RecipeStep {
                phase: RolloutPhase::Switch,
                description: "Deploy code that no longer reads this column".into(),
                sql: "-- Application change: remove all references to this column".into(),
                separate_transaction: false,
            },
            RecipeStep {
                phase: RolloutPhase::Contract,
                description: "Drop the column (brief ACCESS EXCLUSIVE lock, no rewrite)".into(),
                sql: format!("ALTER TABLE \"{table}\" DROP COLUMN \"{column}\";"),
                separate_transaction: false,
            },
        ],
    }
}

pub fn attach_partition_safe(parent: &str) -> RolloutRecipe {
    RolloutRecipe {
        title: format!("Safe partition attachment for \"{parent}\""),
        steps: vec![
            RecipeStep {
                phase: RolloutPhase::Expand,
                description: "Add partition constraint as CHECK on child table".into(),
                sql: "ALTER TABLE <partition> ADD CONSTRAINT <name> CHECK (...) NOT VALID;".into(),
                separate_transaction: false,
            },
            RecipeStep {
                phase: RolloutPhase::Validate,
                description: "Validate constraint (scans partition, does not lock parent)".into(),
                sql: "ALTER TABLE <partition> VALIDATE CONSTRAINT <name>;".into(),
                separate_transaction: true,
            },
            RecipeStep {
                phase: RolloutPhase::Switch,
                description: "Attach partition (skips scan because constraint is validated)".into(),
                sql: format!("ALTER TABLE \"{parent}\" ATTACH PARTITION <partition> FOR VALUES ...;"),
                separate_transaction: true,
            },
        ],
    }
}
