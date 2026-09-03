//! compile.rs 测试（2026-09-03 自 compile.rs 拆出；`#[path]` 子模块，可访问
//! 父模块私有项——`use super::*` 语义不变）。

    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;

    use wf_config::ConfigVarContext;
    use wf_config::{ByteSize, EvictPolicy, HumanDuration, LatePolicy};
    use wf_lang::parse_wfl;

    fn defaults() -> WindowDefaults {
        WindowDefaults {
            evict_interval: HumanDuration::from(Duration::from_secs(10)),
            max_window_bytes: ByteSize::from(1024 * 1024usize),
            max_total_bytes: ByteSize::from(16 * 1024 * 1024usize),
            evict_policy: EvictPolicy::TimeFirst,
            watermark: HumanDuration::from(Duration::from_secs(0)),
            allowed_lateness: HumanDuration::from(Duration::from_secs(60)),
            late_policy: LatePolicy::Drop,
        }
    }

    #[test]
    fn build_pipeline_internal_windows_derives_schema_and_config() {
        let base_schemas = vec![
            WindowSchema {
                name: "fw_events".into(),
                streams: vec!["syslog".into()],
                time_field: Some("event_time".into()),
                over: Duration::from_secs(3600),
                fields: vec![
                    FieldDef {
                        name: "event_time".into(),
                        field_type: FieldType::Base(BaseType::Time),
                    },
                    FieldDef {
                        name: "sip".into(),
                        field_type: FieldType::Base(BaseType::Ip),
                    },
                    FieldDef {
                        name: "dport".into(),
                        field_type: FieldType::Base(BaseType::Digit),
                    },
                ],
            },
            WindowSchema {
                name: "alerts".into(),
                streams: vec![],
                time_field: Some("emit_time".into()),
                over: Duration::from_secs(3600),
                fields: vec![
                    FieldDef {
                        name: "emit_time".into(),
                        field_type: FieldType::Base(BaseType::Time),
                    },
                    FieldDef {
                        name: "sip".into(),
                        field_type: FieldType::Base(BaseType::Ip),
                    },
                ],
            },
        ];

        let wfl = parse_wfl(
            r#"
rule pipe {
  events { e: fw_events }
  match<sip,dport:5m> {
    on event { c1: e | count >= 1; }
  }
  |> match<sip:10m> {
    on event { c2: _in | count >= 1; }
  } -> score(80.0)
  entity(ip, _in.sip)
  yield alerts (sip = _in.sip)
}
"#,
        )
        .unwrap();
        let plans = wf_lang::compile_wfl(&wfl, &base_schemas).unwrap();

        let (schemas, configs) =
            build_pipeline_internal_windows(&plans, &base_schemas, &defaults());
        assert_eq!(schemas.len(), 1);
        assert_eq!(configs.len(), 1);

        let ws = &schemas[0];
        assert_eq!(ws.name, "__wf_pipe_pipe_w1");
        assert_eq!(ws.streams, vec!["__wf_pipe_pipe_w1".to_string()]);
        assert_eq!(ws.time_field.as_deref(), Some("__wf_pipe_ts"));
        assert_eq!(ws.over, Duration::from_secs(600));
        assert!(ws.fields.iter().any(|f| f.name == "__wf_pipe_ts"));
        assert!(ws.fields.iter().any(|f| f.name == "sip"));
        assert!(ws.fields.iter().any(|f| f.name == "c1"));

        let cfg = &configs[0];
        assert_eq!(cfg.name, ws.name);
        assert_eq!(cfg.mode, DistMode::Local);
    }

    #[test]
    fn pipeline_hop_stage_window_over_uses_hop_size() {
        // 管道下游 stage 用 hop(size, slide)：内部管道窗 over = size（下游需
        // 保留整窗数据），而非 slide（find_pipeline_window_over Hop 臂）。
        let base_schemas = vec![
            WindowSchema {
                name: "fw_events".into(),
                streams: vec!["syslog".into()],
                time_field: Some("event_time".into()),
                over: Duration::from_secs(3600),
                fields: vec![
                    FieldDef {
                        name: "event_time".into(),
                        field_type: FieldType::Base(BaseType::Time),
                    },
                    FieldDef {
                        name: "sip".into(),
                        field_type: FieldType::Base(BaseType::Ip),
                    },
                    FieldDef {
                        name: "dport".into(),
                        field_type: FieldType::Base(BaseType::Digit),
                    },
                ],
            },
            WindowSchema {
                name: "alerts".into(),
                streams: vec![],
                time_field: Some("emit_time".into()),
                over: Duration::from_secs(3600),
                fields: vec![
                    FieldDef {
                        name: "emit_time".into(),
                        field_type: FieldType::Base(BaseType::Time),
                    },
                    FieldDef {
                        name: "sip".into(),
                        field_type: FieldType::Base(BaseType::Ip),
                    },
                ],
            },
        ];

        let wfl = parse_wfl(
            r#"
rule pipe {
  events { e: fw_events }
  match<sip,dport:5m> {
    on event { c1: e | count >= 1; }
  }
  |> match<sip:hop(10s, 2s)> {
    on event { c2: _in | count >= 1; }
  } -> score(80.0)
  entity(ip, _in.sip)
  yield alerts (sip = _in.sip)
}
"#,
        )
        .unwrap();
        let plans = wf_lang::compile_wfl(&wfl, &base_schemas).unwrap();

        let (schemas, _configs) =
            build_pipeline_internal_windows(&plans, &base_schemas, &defaults());
        assert_eq!(schemas.len(), 1);
        let ws = &schemas[0];
        assert_eq!(ws.name, "__wf_pipe_pipe_w1");
        assert_eq!(
            ws.over,
            Duration::from_secs(10),
            "hop 管道 over = 窗口 size(10s)，而非 slide(2s)"
        );
    }

    #[test]
    fn compile_rules_reports_source_aware_rule_diagnostics() {
        let dir = tempfile::tempdir().unwrap();
        let rule_path = dir.path().join("bad_rule.wfl");
        std::fs::write(
            &rule_path,
            r#"
rule bad_yield {
  events { e: fw_events }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield missing_alerts (sip = e.sip)
}
"#,
        )
        .unwrap();

        let schemas = vec![WindowSchema {
            name: "fw_events".into(),
            streams: vec!["syslog".into()],
            time_field: Some("event_time".into()),
            over: Duration::from_secs(3600),
            fields: vec![
                FieldDef {
                    name: "event_time".into(),
                    field_type: FieldType::Base(BaseType::Time),
                },
                FieldDef {
                    name: "sip".into(),
                    field_type: FieldType::Base(BaseType::Ip),
                },
            ],
        }];
        let err = compile_rules(
            "*.wfl",
            dir.path(),
            &ConfigVarContext::from_explicit_vars(HashMap::new()),
            &schemas,
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("bad_rule.wfl"), "{text}");
        assert!(text.contains("category: yield"), "{text}");
        assert!(text.contains("rule: bad_yield"), "{text}");
        assert!(text.contains("location: line 6, column 9"), "{text}");
        assert!(text.contains("yield missing_alerts"), "{text}");
    }

    #[test]
    fn compile_rules_reports_source_aware_topology_diagnostics() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("a.wfl"),
            r#"
rule make_b {
  events { e: win_a }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield win_b (sip = e.sip)
}
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("b.wfl"),
            r#"
rule make_a {
  events { e: win_b }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield win_a (sip = e.sip)
}
"#,
        )
        .unwrap();

        let schemas = vec![
            WindowSchema {
                name: "win_a".into(),
                streams: vec!["a".into()],
                time_field: Some("event_time".into()),
                over: Duration::from_secs(3600),
                fields: vec![
                    FieldDef {
                        name: "event_time".into(),
                        field_type: FieldType::Base(BaseType::Time),
                    },
                    FieldDef {
                        name: "sip".into(),
                        field_type: FieldType::Base(BaseType::Ip),
                    },
                ],
            },
            WindowSchema {
                name: "win_b".into(),
                streams: vec![],
                time_field: Some("event_time".into()),
                over: Duration::from_secs(3600),
                fields: vec![
                    FieldDef {
                        name: "event_time".into(),
                        field_type: FieldType::Base(BaseType::Time),
                    },
                    FieldDef {
                        name: "sip".into(),
                        field_type: FieldType::Base(BaseType::Ip),
                    },
                ],
            },
        ];
        let err = compile_rules(
            "*.wfl",
            dir.path(),
            &ConfigVarContext::from_explicit_vars(HashMap::new()),
            &schemas,
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("category: topology"), "{text}");
        assert!(text.contains("rule:"), "{text}");
        assert!(text.contains("location: line 2, column 1"), "{text}");
        assert!(
            text.contains("rule make_a") || text.contains("rule make_b"),
            "{text}"
        );
    }

    fn prelude_test_schemas() -> Vec<WindowSchema> {
        vec![
            WindowSchema {
                name: "fw_events".into(),
                streams: vec!["syslog".into()],
                time_field: Some("event_time".into()),
                over: Duration::from_secs(3600),
                fields: vec![
                    FieldDef {
                        name: "event_time".into(),
                        field_type: FieldType::Base(BaseType::Time),
                    },
                    FieldDef {
                        name: "sip".into(),
                        field_type: FieldType::Base(BaseType::Ip),
                    },
                ],
            },
            WindowSchema {
                name: "alerts".into(),
                streams: vec![],
                time_field: Some("event_time".into()),
                over: Duration::from_secs(3600),
                fields: vec![
                    FieldDef {
                        name: "event_time".into(),
                        field_type: FieldType::Base(BaseType::Time),
                    },
                    FieldDef {
                        name: "sip".into(),
                        field_type: FieldType::Base(BaseType::Ip),
                    },
                    FieldDef {
                        name: "severity".into(),
                        field_type: FieldType::Base(BaseType::Chars),
                    },
                    FieldDef {
                        name: "rule_name".into(),
                        field_type: FieldType::Base(BaseType::Chars),
                    },
                ],
            },
        ]
    }

    #[test]
    fn compile_rules_loads_global_prelude_yield_presets() {
        let dir = tempfile::tempdir().unwrap();
        let rules_dir = dir.path().join("rules");
        std::fs::create_dir_all(rules_dir.join("detections")).unwrap();
        std::fs::write(
            rules_dir.join(RULE_PRELUDE_FILE),
            r#"
yield preset base_alerts (
  severity = "medium",
  rule_name = "global"
)
"#,
        )
        .unwrap();
        std::fs::write(
            rules_dir.join("detections/ssh.wfl"),
            r#"
rule ssh {
  events { e: fw_events }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield alerts : base_alerts (
    event_time = e.event_time,
    sip = e.sip
  )
}
"#,
        )
        .unwrap();

        let (plans, _) = compile_rules(
            "rules/**/*.wfl",
            dir.path(),
            &ConfigVarContext::from_explicit_vars(HashMap::new()),
            &prelude_test_schemas(),
        )
        .unwrap();

        assert_eq!(plans.len(), 1);
        let fields: HashSet<_> = plans[0]
            .yield_plan
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect();
        assert!(fields.contains("severity"));
        assert!(fields.contains("rule_name"));
        assert!(fields.contains("event_time"));
        assert!(fields.contains("sip"));
    }

    #[test]
    fn compile_rules_loads_parameterized_global_prelude_yield_presets() {
        let dir = tempfile::tempdir().unwrap();
        let rules_dir = dir.path().join("rules");
        std::fs::create_dir_all(rules_dir.join("detections")).unwrap();
        std::fs::write(
            rules_dir.join(RULE_PRELUDE_FILE),
            r#"
yield preset base_alerts <severity, rule_name = "global"> (
  severity = $severity,
  rule_name = $rule_name
)
"#,
        )
        .unwrap();
        std::fs::write(
            rules_dir.join("detections/ssh.wfl"),
            r#"
rule ssh {
  events { e: fw_events }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield alerts : base_alerts<"high"> (
    event_time = e.event_time,
    sip = e.sip
  )
}
"#,
        )
        .unwrap();

        let (plans, _) = compile_rules(
            "rules/**/*.wfl",
            dir.path(),
            &ConfigVarContext::from_explicit_vars(HashMap::new()),
            &prelude_test_schemas(),
        )
        .unwrap();

        assert_eq!(plans.len(), 1);
        let field_value = |name: &str| {
            &plans[0]
                .yield_plan
                .fields
                .iter()
                .find(|field| field.name == name)
                .unwrap()
                .value
        };
        assert_eq!(
            field_value("severity"),
            &wf_lang::ast::Expr::StringLit("high".into())
        );
        assert_eq!(
            field_value("rule_name"),
            &wf_lang::ast::Expr::StringLit("global".into())
        );
    }

    #[test]
    fn compile_rules_rejects_rules_in_global_prelude() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(RULE_PRELUDE_FILE),
            r#"
rule hidden {
  events { e: fw_events }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield alerts (event_time = e.event_time, sip = e.sip)
}
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("visible.wfl"),
            r#"
rule visible {
  events { e: fw_events }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield alerts (event_time = e.event_time, sip = e.sip)
}
"#,
        )
        .unwrap();

        let err = compile_rules(
            "*.wfl",
            dir.path(),
            &ConfigVarContext::from_explicit_vars(HashMap::new()),
            &prelude_test_schemas(),
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("_global.wfl"), "{text}");
        assert!(text.contains("only allows `yield preset`"), "{text}");
        assert!(text.contains("rule declarations"), "{text}");
    }

    #[test]
    fn compile_rules_reports_global_prelude_preset_field_diagnostics() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(RULE_PRELUDE_FILE),
            r#"
yield preset base_alerts (
  event_time = "not-a-time",
  severity = "medium"
)
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("visible.wfl"),
            r#"
rule visible {
  events { e: fw_events }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield alerts : base_alerts (
    sip = e.sip,
    rule_name = "visible"
  )
}
"#,
        )
        .unwrap();

        let err = compile_rules(
            "*.wfl",
            dir.path(),
            &ConfigVarContext::from_explicit_vars(HashMap::new()),
            &prelude_test_schemas(),
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("_global.wfl"), "{text}");
        assert!(text.contains("location: line 3, column 3"), "{text}");
        assert!(text.contains("event_time = \"not-a-time\""), "{text}");
    }

    #[test]
    fn compile_rules_reports_global_prelude_expression_diagnostics() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(RULE_PRELUDE_FILE),
            r#"
yield preset base_alerts (
  severity = e.missing,
  rule_name = "global"
)
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("visible.wfl"),
            r#"
rule visible {
  events { e: fw_events }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield alerts : base_alerts (
    event_time = e.event_time,
    sip = e.sip
  )
}
"#,
        )
        .unwrap();

        let err = compile_rules(
            "*.wfl",
            dir.path(),
            &ConfigVarContext::from_explicit_vars(HashMap::new()),
            &prelude_test_schemas(),
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("_global.wfl"), "{text}");
        assert!(text.contains("field `missing` not found"), "{text}");
        assert!(text.contains("location: line 3, column 16"), "{text}");
        assert!(text.contains("severity = e.missing"), "{text}");
    }

    #[test]
    fn compile_rules_reports_split_global_prelude_expression_diagnostics() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(RULE_PRELUDE_FILE),
            r#"
yield // split header
preset
base_alerts (
  severity = e.missing,
  rule_name = "global"
)
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("visible.wfl"),
            r#"
rule visible {
  events { e: fw_events }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield alerts : base_alerts (
    event_time = e.event_time,
    sip = e.sip
  )
}
"#,
        )
        .unwrap();

        let err = compile_rules(
            "*.wfl",
            dir.path(),
            &ConfigVarContext::from_explicit_vars(HashMap::new()),
            &prelude_test_schemas(),
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("_global.wfl"), "{text}");
        assert!(text.contains("field `missing` not found"), "{text}");
        assert!(text.contains("location: line 5, column 16"), "{text}");
        assert!(text.contains("severity = e.missing"), "{text}");
    }

    #[test]
    fn compile_rules_rejects_duplicate_presets_inside_global_prelude() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(RULE_PRELUDE_FILE),
            r#"
yield preset base_alerts (severity = "medium")
yield preset base_alerts (severity = "high")
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("visible.wfl"),
            r#"
rule visible {
  events { e: fw_events }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield alerts (event_time = e.event_time, sip = e.sip, severity = "medium", rule_name = "visible")
}
"#,
        )
        .unwrap();

        let err = compile_rules(
            "*.wfl",
            dir.path(),
            &ConfigVarContext::from_explicit_vars(HashMap::new()),
            &prelude_test_schemas(),
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("_global.wfl"), "{text}");
        assert!(
            text.contains("duplicate yield preset `base_alerts`"),
            "{text}"
        );
        assert!(text.contains("rule prelude"), "{text}");
        assert!(text.contains("location: line 3, column 1"), "{text}");
    }

    #[test]
    fn compile_rules_rejects_rule_file_preset_conflict_with_global_prelude() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(RULE_PRELUDE_FILE),
            r#"
yield preset base_alerts (severity = "medium")
"#,
        )
        .unwrap();
        std::fs::write(
            dir.path().join("visible.wfl"),
            r#"
yield preset base_alerts (severity = "local")

rule visible {
  events { e: fw_events }
  match<sip:5m> { on event { e | count >= 1; } } -> score(50.0)
  entity(ip, e.sip)
  yield alerts : base_alerts (event_time = e.event_time, sip = e.sip, rule_name = "visible")
}
"#,
        )
        .unwrap();

        let err = compile_rules(
            "*.wfl",
            dir.path(),
            &ConfigVarContext::from_explicit_vars(HashMap::new()),
            &prelude_test_schemas(),
        )
        .unwrap_err();
        let text = err.to_string();
        assert!(text.contains("visible.wfl"), "{text}");
        assert!(text.contains("already exists in prelude"), "{text}");
        assert!(text.contains("_global.wfl"), "{text}");
        assert!(text.contains("location: line 2, column 1"), "{text}");
    }

    #[test]
    fn rule_prelude_path_uses_non_glob_prefix_as_rule_root() {
        let base = Path::new("/project");
        assert_eq!(
            rule_prelude_path("rules/current/*.wfl", base),
            PathBuf::from("/project/rules/current/_global.wfl")
        );
        assert_eq!(
            rule_prelude_path("rules/**/*.wfl", base),
            PathBuf::from("/project/rules/_global.wfl")
        );
    }

