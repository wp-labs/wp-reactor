// ── Security domain windows ──

window auth_events {
  stream = "auth_stream"
  time = event_time
  over = 24h

  fields {
    sip: ip
    uid: chars
    action: chars
    event_time: time
    geo_city: chars
  }
}

window fw_events {
  stream = "firewall_stream"
  time = event_time
  over = 24h

  fields {
    sip: ip
    dip: ip
    dport: digit
    action: chars
    event_time: time
  }
}

window dns_query {
  stream = "dns_stream"
  time = event_time
  over = 12h

  fields {
    query_id: chars
    sip: ip
    domain: chars
    event_time: time
  }
}

window dns_response {
  stream = "dns_stream"
  time = event_time
  over = 12h

  fields {
    query_id: chars
    sip: ip
    rcode: digit
    event_time: time
  }
}

// ── Endpoint telemetry ──

window endpoint_events {
  stream = ["endpoint_telemetry", "edr_events"]
  time = event_time
  over = 7d

  fields {
    host_id: chars
    process: chars
    dest_ip: ip
    bytes_out: float
    event_time: time
    `detail.sha256`: hex
    `detail.cmd_line`: chars
  }
}

// ── User operations (behavior analysis) ──

window user_operations {
  stream = "access_log"
  time = event_time
  over = 30d

  fields {
    uid: chars
    resource: chars
    action: chars
    event_time: time
    response_ms: digit
  }
}

// ── Output windows (no stream, yield-only) ──

window security_alerts {
  over = 90d

  fields {
    sip: ip
    uid: chars
    fail_count: digit
    threat: chars
    port_count: digit
    domain: chars
    message: chars
  }
}

window behavior_alerts {
  over = 90d

  fields {
    uid: chars
    resource_count: digit
    resources: array/chars
    op_sequence: array/chars
    first_seen: time
    last_seen: time
    session_duration: float
    login_count: digit
    baseline_count: float
    locations: array/chars
    login_category: chars
    message: chars
  }
}

window risk_scores {
  over = 90d

  fields {
    host_id: chars
    event_count: digit
    unique_dests: digit
    p95_bytes: float
    processes: array/chars
    message: chars
  }
}

// ── Static dimension tables ──

window ip_blocklist {
  over = 0

  fields {
    ip: ip
    threat_level: chars
    source: chars
    last_seen: time
  }
}

window bad_domains {
  over = 0

  fields {
    domain: chars
    category: chars
  }
}
