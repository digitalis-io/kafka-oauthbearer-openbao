pid_file = "/tmp/agent.pid"

vault {
  address = "http://openbao:8200"
}

auto_auth {
  method "approle" {
    config = {
      role_id_file_path                   = "/agent/creds/role_id"
      secret_id_file_path                 = "/agent/creds/secret_id"
      remove_secret_id_file_after_reading = false
    }
  }
}

template_config {
  exit_on_retry_failure = true
  # Identity tokens have no lease, so the agent fetches a new one on this interval.
  static_secret_render_interval = "30s"
}

template {
  destination = "/tokens/billing-service.jwt"
  perms       = "0644"
  contents    = "{{ with secret \"identity/oidc/token/kafka\" }}{{ .Data.token }}{{ end }}"
}
