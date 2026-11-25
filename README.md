<p align="center">
  <a href="https://digitalis.io">
    <img src="https://cdn.prod.website-files.com/68370d67a3c849c28c22f703/68370d67a3c849c28c22f803_digitalis-logo.svg" alt="Digitalis.IO" width="300">
  </a>
</p>

<p align="center">
  <em>Built and maintained by <a href="https://digitalis.io">Digitalis.IO</a></em>
</p>

# kafka-oauthbearer-openbao

A Docker Compose lab where **Kafka clients authenticate with SASL OAUTHBEARER using two minute JWTs signed by OpenBao**, and the broker forces every connection to re-authenticate.

This is the follow-up to [kafka-mtls-openbao](https://github.com/digitalis-io/kafka-mtls-openbao). With mTLS the broker checks the certificate only during the TLS handshake, so an open connection outlives its certificate. With OAUTHBEARER and `connections.max.reauth.ms` the broker checks the token again on the same connection, so a client that stops getting new tokens is cut off shortly after the last one expires.

## Quick start

Requires Docker with Compose, `jq` and `openssl`.

```bash
./setup.sh      # about 1 minute
./test.sh       # about 6 minutes, waits for real refreshes and a real expiry
./teardown.sh
```

## What is running

| Service | Image | Role |
|---------|-------|------|
| `openbao` | `openbao/openbao:2.4.1` | Dev mode server. Signs JWTs with the identity OIDC engine and publishes the public keys. UI on <http://localhost:8200>, token `root` |
| `kafka` | `apache/kafka:4.4.0` | Single KRaft node. `CLIENT` listener is SASL_SSL with OAUTHBEARER only. Inter-broker, controller and admin traffic use mTLS |
| `billing-agent` | `openbao/openbao:2.4.1` | `bao agent` logs in with AppRole and writes a fresh JWT to `tokens/billing-service/billing-service.jwt` every 30 seconds |
| `billing-app` | `apache/kafka:4.4.0` | One long-lived producer sending a message a second to `billing.events` |

```text
billing-agent --AppRole login--> openbao --signs JWT {aud: kafka, kafka_principal: billing-service}, ttl 2m
      |
      v writes token file
billing-app --SASL_SSL OAUTHBEARER--> kafka --fetches JWKS--> openbao
                                        |
                                        principal User:billing-service --> ACL: Write billing.events
```

## How the principal is chosen

OpenBao signs every Kafka token through one OIDC role, `identity/oidc/role/kafka`. The role template copies the caller's **entity name** into a `kafka_principal` claim and the broker reads that claim with `sasl.oauthbearer.sub.claim.name=kafka_principal`. The application never states who it is: `setup.sh` binds the AppRole to an entity called `billing-service`, so anything that logs in with that AppRole gets tokens for `User:billing-service` and nothing else.

## Examples

Decode the current token:

```bash
cut -d. -f2 tokens/billing-service/billing-service.jwt | tr '_-' '/+' | base64 -d 2>/dev/null | jq
# {"aud":"kafka","exp":...,"iss":"http://openbao:8200/v1/identity/oidc","kafka_principal":"billing-service",...}
```

Watch the long-lived producer across several token refreshes:

```bash
docker compose logs -f billing-app | grep -E 'send_success|ERROR' 
```

Stop the agent and watch the same connection fail once the token expires:

```bash
docker compose stop billing-agent
docker compose logs -f billing-app | grep -m1 ERROR
```

## What `test.sh` checks

| Check | Expected result |
|-------|-----------------|
| `billing-service` writes to `billing.events` | Accepted |
| `billing-service` writes to `payments.events` | `TopicAuthorizationException` |
| `kafka_principal` claim in the token | `billing-service`, taken from the OpenBao entity |
| The application tries to rename its own entity | `permission denied` |
| A token with the same claims and `kid`, signed by another key | `invalid_token` |
| One producer connection over 2.5 minutes of refreshes | No errors, new token on disk |
| Agent stopped, same producer connection | Fails shortly after the last token expires |

## Configuration

| Setting | Where | Value | Why |
|---------|-------|-------|-----|
| `sasl.oauthbearer.jwt.validator.class` | `kafka/server.properties` | `BrokerJwtValidator` | Forces signature checks. Kafka 4.1.0 and 4.1.1 skip them by default (CVE-2026-33557) |
| `sasl.oauthbearer.jwks.endpoint.url` | `kafka/server.properties` | `http://openbao:8200/v1/identity/oidc/.well-known/keys` | Public keys OpenBao signs with |
| `sasl.oauthbearer.expected.audience` | `kafka/server.properties` | `kafka` | Matches the OIDC role `client_id` |
| `sasl.oauthbearer.sub.claim.name` | broker and client | `kafka_principal` | The claim that becomes `User:<name>` |
| `connections.max.reauth.ms` | `kafka/server.properties` | `60000` | Clients must re-authenticate on the same connection at least every minute |
| `sasl.oauthbearer.jwt.retriever.class` | `client/billing.properties` | `FileJwtRetriever` | The client reads the token the agent writes, no client secret anywhere |
| `org.apache.kafka.sasl.oauthbearer.allowed.urls` | `KAFKA_OPTS` | JWKS URL on the broker, `file:///tokens/...` on the client | Kafka 4 refuses any OAuth URL, including `file://`, that is not listed |
| `ttl` | `identity/oidc/role/kafka` | `2m` | Lab value so refresh is visible. Minutes to an hour is more typical |
| `static_secret_render_interval` | agent | `30s` | Identity tokens have no lease, so the agent fetches a new one on this interval |

## Findings worth knowing

- **Check your Kafka version.** On 4.1.0 this lab's broker accepted a token signed by a key it had never seen. That is CVE-2026-33557. 4.1.2 and 4.2.0 fix it. Naming `BrokerJwtValidator` explicitly also blocks it, which we confirmed on 4.1.0. A related issue, CVE-2026-110985, affects brokers without a JWKS URL and is fixed in 4.2.2, 4.3.1 and 4.4.0.
- **Re-authentication closes the gap mTLS leaves.** Without `connections.max.reauth.ms` a SASL session lasts as long as the TCP connection, exactly like mTLS.
- **File URLs need allow-listing too.** `file:///tokens/billing-service.jwt` fails with `The URL cannot be accessed due to restrictions` until it is added to `org.apache.kafka.sasl.oauthbearer.allowed.urls`.
- **The client re-reads the file.** `FileJwtRetriever` picks up the agent's new token on every refresh. The application needs no restart.
- **Failure is loud and recovery is automatic.** With the agent stopped the producer logged `NetworkException: Disconnected from node 1` within seconds of the token expiring. Once the agent was back the same process was sending again about 10 seconds later, because the client retries its refresh every 10 seconds.

## Limitations

- OpenBao runs in dev mode with no TLS on the API. The broker fetches the JWKS over plain HTTP, which is fine in a lab and not in production.
- One OIDC signing key. OpenBao rotates it every 24 hours and keeps the old one published for `verification_ttl`.
- One broker, replication factor 1.

## Contact

Maintained by [Digitalis.io](https://digitalis.io). Support at [digitalis.io/contact](https://digitalis.io/contact).

We run Kafka, Cassandra and secrets management platforms in production for customers 24x7.

## License

See [LICENSE](LICENSE) for details.
