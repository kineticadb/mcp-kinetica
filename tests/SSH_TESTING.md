# Testing with https certificates

This test describes how to setup a test of the MCP oauth-proxy flow. This involves running a test
script on the host that will connect to the container. It will launch a web page prompting the user to delegate 
access to the MCP server so that it can connect to Kinetica. 
See the [MCP diagram](https://gofastmcp.com/servers/auth/oauth-proxy#oauth-flow) for a flowchart.

This document describes how to run the test assuming that:

* Kinetica is running in a container.
* The host has a web browser available. (e.g. desktop)
* Self-signed certificates will be used.

## TOC

- [Processes and ports](#processes-and-ports)
- [Setup](#setup)
  - [Docker container](#docker-container)
  - [Self-signed Certificates](#self-signed-certificates)
  - [Testing the container processes](#testing-the-container-processes)
- [Running the test scripts](#running-the-test-scripts)
- [Other Notes](#other-notes)

## Processes and ports

The `MCP` and `OAUTH` processes each have internal and external URLs. The internal URL's can only be accessed 
from the container. The external URL's are proxied through Kinetica `httpd` and require a valid `https` certificate.

You will need to identify the IP of the host (e.g. desktop) which we will refer to as `<EXT_IP>`.

OAUTH:

* local: `http://127.0.0.1:8380`
* external: `https://<EXT_IP>:8446`
* admin pages: `https://<EXT_IP>:8446/admin`
* starting: `/opt/gpudb/core/bin/gpudb start oauth`

MCP:

* local: `http://127.0.0.1:8390/mcp`
* external: `https://<EXT_IP>:8447/mcp`
* starting: `/opt/gpudb/core/bin/gpudb start mcp`

## Setup

### Docker container

You will need to start the docker container with ports 8446 and 8447 accessible since we will 
be running a test script from outside the container.

```sh
docker run -it  \
    --name oauth-test \
    --hostname kinetica-oauth \
    -p 8080:8080 \
    -p 8447:8447 \
    -p 8446:8446 \
    kineticadevcloud/kinetica-cpu:7.2.3.5.20251124220711-test-mcp-oauth \
    bash
```

In the /opt/gpudb/core/etc/gpudb.conf file you will need to update the `host0.public_address`.

```conf
host0.address = 127.0.0.1
host0.public_address = <EXT_IP>
host0.ha_address = 
host0.host_manager_public_url = 
host0.ram_limit = 4073741824
host0.gpus = 
host0.accepts_failover = false
```

### Self-signed Certificates

We will need to create a new certificate that has the IP of the external host so it appears valid. Edit the 
`-addext` with the EXT_IP.

```sh
$ rm -fv /opt/gpudb/certs/cert.pem /opt/gpudb/certs/key.pem

$ openssl req -x509 \
    -newkey rsa -days 3650 -nodes \
    -keyout /opt/gpudb/certs/key.pem \
    -out /opt/gpudb/certs/cert.pem \
    -subj "/CN=gpudb-dev" \
    -addext "subjectAltName=DNS:kinetica-oauth,DNS:*.example.com,IP:<EXT_IP>"
```

Check that the keys are owned by the `gpudb` user or they may not be accessible. 

```sh
$ ls -l /opt/gpudb/certs
total 12
-rw-r--r-- 1 gpudb gpudb 1180 Jan  9 21:09 cert.pem
-rw------- 1 gpudb gpudb 1704 Jan  9 21:09 key.pem
-rw-rw-rw- 1 gpudb gpudb  913 Nov 26 13:58 truststore.jks
```

You can view the contents of the cert with 

```sh
openssl x509 -in /opt/gpudb/certs/cert.pem  -noout -text
```

Since this is self signed it is its own CA and so you can validate it with:

```sh
openssl verify -CAfile /opt/gpudb/certs/cert.pem /opt/gpudb/certs/cert.pem
```

Since the IP was updated the oauth database will need to be re-generated and so we should delete it. It will be
re-created during startup.

``` sh
rm -f /opt/gpudb/connectors/oauth/src/db.sqlite3
```

### Testing the container processes

We can now start the container processes.

```sh
/opt/gpudb/core/bin/gpudb start
```

Now we need to copy the certificate from the container to the host. We will be running some pytest scripts and so 
we will copy it to the `mcp-kinetica/tests` directory.

```sh
$ cd mcp-kinetica/tests
$ docker cp kientica-oauth:/opt/gpudb/certs/cert.pem ./cert.pem
```

We can check that the certificate is valid for the oauth server.

```sh
$ curl --cacert ./cert.pem https://<EXT_IP>:8446
!-- templates/home.html-->
<!-- templates/base.html -->
[...]
```

We can validate a connection to the MCP server. 
If you get the `Authentication failed` error without any certificate errors then its successful for this step.

```sh
curl --cacert ./cert.pem https://<EXT_IP>:8447/mcp
"error": "invalid_token", "error_description": "Authentication failed."
[...]
```

At this point we should be able to login to the oauth server with a valid Kienetica user. Open a browser
to the external URL at `https://<EXT_IP>:8446`. When you see a login prompt enter a valid Kinetica user (e.g. admin). 

Note that you can also access the oauth admin page at `https://<EXT_IP>:8446/admin/`. This is only accessible for 
Kinetica administrative users.

## Running the test scripts

At this point you should have a valid certificate saved in `./tests/cert.pem`. Container processes should be
accessible from the host. These tests should be run from the host (not from the container).

Edit `tests/test_oauth.py` and update MCP_URL with the your IP.

```
# Edit this to point to your MCP instance
MCP_URL = "https://172.31.33.26:8447/mcp"
```

Run the clear_tokens test to remove any existing MCP tokens. This will remove local tokens and does not connect
to the MCP process.

```sh
pytest test_oauth.py::test_clear_tokens
```

Verify that the tests are able to properly use the certificate.

```sh
pytest test_oauth.py::test_ssl_cert_file
```

Run the full E2E test. This consists of:

1. `Application Access Request` page that will prompt you to connect.
2. OAUTH login page. (you won't see this if you previously logged in)
3. `Authorize Kinetica MCP?` page that prompt you for permission.
4. `Authentication successful` page.
5. It will save the token to your local account.
6. It will use the `query_sql` to execute  `SELECT CURRENT_USER() as user`.


```sh
pytest test_oauth.py::test_query_sql_success
[...]
INFO [test_oauth] Result records: [{'user': 'admin'}]
[...]
```

When executing the test will will see `Result records:` indicating the result of the SQL.

## Other Notes

Install python debugger in container.

```sh
sudo apt update
sudo apt install python3-dbg
```



