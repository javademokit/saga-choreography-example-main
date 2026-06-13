# eureka-discovery-service

Standalone Eureka Server for service discovery in this workspace.

## Default settings

- Port: `8761`
- Service name: `eureka-discovery-service`
- Self-registration: disabled

## Run

```powershell
mvn -pl eureka-discovery-service spring-boot:run
```

## Eureka dashboard

- `http://localhost:8761/`

## Notes

Start this module before other services so they can register successfully.

